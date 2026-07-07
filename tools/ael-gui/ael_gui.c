//==========================================================
// ael-gui - local browser playground for AEL expressions + error details.
//
// A single binary that connects to a cluster with the C client (this
// repo's error-details branch), serves a small HTTP UI on 127.0.0.1, and
// evaluates AEL source either as a record filter (policy filter_exp on a
// get) or as an expression-read operation, at a chosen error-detail
// verbosity (0-3).
//
// The stock client folds the field-45 subcode/message into as_error and
// skips the expression trace (key 3). This tool builds the read/operate
// wire commands itself - using the same exported as_command_* helpers
// aerospike_key.c uses - with a custom parse callback that captures the
// raw field-45 payload, then decodes the full trace (snippet, path,
// outcome, operands, AEL source span) to JSON for the browser.
//
// Local dev tool - not part of any build or CI.
//==========================================================

//==========================================================
// Includes.
//

#include <errno.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_boolean.h>
#include <aerospike/as_bytes.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_command.h>
#include <aerospike/as_config.h>
#include <aerospike/as_error.h>
#include <aerospike/as_exp.h>
#include <aerospike/as_exp_operations.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_key.h>
#include <aerospike/as_msgpack.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_orderedmap.h>
#include <aerospike/as_partition.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_proto.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_string.h>
#include <citrusleaf/cf_byte_order.h>


//==========================================================
// Constants & typedefs.
//

// Server-side constants (mirrors as/include/base/proto.h on the
// error-details branches - keep in sync by hand).

#define WIRE_EXP_AEL_COMPILE 128

#define F45_KEY_SUBCODE   1
#define F45_KEY_MESSAGE   2
#define F45_KEY_EXP_TRACE 3

#define TRACE_KEY_PHASE       1
#define TRACE_KEY_BYTE_OFFSET 2
#define TRACE_KEY_OP          3
#define TRACE_KEY_DEPTH       4
#define TRACE_KEY_PATH        5
#define TRACE_KEY_SNIPPET     6
#define TRACE_KEY_OUTCOME     7
#define TRACE_KEY_LANG        8
#define TRACE_KEY_AEL_OFFSET  9
#define TRACE_KEY_AEL_SPAN    10
#define TRACE_KEY_AEL_LINE    11
#define TRACE_KEY_AEL_COL     12
#define TRACE_KEY_OPERANDS    13

#define HTTP_MAX_REQUEST (4 * 1024 * 1024)

typedef struct app_s {
	aerospike as;
	char host[256];
	int port;
	char ns[32];
	char set[64];
	as_key key; // the sample record every eval runs against
} app;

// Growable string builder for JSON / HTTP responses.
typedef struct sb_s {
	char* buf;
	size_t len;
	size_t cap;
} sb;

// Msgpack read cursor.
typedef struct mp_s {
	const uint8_t* p;
	uint32_t len;
	uint32_t off;
} mp;

typedef struct f45_info_s {
	bool has_subcode;
	uint64_t subcode;
	const uint8_t* msg;
	uint32_t msg_len;
	const uint8_t* trace; // raw msgpack map slice (or NULL)
	uint32_t trace_len;
} f45_info;

// Filled by the custom parse callback.
typedef struct eval_capture_s {
	int result_code; // -1 = no server response parsed
	uint32_t generation;
	uint8_t* f45; // malloc'd copy of the field-45 payload (or NULL)
	uint32_t f45_len;
	sb bins; // JSON object of returned bins ("" if none)
} eval_capture;

static volatile sig_atomic_t g_stop = 0;
static int g_listen_fd = -1;


//==========================================================
// String builder.
//

static void
sb_reserve(sb* b, size_t need)
{
	if (b->len + need + 1 <= b->cap) {
		return;
	}

	size_t cap = b->cap == 0 ? 256 : b->cap;

	while (cap < b->len + need + 1) {
		cap *= 2;
	}

	b->buf = realloc(b->buf, cap);
	b->cap = cap;
}

static void
sb_putn(sb* b, const char* s, size_t n)
{
	sb_reserve(b, n);
	memcpy(b->buf + b->len, s, n);
	b->len += n;
	b->buf[b->len] = 0;
}

static void
sb_puts(sb* b, const char* s)
{
	sb_putn(b, s, strlen(s));
}

static void
sb_fmt(sb* b, const char* fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);

	int n = vsnprintf(NULL, 0, fmt, ap);

	va_end(ap);

	if (n < 0) {
		return;
	}

	sb_reserve(b, (size_t)n);
	va_start(ap, fmt);
	vsnprintf(b->buf + b->len, (size_t)n + 1, fmt, ap);
	va_end(ap);
	b->len += (size_t)n;
}

// JSON string literal (with quotes). Escapes controls, quote, backslash;
// passes multi-byte UTF-8 (e.g. the snippet's guillemets) through raw.
static void
sb_json_str(sb* b, const uint8_t* s, uint32_t n)
{
	sb_puts(b, "\"");

	for (uint32_t i = 0; i < n; i++) {
		uint8_t c = s[i];

		switch (c) {
		case '"':
			sb_puts(b, "\\\"");
			break;
		case '\\':
			sb_puts(b, "\\\\");
			break;
		case '\n':
			sb_puts(b, "\\n");
			break;
		case '\r':
			sb_puts(b, "\\r");
			break;
		case '\t':
			sb_puts(b, "\\t");
			break;
		default:
			if (c < 0x20) {
				sb_fmt(b, "\\u%04x", c);
			}
			else {
				sb_putn(b, (const char*)&s[i], 1);
			}
			break;
		}
	}

	sb_puts(b, "\"");
}

static void
sb_json_cstr(sb* b, const char* s)
{
	sb_json_str(b, (const uint8_t*)s, (uint32_t)strlen(s));
}

static void
sb_free(sb* b)
{
	free(b->buf);
	b->buf = NULL;
	b->len = 0;
	b->cap = 0;
}


//==========================================================
// Minimal msgpack reader (decode only - subset the server emits).
//

static bool
mp_need(const mp* m, uint32_t n)
{
	return m->off + n <= m->len;
}

static bool
mp_read_be(mp* m, uint32_t n, uint64_t* out)
{
	if (! mp_need(m, n)) {
		return false;
	}

	uint64_t v = 0;

	for (uint32_t i = 0; i < n; i++) {
		v = (v << 8) | m->p[m->off + i];
	}

	m->off += n;
	*out = v;
	return true;
}

// Reads any int-family value as signed 64-bit.
static bool
mp_read_int(mp* m, int64_t* out)
{
	if (! mp_need(m, 1)) {
		return false;
	}

	uint8_t b = m->p[m->off++];
	uint64_t v;

	if (b <= 0x7f) {
		*out = b;
		return true;
	}

	if (b >= 0xe0) {
		*out = (int64_t)b - 0x100;
		return true;
	}

	switch (b) {
	case 0xcc:
		if (! mp_read_be(m, 1, &v)) return false;
		*out = (int64_t)v;
		return true;
	case 0xcd:
		if (! mp_read_be(m, 2, &v)) return false;
		*out = (int64_t)v;
		return true;
	case 0xce:
		if (! mp_read_be(m, 4, &v)) return false;
		*out = (int64_t)v;
		return true;
	case 0xcf:
		if (! mp_read_be(m, 8, &v)) return false;
		*out = (int64_t)v;
		return true;
	case 0xd0:
		if (! mp_read_be(m, 1, &v)) return false;
		*out = (int8_t)v;
		return true;
	case 0xd1:
		if (! mp_read_be(m, 2, &v)) return false;
		*out = (int16_t)v;
		return true;
	case 0xd2:
		if (! mp_read_be(m, 4, &v)) return false;
		*out = (int32_t)v;
		return true;
	case 0xd3:
		if (! mp_read_be(m, 8, &v)) return false;
		*out = (int64_t)v;
		return true;
	default:
		m->off--;
		return false;
	}
}

// Reads a str-family value as a slice into the buffer.
static bool
mp_read_str(mp* m, const uint8_t** s, uint32_t* n)
{
	if (! mp_need(m, 1)) {
		return false;
	}

	uint8_t b = m->p[m->off];
	uint64_t len;

	if (b >= 0xa0 && b <= 0xbf) {
		m->off++;
		len = b & 0x1f;
	}
	else if (b == 0xd9) {
		m->off++;
		if (! mp_read_be(m, 1, &len)) return false;
	}
	else if (b == 0xda) {
		m->off++;
		if (! mp_read_be(m, 2, &len)) return false;
	}
	else if (b == 0xdb) {
		m->off++;
		if (! mp_read_be(m, 4, &len)) return false;
	}
	else {
		return false;
	}

	if (! mp_need(m, (uint32_t)len)) {
		return false;
	}

	*s = m->p + m->off;
	*n = (uint32_t)len;
	m->off += (uint32_t)len;
	return true;
}

static bool
mp_read_container(mp* m, uint8_t fix_base, uint8_t code16, uint8_t code32,
		uint32_t* count)
{
	if (! mp_need(m, 1)) {
		return false;
	}

	uint8_t b = m->p[m->off];
	uint64_t n;

	if ((b & 0xf0) == fix_base) {
		m->off++;
		*count = b & 0x0f;
		return true;
	}

	if (b == code16) {
		m->off++;
		if (! mp_read_be(m, 2, &n)) return false;
		*count = (uint32_t)n;
		return true;
	}

	if (b == code32) {
		m->off++;
		if (! mp_read_be(m, 4, &n)) return false;
		*count = (uint32_t)n;
		return true;
	}

	return false;
}

static bool
mp_read_list(mp* m, uint32_t* count)
{
	return mp_read_container(m, 0x90, 0xdc, 0xdd, count);
}

static bool
mp_read_map(mp* m, uint32_t* count)
{
	return mp_read_container(m, 0x80, 0xde, 0xdf, count);
}

static bool mp_skip(mp* m);

static bool
mp_skip_n(mp* m, uint32_t n)
{
	for (uint32_t i = 0; i < n; i++) {
		if (! mp_skip(m)) {
			return false;
		}
	}

	return true;
}

static bool
mp_skip(mp* m)
{
	if (! mp_need(m, 1)) {
		return false;
	}

	uint8_t b = m->p[m->off];
	uint64_t n;
	uint32_t count;

	if (b <= 0x7f || b >= 0xe0 || b == 0xc0 || b == 0xc2 || b == 0xc3) {
		m->off++;
		return true;
	}

	if ((b >= 0xa0 && b <= 0xbf) || b == 0xd9 || b == 0xda || b == 0xdb) {
		const uint8_t* s;
		uint32_t sn;
		return mp_read_str(m, &s, &sn);
	}

	if ((b & 0xf0) == 0x90 || b == 0xdc || b == 0xdd) {
		if (! mp_read_list(m, &count)) return false;
		return mp_skip_n(m, count);
	}

	if ((b & 0xf0) == 0x80 || b == 0xde || b == 0xdf) {
		if (! mp_read_map(m, &count)) return false;
		return mp_skip_n(m, count * 2);
	}

	m->off++;

	switch (b) {
	case 0xc4: // bin8
	case 0xc7: // ext8
		if (! mp_read_be(m, 1, &n)) return false;
		n += (b == 0xc7) ? 1 : 0;
		break;
	case 0xc5: // bin16
	case 0xc8: // ext16
		if (! mp_read_be(m, 2, &n)) return false;
		n += (b == 0xc8) ? 1 : 0;
		break;
	case 0xc6: // bin32
	case 0xc9: // ext32
		if (! mp_read_be(m, 4, &n)) return false;
		n += (b == 0xc9) ? 1 : 0;
		break;
	case 0xca:
		n = 4;
		break;
	case 0xcb:
	case 0xcf:
	case 0xd3:
		n = 8;
		break;
	case 0xcc:
	case 0xd0:
		n = 1;
		break;
	case 0xcd:
	case 0xd1:
		n = 2;
		break;
	case 0xce:
	case 0xd2:
		n = 4;
		break;
	case 0xd4:
		n = 2;
		break;
	case 0xd5:
		n = 3;
		break;
	case 0xd6:
		n = 5;
		break;
	case 0xd7:
		n = 9;
		break;
	case 0xd8:
		n = 17;
		break;
	default:
		return false;
	}

	if (! mp_need(m, (uint32_t)n)) {
		return false;
	}

	m->off += (uint32_t)n;
	return true;
}

// In CDT particle payloads (list/map bins), str/bin values carry a leading
// particle-type byte (3 = string, 4 = blob, ...). Field-45 strings don't.
static void
emit_str(sb* out, const uint8_t* s, uint32_t n, bool cdt)
{
	if (cdt && n > 0 && s[0] == AS_BYTES_BLOB) {
		sb_puts(out, "\"0x");

		for (uint32_t i = 1; i < n; i++) {
			sb_fmt(out, "%02x", s[i]);
		}

		sb_puts(out, "\"");
		return;
	}

	if (cdt && n > 0 && s[0] < 0x20) {
		s++;
		n--;
	}

	sb_json_str(out, s, n);
}

// Renders any msgpack value as JSON. Map keys are JSON-stringified;
// CDT order-flag ext pairs are dropped; bin payloads become hex strings.
static bool
mp_json(mp* m, sb* out, bool cdt)
{
	if (! mp_need(m, 1)) {
		return false;
	}

	uint8_t b = m->p[m->off];
	int64_t i64;
	uint64_t n;
	uint32_t count;

	if (b <= 0x7f || b >= 0xe0 || (b >= 0xcc && b <= 0xd3)) {
		if (! mp_read_int(m, &i64)) return false;
		sb_fmt(out, "%lld", (long long)i64);
		return true;
	}

	if ((b >= 0xa0 && b <= 0xbf) || b == 0xd9 || b == 0xda || b == 0xdb) {
		const uint8_t* s;
		uint32_t sn;
		if (! mp_read_str(m, &s, &sn)) return false;
		emit_str(out, s, sn, cdt);
		return true;
	}

	switch (b) {
	case 0xc0:
		m->off++;
		sb_puts(out, "null");
		return true;
	case 0xc2:
		m->off++;
		sb_puts(out, "false");
		return true;
	case 0xc3:
		m->off++;
		sb_puts(out, "true");
		return true;
	case 0xca: {
		m->off++;
		if (! mp_read_be(m, 4, &n)) return false;
		uint32_t u32 = (uint32_t)n;
		float f;
		memcpy(&f, &u32, 4);
		sb_fmt(out, "%g", (double)f);
		return true;
	}
	case 0xcb: {
		m->off++;
		if (! mp_read_be(m, 8, &n)) return false;
		double d;
		memcpy(&d, &n, 8);
		sb_fmt(out, "%g", d);
		return true;
	}
	case 0xc4:
	case 0xc5:
	case 0xc6: {
		m->off++;
		uint32_t sz_n = (b == 0xc4) ? 1 : (b == 0xc5) ? 2 : 4;
		if (! mp_read_be(m, sz_n, &n)) return false;
		if (! mp_need(m, (uint32_t)n)) return false;

		const uint8_t* s = m->p + m->off;
		uint32_t sn = (uint32_t)n;

		m->off += sn;

		// CDT bins carry the particle-type prefix here too.
		if (cdt && sn > 0 && s[0] == AS_BYTES_STRING) {
			sb_json_str(out, s + 1, sn - 1);
			return true;
		}

		if (cdt && sn > 0 && s[0] == AS_BYTES_BLOB) {
			s++;
			sn--;
		}

		sb_puts(out, "\"0x");

		for (uint32_t i = 0; i < sn; i++) {
			sb_fmt(out, "%02x", s[i]);
		}

		sb_puts(out, "\"");
		return true;
	}
	default:
		break;
	}

	if ((b & 0xf0) == 0x90 || b == 0xdc || b == 0xdd) {
		if (! mp_read_list(m, &count)) return false;
		sb_puts(out, "[");
		for (uint32_t i = 0; i < count; i++) {
			if (i != 0) sb_puts(out, ",");
			if (! mp_json(m, out, cdt)) return false;
		}
		sb_puts(out, "]");
		return true;
	}

	if ((b & 0xf0) == 0x80 || b == 0xde || b == 0xdf) {
		if (! mp_read_map(m, &count)) return false;
		sb_puts(out, "{");
		bool first = true;
		for (uint32_t i = 0; i < count; i++) {
			uint8_t kb = mp_need(m, 1) ? m->p[m->off] : 0;

			// CDT ordered-map marker pair (ext key) - not user data.
			if (kb == 0xc7 || kb == 0xc8 || kb == 0xc9 ||
					(kb >= 0xd4 && kb <= 0xd8)) {
				if (! mp_skip(m) || ! mp_skip(m)) return false;
				continue;
			}

			if (! first) sb_puts(out, ",");
			first = false;

			if ((kb >= 0xa0 && kb <= 0xbf) || kb == 0xd9 || kb == 0xda ||
					kb == 0xdb) {
				const uint8_t* s;
				uint32_t sn;
				if (! mp_read_str(m, &s, &sn)) return false;
				emit_str(out, s, sn, cdt);
			}
			else {
				// Non-string key (e.g. int) - stringify for JSON.
				sb tmp = { 0 };
				if (! mp_json(m, &tmp, cdt)) {
					sb_free(&tmp);
					return false;
				}
				if (tmp.buf != NULL && tmp.buf[0] == '"') {
					sb_puts(out, tmp.buf);
				}
				else {
					sb_fmt(out, "\"%s\"", tmp.buf == NULL ? "" : tmp.buf);
				}
				sb_free(&tmp);
			}

			sb_puts(out, ":");
			if (! mp_json(m, out, cdt)) return false;
		}
		sb_puts(out, "}");
		return true;
	}

	// Unrenderable (top-level ext etc.) - skip and mark.
	if (! mp_skip(m)) return false;
	sb_puts(out, "\"<ext>\"");
	return true;
}


//==========================================================
// Field-45 (error details) decode.
//

static bool
f45_decode(const uint8_t* buf, uint32_t len, f45_info* out)
{
	memset(out, 0, sizeof(*out));

	mp m = { buf, len, 0 };
	uint32_t pairs;

	if (! mp_read_map(&m, &pairs)) {
		return false;
	}

	for (uint32_t i = 0; i < pairs; i++) {
		int64_t k;

		if (! mp_read_int(&m, &k)) {
			return false;
		}

		switch (k) {
		case F45_KEY_SUBCODE: {
			int64_t v;
			if (! mp_read_int(&m, &v)) return false;
			out->subcode = (uint64_t)v;
			out->has_subcode = true;
			break;
		}
		case F45_KEY_MESSAGE:
			if (! mp_read_str(&m, &out->msg, &out->msg_len)) return false;
			break;
		case F45_KEY_EXP_TRACE: {
			uint32_t start = m.off;
			if (! mp_skip(&m)) return false;
			out->trace = buf + start;
			out->trace_len = m.off - start;
			break;
		}
		default:
			if (! mp_skip(&m)) return false;
			break;
		}
	}

	return true;
}

static const char*
outcome_name(int64_t v)
{
	switch (v) {
	case 1: return "fault";
	case 2: return "false";
	case 3: return "absent";
	default: return "?";
	}
}

// Emits the trace map as a JSON object with named keys.
static bool
trace_json(const uint8_t* buf, uint32_t len, sb* out)
{
	mp m = { buf, len, 0 };
	uint32_t pairs;

	if (! mp_read_map(&m, &pairs)) {
		return false;
	}

	sb_puts(out, "{");

	bool first = true;

	for (uint32_t i = 0; i < pairs; i++) {
		int64_t k;

		if (! mp_read_int(&m, &k)) {
			return false;
		}

		if (! first) {
			sb_puts(out, ",");
		}

		first = false;

		int64_t v;
		const uint8_t* s;
		uint32_t sn;

		switch (k) {
		case TRACE_KEY_PHASE:
			if (! mp_read_int(&m, &v)) return false;
			sb_fmt(out, "\"phase\":%lld,\"phase_name\":", (long long)v);
			sb_json_cstr(out, v == 1 ? "build" : v == 2 ? "eval" : "?");
			break;
		case TRACE_KEY_BYTE_OFFSET:
			if (! mp_read_int(&m, &v)) return false;
			sb_fmt(out, "\"byte_offset\":%lld", (long long)v);
			break;
		case TRACE_KEY_OP:
			if (! mp_read_str(&m, &s, &sn)) return false;
			sb_puts(out, "\"op\":");
			sb_json_str(out, s, sn);
			break;
		case TRACE_KEY_DEPTH:
			if (! mp_read_int(&m, &v)) return false;
			sb_fmt(out, "\"depth\":%lld", (long long)v);
			break;
		case TRACE_KEY_PATH:
			sb_puts(out, "\"path\":");
			if (! mp_json(&m, out, false)) return false;
			break;
		case TRACE_KEY_SNIPPET:
			if (! mp_read_str(&m, &s, &sn)) return false;
			sb_puts(out, "\"snippet\":");
			sb_json_str(out, s, sn);
			break;
		case TRACE_KEY_OUTCOME:
			if (! mp_read_int(&m, &v)) return false;
			sb_fmt(out, "\"outcome\":%lld,\"outcome_name\":", (long long)v);
			sb_json_cstr(out, outcome_name(v));
			break;
		case TRACE_KEY_LANG:
			if (! mp_read_int(&m, &v)) return false;
			sb_fmt(out, "\"lang\":%lld,\"lang_name\":", (long long)v);
			sb_json_cstr(out, v == 2 ? "ael" : "msgpack");
			break;
		case TRACE_KEY_AEL_OFFSET:
			if (! mp_read_int(&m, &v)) return false;
			sb_fmt(out, "\"ael_offset\":%lld", (long long)v);
			break;
		case TRACE_KEY_AEL_SPAN:
			if (! mp_read_int(&m, &v)) return false;
			sb_fmt(out, "\"ael_span\":%lld", (long long)v);
			break;
		case TRACE_KEY_AEL_LINE:
			if (! mp_read_int(&m, &v)) return false;
			sb_fmt(out, "\"ael_line\":%lld", (long long)v);
			break;
		case TRACE_KEY_AEL_COL:
			if (! mp_read_int(&m, &v)) return false;
			sb_fmt(out, "\"ael_col\":%lld", (long long)v);
			break;
		case TRACE_KEY_OPERANDS:
			sb_puts(out, "\"operands\":");
			if (! mp_json(&m, out, false)) return false;
			break;
		default:
			sb_fmt(out, "\"key_%lld\":", (long long)k);
			if (! mp_json(&m, out, false)) return false;
			break;
		}
	}

	sb_puts(out, "}");
	return true;
}


//==========================================================
// AEL expression packing (mirrors the EE unit tests' pack_ael()).
//

static as_exp*
ael_exp(const char* src, uint32_t src_len)
{
	as_packer pk = { .buffer = NULL, .capacity = 0 };

	as_pack_list_header(&pk, 2);
	as_pack_uint64(&pk, WIRE_EXP_AEL_COMPILE);
	as_pack_bin(&pk, (const uint8_t*)src, src_len);

	uint32_t sz = (uint32_t)pk.offset;
	as_exp* e = malloc(sizeof(as_exp) + sz);

	pk = (as_packer){ .buffer = e->packed, .capacity = (int)sz };
	as_pack_list_header(&pk, 2);
	as_pack_uint64(&pk, WIRE_EXP_AEL_COMPILE);
	as_pack_bin(&pk, (const uint8_t*)src, src_len);

	e->packed_sz = sz;
	return e;
}


//==========================================================
// Wire command execution with field-45 capture.
//
// as_command_init_read()/as_command_execute_read() and the filter-field
// helpers are static in the client's aerospike_key.c - these are local
// copies built on the same exported as_command_* API.
//

static as_status
key_partition_init(as_cluster* cluster, as_error* err, const as_key* key,
		as_partition_info* pi)
{
	as_error_reset(err);

	as_status status = as_key_set_digest(err, (as_key*)key);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	return as_partition_info_init(pi, cluster, err, key);
}

static void
cmd_init_read(as_command* cmd, as_cluster* cluster, const as_policy_base* policy,
		as_policy_replica replica, as_policy_read_mode_sc read_mode_sc,
		const as_key* key, size_t size, as_partition_info* pi,
		const as_parse_results_fn fn, void* udata)
{
	cmd->cluster = cluster;
	cmd->policy = policy;
	cmd->node = NULL;
	cmd->key = key;
	cmd->ns = pi->ns;
	cmd->partition = pi->partition;
	cmd->parse_results_fn = fn;
	cmd->udata = udata;
	cmd->buf_size = size;
	cmd->partition_id = pi->partition_id;
	cmd->latency_type = AS_LATENCY_TYPE_READ;
	as_cluster_add_command_count(cluster);

	if (pi->sc_mode) {
		switch (read_mode_sc) {
		case AS_POLICY_READ_MODE_SC_SESSION:
			cmd->replica = AS_POLICY_REPLICA_MASTER;
			cmd->flags = AS_COMMAND_FLAGS_READ;
			break;
		case AS_POLICY_READ_MODE_SC_LINEARIZE:
			cmd->replica = (replica != AS_POLICY_REPLICA_PREFER_RACK) ?
					replica : AS_POLICY_REPLICA_SEQUENCE;
			cmd->flags = AS_COMMAND_FLAGS_READ | AS_COMMAND_FLAGS_LINEARIZE;
			break;
		default:
			cmd->replica = replica;
			cmd->flags = AS_COMMAND_FLAGS_READ;
			break;
		}
	}
	else {
		cmd->replica = replica;
		cmd->flags = AS_COMMAND_FLAGS_READ;
	}

	cmd->replica_size = pi->replica_size;
	cmd->replica_index = as_replica_index_init_read(cluster, cmd->replica);
}

static as_status
cmd_execute_read(as_cluster* cluster, as_error* err, const as_policy_base* policy,
		as_policy_replica replica, as_policy_read_mode_sc read_mode_sc,
		const as_key* key, uint8_t* buf, size_t size, as_partition_info* pi,
		const as_parse_results_fn fn, void* udata)
{
	as_command cmd;

	cmd_init_read(&cmd, cluster, policy, replica, read_mode_sc, key, size, pi,
			fn, udata);

	cmd.buf = buf;
	as_command_start_timer(&cmd);
	return as_command_execute(&cmd, err);
}

static uint32_t
filter_field_size(const as_policy_base* policy, uint16_t* n_fields)
{
	if (policy->filter_exp) {
		(*n_fields)++;
		return AS_FIELD_HEADER_SIZE + policy->filter_exp->packed_sz;
	}

	return 0;
}

static uint8_t*
write_filter_field(const as_policy_base* policy, uint8_t* p)
{
	if (policy->filter_exp) {
		return as_exp_write(policy->filter_exp, p);
	}

	return p;
}

// Decode one returned operation's particle to JSON.
static void
particle_json(uint8_t type, const uint8_t* v, uint32_t sz, sb* out)
{
	switch (type) {
	case AS_BYTES_UNDEF:
		sb_puts(out, "null");
		break;
	case AS_BYTES_BOOL:
		sb_puts(out, (sz > 0 && v[0] != 0) ? "true" : "false");
		break;
	case AS_BYTES_INTEGER: {
		int64_t val = 0;

		if (sz == 8) {
			uint64_t u;
			memcpy(&u, v, 8);
			val = (int64_t)cf_swap_from_be64(u);
		}

		sb_fmt(out, "%lld", (long long)val);
		break;
	}
	case AS_BYTES_DOUBLE: {
		double d = 0;

		if (sz == 8) {
			uint64_t u;
			memcpy(&u, v, 8);
			u = cf_swap_from_be64(u);
			memcpy(&d, &u, 8);
		}

		sb_fmt(out, "%g", d);
		break;
	}
	case AS_BYTES_STRING:
		sb_json_str(out, v, sz);
		break;
	case AS_BYTES_GEOJSON: {
		// [1 flags][2 ncells][ncells * 8][json]
		if (sz >= 3) {
			uint16_t ncells = (uint16_t)((v[1] << 8) | v[2]);
			uint32_t skip = 3 + (uint32_t)ncells * 8;

			if (sz >= skip) {
				sb_json_str(out, v + skip, sz - skip);
				break;
			}
		}

		sb_puts(out, "\"<geojson?>\"");
		break;
	}
	case AS_BYTES_LIST:
	case AS_BYTES_MAP: {
		mp m = { v, sz, 0 };

		if (! mp_json(&m, out, true)) {
			sb_puts(out, "\"<msgpack decode error>\"");
		}

		break;
	}
	default: {
		sb_fmt(out, "\"0x");

		for (uint32_t i = 0; i < sz && i < 64; i++) {
			sb_fmt(out, "%02x", v[i]);
		}

		if (sz > 64) {
			sb_puts(out, "...");
		}

		if (type != AS_BYTES_BLOB) {
			sb_fmt(out, " (type %u)", type);
		}

		sb_puts(out, "\"");
		break;
	}
	}
}

// Custom parse callback: capture result code, field-45 payload and bins.
static as_status
capture_parse(as_error* err, as_command* cmd, as_node* node, uint8_t* buf,
		size_t size)
{
	(void)node;

	eval_capture* cap = cmd->udata;
	as_msg* msg = (as_msg*)buf;
	as_status st = as_msg_parse(err, msg, size);

	if (st != AEROSPIKE_OK) {
		return st;
	}

	cap->result_code = msg->result_code;
	cap->generation = msg->generation;

	uint8_t* p = buf + sizeof(as_msg);

	for (uint32_t i = 0; i < msg->n_fields; i++) {
		uint32_t len = cf_swap_from_be32(*(uint32_t*)p) - 1;

		p += 4;

		uint8_t type = *p++;

		if (type == AS_FIELD_ERROR_MESSAGE && len > 0 && cap->f45 == NULL) {
			cap->f45 = malloc(len);
			memcpy(cap->f45, p, len);
			cap->f45_len = len;
		}

		p += len;
	}

	if (msg->n_ops > 0) {
		sb_puts(&cap->bins, "{");

		for (uint32_t i = 0; i < msg->n_ops; i++) {
			uint32_t op_size = cf_swap_from_be32(*(uint32_t*)p);

			p += 5; // size + op
			uint8_t type = *p;
			p += 2; // particle type + version

			uint8_t name_sz = *p++;

			if (i != 0) {
				sb_puts(&cap->bins, ",");
			}

			sb_json_str(&cap->bins, p, name_sz);
			sb_puts(&cap->bins, ":");
			p += name_sz;

			uint32_t val_sz = op_size - ((uint32_t)name_sz + 4);

			particle_json(type, p, val_sz, &cap->bins);
			p += val_sz;
		}

		sb_puts(&cap->bins, "}");
	}

	if (msg->result_code == AEROSPIKE_OK) {
		return AEROSPIKE_OK;
	}

	return as_error_update(err, msg->result_code, "%s",
			as_error_string(msg->result_code));
}

// AEL as a record filter on a get of the sample record.
static as_status
eval_filter_get(app* g, as_error* err, as_exp* filter, int verbosity,
		eval_capture* cap)
{
	as_policy_read pol;

	as_policy_read_init(&pol);
	pol.base.filter_exp = filter;
	pol.base.error_detail_verbosity = (uint8_t)verbosity;

	as_cluster* cluster = g->as.cluster;
	as_partition_info pi;
	as_status status = key_partition_init(cluster, err, &g->key, &pi);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	as_command_txn_data tdata;
	size_t size = as_command_key_size(&pol.base, pol.key, &g->key, false,
			&tdata);
	uint32_t filter_size = filter_field_size(&pol.base, &tdata.n_fields);

	size += filter_size;

	uint8_t* buf = as_command_buffer_init(size);
	uint32_t timeout = as_command_server_timeout(&pol.base);
	uint8_t* p = as_command_write_header_read(buf, &pol.base, pol.read_mode_ap,
			pol.read_mode_sc, pol.read_touch_ttl_percent, timeout,
			tdata.n_fields, 0, AS_MSG_INFO1_READ | AS_MSG_INFO1_GET_ALL, 0, 0);

	p = as_command_write_key(p, &pol.base, pol.key, &g->key, &tdata);
	p = write_filter_field(&pol.base, p);
	size = as_command_write_end(buf, p);

	status = cmd_execute_read(cluster, err, &pol.base, pol.replica,
			pol.read_mode_sc, &g->key, buf, size, &pi, capture_parse, cap);

	as_command_buffer_free(buf, size);
	return status;
}

// AEL as an expression-read operation returning its value in bin "result".
static as_status
eval_exp_read_op(app* g, as_error* err, as_exp* e, int verbosity, bool no_fail,
		eval_capture* cap)
{
	as_operations ops;

	as_operations_inita(&ops, 1);
	as_operations_exp_read(&ops, "result", e,
			no_fail ? AS_EXP_READ_EVAL_NO_FAIL : AS_EXP_READ_DEFAULT);

	as_policy_operate pol;

	as_policy_operate_init(&pol);
	pol.base.error_detail_verbosity = (uint8_t)verbosity;

	as_queue buffers;

	as_queue_inita(&buffers, sizeof(as_buffer), 1);

	as_binop* op = &ops.binops.entries[0];
	size_t size = 0;
	as_status status = as_command_bin_size(&op->bin, &buffers, &size, err);

	if (status != AEROSPIKE_OK) {
		as_buffers_destroy(&buffers);
		as_operations_destroy(&ops);
		return status;
	}

	uint8_t read_attr = AS_MSG_INFO1_READ;
	uint8_t write_attr = AS_MSG_INFO2_RESPOND_ALL_OPS;
	uint8_t info_attr = 0;

	as_command_set_attr_read(pol.read_mode_ap, pol.read_mode_sc,
			pol.base.compress, &read_attr, &info_attr);

	as_cluster* cluster = g->as.cluster;
	as_partition_info pi;

	status = key_partition_init(cluster, err, &g->key, &pi);

	if (status != AEROSPIKE_OK) {
		as_buffers_destroy(&buffers);
		as_operations_destroy(&ops);
		return status;
	}

	as_command_txn_data tdata;

	size += as_command_key_size(&pol.base, pol.key, &g->key, false, &tdata);

	uint8_t* buf = as_command_buffer_init(size);
	uint8_t* p = as_command_write_header_write(buf, &pol.base,
			pol.commit_level, pol.exists, pol.gen, 0,
			(uint32_t)pol.read_touch_ttl_percent, tdata.n_fields, 1, false,
			false, read_attr, write_attr, info_attr);

	p = as_command_write_key(p, &pol.base, pol.key, &g->key, &tdata);
	p = as_command_write_bin(p, op->op, &op->bin, &buffers);
	as_buffers_destroy(&buffers);
	size = as_command_write_end(buf, p);

	status = cmd_execute_read(cluster, err, &pol.base, pol.replica,
			pol.read_mode_sc, &g->key, buf, size, &pi, capture_parse, cap);

	as_command_buffer_free(buf, size);
	as_operations_destroy(&ops);
	return status;
}


//==========================================================
// Sample record.
//

static as_status
reset_sample(app* g, as_error* err)
{
	as_record rec;

	as_record_inita(&rec, 7);
	as_record_set_int64(&rec, "x", 10);
	as_record_set_double(&rec, "y", 2.5);
	as_record_set_str(&rec, "name", "ael");
	as_record_set_bool(&rec, "flag", true);

	as_arraylist* xs = as_arraylist_new(3, 0);

	as_arraylist_append_int64(xs, 1);
	as_arraylist_append_int64(xs, 2);
	as_arraylist_append_int64(xs, 3);
	as_record_set_list(&rec, "xs", (as_list*)xs);

	as_orderedmap* m = as_orderedmap_new(4);

	as_orderedmap_set(m, (as_val*)as_string_new_strdup("a"),
			(as_val*)as_integer_new(1));
	as_orderedmap_set(m, (as_val*)as_string_new_strdup("b"),
			(as_val*)as_integer_new(2));
	as_record_set_map(&rec, "m", (as_map*)m);

	static uint8_t blob[] = { 0xde, 0xad, 0xbe, 0xef };

	as_record_set_raw(&rec, "blob", blob, sizeof(blob));

	as_policy_write wpol;

	as_policy_write_init(&wpol);

	as_status s = aerospike_key_put(&g->as, err, &wpol, &g->key, &rec);

	as_record_destroy(&rec);
	return s;
}


//==========================================================
// JSON response assembly.
//

static const char*
status_name(int code)
{
	switch (code) {
	case AEROSPIKE_OK: return "AEROSPIKE_OK";
	case AEROSPIKE_ERR_SERVER: return "AEROSPIKE_ERR_SERVER";
	case AEROSPIKE_ERR_RECORD_NOT_FOUND: return "AEROSPIKE_ERR_RECORD_NOT_FOUND";
	case AEROSPIKE_ERR_RECORD_GENERATION: return "AEROSPIKE_ERR_RECORD_GENERATION";
	case AEROSPIKE_ERR_REQUEST_INVALID: return "AEROSPIKE_ERR_REQUEST_INVALID";
	case AEROSPIKE_ERR_RECORD_EXISTS: return "AEROSPIKE_ERR_RECORD_EXISTS";
	case AEROSPIKE_ERR_BIN_EXISTS: return "AEROSPIKE_ERR_BIN_EXISTS";
	case AEROSPIKE_ERR_CLUSTER_CHANGE: return "AEROSPIKE_ERR_CLUSTER_CHANGE";
	case AEROSPIKE_ERR_SERVER_FULL: return "AEROSPIKE_ERR_SERVER_FULL";
	case AEROSPIKE_ERR_TIMEOUT: return "AEROSPIKE_ERR_TIMEOUT";
	case AEROSPIKE_ERR_ALWAYS_FORBIDDEN: return "AEROSPIKE_ERR_ALWAYS_FORBIDDEN";
	case AEROSPIKE_ERR_CLUSTER: return "AEROSPIKE_ERR_CLUSTER";
	case AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE: return "AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE";
	case AEROSPIKE_ERR_RECORD_TOO_BIG: return "AEROSPIKE_ERR_RECORD_TOO_BIG";
	case AEROSPIKE_ERR_RECORD_BUSY: return "AEROSPIKE_ERR_RECORD_BUSY";
	case AEROSPIKE_ERR_SCAN_ABORTED: return "AEROSPIKE_ERR_SCAN_ABORTED";
	case AEROSPIKE_ERR_UNSUPPORTED_FEATURE: return "AEROSPIKE_ERR_UNSUPPORTED_FEATURE";
	case AEROSPIKE_ERR_BIN_NOT_FOUND: return "AEROSPIKE_ERR_BIN_NOT_FOUND";
	case AEROSPIKE_ERR_DEVICE_OVERLOAD: return "AEROSPIKE_ERR_DEVICE_OVERLOAD";
	case AEROSPIKE_ERR_RECORD_KEY_MISMATCH: return "AEROSPIKE_ERR_RECORD_KEY_MISMATCH";
	case AEROSPIKE_ERR_NAMESPACE_NOT_FOUND: return "AEROSPIKE_ERR_NAMESPACE_NOT_FOUND";
	case AEROSPIKE_ERR_BIN_NAME: return "AEROSPIKE_ERR_BIN_NAME";
	case AEROSPIKE_ERR_FAIL_FORBIDDEN: return "AEROSPIKE_ERR_FAIL_FORBIDDEN";
	case AEROSPIKE_ERR_FAIL_ELEMENT_NOT_FOUND: return "AEROSPIKE_ERR_FAIL_ELEMENT_NOT_FOUND";
	case AEROSPIKE_ERR_FAIL_ELEMENT_EXISTS: return "AEROSPIKE_ERR_FAIL_ELEMENT_EXISTS";
	case AEROSPIKE_ERR_OP_NOT_APPLICABLE: return "AEROSPIKE_ERR_OP_NOT_APPLICABLE";
	case AEROSPIKE_FILTERED_OUT: return "AEROSPIKE_FILTERED_OUT";
	case AEROSPIKE_LOST_CONFLICT: return "AEROSPIKE_LOST_CONFLICT";
	case AEROSPIKE_ERR_CLIENT: return "AEROSPIKE_ERR_CLIENT";
	case AEROSPIKE_ERR_PARAM: return "AEROSPIKE_ERR_PARAM";
	case AEROSPIKE_ERR_CONNECTION: return "AEROSPIKE_ERR_CONNECTION";
	case AEROSPIKE_ERR_TLS_ERROR: return "AEROSPIKE_ERR_TLS_ERROR";
	case AEROSPIKE_ERR_INVALID_NODE: return "AEROSPIKE_ERR_INVALID_NODE";
	case AEROSPIKE_ERR_NO_MORE_CONNECTIONS: return "AEROSPIKE_ERR_NO_MORE_CONNECTIONS";
	default: return NULL;
	}
}

static void
eval_json(app* g, const char* mode, int verbosity, bool no_fail,
		const char* src, uint32_t src_len, sb* out)
{
	as_exp* e = ael_exp(src, src_len);
	eval_capture cap = { .result_code = -1 };
	as_error err;

	as_error_init(&err);

	as_status status;

	if (strcmp(mode, "value") == 0) {
		status = eval_exp_read_op(g, &err, e, verbosity, no_fail, &cap);
	}
	else {
		status = eval_filter_get(g, &err, e, verbosity, &cap);
	}

	free(e);

	sb_fmt(out, "{\"status\":%d,\"ok\":%s,", (int)status,
			status == AEROSPIKE_OK ? "true" : "false");

	const char* name = status_name((int)status);

	sb_puts(out, "\"status_name\":");

	if (name != NULL) {
		sb_json_cstr(out, name);
	}
	else {
		sb tmp = { 0 };
		sb_fmt(&tmp, "STATUS_%d", (int)status);
		sb_json_cstr(out, tmp.buf);
		sb_free(&tmp);
	}

	sb_puts(out, ",\"status_desc\":");
	sb_json_cstr(out, as_error_string(status));
	sb_fmt(out, ",\"in_doubt\":%s,", err.in_doubt ? "true" : "false");
	sb_puts(out, "\"client_message\":");
	sb_json_cstr(out, err.message);
	sb_fmt(out, ",\"generation\":%u,", cap.generation);

	sb_puts(out, "\"bins\":");
	sb_puts(out, cap.bins.len > 0 ? cap.bins.buf : "null");

	sb_puts(out, ",\"details\":");

	if (cap.f45 != NULL) {
		f45_info d;

		if (f45_decode(cap.f45, cap.f45_len, &d)) {
			sb_puts(out, "{");
			sb_fmt(out, "\"has_subcode\":%s,\"subcode\":%llu,",
					d.has_subcode ? "true" : "false",
					(unsigned long long)d.subcode);
			sb_puts(out, "\"message\":");

			if (d.msg != NULL) {
				sb_json_str(out, d.msg, d.msg_len);
			}
			else {
				sb_puts(out, "null");
			}

			sb_puts(out, ",\"trace\":");

			if (d.trace != NULL) {
				sb tr = { 0 };

				if (trace_json(d.trace, d.trace_len, &tr)) {
					sb_puts(out, tr.buf);
				}
				else {
					sb_puts(out, "null");
				}

				sb_free(&tr);
			}
			else {
				sb_puts(out, "null");
			}

			// Full generic decode, for the raw view.
			sb raw = { 0 };
			mp m = { cap.f45, cap.f45_len, 0 };

			sb_puts(out, ",\"raw\":");

			if (mp_json(&m, &raw, false)) {
				sb_puts(out, raw.buf);
			}
			else {
				sb_puts(out, "null");
			}

			sb_free(&raw);
			sb_puts(out, "}");
		}
		else {
			sb_puts(out, "{\"decode_error\":true}");
		}
	}
	else {
		sb_puts(out, "null");
	}

	sb_puts(out, "}");

	free(cap.f45);
	sb_free(&cap.bins);
}

static void
info_json(app* g, sb* out)
{
	// Read the sample record (no filter) for the record panel.
	eval_capture cap = { .result_code = -1 };
	as_error err;

	as_error_init(&err);

	as_status status = eval_filter_get(g, &err, NULL, 0, &cap);

	if (status == AEROSPIKE_ERR_RECORD_NOT_FOUND) {
		// First run against this namespace - create the sample record.
		if (reset_sample(g, &err) == AEROSPIKE_OK) {
			free(cap.f45);
			sb_free(&cap.bins);
			memset(&cap, 0, sizeof(cap));
			cap.result_code = -1;
			as_error_init(&err);
			status = eval_filter_get(g, &err, NULL, 0, &cap);
		}
	}

	sb_fmt(out, "{\"host\":\"%s\",\"port\":%d,", g->host, g->port);
	sb_fmt(out, "\"namespace\":\"%s\",\"set\":\"%s\",\"key\":\"sample\",",
			g->ns, g->set);
	sb_fmt(out, "\"connected\":%s,", status == AEROSPIKE_OK ? "true" : "false");
	sb_puts(out, "\"error\":");

	if (status == AEROSPIKE_OK) {
		sb_puts(out, "null");
	}
	else {
		sb_json_cstr(out, err.message);
	}

	sb_puts(out, ",\"bins\":");
	sb_puts(out, cap.bins.len > 0 ? cap.bins.buf : "null");
	sb_puts(out, "}");

	free(cap.f45);
	sb_free(&cap.bins);
}


//==========================================================
// HTTP server.
//

static void
http_send(int fd, int code, const char* code_str, const char* ctype,
		const char* body, size_t body_len)
{
	sb h = { 0 };

	sb_fmt(&h, "HTTP/1.1 %d %s\r\nContent-Type: %s\r\n"
			"Content-Length: %zu\r\nCache-Control: no-store\r\n"
			"Connection: close\r\n\r\n", code, code_str, ctype, body_len);

	(void)! write(fd, h.buf, h.len);

	size_t off = 0;

	while (off < body_len) {
		ssize_t w = write(fd, body + off, body_len - off);

		if (w <= 0) {
			break;
		}

		off += (size_t)w;
	}

	sb_free(&h);
}

static void
http_send_json(int fd, const sb* body)
{
	http_send(fd, 200, "OK", "application/json", body->buf == NULL ? "null" :
			body->buf, body->len);
}

// Returns malloc'd, percent-decoded value of a query parameter, or NULL.
static char*
query_param(const char* path, const char* name)
{
	const char* q = strchr(path, '?');

	if (q == NULL) {
		return NULL;
	}

	q++;

	size_t name_len = strlen(name);

	while (*q != 0) {
		const char* eq = strchr(q, '=');
		const char* amp = strchr(q, '&');
		const char* end = amp != NULL ? amp : q + strlen(q);

		if (eq != NULL && eq < end && (size_t)(eq - q) == name_len &&
				strncmp(q, name, name_len) == 0) {
			size_t vlen = (size_t)(end - eq - 1);
			char* out = malloc(vlen + 1);
			size_t o = 0;

			for (size_t i = 0; i < vlen; i++) {
				char c = eq[1 + i];

				if (c == '%' && i + 2 < vlen) {
					char hex[3] = { eq[2 + i], eq[3 + i], 0 };

					out[o++] = (char)strtol(hex, NULL, 16);
					i += 2;
				}
				else if (c == '+') {
					out[o++] = ' ';
				}
				else {
					out[o++] = c;
				}
			}

			out[o] = 0;
			return out;
		}

		if (amp == NULL) {
			break;
		}

		q = amp + 1;
	}

	return NULL;
}

static void
serve_ui(int fd, const char* ui_path)
{
	FILE* f = fopen(ui_path, "rb");

	if (f == NULL) {
		const char* msg =
				"<!doctype html><meta charset='utf-8'><body>"
				"<h1>ael-gui</h1><p>ui.html not found - run from the "
				"ael-gui directory or pass -u /path/to/ui.html</p>";

		http_send(fd, 200, "OK", "text/html; charset=utf-8", msg, strlen(msg));
		return;
	}

	sb body = { 0 };
	char chunk[65536];
	size_t n;

	while ((n = fread(chunk, 1, sizeof(chunk), f)) > 0) {
		sb_putn(&body, chunk, n);
	}

	fclose(f);
	http_send(fd, 200, "OK", "text/html; charset=utf-8", body.buf, body.len);
	sb_free(&body);
}

static void
handle_conn(app* g, int fd, const char* ui_path)
{
	sb req = { 0 };
	char chunk[16384];
	char* hdr_end = NULL;

	// Read headers.
	while (hdr_end == NULL && req.len < HTTP_MAX_REQUEST) {
		ssize_t n = read(fd, chunk, sizeof(chunk));

		if (n <= 0) {
			sb_free(&req);
			return;
		}

		sb_putn(&req, chunk, (size_t)n);
		hdr_end = strstr(req.buf, "\r\n\r\n");
	}

	if (hdr_end == NULL) {
		sb_free(&req);
		return;
	}

	size_t body_off = (size_t)(hdr_end - req.buf) + 4;

	// Request line.
	char method[8] = { 0 };
	char path[2048] = { 0 };

	sscanf(req.buf, "%7s %2047s", method, path);

	// Content-Length (case-insensitive).
	size_t content_len = 0;
	const char* cl = strcasestr(req.buf, "content-length:");

	if (cl != NULL && cl < hdr_end) {
		content_len = (size_t)strtoul(cl + 15, NULL, 10);
	}

	if (content_len > HTTP_MAX_REQUEST) {
		sb_free(&req);
		return;
	}

	// Read remaining body.
	while (req.len < body_off + content_len) {
		ssize_t n = read(fd, chunk, sizeof(chunk));

		if (n <= 0) {
			break;
		}

		sb_putn(&req, chunk, (size_t)n);
	}

	const char* body = req.buf + body_off;
	size_t body_len = req.len >= body_off ? req.len - body_off : 0;

	if (body_len > content_len) {
		body_len = content_len;
	}

	if (strcmp(method, "GET") == 0 && (strcmp(path, "/") == 0 ||
			strncmp(path, "/index.html", 11) == 0)) {
		serve_ui(fd, ui_path);
	}
	else if (strcmp(method, "GET") == 0 && strncmp(path, "/info", 5) == 0) {
		sb out = { 0 };

		info_json(g, &out);
		http_send_json(fd, &out);
		sb_free(&out);
	}
	else if (strcmp(method, "POST") == 0 && strncmp(path, "/reset", 6) == 0) {
		as_error err;

		as_error_init(&err);

		as_status s = reset_sample(g, &err);
		sb out = { 0 };

		if (s == AEROSPIKE_OK) {
			info_json(g, &out);
		}
		else {
			sb_puts(&out, "{\"connected\":false,\"error\":");
			sb_json_cstr(&out, err.message);
			sb_puts(&out, "}");
		}

		http_send_json(fd, &out);
		sb_free(&out);
	}
	else if (strcmp(method, "POST") == 0 && strncmp(path, "/eval", 5) == 0) {
		char* mode = query_param(path, "mode");
		char* verb = query_param(path, "verbosity");
		char* nofail = query_param(path, "nofail");
		int v = verb != NULL ? atoi(verb) : 3;

		if (v < 0) {
			v = 0;
		}

		if (v > 3) {
			v = 3;
		}

		if (body_len == 0) {
			const char* msg = "{\"error\":\"empty expression\"}";

			http_send(fd, 400, "Bad Request", "application/json", msg,
					strlen(msg));
		}
		else {
			sb out = { 0 };

			eval_json(g, mode != NULL ? mode : "filter", v,
					nofail != NULL && strcmp(nofail, "1") == 0, body,
					(uint32_t)body_len, &out);
			http_send_json(fd, &out);
			sb_free(&out);
		}

		free(mode);
		free(verb);
		free(nofail);
	}
	else {
		const char* msg = "{\"error\":\"not found\"}";

		http_send(fd, 404, "Not Found", "application/json", msg, strlen(msg));
	}

	sb_free(&req);
}

static void
on_signal(int sig)
{
	(void)sig;
	g_stop = 1;

	if (g_listen_fd >= 0) {
		close(g_listen_fd);
	}
}


//==========================================================
// Main.
//

int
main(int argc, char* argv[])
{
	app g = { 0 };

	strcpy(g.host, "127.0.0.1");
	g.port = 3000;
	strcpy(g.ns, "test");
	strcpy(g.set, "ael-gui");

	int http_port = 8280;
	const char* ui_path = NULL;
	int c;

	while ((c = getopt(argc, argv, "h:p:n:s:l:u:?")) != -1) {
		switch (c) {
		case 'h':
			snprintf(g.host, sizeof(g.host), "%s", optarg);
			break;
		case 'p':
			g.port = atoi(optarg);
			break;
		case 'n':
			snprintf(g.ns, sizeof(g.ns), "%s", optarg);
			break;
		case 's':
			snprintf(g.set, sizeof(g.set), "%s", optarg);
			break;
		case 'l':
			http_port = atoi(optarg);
			break;
		case 'u':
			ui_path = optarg;
			break;
		default:
			printf("usage: %s [-h host] [-p port] [-n namespace] [-s set] "
					"[-l http-port] [-u ui.html]\n", argv[0]);
			return 1;
		}
	}

	// Default ui.html: next to the binary's source (cwd), falling back to
	// the directory of argv[0]/../ui.html (binary lives in target/).
	static char ui_buf[2048];

	if (ui_path == NULL) {
		if (access("ui.html", R_OK) == 0) {
			ui_path = "ui.html";
		}
		else {
			snprintf(ui_buf, sizeof(ui_buf), "%s", argv[0]);

			char* slash = strrchr(ui_buf, '/');

			if (slash != NULL) {
				snprintf(slash + 1, sizeof(ui_buf) - (size_t)(slash + 1 -
						ui_buf), "../ui.html");
				ui_path = ui_buf;
			}
			else {
				ui_path = "ui.html";
			}
		}
	}

	as_key_init_str(&g.key, g.ns, g.set, "sample");

	as_config config;

	as_config_init(&config);
	as_config_add_host(&config, g.host, (uint16_t)g.port);
	aerospike_init(&g.as, &config);

	as_error err;

	if (aerospike_connect(&g.as, &err) != AEROSPIKE_OK) {
		fprintf(stderr, "failed to connect to %s:%d - %s\n", g.host, g.port,
				err.message);
		aerospike_destroy(&g.as);
		return 1;
	}

	if (reset_sample(&g, &err) != AEROSPIKE_OK) {
		fprintf(stderr, "warning: could not write sample record - %s\n",
				err.message);
	}

	signal(SIGINT, on_signal);
	signal(SIGTERM, on_signal);
	signal(SIGPIPE, SIG_IGN);

	g_listen_fd = socket(AF_INET, SOCK_STREAM, 0);

	int one = 1;

	setsockopt(g_listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

	struct sockaddr_in addr = { 0 };

	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	addr.sin_port = htons((uint16_t)http_port);

	if (bind(g_listen_fd, (struct sockaddr*)&addr, sizeof(addr)) != 0 ||
			listen(g_listen_fd, 8) != 0) {
		fprintf(stderr, "failed to listen on 127.0.0.1:%d - %s\n", http_port,
				strerror(errno));
		aerospike_close(&g.as, &err);
		aerospike_destroy(&g.as);
		return 1;
	}

	printf("ael-gui: cluster %s:%d ns=%s set=%s\n", g.host, g.port, g.ns,
			g.set);
	printf("ael-gui: open http://127.0.0.1:%d/\n", http_port);

	while (! g_stop) {
		int fd = accept(g_listen_fd, NULL, NULL);

		if (fd < 0) {
			if (errno == EINTR || g_stop) {
				break;
			}

			continue;
		}

		handle_conn(&g, fd, ui_path);
		close(fd);
	}

	printf("\nael-gui: shutting down\n");

	if (g_listen_fd >= 0) {
		close(g_listen_fd);
	}

	aerospike_close(&g.as, &err);
	aerospike_destroy(&g.as);
	return 0;
}
