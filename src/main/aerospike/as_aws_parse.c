/*
 * Copyright 2008-2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
#include <openssl/opensslv.h>

#if OPENSSL_VERSION_NUMBER < 0x10100000L
#error "the aws-tags seed provider requires OpenSSL 1.1.0 or later"
#endif

#include <aerospike/as_aws_internal.h>
#include <citrusleaf/alloc.h>

#include <openssl/crypto.h>

#include <stdarg.h>
#include <stdio.h>
#include <string.h>

#include "as_aws_private.h"

//---------------------------------
// Macros
//---------------------------------

#define XML_MAX_DEPTH 64
#define XML_MAX_NODES 1000000
#define JSON_MAX_DEPTH 64

// Node links are index + 1; 0 is none.
#define XML_NONE 0

//---------------------------------
// Types
//---------------------------------

struct as_aws_xml_node_s {
	struct as_aws_xml_s* doc;
	uint32_t name;
	uint32_t first_child;
	uint32_t next;

	// One past the index of the last descendant.
	uint32_t end;

	// Range of the document text inside the element.
	uint32_t text_start;
	uint32_t text_end;

	// Trimmed text content, created on first use.
	char* content;
};

struct as_aws_xml_s {
	as_aws_xml_node* nodes;
	uint32_t nodes_size;
	uint32_t nodes_capacity;
	as_aws_buf names;
	as_aws_buf text;

	// A content allocation failed.
	bool failed;
};

typedef struct xml_parser_s {
	const uint8_t* data;
	size_t size;
	size_t pos;
	as_aws_xml* doc;
	uint32_t stack[XML_MAX_DEPTH];
	uint32_t last[XML_MAX_DEPTH + 1];
	uint32_t depth;
	bool root_done;
	char* reason;
	size_t reason_size;
} xml_parser;

typedef struct json_parser_s {
	const uint8_t* data;
	size_t size;
	size_t pos;
} json_parser;

//---------------------------------
// Static Functions: buffers
//---------------------------------

static bool
buf_reserve(as_aws_buf* buf, size_t extra)
{
	if (buf->failed) {
		return false;
	}

	if (buf->data && buf->size + extra < buf->capacity) {
		return true;
	}

	size_t need = buf->size + extra + 1;

	if (need < buf->size) {
		buf->failed = true;
		return false;
	}

	size_t capacity = buf->capacity ? buf->capacity * 2 : 64;

	while (capacity < need) {
		capacity *= 2;
	}

	char* data = cf_malloc(capacity);

	if (!data) {
		buf->failed = true;
		return false;
	}

	if (buf->data) {
		memcpy(data, buf->data, buf->size + 1);
		OPENSSL_cleanse(buf->data, buf->capacity);
		cf_free(buf->data);
	}
	else {
		data[0] = 0;
	}
	buf->data = data;
	buf->capacity = capacity;
	return true;
}

//---------------------------------
// Functions: buffers and messages
//---------------------------------

void
as_aws_buf_init(as_aws_buf* buf)
{
	memset(buf, 0, sizeof(as_aws_buf));
}

bool
as_aws_buf_append(as_aws_buf* buf, const void* data, size_t size)
{
	if (!buf_reserve(buf, size)) {
		return false;
	}

	if (size > 0) {
		memcpy(buf->data + buf->size, data, size);
	}
	buf->size += size;
	buf->data[buf->size] = 0;
	return true;
}

bool
as_aws_buf_append_str(as_aws_buf* buf, const char* text)
{
	return as_aws_buf_append(buf, text, strlen(text));
}

bool
as_aws_buf_printf(as_aws_buf* buf, const char* fmt, ...)
{
	if (buf->failed) {
		return false;
	}

	va_list ap;
	va_start(ap, fmt);
	va_list copy;
	va_copy(copy, ap);
	int len = vsnprintf(NULL, 0, fmt, copy);
	va_end(copy);

	if (len < 0 || !buf_reserve(buf, (size_t)len)) {
		buf->failed = true;
		va_end(ap);
		return false;
	}

	vsnprintf(buf->data + buf->size, (size_t)len + 1, fmt, ap);
	va_end(ap);
	buf->size += (size_t)len;
	return true;
}

char*
as_aws_buf_detach(as_aws_buf* buf)
{
	if (buf->failed || (!buf->data && !buf_reserve(buf, 0))) {
		as_aws_buf_destroy(buf);
		return NULL;
	}

	char* data = buf->data;
	memset(buf, 0, sizeof(as_aws_buf));
	return data;
}

void
as_aws_buf_destroy(as_aws_buf* buf)
{
	if (buf->data) {
		OPENSSL_cleanse(buf->data, buf->capacity);
		cf_free(buf->data);
	}
	memset(buf, 0, sizeof(as_aws_buf));
}

void
as_aws_free_secret(char* text)
{
	if (text) {
		OPENSSL_cleanse(text, strlen(text));
		cf_free(text);
	}
}

char*
as_aws_redact(const char* text, const char* const* secrets, uint32_t secrets_size)
{
	char* current = cf_strdup(text);

	for (uint32_t i = 0; current && i < secrets_size; i++) {
		const char* secret = secrets[i];

		if (!secret || !secret[0] || !strstr(current, secret)) {
			continue;
		}

		size_t secret_len = strlen(secret);
		as_aws_buf buf;
		as_aws_buf_init(&buf);
		const char* p = current;
		const char* hit;

		while ((hit = strstr(p, secret)) != NULL) {
			as_aws_buf_append(&buf, p, (size_t)(hit - p));
			as_aws_buf_append_str(&buf, AS_AWS_REDACTED);
			p = hit + secret_len;
		}
		as_aws_buf_append_str(&buf, p);
		as_aws_free_secret(current);
		current = as_aws_buf_detach(&buf);
	}
	return current;
}

as_status
as_aws_errorf(as_error* err, as_status code, const char* const* secrets, uint32_t secrets_size,
	const char* fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	va_list copy;
	va_copy(copy, ap);
	int len = vsnprintf(NULL, 0, fmt, copy);
	va_end(copy);

	char* text = len >= 0 ? cf_malloc((size_t)len + 1) : NULL;

	if (text) {
		vsnprintf(text, (size_t)len + 1, fmt, ap);
	}
	va_end(ap);

	char* redacted = text ? as_aws_redact(text, secrets, secrets_size) : NULL;
	as_aws_free_secret(text);

	if (!redacted) {
		return as_error_set_message(err, code,
			"AWS EC2 seed provider: failed to allocate an error message");
	}

	as_error_update(err, code, "%s", redacted);
	as_aws_free_secret(redacted);
	return code;
}

//---------------------------------
// Static Functions: XML
//---------------------------------

static bool
xml_fail(xml_parser* p, const char* reason)
{
	snprintf(p->reason, p->reason_size, "%s at offset %zu", reason, p->pos);
	return false;
}

static bool
xml_starts(const xml_parser* p, const char* prefix)
{
	size_t len = strlen(prefix);
	return p->size - p->pos >= len && memcmp(p->data + p->pos, prefix, len) == 0;
}

static bool
xml_is_space(uint8_t c)
{
	return c == ' ' || c == '\t' || c == '\r' || c == '\n';
}

static bool
xml_name_start(uint8_t c)
{
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_' || c == ':' || c >= 0x80;
}

static bool
xml_name_char(uint8_t c)
{
	return xml_name_start(c) || (c >= '0' && c <= '9') || c == '-' || c == '.';
}

static void
xml_skip_space(xml_parser* p)
{
	while (p->pos < p->size && xml_is_space(p->data[p->pos])) {
		p->pos++;
	}
}

// Skip to just after terminator. Returns false when the document ends first.
static bool
xml_skip_past(xml_parser* p, const char* terminator)
{
	size_t len = strlen(terminator);

	while (p->size - p->pos >= len) {
		if (memcmp(p->data + p->pos, terminator, len) == 0) {
			p->pos += len;
			return true;
		}
		p->pos++;
	}
	p->pos = p->size;
	return xml_fail(p, "unexpected end of document");
}

static bool
xml_read_name(xml_parser* p, size_t* start, size_t* len)
{
	if (p->pos >= p->size) {
		return xml_fail(p, "unexpected end of document");
	}

	if (!xml_name_start(p->data[p->pos])) {
		return xml_fail(p, "invalid element or attribute name");
	}

	*start = p->pos;

	while (p->pos < p->size && xml_name_char(p->data[p->pos])) {
		p->pos++;
	}
	*len = p->pos - *start;
	return true;
}

// Append raw text with XML line-end normalization (CR LF and lone CR become LF).
static bool
xml_append_raw(xml_parser* p, const uint8_t* s, size_t len)
{
	as_aws_buf* text = &p->doc->text;
	size_t begin = 0;

	for (size_t i = 0; i < len; i++) {
		if (s[i] != '\r') {
			continue;
		}
		as_aws_buf_append(text, s + begin, i - begin);
		as_aws_buf_append(text, "\n", 1);

		if (i + 1 < len && s[i + 1] == '\n') {
			i++;
		}
		begin = i + 1;
	}
	as_aws_buf_append(text, s + begin, len - begin);

	if (!as_aws_buf_ok(text)) {
		return xml_fail(p, "out of memory");
	}
	return true;
}

static bool
xml_valid_char(uint32_t c)
{
	return c == 0x9 || c == 0xA || c == 0xD || (c >= 0x20 && c <= 0xD7FF) ||
		(c >= 0xE000 && c <= 0xFFFD) || (c >= 0x10000 && c <= 0x10FFFF);
}

static bool
xml_append_code_point(xml_parser* p, uint32_t c)
{
	uint8_t out[4];
	size_t len;

	if (c < 0x80) {
		out[0] = (uint8_t)c;
		len = 1;
	}
	else if (c < 0x800) {
		out[0] = (uint8_t)(0xC0 | (c >> 6));
		out[1] = (uint8_t)(0x80 | (c & 0x3F));
		len = 2;
	}
	else if (c < 0x10000) {
		out[0] = (uint8_t)(0xE0 | (c >> 12));
		out[1] = (uint8_t)(0x80 | ((c >> 6) & 0x3F));
		out[2] = (uint8_t)(0x80 | (c & 0x3F));
		len = 3;
	}
	else {
		out[0] = (uint8_t)(0xF0 | (c >> 18));
		out[1] = (uint8_t)(0x80 | ((c >> 12) & 0x3F));
		out[2] = (uint8_t)(0x80 | ((c >> 6) & 0x3F));
		out[3] = (uint8_t)(0x80 | (c & 0x3F));
		len = 4;
	}
	return xml_append_raw(p, out, len);
}

// Decode the reference at p->pos ('&').
static bool
xml_reference(xml_parser* p)
{
	size_t start = p->pos + 1;
	size_t end = start;

	while (end < p->size && end - start <= 10 && p->data[end] != ';') {
		end++;
	}

	if (end >= p->size) {
		return xml_fail(p, "unexpected end of document");
	}

	if (p->data[end] != ';') {
		return xml_fail(p, "invalid reference");
	}

	const char* ref = (const char*)p->data + start;
	size_t len = end - start;

	static const struct {
		const char* name;
		char value;
	} predefined[] = {{"lt", '<'}, {"gt", '>'}, {"amp", '&'}, {"apos", '\''}, {"quot", '"'}};

	for (size_t i = 0; i < sizeof(predefined) / sizeof(predefined[0]); i++) {
		if (len == strlen(predefined[i].name) && memcmp(ref, predefined[i].name, len) == 0) {
			p->pos = end + 1;
			return xml_append_raw(p, (const uint8_t*)&predefined[i].value, 1);
		}
	}

	if (len == 0 || ref[0] != '#') {
		return xml_fail(p, "undefined entity reference");
	}

	bool hex = len > 1 && ref[1] == 'x';
	size_t i = hex ? 2 : 1;
	uint32_t c = 0;

	if (i == len) {
		return xml_fail(p, "invalid character reference");
	}

	for (; i < len; i++) {
		char d = ref[i];
		uint32_t v;

		if (d >= '0' && d <= '9') {
			v = (uint32_t)(d - '0');
		}
		else if (hex && d >= 'a' && d <= 'f') {
			v = (uint32_t)(d - 'a' + 10);
		}
		else if (hex && d >= 'A' && d <= 'F') {
			v = (uint32_t)(d - 'A' + 10);
		}
		else {
			return xml_fail(p, "invalid character reference");
		}

		c = c * (hex ? 16 : 10) + v;

		if (c > 0x10FFFF) {
			return xml_fail(p, "invalid character reference");
		}
	}

	if (!xml_valid_char(c)) {
		return xml_fail(p, "invalid character reference");
	}
	p->pos = end + 1;
	return xml_append_code_point(p, c);
}

static bool
xml_text(xml_parser* p)
{
	size_t begin = p->pos;

	if (p->depth == 0) {
		while (p->pos < p->size && p->data[p->pos] != '<') {
			if (!xml_is_space(p->data[p->pos])) {
				return xml_fail(p, "content outside the root element");
			}
			p->pos++;
		}
		return true;
	}

	while (p->pos < p->size && p->data[p->pos] != '<') {
		if (p->data[p->pos] == '&') {
			if (!xml_append_raw(p, p->data + begin, p->pos - begin) || !xml_reference(p)) {
				return false;
			}
			begin = p->pos;
			continue;
		}
		p->pos++;
	}
	return xml_append_raw(p, p->data + begin, p->pos - begin);
}

static uint32_t
xml_text_size(const xml_parser* p)
{
	return (uint32_t)p->doc->text.size;
}

static bool
xml_start_tag(xml_parser* p)
{
	if (p->depth == 0 && p->root_done) {
		return xml_fail(p, "content outside the root element");
	}

	if (p->depth >= XML_MAX_DEPTH) {
		return xml_fail(p, "nesting deeper than 64 elements");
	}

	as_aws_xml* doc = p->doc;

	if (doc->nodes_size >= XML_MAX_NODES) {
		return xml_fail(p, "more than 1000000 elements");
	}

	p->pos++;

	size_t name_start;
	size_t name_len;

	if (!xml_read_name(p, &name_start, &name_len)) {
		return false;
	}

	if (doc->nodes_size == doc->nodes_capacity) {
		uint32_t capacity = doc->nodes_capacity ? doc->nodes_capacity * 2 : 64;

		if (capacity > XML_MAX_NODES) {
			capacity = XML_MAX_NODES;
		}

		as_aws_xml_node* nodes = cf_realloc(doc->nodes, capacity * sizeof(as_aws_xml_node));

		if (!nodes) {
			return xml_fail(p, "out of memory");
		}
		doc->nodes = nodes;
		doc->nodes_capacity = capacity;
	}

	uint32_t index = doc->nodes_size++;
	as_aws_xml_node* node = &doc->nodes[index];
	memset(node, 0, sizeof(as_aws_xml_node));
	node->name = (uint32_t)doc->names.size;
	node->text_start = xml_text_size(p);

	as_aws_buf_append(&doc->names, p->data + name_start, name_len);
	as_aws_buf_append(&doc->names, "", 1);

	if (!as_aws_buf_ok(&doc->names)) {
		return xml_fail(p, "out of memory");
	}

	if (p->depth > 0) {
		uint32_t prev = p->last[p->depth];

		if (prev != XML_NONE) {
			doc->nodes[prev - 1].next = index + 1;
		}
		else {
			doc->nodes[p->stack[p->depth - 1]].first_child = index + 1;
		}
	}
	p->last[p->depth] = index + 1;

	// Attributes are skipped; quoted values may contain '>'.
	while (true) {
		xml_skip_space(p);

		if (p->pos >= p->size) {
			return xml_fail(p, "unexpected end of document");
		}

		uint8_t c = p->data[p->pos];

		if (c == '>') {
			p->pos++;
			p->stack[p->depth++] = index;
			p->last[p->depth] = XML_NONE;
			return true;
		}

		if (c == '/') {
			p->pos++;

			if (p->pos >= p->size) {
				return xml_fail(p, "unexpected end of document");
			}

			if (p->data[p->pos] != '>') {
				return xml_fail(p, "invalid empty element tag");
			}
			p->pos++;
			node->text_end = node->text_start;
			node->end = index + 1;

			if (p->depth == 0) {
				p->root_done = true;
			}
			return true;
		}

		size_t attr_start;
		size_t attr_len;

		if (!xml_is_space(p->data[p->pos - 1])) {
			return xml_fail(p, "invalid attribute");
		}

		if (!xml_read_name(p, &attr_start, &attr_len)) {
			return false;
		}
		xml_skip_space(p);

		if (p->pos >= p->size) {
			return xml_fail(p, "unexpected end of document");
		}

		if (p->data[p->pos] != '=') {
			return xml_fail(p, "invalid attribute");
		}
		p->pos++;
		xml_skip_space(p);

		if (p->pos >= p->size) {
			return xml_fail(p, "unexpected end of document");
		}

		uint8_t quote = p->data[p->pos];

		if (quote != '"' && quote != '\'') {
			return xml_fail(p, "invalid attribute");
		}
		p->pos++;

		while (p->pos < p->size && p->data[p->pos] != quote) {
			if (p->data[p->pos] == '<') {
				return xml_fail(p, "invalid attribute");
			}
			p->pos++;
		}

		if (p->pos >= p->size) {
			return xml_fail(p, "unexpected end of document");
		}
		p->pos++;
	}
}

static bool
xml_end_tag(xml_parser* p)
{
	if (p->depth == 0) {
		return xml_fail(p, "unexpected end tag");
	}

	size_t tag = p->pos;
	p->pos += 2;

	size_t name_start;
	size_t name_len;

	if (!xml_read_name(p, &name_start, &name_len)) {
		return false;
	}
	xml_skip_space(p);

	if (p->pos >= p->size) {
		return xml_fail(p, "unexpected end of document");
	}

	if (p->data[p->pos] != '>') {
		return xml_fail(p, "invalid end tag");
	}

	uint32_t index = p->stack[p->depth - 1];
	as_aws_xml_node* node = &p->doc->nodes[index];
	const char* open = p->doc->names.data + node->name;

	if (strlen(open) != name_len || memcmp(open, p->data + name_start, name_len) != 0) {
		p->pos = tag;
		return xml_fail(p, "mismatched end tag");
	}

	p->pos++;
	node->text_end = xml_text_size(p);
	node->end = p->doc->nodes_size;
	p->depth--;

	if (p->depth == 0) {
		p->root_done = true;
	}
	return true;
}

static bool
xml_markup(xml_parser* p)
{
	if (xml_starts(p, "</")) {
		return xml_end_tag(p);
	}

	if (xml_starts(p, "<!--")) {
		p->pos += 4;
		return xml_skip_past(p, "-->");
	}

	if (xml_starts(p, "<?")) {
		p->pos += 2;
		return xml_skip_past(p, "?>");
	}

	if (xml_starts(p, "<![CDATA[")) {
		if (p->depth == 0) {
			return xml_fail(p, "content outside the root element");
		}

		p->pos += 9;
		size_t begin = p->pos;

		if (!xml_skip_past(p, "]]>")) {
			return false;
		}
		return xml_append_raw(p, p->data + begin, p->pos - 3 - begin);
	}

	if (xml_starts(p, "<!DOCTYPE")) {
		return xml_fail(p, "DOCTYPE is not allowed");
	}

	if (xml_starts(p, "<!ENTITY")) {
		return xml_fail(p, "ENTITY is not allowed");
	}

	if (xml_starts(p, "<!")) {
		return xml_fail(p, "invalid markup");
	}
	return xml_start_tag(p);
}

static bool
xml_parse(xml_parser* p)
{
	if (p->size >= 3 && memcmp(p->data, "\xEF\xBB\xBF", 3) == 0) {
		p->pos = 3;
	}

	const uint8_t* nul = memchr(p->data, 0, p->size);

	if (nul) {
		p->pos = (size_t)(nul - p->data);
		return xml_fail(p, "NUL character");
	}

	while (p->pos < p->size) {
		bool ok = p->data[p->pos] == '<' ? xml_markup(p) : xml_text(p);

		if (!ok) {
			return false;
		}
	}

	if (p->depth > 0) {
		return xml_fail(p, "unexpected end of document");
	}

	if (!p->root_done) {
		return xml_fail(p, "no root element");
	}
	return true;
}

static const char*
xml_node_name(const as_aws_xml_node* node)
{
	return node->doc->names.data + node->name;
}

//---------------------------------
// Functions: XML
//---------------------------------

as_aws_xml*
as_aws_xml_parse(const uint8_t* data, size_t size, char* reason, size_t reason_size)
{
	reason[0] = 0;

	if (size > UINT32_MAX / 2) {
		snprintf(reason, reason_size, "document too large");
		return NULL;
	}

	as_aws_xml* doc = cf_calloc(1, sizeof(as_aws_xml));

	if (!doc) {
		snprintf(reason, reason_size, "out of memory");
		return NULL;
	}

	xml_parser p = {
		.data = data ? data : (const uint8_t*)"",
		.size = data ? size : 0,
		.doc = doc,
		.reason = reason,
		.reason_size = reason_size
	};

	if (!xml_parse(&p) || !as_aws_buf_append(&doc->text, "", 0)) {
		if (!reason[0]) {
			snprintf(reason, reason_size, "out of memory");
		}
		as_aws_xml_destroy(doc);
		return NULL;
	}

	for (uint32_t i = 0; i < doc->nodes_size; i++) {
		doc->nodes[i].doc = doc;
	}
	return doc;
}

void
as_aws_xml_destroy(as_aws_xml* doc)
{
	if (!doc) {
		return;
	}

	for (uint32_t i = 0; i < doc->nodes_size; i++) {
		as_aws_free_secret(doc->nodes[i].content);
	}
	cf_free(doc->nodes);
	as_aws_buf_destroy(&doc->names);
	as_aws_buf_destroy(&doc->text);
	cf_free(doc);
}

const as_aws_xml_node*
as_aws_xml_root(const as_aws_xml* doc)
{
	return doc && doc->nodes_size ? &doc->nodes[0] : NULL;
}

const char*
as_aws_xml_name(const as_aws_xml_node* node)
{
	return xml_node_name(node);
}

const as_aws_xml_node*
as_aws_xml_child(const as_aws_xml_node* parent, const char* name)
{
	if (!parent) {
		return NULL;
	}

	const as_aws_xml_node* nodes = parent->doc->nodes;

	for (uint32_t i = parent->first_child; i != XML_NONE; i = nodes[i - 1].next) {
		if (strcmp(xml_node_name(&nodes[i - 1]), name) == 0) {
			return &nodes[i - 1];
		}
	}
	return NULL;
}

const as_aws_xml_node*
as_aws_xml_next(const as_aws_xml_node* node)
{
	if (!node) {
		return NULL;
	}

	const as_aws_xml_node* nodes = node->doc->nodes;
	const char* name = xml_node_name(node);

	for (uint32_t i = node->next; i != XML_NONE; i = nodes[i - 1].next) {
		if (strcmp(xml_node_name(&nodes[i - 1]), name) == 0) {
			return &nodes[i - 1];
		}
	}
	return NULL;
}

const char*
as_aws_xml_content(const as_aws_xml_node* node)
{
	if (node->content) {
		return node->content;
	}

	const char* text = node->doc->text.data;
	uint32_t start = node->text_start;
	uint32_t end = node->text_end;

	// Java String.trim() removes every character up to and including the space.
	while (start < end && (unsigned char)text[start] <= ' ') {
		start++;
	}

	while (end > start && (unsigned char)text[end - 1] <= ' ') {
		end--;
	}

	char* content = cf_malloc(end - start + 1);

	if (!content) {
		node->doc->failed = true;
		return NULL;
	}

	memcpy(content, text + start, end - start);
	content[end - start] = 0;
	((as_aws_xml_node*)node)->content = content;
	return content;
}

bool
as_aws_xml_failed(const as_aws_xml* doc)
{
	return doc->failed;
}

const char*
as_aws_xml_text(const as_aws_xml_node* parent, const char* name)
{
	const as_aws_xml_node* child = as_aws_xml_child(parent, name);
	return child ? as_aws_xml_content(child) : NULL;
}

const char*
as_aws_xml_find(const as_aws_xml_node* node, const char* name)
{
	if (!node) {
		return NULL;
	}

	const as_aws_xml_node* nodes = node->doc->nodes;
	uint32_t index = (uint32_t)(node - nodes);

	for (uint32_t i = index + 1; i < node->end; i++) {
		if (strcmp(xml_node_name(&nodes[i]), name) == 0) {
			return as_aws_xml_content(&nodes[i]);
		}
	}
	return NULL;
}

as_status
as_aws_xml_error(as_error* err, const char* action, int status, const uint8_t* body,
	size_t body_size, const char* header_request_id, const char* const* secrets,
	uint32_t secrets_size)
{
	char reason[128];
	as_aws_xml* doc = body_size ? as_aws_xml_parse(body, body_size, reason, sizeof(reason)) : NULL;
	const as_aws_xml_node* root = as_aws_xml_root(doc);
	const char* code = as_aws_xml_find(root, "Code");
	const char* message = as_aws_xml_find(root, "Message");
	const char* request_id = as_aws_xml_find(root, "RequestID");

	if (!request_id) {
		request_id = as_aws_xml_find(root, "RequestId");
	}

	if (!request_id && header_request_id && header_request_id[0]) {
		request_id = header_request_id;
	}

	as_aws_buf buf;
	as_aws_buf_init(&buf);
	as_aws_buf_printf(&buf, "AWS EC2 seed provider: %s failed: HTTP %d", action, status);

	if (code) {
		as_aws_buf_printf(&buf, " %s", code);
	}

	if (message) {
		as_aws_buf_printf(&buf, ": %s", message);
	}

	if (request_id) {
		as_aws_buf_printf(&buf, " (request id %s)", request_id);
	}

	char* text = as_aws_buf_detach(&buf);
	as_aws_xml_destroy(doc);

	if (!text) {
		return as_error_set_message(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED,
			"AWS EC2 seed provider: failed to allocate an error message");
	}

	as_status rv = as_aws_errorf(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED, secrets, secrets_size,
		"%s", text);
	as_aws_free_secret(text);
	return rv;
}

//---------------------------------
// Static Functions: JSON
//---------------------------------

static bool
json_is_space(uint8_t c)
{
	return c == ' ' || c == '\t' || c == '\r' || c == '\n';
}

// Skip whitespace and return the next byte without consuming it. False at the end of the input.
static bool
json_peek(json_parser* p, uint8_t* c)
{
	while (p->pos < p->size && json_is_space(p->data[p->pos])) {
		p->pos++;
	}

	if (p->pos >= p->size) {
		return false;
	}
	*c = p->data[p->pos];
	return true;
}

static bool
json_hex4(json_parser* p, uint32_t* out)
{
	if (p->size - p->pos < 4) {
		return false;
	}

	uint32_t v = 0;

	for (int i = 0; i < 4; i++) {
		uint8_t d = p->data[p->pos++];

		if (d >= '0' && d <= '9') {
			v = v * 16 + (uint32_t)(d - '0');
		}
		else if (d >= 'a' && d <= 'f') {
			v = v * 16 + (uint32_t)(d - 'a' + 10);
		}
		else if (d >= 'A' && d <= 'F') {
			v = v * 16 + (uint32_t)(d - 'A' + 10);
		}
		else {
			return false;
		}
	}
	*out = v;
	return true;
}

static void
json_append_code_point(as_aws_buf* buf, uint32_t c)
{
	uint8_t out[4];
	size_t len;

	if (c < 0x80) {
		out[0] = (uint8_t)c;
		len = 1;
	}
	else if (c < 0x800) {
		out[0] = (uint8_t)(0xC0 | (c >> 6));
		out[1] = (uint8_t)(0x80 | (c & 0x3F));
		len = 2;
	}
	else if (c < 0x10000) {
		out[0] = (uint8_t)(0xE0 | (c >> 12));
		out[1] = (uint8_t)(0x80 | ((c >> 6) & 0x3F));
		out[2] = (uint8_t)(0x80 | (c & 0x3F));
		len = 3;
	}
	else {
		out[0] = (uint8_t)(0xF0 | (c >> 18));
		out[1] = (uint8_t)(0x80 | ((c >> 12) & 0x3F));
		out[2] = (uint8_t)(0x80 | ((c >> 6) & 0x3F));
		out[3] = (uint8_t)(0x80 | (c & 0x3F));
		len = 4;
	}
	as_aws_buf_append(buf, out, len);
}

// Read the string at p->pos (a quote) into out, or skip it when out is NULL.
static bool
json_string(json_parser* p, as_aws_buf* out)
{
	p->pos++;

	while (true) {
		if (p->pos >= p->size) {
			return false;
		}

		uint8_t c = p->data[p->pos++];

		if (c == '"') {
			return true;
		}

		if (c != '\\') {
			if (out) {
				as_aws_buf_append(out, &c, 1);
			}
			continue;
		}

		if (p->pos >= p->size) {
			return false;
		}

		c = p->data[p->pos++];
		uint8_t e;

		switch (c) {
		case '"':
		case '\\':
		case '/':
			e = c;
			break;
		case 'b':
			e = '\b';
			break;
		case 'f':
			e = '\f';
			break;
		case 'n':
			e = '\n';
			break;
		case 'r':
			e = '\r';
			break;
		case 't':
			e = '\t';
			break;
		case 'u': {
			uint32_t cp;

			if (!json_hex4(p, &cp) || cp == 0 || (cp >= 0xDC00 && cp <= 0xDFFF)) {
				return false;
			}

			if (cp >= 0xD800 && cp <= 0xDBFF) {
				uint32_t low;

				if (p->size - p->pos < 2 || p->data[p->pos] != '\\' ||
					p->data[p->pos + 1] != 'u') {
					return false;
				}
				p->pos += 2;

				if (!json_hex4(p, &low) || low < 0xDC00 || low > 0xDFFF) {
					return false;
				}
				cp = 0x10000 + ((cp - 0xD800) << 10) + (low - 0xDC00);
			}

			if (out) {
				json_append_code_point(out, cp);
			}
			continue;
		}
		default:
			return false;
		}

		if (out) {
			as_aws_buf_append(out, &e, 1);
		}
	}
}

// Skip the object or array at p->pos.
static bool
json_skip(json_parser* p)
{
	uint32_t depth = 0;

	do {
		uint8_t c;

		if (!json_peek(p, &c)) {
			return false;
		}

		if (c == '"') {
			if (!json_string(p, NULL)) {
				return false;
			}
			continue;
		}

		if (c == '{' || c == '[') {
			if (++depth > JSON_MAX_DEPTH) {
				return false;
			}
		}
		else if (c == '}' || c == ']') {
			depth--;
		}
		p->pos++;
	} while (depth > 0);
	return true;
}

static bool
json_is_delimiter(uint8_t c)
{
	return c == ',' || c == '}' || c == ']' || json_is_space(c);
}

// -?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?
static bool
json_number(const uint8_t* s, size_t len)
{
	size_t i = 0;

	if (i < len && s[i] == '-') {
		i++;
	}

	if (i >= len || s[i] < '0' || s[i] > '9') {
		return false;
	}

	if (s[i] == '0') {
		i++;
	}
	else {
		while (i < len && s[i] >= '0' && s[i] <= '9') {
			i++;
		}
	}

	if (i < len && s[i] == '.') {
		size_t start = ++i;

		while (i < len && s[i] >= '0' && s[i] <= '9') {
			i++;
		}

		if (i == start) {
			return false;
		}
	}

	if (i < len && (s[i] == 'e' || s[i] == 'E')) {
		i++;

		if (i < len && (s[i] == '+' || s[i] == '-')) {
			i++;
		}

		size_t start = i;

		while (i < len && s[i] >= '0' && s[i] <= '9') {
			i++;
		}

		if (i == start) {
			return false;
		}
	}
	return i == len;
}

static bool
json_literal(const uint8_t* s, size_t len, const char* word)
{
	return len == strlen(word) && memcmp(s, word, len) == 0;
}

// Read a value into *value, which stays NULL for null, an object or an array.
static bool
json_value(json_parser* p, char** value)
{
	*value = NULL;
	uint8_t c;

	if (!json_peek(p, &c)) {
		return false;
	}

	if (c == '"') {
		as_aws_buf buf;
		as_aws_buf_init(&buf);

		if (!json_string(p, &buf)) {
			as_aws_buf_destroy(&buf);
			return false;
		}
		*value = as_aws_buf_detach(&buf);
		return *value != NULL;
	}

	if (c == '{' || c == '[') {
		return json_skip(p);
	}

	const uint8_t* start = p->data + p->pos;

	while (p->pos < p->size && !json_is_delimiter(p->data[p->pos])) {
		p->pos++;
	}

	size_t len = (size_t)(p->data + p->pos - start);

	if (json_literal(start, len, "null")) {
		return true;
	}

	if (!json_literal(start, len, "true") && !json_literal(start, len, "false") &&
		!json_number(start, len)) {
		return false;
	}

	*value = cf_malloc(len + 1);

	if (!*value) {
		return false;
	}
	memcpy(*value, start, len);
	(*value)[len] = 0;
	return true;
}

// Take ownership of key and value. Returns false, releasing neither, on allocation failure.
static bool
json_put(as_aws_json* json, char* key, char* value)
{
	for (uint32_t i = 0; i < json->size; i++) {
		as_aws_json_entry* e = &json->entries[i];

		if (strcmp(e->key, key) == 0) {
			as_aws_free_secret(e->value);
			e->value = value;
			as_aws_free_secret(key);
			return true;
		}
	}

	if (json->size == json->capacity) {
		uint32_t capacity = json->capacity ? json->capacity * 2 : 8;
		as_aws_json_entry* entries = cf_realloc(json->entries,
			capacity * sizeof(as_aws_json_entry));

		if (!entries) {
			return false;
		}
		json->entries = entries;
		json->capacity = capacity;
	}
	json->entries[json->size].key = key;
	json->entries[json->size].value = value;
	json->size++;
	return true;
}

static bool
json_object(json_parser* p, as_aws_json* json)
{
	uint8_t c;

	if (!json_peek(p, &c) || c != '{') {
		return false;
	}
	p->pos++;

	if (!json_peek(p, &c)) {
		return false;
	}

	if (c == '}') {
		p->pos++;
		return true;
	}

	while (true) {
		if (!json_peek(p, &c) || c != '"') {
			return false;
		}

		as_aws_buf buf;
		as_aws_buf_init(&buf);

		if (!json_string(p, &buf)) {
			as_aws_buf_destroy(&buf);
			return false;
		}

		char* key = as_aws_buf_detach(&buf);

		if (!key) {
			return false;
		}

		char* value = NULL;

		if (!json_peek(p, &c) || c != ':') {
			as_aws_free_secret(key);
			return false;
		}
		p->pos++;

		if (!json_value(p, &value)) {
			as_aws_free_secret(key);
			return false;
		}

		if (!value) {
			as_aws_free_secret(key);
		}
		else if (!json_put(json, key, value)) {
			as_aws_free_secret(key);
			as_aws_free_secret(value);
			return false;
		}

		if (!json_peek(p, &c)) {
			return false;
		}
		p->pos++;

		if (c == '}') {
			return true;
		}

		if (c != ',') {
			return false;
		}
	}
}

//---------------------------------
// Static Functions: INI and ISO-8601
//---------------------------------

// Java String.trim(): every byte up to and including the space, at both ends.
static void
ini_trim(const char* text, size_t* start, size_t* end)
{
	while (*start < *end && (unsigned char)text[*start] <= ' ') {
		(*start)++;
	}

	while (*end > *start && (unsigned char)text[*end - 1] <= ' ') {
		(*end)--;
	}
}

static bool
ini_equals(const char* text, size_t start, size_t end, const char* word)
{
	return end - start == strlen(word) && memcmp(text + start, word, end - start) == 0;
}

static bool
ini_set(char** field, const char* text, size_t start, size_t end)
{
	char* value = cf_malloc(end - start + 1);

	if (!value) {
		return false;
	}
	memcpy(value, text + start, end - start);
	value[end - start] = 0;
	as_aws_free_secret(*field);
	*field = value;
	return true;
}

static bool
iso_digits(const char* s, int n, int* out)
{
	int v = 0;

	for (int i = 0; i < n; i++) {
		if (s[i] < '0' || s[i] > '9') {
			return false;
		}
		v = v * 10 + (s[i] - '0');
	}
	*out = v;
	return true;
}

static int
iso_days_in_month(int year, int month)
{
	static const int days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

	if (month == 2 && (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0))) {
		return 29;
	}
	return days[month - 1];
}

// Days from 1970-01-01 to the proleptic Gregorian date (H. Hinnant, days_from_civil).
static int64_t
iso_days_from_civil(int64_t y, int64_t m, int64_t d)
{
	y -= m <= 2;
	int64_t era = (y >= 0 ? y : y - 399) / 400;
	int64_t yoe = y - era * 400;
	int64_t doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
	int64_t doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
	return era * 146097 + doe - 719468;
}

//---------------------------------
// Functions: JSON, INI and ISO-8601
//---------------------------------

bool
as_aws_json_parse(const uint8_t* data, size_t size, as_aws_json* json)
{
	memset(json, 0, sizeof(as_aws_json));

	if (!data) {
		data = (const uint8_t*)"";
		size = 0;
	}

	json_parser p = {.data = data, .size = size, .pos = 0};

	if (memchr(data, 0, size) || !json_object(&p, json)) {
		as_aws_json_destroy(json);
		return false;
	}

	while (p.pos < p.size && json_is_space(p.data[p.pos])) {
		p.pos++;
	}

	if (p.pos != p.size) {
		as_aws_json_destroy(json);
		return false;
	}
	return true;
}

const char*
as_aws_json_get(const as_aws_json* json, const char* key)
{
	for (uint32_t i = 0; i < json->size; i++) {
		if (strcmp(json->entries[i].key, key) == 0) {
			return json->entries[i].value;
		}
	}
	return NULL;
}

void
as_aws_json_destroy(as_aws_json* json)
{
	for (uint32_t i = 0; i < json->size; i++) {
		as_aws_free_secret(json->entries[i].key);
		as_aws_free_secret(json->entries[i].value);
	}
	cf_free(json->entries);
	memset(json, 0, sizeof(as_aws_json));
}

bool
as_aws_ini_profile(const char* text, size_t size, const char* profile, as_aws_profile* out)
{
	memset(out, 0, sizeof(as_aws_profile));

	bool in_profile = false;
	size_t pos = 0;

	if (size >= 3 && memcmp(text, "\xEF\xBB\xBF", 3) == 0) {
		pos = 3;
	}

	while (pos < size) {
		size_t start = pos;

		while (pos < size && text[pos] != '\n' && text[pos] != '\r') {
			pos++;
		}

		size_t end = pos;

		if (pos < size) {
			pos += (text[pos] == '\r' && pos + 1 < size && text[pos + 1] == '\n') ? 2 : 1;
		}
		ini_trim(text, &start, &end);

		if (start == end || text[start] == '#' || text[start] == ';') {
			continue;
		}

		// Any '[' line ends the current section, so a malformed header cannot extend the
		// previous profile.
		if (text[start] == '[') {
			const char* close = memchr(text + start + 1, ']', end - start - 1);
			in_profile = false;

			if (close) {
				size_t name_start = start + 1;
				size_t name_end = (size_t)(close - text);
				size_t rest_start = name_end + 1;
				size_t rest_end = end;
				ini_trim(text, &rest_start, &rest_end);

				if (rest_start == rest_end || text[rest_start] == '#' ||
					text[rest_start] == ';') {
					ini_trim(text, &name_start, &name_end);
					in_profile = ini_equals(text, name_start, name_end, profile);
				}
			}
			continue;
		}

		const char* eq = memchr(text + start, '=', end - start);

		if (!in_profile || !eq || eq == text + start) {
			continue;
		}

		size_t key_start = start;
		size_t key_end = (size_t)(eq - text);
		size_t value_start = key_end + 1;
		size_t value_end = end;
		ini_trim(text, &key_start, &key_end);
		ini_trim(text, &value_start, &value_end);

		char** field = NULL;

		if (ini_equals(text, key_start, key_end, "aws_access_key_id")) {
			field = &out->access_key_id;
		}
		else if (ini_equals(text, key_start, key_end, "aws_secret_access_key")) {
			field = &out->secret_access_key;
		}
		else if (ini_equals(text, key_start, key_end, "aws_session_token")) {
			field = &out->session_token;
		}

		if (field && !ini_set(field, text, value_start, value_end)) {
			as_aws_profile_destroy(out);
			return false;
		}
	}
	return true;
}

void
as_aws_profile_destroy(as_aws_profile* profile)
{
	as_aws_free_secret(profile->access_key_id);
	as_aws_free_secret(profile->secret_access_key);
	as_aws_free_secret(profile->session_token);
	memset(profile, 0, sizeof(as_aws_profile));
}

bool
as_aws_iso8601(const char* text, uint64_t* epoch_ms)
{
	int year, month, day, hour, minute, second;
	const char* p = text;

	if (!p || !iso_digits(p, 4, &year) || p[4] != '-' || !iso_digits(p + 5, 2, &month) ||
		p[7] != '-' || !iso_digits(p + 8, 2, &day) || p[10] != 'T' ||
		!iso_digits(p + 11, 2, &hour) || p[13] != ':' || !iso_digits(p + 14, 2, &minute) ||
		p[16] != ':' || !iso_digits(p + 17, 2, &second)) {
		return false;
	}
	p += 19;

	int64_t ms = 0;

	if (*p == '.') {
		int digits = 0;
		p++;

		while (*p >= '0' && *p <= '9') {
			if (digits < 3) {
				ms = ms * 10 + (*p - '0');
			}
			digits++;
			p++;
		}

		if (digits == 0 || digits > 9) {
			return false;
		}

		for (; digits < 3; digits++) {
			ms *= 10;
		}
	}

	int64_t offset_ms = 0;

	if (*p == 'Z') {
		p++;
	}
	else if (*p == '+' || *p == '-') {
		int sign = *p == '-' ? -1 : 1;
		int offset_hour, offset_minute;

		if (!iso_digits(p + 1, 2, &offset_hour) || p[3] != ':' ||
			!iso_digits(p + 4, 2, &offset_minute) || offset_hour > 18 || offset_minute > 59 ||
			(offset_hour == 18 && offset_minute > 0)) {
			return false;
		}
		offset_ms = sign * ((int64_t)offset_hour * 60 + offset_minute) * 60000;
		p += 6;
	}
	else {
		return false;
	}

	if (*p || month < 1 || month > 12 || day < 1 || day > iso_days_in_month(year, month) ||
		hour > 23 || minute > 59 || second > 59) {
		return false;
	}

	int64_t days = iso_days_from_civil(year, month, day);
	int64_t t = (((days * 24 + hour) * 60 + minute) * 60 + second) * 1000 + ms - offset_ms;

	if (t <= 0) {
		return false;
	}
	*epoch_ms = (uint64_t)t;
	return true;
}
