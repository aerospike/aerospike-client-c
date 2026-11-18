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
#include <aerospike/as_discovery.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_random.h>
#include <aerospike/as_socket.h>
#include <aerospike/as_vector.h>
#include <citrusleaf/alloc.h>
#include <citrusleaf/cf_clock.h>

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <aerospike/as_poll.h>

#if !defined(_MSC_VER)
#include <fcntl.h>
#include <net/if.h>
#else
#include <iphlpapi.h>
#endif

#include "as_dns_internal.h"

//---------------------------------
// Macros
//---------------------------------

#define DNS_STATE_MAGIC 0x444e5331

#define DNS_TYPE_A 1
#define DNS_TYPE_CNAME 5
#define DNS_TYPE_AAAA 28
#define DNS_TYPE_SRV 33
#define DNS_CLASS_IN 1

#define DNS_HEADER_SIZE 12
#define DNS_FLAG_QR 0x8000
#define DNS_FLAG_TC 0x0200
#define DNS_FLAG_RD 0x0100

#define DNS_RCODE_NOERROR 0
#define DNS_RCODE_SERVFAIL 2
#define DNS_RCODE_NXDOMAIN 3
#define DNS_RCODE_REFUSED 5

// Wire limits from RFC 1035: 255 bytes per name, 63 per label.
#define DNS_MAX_WIRE_NAME 255
#define DNS_MAX_LABEL 63

// Text form of a name without the trailing dot, plus the terminator.
#define DNS_NAME_SIZE 256

#define DNS_MAX_QUERY (DNS_HEADER_SIZE + DNS_MAX_WIRE_NAME + 4)

// Without EDNS a resolver answers in at most 512 bytes over UDP; accept more defensively.
#define DNS_UDP_BUFFER 4096

#define DNS_MAX_POINTERS 32
#define DNS_MAX_CNAMES 8
#define DNS_MAX_RECORDS 256

// More CNAME records in one answer than a chain of DNS_MAX_CNAMES hops can use.
#define DNS_MAX_CNAME_RECORDS 32

// No query or resolver attempt starts with less time left, and no share is shorter. An exchange
// is still cut at the refresh deadline.
#define DNS_MIN_SHARE_NS (1000 * 1000)

#define DNS_REASON_SIZE 64

#define DNS_RESOLV_CONF "/etc/resolv.conf"

#if !defined(_MSC_VER)
#define DNS_EINTR EINTR
#if defined(MSG_NOSIGNAL)
#define DNS_SEND_FLAGS MSG_NOSIGNAL
#else
#define DNS_SEND_FLAGS 0
#endif
#else
#define DNS_EINTR WSAEINTR
#endif

//---------------------------------
// Types
//---------------------------------

typedef struct dns_name_s {
	// Configured spelling, used in log and error messages.
	char* name;

	// Lowercase, without the trailing dot.
	char* query;
} dns_name;

typedef struct dns_server_s {
	struct sockaddr_storage addr;
	socklen_t addr_len;
} dns_server;

// Immutable after creation.
typedef struct dns_state_s {
	uint32_t magic;
	dns_name* names;
	uint32_t names_size;
	dns_server* servers;
	uint32_t servers_size;
	as_dns_record_type record_type;
	uint16_t port;
	char* tls_name;
	uint32_t timeout_ms;
} dns_state;

typedef enum {
	DNS_OK,
	DNS_NXDOMAIN,
	DNS_FAILED,
	DNS_TIMEOUT,

	// Transport only: a UDP answer was truncated and the query is retried over TCP.
	DNS_TRUNCATED,

	// Transport only: a datagram that does not answer the query is ignored.
	DNS_MISMATCH
} dns_status;

typedef struct dns_addr_s {
	char owner[DNS_NAME_SIZE];
	char text[INET6_ADDRSTRLEN];
} dns_addr;

typedef struct dns_srv_s {
	char owner[DNS_NAME_SIZE];
	char target[DNS_NAME_SIZE];
	uint16_t priority;
	uint16_t weight;
	uint16_t port;
} dns_srv;

typedef struct dns_cname_s {
	char owner[DNS_NAME_SIZE];
	char target[DNS_NAME_SIZE];
} dns_cname;

// Records of one answer. Only records owned by the queried name or by its CNAME chain are kept.
typedef struct dns_answer_s {
	// Answer records of the queried type: A or AAAA (dns_addr), or SRV (dns_srv).
	as_vector addrs;
	as_vector srvs;

	// Additional section A records (extra_a), for SRV targets.
	as_vector extra_a;

	as_vector cnames;
} dns_answer;

typedef struct dns_query_s {
	const char* name;
	uint16_t type;
	uint16_t id;
	uint32_t size;
	uint8_t packet[DNS_MAX_QUERY];
} dns_query;

typedef struct dns_target_s {
	char target[DNS_NAME_SIZE];
	char text[INET6_ADDRSTRLEN];
	bool resolved;
} dns_target;

// State of one refresh.
typedef struct dns_run_s {
	const dns_state* st;
	uint64_t deadline;

	// Resolver asked first: the last one that answered or returned NXDOMAIN in this refresh.
	uint32_t preferred;
} dns_run;

//---------------------------------
// Static Functions: byte order
//---------------------------------

static inline uint16_t
dns_get16(const uint8_t* p)
{
	return (uint16_t)((p[0] << 8) | p[1]);
}

static inline void
dns_put16(uint8_t* p, uint16_t v)
{
	p[0] = (uint8_t)(v >> 8);
	p[1] = (uint8_t)v;
}

//---------------------------------
// Static Functions: names
//---------------------------------

// Validate a configured name and write its query form: lowercase without the trailing dot.
static bool
dns_normalize_name(const char* name, char* out)
{
	size_t len = strlen(name);

	if (len > 0 && name[len - 1] == '.') {
		len--;
	}

	// The wire form adds one length byte before the first label and the root byte.
	if (len == 0 || len + 2 > DNS_MAX_WIRE_NAME) {
		return false;
	}

	size_t label = 0;

	for (size_t i = 0; i < len; i++) {
		unsigned char c = (unsigned char)name[i];

		if (c == '.') {
			if (label == 0) {
				return false;
			}
			label = 0;
			out[i] = '.';
			continue;
		}

		if (c <= ' ' || c >= 0x7f || c == '\\' || ++label > DNS_MAX_LABEL) {
			return false;
		}
		out[i] = (char)tolower(c);
	}
	out[len] = 0;
	return label > 0;
}

// Encode a valid query-form name. Returns the wire size.
static uint32_t
dns_encode_name(const char* name, uint8_t* out)
{
	uint32_t pos = 0;
	const char* p = name;

	while (*p) {
		const char* dot = strchr(p, '.');
		size_t len = dot ? (size_t)(dot - p) : strlen(p);

		out[pos++] = (uint8_t)len;
		memcpy(out + pos, p, len);
		pos += (uint32_t)len;
		p += len;

		if (*p == '.') {
			p++;
		}
	}
	out[pos++] = 0;
	return pos;
}

// Decode the possibly compressed name at *pos into query form ("" for the root) and advance *pos
// past it. Every compression pointer must point before the start of the segment that contains
// it, so segments strictly decrease and cannot loop.
static bool
dns_read_name(const uint8_t* p, size_t size, size_t* pos, char* out)
{
	size_t cur = *pos;
	size_t limit = cur;
	size_t end = 0;
	bool jumped = false;
	uint32_t pointers = 0;
	size_t wire = 1;
	size_t len = 0;

	while (true) {
		if (cur >= size) {
			return false;
		}

		uint8_t b = p[cur];

		if ((b & 0xc0) == 0xc0) {
			if (cur + 1 >= size) {
				return false;
			}

			size_t target = ((size_t)(b & 0x3f) << 8) | p[cur + 1];

			if (target >= limit || ++pointers > DNS_MAX_POINTERS) {
				return false;
			}

			if (!jumped) {
				end = cur + 2;
				jumped = true;
			}
			cur = target;
			limit = target;
			continue;
		}

		if (b & 0xc0) {
			// Reserved label types.
			return false;
		}

		if (b == 0) {
			if (!jumped) {
				end = cur + 1;
			}
			break;
		}

		if (cur + 1 + b > size) {
			return false;
		}

		wire += (size_t)b + 1;

		if (wire > DNS_MAX_WIRE_NAME) {
			return false;
		}

		if (len) {
			out[len++] = '.';
		}

		for (size_t i = 0; i < b; i++) {
			unsigned char c = p[cur + 1 + i];

			if (c <= ' ' || c >= 0x7f || c == '.' || c == '\\') {
				return false;
			}
			out[len++] = (char)tolower(c);
		}
		cur += (size_t)b + 1;
	}
	out[len] = 0;
	*pos = end;
	return true;
}

//---------------------------------
// Static Functions: resolver addresses
//---------------------------------

static bool
dns_parse_port(const char* text, uint16_t* port)
{
	if (!*text) {
		return false;
	}

	uint32_t value = 0;

	for (const char* p = text; *p; p++) {
		if (!isdigit((unsigned char)*p)) {
			return false;
		}

		value = value * 10 + (uint32_t)(*p - '0');

		if (value > 65535) {
			return false;
		}
	}

	if (value == 0) {
		return false;
	}
	*port = (uint16_t)value;
	return true;
}

// Fill server from a numeric IPv4 or IPv6 address. An IPv6 zone ("%eth0") is accepted only when
// allow_zone is set. No name resolution is done.
static bool
dns_parse_ip(const char* host, uint16_t port, bool allow_zone, dns_server* server)
{
	memset(server, 0, sizeof(dns_server));

	if (!strchr(host, ':')) {
		struct sockaddr_in* sa = (struct sockaddr_in*)&server->addr;

		if (inet_pton(AF_INET, host, &sa->sin_addr) != 1) {
			return false;
		}
		sa->sin_family = AF_INET;
		sa->sin_port = htons(port);
		server->addr_len = sizeof(struct sockaddr_in);
		return true;
	}

	char addr[INET6_ADDRSTRLEN];
	const char* zone = strchr(host, '%');
	size_t len = zone ? (size_t)(zone - host) : strlen(host);

	if (len == 0 || len >= sizeof(addr) || (zone && (!allow_zone || !zone[1]))) {
		return false;
	}
	memcpy(addr, host, len);
	addr[len] = 0;

	struct sockaddr_in6* sa = (struct sockaddr_in6*)&server->addr;

	if (inet_pton(AF_INET6, addr, &sa->sin6_addr) != 1) {
		return false;
	}

	if (zone) {
#if !defined(_MSC_VER)
		unsigned int index = if_nametoindex(zone + 1);

		if (index == 0) {
			char* last = NULL;
			unsigned long value = strtoul(zone + 1, &last, 10);

			if (*last || value == 0 || value > UINT32_MAX) {
				return false;
			}
			index = (unsigned int)value;
		}
		sa->sin6_scope_id = index;
#else
		return false;
#endif
	}
	sa->sin6_family = AF_INET6;
	sa->sin6_port = htons(port);
	server->addr_len = sizeof(struct sockaddr_in6);
	return true;
}

// Parse "ip[:port]", "ipv6", "[ipv6]" or "[ipv6]:port". Returns NULL on success or a reason, which
// is empty when the value is malformed as a whole.
static const char*
dns_parse_server(const char* text, dns_server* server)
{
	char host[INET6_ADDRSTRLEN];
	const char* port_text = NULL;
	size_t len;

	if (text[0] == '[') {
		const char* close = strchr(text, ']');

		if (!close || (close[1] && close[1] != ':')) {
			return "";
		}
		len = (size_t)(close - text - 1);

		if (len >= sizeof(host)) {
			return ": expected an IP address";
		}
		memcpy(host, text + 1, len);
		host[len] = 0;

		// Brackets are only valid around an IPv6 address.
		if (!strchr(host, ':')) {
			return ": expected an IPv6 address in brackets";
		}

		if (close[1] == ':') {
			port_text = close + 2;
		}
	}
	else {
		const char* colon = strchr(text, ':');

		// More than one colon is an IPv6 address without a port.
		len = (colon && !strchr(colon + 1, ':')) ? (size_t)(colon - text) : strlen(text);

		if (len >= sizeof(host)) {
			return ": expected an IP address";
		}
		memcpy(host, text, len);
		host[len] = 0;

		if (len < strlen(text)) {
			port_text = text + len + 1;
		}
	}

	uint16_t port = AS_DNS_DEFAULT_SERVER_PORT;

	if (port_text && !dns_parse_port(port_text, &port)) {
		return ": invalid port";
	}

	if (!dns_parse_ip(host, port, false, server)) {
		return ": expected an IP address";
	}
	return NULL;
}

static bool
dns_server_equal(const dns_server* a, const dns_server* b)
{
	if (a->addr.ss_family != b->addr.ss_family) {
		return false;
	}

	if (a->addr.ss_family == AF_INET) {
		const struct sockaddr_in* x = (const struct sockaddr_in*)&a->addr;
		const struct sockaddr_in* y = (const struct sockaddr_in*)&b->addr;
		return x->sin_port == y->sin_port &&
			memcmp(&x->sin_addr, &y->sin_addr, sizeof(x->sin_addr)) == 0;
	}

	const struct sockaddr_in6* x = (const struct sockaddr_in6*)&a->addr;
	const struct sockaddr_in6* y = (const struct sockaddr_in6*)&b->addr;
	return x->sin6_port == y->sin6_port && x->sin6_scope_id == y->sin6_scope_id &&
		memcmp(&x->sin6_addr, &y->sin6_addr, sizeof(x->sin6_addr)) == 0;
}

// Append server unless an equal address and port is already present.
static void
dns_servers_add(as_vector* servers, const dns_server* server)
{
	for (uint32_t i = 0; i < servers->size; i++) {
		if (dns_server_equal(as_vector_get(servers, i), server)) {
			return;
		}
	}
	as_vector_append(servers, (void*)server);
}

#if !defined(_MSC_VER)

static as_status
dns_system_servers(as_error* err, as_vector* servers)
{
	FILE* f = fopen(DNS_RESOLV_CONF, "r");

	if (!f) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
			"DNS seed provider: failed to read system DNS servers from %s (errno %d). Set servers",
			DNS_RESOLV_CONF, errno);
	}

	char line[512];

	while (fgets(line, sizeof(line), f)) {
		char* p = line;

		while (*p == ' ' || *p == '\t') {
			p++;
		}

		if (strncmp(p, "nameserver", 10) != 0 || (p[10] != ' ' && p[10] != '\t')) {
			continue;
		}
		p += 10;

		while (*p == ' ' || *p == '\t') {
			p++;
		}

		char* end = p;

		while (*end && !isspace((unsigned char)*end) && *end != '#' && *end != ';') {
			end++;
		}
		*end = 0;

		dns_server server;

		if (dns_parse_ip(p, AS_DNS_DEFAULT_SERVER_PORT, true, &server)) {
			dns_servers_add(servers, &server);
		}
	}
	fclose(f);

	if (servers->size == 0) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
			"DNS seed provider: no usable system DNS server in %s. Set servers", DNS_RESOLV_CONF);
	}
	return AEROSPIKE_OK;
}

#else // _MSC_VER

// Windows reports these deprecated site-local addresses when no IPv6 resolver is configured.
static bool
dns_is_default_site_local(const struct sockaddr_in6* sa)
{
	static const uint8_t prefix[8] = {0xfe, 0xc0, 0, 0, 0, 0, 0xff, 0xff};
	return memcmp(&sa->sin6_addr, prefix, sizeof(prefix)) == 0;
}

static as_status
dns_system_servers(as_error* err, as_vector* servers)
{
	ULONG flags = GAA_FLAG_SKIP_UNICAST | GAA_FLAG_SKIP_ANYCAST | GAA_FLAG_SKIP_MULTICAST |
		GAA_FLAG_SKIP_FRIENDLY_NAME;
	ULONG size = 16 * 1024;
	IP_ADAPTER_ADDRESSES* list = NULL;
	ULONG rv = ERROR_BUFFER_OVERFLOW;

	for (int i = 0; i < 3 && rv == ERROR_BUFFER_OVERFLOW; i++) {
		cf_free(list);
		list = cf_malloc(size);

		if (!list) {
			return as_error_set_message(err, AEROSPIKE_ERR_CLIENT,
				"DNS seed provider: failed to allocate the adapter list");
		}
		rv = GetAdaptersAddresses(AF_UNSPEC, flags, NULL, list, &size);
	}

	if (rv != NO_ERROR && rv != ERROR_NO_DATA) {
		cf_free(list);
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
			"DNS seed provider: failed to read system DNS servers (error %lu). Set servers",
			(unsigned long)rv);
	}

	for (IP_ADAPTER_ADDRESSES* a = (rv == NO_ERROR) ? list : NULL; a; a = a->Next) {
		if (a->OperStatus != IfOperStatusUp) {
			continue;
		}

		for (IP_ADAPTER_DNS_SERVER_ADDRESS* d = a->FirstDnsServerAddress; d; d = d->Next) {
			const struct sockaddr* sa = d->Address.lpSockaddr;
			int len = d->Address.iSockaddrLength;
			dns_server server;
			memset(&server, 0, sizeof(server));

			if (!sa) {
				continue;
			}

			if (sa->sa_family == AF_INET && len >= (int)sizeof(struct sockaddr_in)) {
				memcpy(&server.addr, sa, sizeof(struct sockaddr_in));
				((struct sockaddr_in*)&server.addr)->sin_port = htons(AS_DNS_DEFAULT_SERVER_PORT);
				server.addr_len = sizeof(struct sockaddr_in);
			}
			else if (sa->sa_family == AF_INET6 && len >= (int)sizeof(struct sockaddr_in6)) {
				struct sockaddr_in6* sa6 = (struct sockaddr_in6*)&server.addr;
				memcpy(sa6, sa, sizeof(struct sockaddr_in6));

				if (dns_is_default_site_local(sa6)) {
					continue;
				}
				sa6->sin6_port = htons(AS_DNS_DEFAULT_SERVER_PORT);
				server.addr_len = sizeof(struct sockaddr_in6);
			}
			else {
				continue;
			}
			dns_servers_add(servers, &server);
		}
	}
	cf_free(list);

	if (servers->size == 0) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
			"DNS seed provider: no usable system DNS server on an active network adapter. "
			"Set servers");
	}
	return AEROSPIKE_OK;
}

#endif // _MSC_VER

//---------------------------------
// Static Functions: sockets
//---------------------------------

static inline bool
dns_would_block(int e)
{
#if !defined(_MSC_VER)
	return e == EAGAIN || e == EWOULDBLOCK || e == EINPROGRESS;
#else
	return e == WSAEWOULDBLOCK || e == WSAEINPROGRESS;
#endif
}

static bool
dns_socket(int family, int type, as_socket_fd* out)
{
	as_socket_fd fd = socket(family, type, 0);

#if !defined(_MSC_VER)
	if (fd < 0) {
		return false;
	}

	int flags = fcntl(fd, F_GETFL, 0);

	if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
		as_close(fd);
		return false;
	}

#if defined(__APPLE__)
	int f = 1;

	if (setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &f, sizeof(f)) < 0) {
		as_close(fd);
		return false;
	}
#endif
#else
	if (fd == INVALID_SOCKET) {
		return false;
	}

	unsigned long mode = 1;

	if (ioctlsocket(fd, FIONBIO, &mode) != 0) {
		as_close(fd);
		return false;
	}
#endif
	*out = fd;
	return true;
}

static inline int
dns_send(as_socket_fd fd, const uint8_t* buf, size_t len)
{
#if !defined(_MSC_VER)
	return (int)send(fd, buf, len, DNS_SEND_FLAGS);
#else
	return send(fd, (const char*)buf, (int)len, 0);
#endif
}

static inline int
dns_recv(as_socket_fd fd, uint8_t* buf, size_t len)
{
#if !defined(_MSC_VER)
	return (int)recv(fd, buf, len, 0);
#else
	return recv(fd, (char*)buf, (int)len, 0);
#endif
}

// Wait until fd is readable (or writable) or the deadline passes. Returns 1 when ready, 0 at the
// deadline and -1 on error.
static int
dns_wait(as_socket_fd fd, bool read, uint64_t deadline)
{
	as_poll poll;
	as_poll_init(&poll, fd);
	int rv;

	while (true) {
		uint64_t now = cf_getns();

		if (now >= deadline) {
			rv = 0;
			break;
		}

		// as_poll_socket() has millisecond resolution and waits forever for a zero timeout, so
		// round to the nearest millisecond and treat less than half a millisecond as a timeout.
		uint64_t ms = (deadline - now + 500000) / 1000000;

		if (ms == 0) {
			rv = 0;
			break;
		}
		rv = as_poll_socket(&poll, fd, ms > INT32_MAX ? INT32_MAX : (uint32_t)ms, read);

		if (rv > 0) {
			rv = 1;
			break;
		}

		if (rv == 0 || (rv == -1 && as_last_error() == DNS_EINTR)) {
			continue;
		}
		rv = -1;
		break;
	}
	as_poll_destroy(&poll);
	return rv;
}

static dns_status
dns_write_all(as_socket_fd fd, const uint8_t* buf, size_t len, uint64_t deadline)
{
	size_t pos = 0;

	while (pos < len) {
		int rv = dns_wait(fd, false, deadline);

		if (rv <= 0) {
			return rv == 0 ? DNS_TIMEOUT : DNS_FAILED;
		}

		int n = dns_send(fd, buf + pos, len - pos);

		if (n > 0) {
			pos += (size_t)n;
			continue;
		}

		int e = as_last_error();

		if (n == 0 || (e != DNS_EINTR && !dns_would_block(e))) {
			return DNS_FAILED;
		}
	}
	return DNS_OK;
}

static dns_status
dns_read_all(as_socket_fd fd, uint8_t* buf, size_t len, uint64_t deadline)
{
	size_t pos = 0;

	while (pos < len) {
		int rv = dns_wait(fd, true, deadline);

		if (rv <= 0) {
			return rv == 0 ? DNS_TIMEOUT : DNS_FAILED;
		}

		int n = dns_recv(fd, buf + pos, len - pos);

		if (n > 0) {
			pos += (size_t)n;
			continue;
		}

		if (n == 0) {
			// Closed by the resolver.
			return DNS_FAILED;
		}

		int e = as_last_error();

		if (e != DNS_EINTR && !dns_would_block(e)) {
			return DNS_FAILED;
		}
	}
	return DNS_OK;
}

//---------------------------------
// Static Functions: answers
//---------------------------------

static void
dns_answer_init(dns_answer* answer)
{
	as_vector_init(&answer->addrs, sizeof(dns_addr), 4);
	as_vector_init(&answer->srvs, sizeof(dns_srv), 4);
	as_vector_init(&answer->extra_a, sizeof(dns_addr), 4);
	as_vector_init(&answer->cnames, sizeof(dns_cname), 2);
}

static void
dns_answer_clear(dns_answer* answer)
{
	as_vector_clear(&answer->addrs);
	as_vector_clear(&answer->srvs);
	as_vector_clear(&answer->extra_a);
	as_vector_clear(&answer->cnames);
}

static void
dns_answer_destroy(dns_answer* answer)
{
	as_vector_destroy(&answer->addrs);
	as_vector_destroy(&answer->srvs);
	as_vector_destroy(&answer->extra_a);
	as_vector_destroy(&answer->cnames);
}

// Return true if owner is name or an alias of it through the CNAME records of the answer.
static bool
dns_owned_by(const dns_answer* answer, const char* name, const char* owner)
{
	const char* current = name;

	for (uint32_t hop = 0; hop <= DNS_MAX_CNAMES; hop++) {
		if (strcmp(owner, current) == 0) {
			return true;
		}

		const char* next = NULL;

		for (uint32_t i = 0; i < answer->cnames.size; i++) {
			const dns_cname* c = as_vector_get((as_vector*)&answer->cnames, i);

			if (strcmp(c->owner, current) == 0) {
				next = c->target;
				break;
			}
		}

		if (!next) {
			return false;
		}
		current = next;
	}
	return false;
}

// Drop answer records that are not owned by the queried name or its aliases.
static void
dns_answer_filter(dns_answer* answer, const char* name)
{
	as_vector* lists[] = {&answer->addrs, &answer->srvs};

	for (uint32_t l = 0; l < 2; l++) {
		as_vector* v = lists[l];
		uint32_t keep = 0;

		for (uint32_t i = 0; i < v->size; i++) {
			// owner is the first field of both record types.
			const char* owner = as_vector_get(v, i);

			if (dns_owned_by(answer, name, owner)) {
				if (keep != i) {
					memcpy(as_vector_get(v, keep), as_vector_get(v, i), v->item_size);
				}
				keep++;
			}
		}
		v->size = keep;
	}
}

static void
dns_format_addr(int family, const uint8_t* data, const char* owner, as_vector* out)
{
	dns_addr* addr = as_vector_reserve(out);
	snprintf(addr->owner, sizeof(addr->owner), "%s", owner);
	inet_ntop(family, (void*)data, addr->text, sizeof(addr->text));
}

// Parse one answer. A datagram that is not a response to the query returns DNS_MISMATCH; a
// response to it that cannot be fully parsed returns DNS_FAILED with a "malformed answer" reason.
static dns_status
dns_parse(
	const uint8_t* p, size_t size, const dns_query* q, bool tcp, dns_answer* answer, char* reason
	)
{
	if (size < DNS_HEADER_SIZE) {
		return DNS_MISMATCH;
	}

	uint16_t flags = dns_get16(p + 2);
	uint16_t qdcount = dns_get16(p + 4);
	uint32_t ancount = dns_get16(p + 6);
	uint32_t nscount = dns_get16(p + 8);
	uint32_t arcount = dns_get16(p + 10);
	uint8_t rcode = flags & 0xf;

	if (dns_get16(p) != q->id || !(flags & DNS_FLAG_QR) || ((flags >> 11) & 0xf) != 0) {
		return DNS_MISMATCH;
	}

	size_t pos = DNS_HEADER_SIZE;

	if (qdcount != 1) {
		// Some resolvers omit the question from an error response.
		if (qdcount != 0 || rcode == DNS_RCODE_NOERROR || rcode == DNS_RCODE_NXDOMAIN) {
			return DNS_MISMATCH;
		}
	}
	else {
		char name[DNS_NAME_SIZE];

		if (!dns_read_name(p, size, &pos, name) || pos + 4 > size ||
			strcmp(name, q->name) != 0 || dns_get16(p + pos) != q->type ||
			dns_get16(p + pos + 2) != DNS_CLASS_IN) {
			return DNS_MISMATCH;
		}
		pos += 4;
	}

	if (flags & DNS_FLAG_TC) {
		if (!tcp) {
			return DNS_TRUNCATED;
		}
		snprintf(reason, DNS_REASON_SIZE, "malformed answer");
		return DNS_FAILED;
	}

	switch (rcode) {
	case DNS_RCODE_NOERROR:
		break;
	case DNS_RCODE_NXDOMAIN:
		snprintf(reason, DNS_REASON_SIZE, "NXDOMAIN");
		return DNS_NXDOMAIN;
	case DNS_RCODE_SERVFAIL:
		snprintf(reason, DNS_REASON_SIZE, "SERVFAIL");
		return DNS_FAILED;
	case DNS_RCODE_REFUSED:
		snprintf(reason, DNS_REASON_SIZE, "REFUSED");
		return DNS_FAILED;
	default:
		snprintf(reason, DNS_REASON_SIZE, "rcode %u", (unsigned)rcode);
		return DNS_FAILED;
	}

	uint32_t total = ancount + nscount + arcount;

	for (uint32_t i = 0; i < total; i++) {
		char owner[DNS_NAME_SIZE];

		if (!dns_read_name(p, size, &pos, owner) || pos + 10 > size) {
			goto malformed;
		}

		uint16_t type = dns_get16(p + pos);
		uint16_t rclass = dns_get16(p + pos + 2);
		size_t rdlen = dns_get16(p + pos + 8);
		pos += 10;

		if (rdlen > size - pos) {
			goto malformed;
		}

		const uint8_t* rd = p + pos;
		size_t rd_end = pos + rdlen;
		bool in_answer = i < ancount;
		bool in_additional = i >= ancount + nscount;

		if (rclass == DNS_CLASS_IN) {
			switch (type) {
			case DNS_TYPE_A:
				if (rdlen != 4) {
					goto malformed;
				}

				if (in_answer && q->type == DNS_TYPE_A) {
					dns_format_addr(AF_INET, rd, owner, &answer->addrs);
				}
				else if (in_additional && q->type == DNS_TYPE_SRV) {
					dns_format_addr(AF_INET, rd, owner, &answer->extra_a);
				}
				break;

			case DNS_TYPE_AAAA:
				if (rdlen != 16) {
					goto malformed;
				}

				if (in_answer && q->type == DNS_TYPE_AAAA) {
					dns_format_addr(AF_INET6, rd, owner, &answer->addrs);
				}
				break;

			case DNS_TYPE_CNAME:
				if (in_answer) {
					if (answer->cnames.size >= DNS_MAX_CNAME_RECORDS) {
						goto malformed;
					}

					size_t name_pos = pos;
					dns_cname* c = as_vector_reserve(&answer->cnames);
					snprintf(c->owner, sizeof(c->owner), "%s", owner);

					if (!dns_read_name(p, size, &name_pos, c->target) || name_pos != rd_end) {
						goto malformed;
					}
				}
				break;

			case DNS_TYPE_SRV:
				if (in_answer && q->type == DNS_TYPE_SRV) {
					if (rdlen < 7) {
						goto malformed;
					}

					size_t name_pos = pos + 6;
					dns_srv* s = as_vector_reserve(&answer->srvs);
					snprintf(s->owner, sizeof(s->owner), "%s", owner);
					s->priority = dns_get16(rd);
					s->weight = dns_get16(rd + 2);
					s->port = dns_get16(rd + 4);

					if (!dns_read_name(p, size, &name_pos, s->target) || name_pos != rd_end) {
						goto malformed;
					}
				}
				break;

			default:
				break;
			}
		}
		pos = rd_end;
	}

	dns_answer_filter(answer, q->name);
	return DNS_OK;

malformed:
	snprintf(reason, DNS_REASON_SIZE, "malformed answer");
	return DNS_FAILED;
}

//---------------------------------
// Static Functions: transport
//---------------------------------

static void
dns_query_init(dns_query* q, const char* name, uint16_t type)
{
	q->name = name;
	q->type = type;
	q->id = 0;

	uint8_t* p = q->packet;
	memset(p, 0, DNS_HEADER_SIZE);
	dns_put16(p + 2, DNS_FLAG_RD);
	dns_put16(p + 4, 1);

	uint32_t pos = DNS_HEADER_SIZE + dns_encode_name(name, p + DNS_HEADER_SIZE);
	dns_put16(p + pos, type);
	dns_put16(p + pos + 2, DNS_CLASS_IN);
	q->size = pos + 4;
}

static void
dns_query_new_id(dns_query* q)
{
	q->id = (uint16_t)as_random_get_uint32();
	dns_put16(q->packet, q->id);
}

static dns_status
dns_udp(
	const dns_server* server, const dns_query* q, uint64_t deadline, dns_answer* answer,
	char* reason
	)
{
	as_socket_fd fd;

	if (!dns_socket(server->addr.ss_family, SOCK_DGRAM, &fd)) {
		snprintf(reason, DNS_REASON_SIZE, "socket error %d", as_last_error());
		return DNS_FAILED;
	}

	// A connected UDP socket only receives datagrams from the resolver and reports ICMP errors.
	if (connect(fd, (struct sockaddr*)&server->addr, server->addr_len) != 0 ||
		dns_send(fd, q->packet, q->size) != (int)q->size) {
		snprintf(reason, DNS_REASON_SIZE, "network error %d", as_last_error());
		as_close(fd);
		return DNS_FAILED;
	}

	uint8_t buf[DNS_UDP_BUFFER];
	dns_status status;

	while (true) {
		int rv = dns_wait(fd, true, deadline);

		if (rv == 0) {
			status = DNS_TIMEOUT;
			break;
		}

		if (rv < 0) {
			snprintf(reason, DNS_REASON_SIZE, "network error %d", as_last_error());
			status = DNS_FAILED;
			break;
		}

		int n = dns_recv(fd, buf, sizeof(buf));

		if (n < 0) {
			int e = as_last_error();

			if (e == DNS_EINTR || dns_would_block(e)) {
				continue;
			}
			snprintf(reason, DNS_REASON_SIZE, "network error %d", e);
			status = DNS_FAILED;
			break;
		}

		status = dns_parse(buf, (size_t)n, q, false, answer, reason);

		if (status != DNS_MISMATCH) {
			break;
		}
		dns_answer_clear(answer);
	}
	as_close(fd);
	return status;
}

static dns_status
dns_tcp(
	const dns_server* server, const dns_query* q, uint64_t deadline, dns_answer* answer,
	char* reason
	)
{
	as_socket_fd fd;

	if (!dns_socket(server->addr.ss_family, SOCK_STREAM, &fd)) {
		snprintf(reason, DNS_REASON_SIZE, "socket error %d", as_last_error());
		return DNS_FAILED;
	}

	dns_status status;

	if (connect(fd, (struct sockaddr*)&server->addr, server->addr_len) != 0 &&
		!dns_would_block(as_last_error())) {
		snprintf(reason, DNS_REASON_SIZE, "TCP connect error %d", as_last_error());
		as_close(fd);
		return DNS_FAILED;
	}

	int rv = dns_wait(fd, false, deadline);

	if (rv <= 0) {
		status = rv == 0 ? DNS_TIMEOUT : DNS_FAILED;

		if (rv < 0) {
			snprintf(reason, DNS_REASON_SIZE, "TCP connect error %d", as_last_error());
		}
		as_close(fd);
		return status;
	}

	int so_error = 0;
	socklen_t so_len = sizeof(so_error);

	if (getsockopt(fd, SOL_SOCKET, SO_ERROR, (char*)&so_error, &so_len) != 0 || so_error != 0) {
		snprintf(reason, DNS_REASON_SIZE, "TCP connect error %d", so_error);
		as_close(fd);
		return DNS_FAILED;
	}

	uint8_t out[2 + DNS_MAX_QUERY];
	dns_put16(out, (uint16_t)q->size);
	memcpy(out + 2, q->packet, q->size);
	status = dns_write_all(fd, out, 2 + q->size, deadline);

	uint8_t prefix[2];

	if (status == DNS_OK) {
		status = dns_read_all(fd, prefix, sizeof(prefix), deadline);
	}

	if (status == DNS_OK) {
		size_t size = dns_get16(prefix);

		if (size < DNS_HEADER_SIZE) {
			snprintf(reason, DNS_REASON_SIZE, "malformed answer");
			as_close(fd);
			return DNS_FAILED;
		}

		uint8_t* buf = cf_malloc(size);

		if (!buf) {
			snprintf(reason, DNS_REASON_SIZE, "allocation failure");
			as_close(fd);
			return DNS_FAILED;
		}

		status = dns_read_all(fd, buf, size, deadline);

		if (status == DNS_OK) {
			status = dns_parse(buf, size, q, true, answer, reason);

			// The connection is dedicated to this query, so any mismatch is an invalid answer.
			if (status == DNS_MISMATCH) {
				dns_answer_clear(answer);
				snprintf(reason, DNS_REASON_SIZE, "malformed answer");
				status = DNS_FAILED;
			}
		}
		cf_free(buf);
	}

	if (status == DNS_FAILED && !reason[0]) {
		snprintf(reason, DNS_REASON_SIZE, "TCP error");
	}
	as_close(fd);
	return status;
}

static bool
dns_late(const dns_run* run)
{
	return cf_getns() + DNS_MIN_SHARE_NS > run->deadline;
}

static uint64_t
dns_attempt_deadline(uint64_t share, uint64_t deadline)
{
	uint64_t now = cf_getns();
	return (now >= deadline || deadline - now <= share) ? deadline : now + share;
}

// Ask one resolver. The UDP exchange and the TCP retry of a truncated answer each get share,
// cut at deadline.
static dns_status
dns_exchange(
	const dns_server* server, dns_query* q, uint64_t share, uint64_t deadline, dns_answer* answer,
	char* reason
	)
{
	dns_query_new_id(q);
	reason[0] = 0;

	dns_status status = dns_udp(server, q, dns_attempt_deadline(share, deadline), answer, reason);

	if (status == DNS_TRUNCATED) {
		dns_answer_clear(answer);
		status = dns_tcp(server, q, dns_attempt_deadline(share, deadline), answer, reason);
	}
	return status;
}

// Resolve one question. As in the Java client, each resolver is asked once and each exchange gets
// 1/(2 * resolvers) of the time left when the query starts, at least 1 ms, so a silent resolver
// costs one share; no query starts with less than 1 ms left. The resolver that last answered in
// this refresh is asked first. An answer or NXDOMAIN ends the query; other failures try the next
// resolver. On failure reason names the last failure, where a timeout never hides another reason.
static dns_status
dns_query_run(dns_run* run, const char* name, uint16_t type, dns_answer* answer, char* reason)
{
	const dns_state* st = run->st;
	dns_status result = DNS_TIMEOUT;
	snprintf(reason, DNS_REASON_SIZE, "timeout");

	uint64_t now = cf_getns();

	if (now + DNS_MIN_SHARE_NS > run->deadline) {
		return result;
	}

	uint64_t share = (run->deadline - now) / (2 * (uint64_t)st->servers_size);

	if (share < DNS_MIN_SHARE_NS) {
		share = DNS_MIN_SHARE_NS;
	}

	dns_query q;
	dns_query_init(&q, name, type);

	for (uint32_t i = 0; i < st->servers_size; i++) {
		if (i > 0 && dns_late(run)) {
			break;
		}

		uint32_t index = (run->preferred + i) % st->servers_size;
		char attempt_reason[DNS_REASON_SIZE];
		dns_status status = dns_exchange(&st->servers[index], &q, share, run->deadline, answer,
			attempt_reason);

		if (status == DNS_OK || status == DNS_NXDOMAIN) {
			run->preferred = index;
			snprintf(reason, DNS_REASON_SIZE, "%s", attempt_reason);
			return status;
		}

		dns_answer_clear(answer);

		if (status != DNS_TIMEOUT) {
			result = DNS_FAILED;
			snprintf(reason, DNS_REASON_SIZE, "%s", attempt_reason);
		}
	}
	return result;
}

//---------------------------------
// Static Functions: resolution
//---------------------------------

static int
dns_compare_addr(const void* a, const void* b)
{
	return strcmp(((const dns_addr*)a)->text, ((const dns_addr*)b)->text);
}

static int
dns_compare_srv(const void* a, const void* b)
{
	const dns_srv* x = a;
	const dns_srv* y = b;

	if (x->priority != y->priority) {
		return x->priority < y->priority ? -1 : 1;
	}

	if (x->weight != y->weight) {
		return x->weight > y->weight ? -1 : 1;
	}

	int rv = strcmp(x->target, y->target);

	if (rv != 0) {
		return rv;
	}
	return (x->port > y->port) - (x->port < y->port);
}

static void
dns_sort_addrs(as_vector* addrs)
{
	if (addrs->size > 1) {
		qsort(addrs->list, addrs->size, addrs->item_size, dns_compare_addr);
	}
}

static void
dns_append_addrs(as_vector* out, const as_vector* addrs)
{
	for (uint32_t i = 0; i < addrs->size; i++) {
		as_vector_append(out, as_vector_get((as_vector*)addrs, i));
	}
}

// Keep the first DNS_MAX_RECORDS of a sorted record list.
static void
dns_cap(as_vector* records, const char* name, const char* type)
{
	if (records->size > DNS_MAX_RECORDS) {
		as_log_warn("DNS seed name %s returned %u %s records. Using the first %u", name,
			records->size, type, (uint32_t)DNS_MAX_RECORDS);
		records->size = DNS_MAX_RECORDS;
	}
}

// Append the A, then the AAAA, addresses of query to out (dns_addr), each family sorted. With
// first_family the AAAA query is skipped when an A address was found and only the first address
// is used, so no cap applies; otherwise each family is capped at DNS_MAX_RECORDS. Returns true when
// an address was found, otherwise sets reason. NXDOMAIN for A ends the lookup.
static bool
dns_resolve_addresses(
	dns_run* run, const char* query, const char* log_name, bool first_family, as_vector* out,
	char* reason
	)
{
	static const uint16_t types[] = {DNS_TYPE_A, DNS_TYPE_AAAA};
	char failure[DNS_REASON_SIZE];
	bool failed = false;
	bool nxdomain = false;
	dns_answer answer;
	dns_answer_init(&answer);

	for (uint32_t t = 0; t < 2; t++) {
		if (t == 1 && first_family && out->size > 0) {
			break;
		}

		char query_reason[DNS_REASON_SIZE];
		dns_status status = dns_query_run(run, query, types[t], &answer, query_reason);

		if (status == DNS_OK) {
			dns_sort_addrs(&answer.addrs);

			if (!first_family) {
				dns_cap(&answer.addrs, log_name, t == 0 ? "A" : "AAAA");
			}
			dns_append_addrs(out, &answer.addrs);
		}
		else if (status == DNS_NXDOMAIN) {
			nxdomain = true;

			if (t == 0) {
				break;
			}
		}
		else {
			failed = true;
			snprintf(failure, sizeof(failure), "%s", query_reason);
		}
		dns_answer_clear(&answer);
	}
	dns_answer_destroy(&answer);

	if (out->size > 0) {
		return true;
	}

	snprintf(reason, DNS_REASON_SIZE, "%s",
		nxdomain ? "NXDOMAIN" : failed ? failure : "empty answer");
	return false;
}

// Append a host unless the same address and port is already present.
static bool
dns_emit(const dns_run* run, as_seed_candidates* candidates, const char* address, uint16_t port)
{
	for (uint32_t i = 0; i < candidates->hosts->size; i++) {
		const as_host* host = as_vector_get(candidates->hosts, i);

		if (host->port == port && strcmp(host->name, address) == 0) {
			return true;
		}
	}
	return as_seed_candidates_append(candidates, address, run->st->tls_name, port);
}

static as_status
dns_append_failed(as_error* err)
{
	return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to append DNS seed candidate");
}

static as_status
dns_resolve_a(
	dns_run* run, as_error* err, const dns_name* n, as_seed_candidates* candidates,
	uint32_t* found, char* reason
	)
{
	as_vector addrs;
	as_vector_init(&addrs, sizeof(dns_addr), 8);

	if (dns_resolve_addresses(run, n->query, n->name, false, &addrs, reason)) {
		for (uint32_t i = 0; i < addrs.size; i++) {
			const dns_addr* addr = as_vector_get(&addrs, i);

			if (!dns_emit(run, candidates, addr->text, run->st->port)) {
				as_vector_destroy(&addrs);
				return dns_append_failed(err);
			}
		}
		*found = addrs.size;
	}
	as_vector_destroy(&addrs);
	return AEROSPIKE_OK;
}

// Pick the first sorted address of target among the additional section records of one family.
static bool
dns_pick_extra(const as_vector* extras, const char* target, char* text)
{
	const char* best = NULL;

	for (uint32_t i = 0; i < extras->size; i++) {
		const dns_addr* addr = as_vector_get((as_vector*)extras, i);

		if (strcmp(addr->owner, target) == 0 && (!best || strcmp(addr->text, best) < 0)) {
			best = addr->text;
		}
	}

	if (best) {
		snprintf(text, INET6_ADDRSTRLEN, "%s", best);
	}
	return best != NULL;
}

static dns_target*
dns_find_target(as_vector* targets, const char* target)
{
	for (uint32_t i = 0; i < targets->size; i++) {
		dns_target* t = as_vector_get(targets, i);

		if (strcmp(t->target, target) == 0) {
			return t;
		}
	}
	return NULL;
}

// Resolve target to one address. Returns false, without a query or a warning, when the deadline
// leaves no time for a query.
static bool
dns_resolve_target(
	dns_run* run, const dns_name* n, const dns_answer* answer, const char* target,
	dns_target* out
	)
{
	snprintf(out->target, sizeof(out->target), "%s", target);
	out->resolved = false;

	// An A RRset in the additional section is the complete answer to an A query for the target.
	if (dns_pick_extra(&answer->extra_a, target, out->text)) {
		out->resolved = true;
		return true;
	}

	if (dns_late(run)) {
		return false;
	}

	as_vector addrs;
	as_vector_init(&addrs, sizeof(dns_addr), 4);
	char reason[DNS_REASON_SIZE];

	if (dns_resolve_addresses(run, target, target, true, &addrs, reason)) {
		const dns_addr* addr = as_vector_get(&addrs, 0);
		snprintf(out->text, sizeof(out->text), "%s", addr->text);
		out->resolved = true;
	}
	else {
		as_log_warn("DNS seed name %s SRV target %s. dropped: %s", n->name, target, reason);
	}
	as_vector_destroy(&addrs);
	return true;
}

static as_status
dns_resolve_srv(
	dns_run* run, as_error* err, const dns_name* n, as_seed_candidates* candidates,
	uint32_t* found, char* reason
	)
{
	dns_answer answer;
	dns_answer_init(&answer);

	dns_status status = dns_query_run(run, n->query, DNS_TYPE_SRV, &answer, reason);

	if (status != DNS_OK) {
		dns_answer_destroy(&answer);
		return AEROSPIKE_OK;
	}

	// Drop unusable records. A target of "." means the service is not available at this name.
	as_vector* srvs = &answer.srvs;
	uint32_t keep = 0;
	uint32_t invalid = 0;

	for (uint32_t i = 0; i < srvs->size; i++) {
		dns_srv* s = as_vector_get(srvs, i);

		if (!s->target[0]) {
			continue;
		}

		if (s->port == 0) {
			if (++invalid == 1) {
				as_log_warn("DNS seed name %s returned an invalid SRV record: %u %u %u %s.",
					n->name, (unsigned)s->priority, (unsigned)s->weight, (unsigned)s->port,
					s->target);
			}
			continue;
		}

		if (keep != i) {
			memcpy(as_vector_get(srvs, keep), s, sizeof(dns_srv));
		}
		keep++;
	}
	srvs->size = keep;

	if (invalid > 1) {
		as_log_warn("DNS seed name %s returned %u invalid SRV records; only the first was logged",
			n->name, invalid);
	}

	if (srvs->size == 0) {
		snprintf(reason, DNS_REASON_SIZE, "empty answer");
		dns_answer_destroy(&answer);
		return AEROSPIKE_OK;
	}

	qsort(srvs->list, srvs->size, sizeof(dns_srv), dns_compare_srv);
	dns_cap(srvs, n->name, "SRV");

	as_vector targets;
	as_vector_init(&targets, sizeof(dns_target), srvs->size);
	uint32_t count = 0;
	uint32_t late = 0;
	as_status rv = AEROSPIKE_OK;

	for (uint32_t i = 0; i < srvs->size; i++) {
		const dns_srv* s = as_vector_get(srvs, i);
		dns_target* t = dns_find_target(&targets, s->target);

		if (!t) {
			t = as_vector_reserve(&targets);

			if (!dns_resolve_target(run, n, &answer, s->target, t)) {
				late++;
			}
		}

		if (t->resolved) {
			if (!dns_emit(run, candidates, t->text, s->port)) {
				rv = dns_append_failed(err);
				break;
			}
			count++;
		}
	}

	if (late > 0) {
		as_log_warn("DNS seed name %s SRV targets not queried before the deadline: %u", n->name,
			late);
	}

	if (rv == AEROSPIKE_OK && count == 0) {
		snprintf(reason, DNS_REASON_SIZE, "no SRV target resolved");
	}
	*found = count;
	as_vector_destroy(&targets);
	dns_answer_destroy(&answer);
	return rv;
}

//---------------------------------
// Static Functions: provider
//---------------------------------

static as_status
dns_refresh(as_error* err, const as_seed_candidate_context* context,
	as_seed_candidates* candidates, void* udata)
{
	(void)context;
	const dns_state* st = udata;

	if (!st || !candidates || !candidates->hosts) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM, "Invalid DNS seed refresh arguments");
	}

	// One deadline for the whole refresh. Each query takes a share of the time left when it starts
	// (dns_query_run); with every resolver silent, one name uses up to 3/4 of it.
	dns_run run = {
		.st = st,
		.deadline = cf_getns() + (uint64_t)st->timeout_ms * 1000 * 1000,
		.preferred = 0
	};
	char (*reasons)[DNS_REASON_SIZE] = cf_calloc(st->names_size, DNS_REASON_SIZE);

	if (!reasons) {
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT,
			"Failed to allocate DNS seed refresh state");
	}

	uint32_t total = 0;

	for (uint32_t i = 0; i < st->names_size; i++) {
		const dns_name* n = &st->names[i];
		uint32_t found = 0;
		as_status status;

		if (st->record_type == AS_DNS_RECORD_SRV) {
			status = dns_resolve_srv(&run, err, n, candidates, &found, reasons[i]);
		}
		else {
			status = dns_resolve_a(&run, err, n, candidates, &found, reasons[i]);
		}

		if (status != AEROSPIKE_OK) {
			cf_free(reasons);
			return status;
		}

		if (found > 0) {
			reasons[i][0] = 0;
			total += found;
		}
	}

	if (total == 0) {
		char msg[AS_ERROR_MESSAGE_MAX_SIZE];
		size_t len = (size_t)snprintf(msg, sizeof(msg), "DNS seed discovery failed for every name:");

		for (uint32_t i = 0; i < st->names_size && len < sizeof(msg); i++) {
			len += (size_t)snprintf(msg + len, sizeof(msg) - len, "%s %s: %s", i ? ";" : "",
				st->names[i].name, reasons[i]);
		}
		cf_free(reasons);
		return as_error_update(err, AEROSPIKE_ERR_SEED_DISCOVERY_FAILED, "%s", msg);
	}

	for (uint32_t i = 0; i < st->names_size; i++) {
		if (reasons[i][0]) {
			as_log_warn("DNS seed name %s failed: %s", st->names[i].name, reasons[i]);
		}
	}
	cf_free(reasons);

	as_log_debug("DNS seed names resolved to %u hosts", candidates->hosts->size);
	return AEROSPIKE_OK;
}

static bool
dns_true(void* udata)
{
	(void)udata;
	return true;
}

static void
dns_destroy(void* udata)
{
	dns_state* st = udata;

	if (!st) {
		return;
	}

	for (uint32_t i = 0; i < st->names_size; i++) {
		cf_free(st->names[i].name);
		cf_free(st->names[i].query);
	}
	cf_free(st->names);
	cf_free(st->servers);
	cf_free(st->tls_name);
	st->magic = 0;
	cf_free(st);
}

static as_status
dns_invalid_server(as_error* err, const char* server, const char* reason)
{
	if (!server) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
			"Invalid DNS seed provider server: null");
	}
	return as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid DNS seed provider server: '%s'%s",
		server, reason);
}

static as_status
dns_validate(as_error* err, const as_dns_seed_provider_config* config)
{
	if (!config->names || config->names_size == 0) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
			"Invalid DNS seed provider names: at least one name is required");
	}

	if (config->record_type != AS_DNS_RECORD_A && config->record_type != AS_DNS_RECORD_SRV) {
		return as_error_update(err, AEROSPIKE_ERR_PARAM,
			"Invalid DNS seed provider record_type: %d", (int)config->record_type);
	}

	if (config->port == 0) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM, "Invalid DNS seed provider port: 0");
	}

	if (config->tls_name && !config->tls_name[0]) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
			"Invalid DNS seed provider tls_name: empty");
	}

	if (config->timeout_ms == 0) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
			"Invalid DNS seed provider timeout_ms: 0");
	}

	if (config->servers_size && !config->servers) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM,
			"Invalid DNS seed provider servers: null");
	}
	return AEROSPIKE_OK;
}

static as_status
dns_create(as_error* err, const as_dns_seed_provider_config* config, dns_state** out)
{
	as_status status = dns_validate(err, config);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	dns_state* st = cf_calloc(1, sizeof(dns_state));

	if (!st) {
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to allocate DNS seed provider");
	}

	st->magic = DNS_STATE_MAGIC;
	st->record_type = config->record_type;
	st->port = config->port;
	st->timeout_ms = config->timeout_ms;
	st->names = cf_calloc(config->names_size, sizeof(dns_name));

	if (!st->names || (config->tls_name && !(st->tls_name = cf_strdup(config->tls_name)))) {
		dns_destroy(st);
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to allocate DNS seed provider");
	}

	for (uint32_t i = 0; i < config->names_size; i++) {
		const char* name = config->names[i];
		char query[DNS_NAME_SIZE];

		if (!name || strlen(name) >= DNS_NAME_SIZE || !dns_normalize_name(name, query)) {
			as_error_update(err, AEROSPIKE_ERR_PARAM, "Invalid DNS seed provider name: '%s'",
				name ? name : "(null)");
			dns_destroy(st);
			return AEROSPIKE_ERR_PARAM;
		}

		for (uint32_t j = 0; j < st->names_size; j++) {
			if (strcmp(st->names[j].query, query) == 0) {
				as_error_update(err, AEROSPIKE_ERR_PARAM,
					"Invalid DNS seed provider names: duplicate name '%s'", name);
				dns_destroy(st);
				return AEROSPIKE_ERR_PARAM;
			}
		}

		dns_name* n = &st->names[st->names_size];
		n->name = cf_strdup(name);
		n->query = cf_strdup(query);

		if (!n->name || !n->query) {
			cf_free(n->name);
			cf_free(n->query);
			dns_destroy(st);
			return as_error_set_message(err, AEROSPIKE_ERR_CLIENT,
				"Failed to allocate DNS seed provider");
		}
		st->names_size++;
	}

	as_vector servers;
	as_vector_init(&servers, sizeof(dns_server), config->servers_size ? config->servers_size : 4);

	if (config->servers_size) {
		for (uint32_t i = 0; i < config->servers_size; i++) {
			const char* text = config->servers[i];
			dns_server server;
			const char* reason = text ? dns_parse_server(text, &server) : "";

			if (!text || reason) {
				as_vector_destroy(&servers);
				dns_destroy(st);
				return dns_invalid_server(err, text, reason);
			}
			dns_servers_add(&servers, &server);
		}
	}
	else {
		status = dns_system_servers(err, &servers);

		if (status != AEROSPIKE_OK) {
			as_vector_destroy(&servers);
			dns_destroy(st);
			return status;
		}
	}

	st->servers = cf_malloc(servers.size * sizeof(dns_server));

	if (!st->servers) {
		as_vector_destroy(&servers);
		dns_destroy(st);
		return as_error_set_message(err, AEROSPIKE_ERR_CLIENT, "Failed to allocate DNS seed provider");
	}

	memcpy(st->servers, servers.list, servers.size * sizeof(dns_server));
	st->servers_size = servers.size;
	as_vector_destroy(&servers);

	*out = st;
	return AEROSPIKE_OK;
}

//---------------------------------
// Functions
//---------------------------------

void
as_dns_seed_provider_config_init(as_dns_seed_provider_config* config)
{
	memset(config, 0, sizeof(as_dns_seed_provider_config));
	config->record_type = AS_DNS_RECORD_A;
	config->port = AS_DNS_DEFAULT_PORT;
	config->timeout_ms = AS_DNS_DEFAULT_TIMEOUT_MS;
}

as_status
as_discovery_dns_seed_provider(as_error* err, const as_dns_seed_provider_config* config,
	as_seed_provider* provider)
{
	as_error_reset(err);

	if (!config || !provider) {
		return as_error_set_message(err, AEROSPIKE_ERR_PARAM, "Invalid DNS seed provider");
	}

	dns_state* st = NULL;
	as_status status = dns_create(err, config, &st);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	memset(provider, 0, sizeof(as_seed_provider));
	provider->size = sizeof(as_seed_provider);
	provider->api_version = AS_DISCOVERY_API_VERSION;
	provider->name = "dns";
	provider->refresh = dns_refresh;
	provider->needs_periodic_refresh = dns_true;
	provider->supports_deadline = dns_true;
	provider->destroy = dns_destroy;
	provider->udata = st;
	return AEROSPIKE_OK;
}

bool
as_dns_seed_provider_timeout(const as_seed_provider* provider, uint32_t* timeout_ms)
{
	if (!provider || provider->refresh != dns_refresh || !provider->udata) {
		return false;
	}

	const dns_state* st = provider->udata;

	if (st->magic != DNS_STATE_MAGIC) {
		return false;
	}
	*timeout_ms = st->timeout_ms;
	return true;
}
