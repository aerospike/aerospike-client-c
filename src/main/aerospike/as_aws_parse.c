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
