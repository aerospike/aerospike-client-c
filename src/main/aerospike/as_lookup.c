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
#include <aerospike/as_lookup.h>
#include <aerospike/as_lookup_internal.h>
#include <aerospike/as_admin.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_info.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_string_builder.h>
#include <aerospike/as_tls.h>
#include <aerospike/as_version.h>
#include <citrusleaf/cf_clock.h>

#if !defined(_MSC_VER)
#include <errno.h>
#include <poll.h>
#endif

//---------------------------------
// Macros
//---------------------------------

// Most milliseconds between the connect starts to the addresses of one host.
#define AS_LOOKUP_STAGGER_DELAY 100

//---------------------------------
// Types
//---------------------------------

// Connect attempt to one address of a host.
typedef struct as_lookup_probe_s {
	struct sockaddr_storage addr;
	as_socket_fd fd;
	bool open;
	bool ready;
} as_lookup_probe;

// First connect or TLS failure of the addresses of one host.
typedef struct as_lookup_failure_s {
	struct sockaddr_storage addr;
	char message[64];
	bool set;
} as_lookup_failure;

//---------------------------------
// Resolver
//---------------------------------

static int
as_lookup_default_getaddrinfo(
	const char* node, const char* service, const struct addrinfo* hints, struct addrinfo** res
	)
{
	return getaddrinfo(node, service, hints, res);
}

static void
as_lookup_default_freeaddrinfo(struct addrinfo* res)
{
	freeaddrinfo(res);
}

static as_lookup_getaddrinfo_fn as_lookup_getaddrinfo = as_lookup_default_getaddrinfo;
static as_lookup_freeaddrinfo_fn as_lookup_freeaddrinfo = as_lookup_default_freeaddrinfo;

//---------------------------------
// Static Functions
//---------------------------------

static void
as_lookup_record_connect_failure(as_cluster* cluster, as_socket* sock)
{
	if (sock->tls_handshake_failed) {
		as_cluster_add_validation_tls_handshake_failure(cluster);
	}
	else {
		as_cluster_add_validation_open_failure(cluster);
	}
}

static void
as_lookup_destroy_hosts(as_vector* hosts)
{
	for (uint32_t i = 0; i < hosts->size; i++) {
		as_host_destroy(as_vector_get(hosts, i));
	}
	as_vector_destroy(hosts);
}

static as_status
as_lookup_alternate_missing(
	as_error* err, const char* node_name, const char* where, bool* alternate_missing
	)
{
	if (alternate_missing) {
		*alternate_missing = true;
	}

	return as_error_update(err, AEROSPIKE_ERR_INVALID_NODE,
		"Node %s%s%s has no alternate-access-address and use_services_alternate is true. "
		"Configure alternate-access-address on the server or set use_services_alternate to false",
		node_name, *where ? " " : "", where);
}

// Parse a service response and translate each advertised address. Addresses that cannot be
// translated are skipped and the first translation error is retained in first_error.
static as_status
as_lookup_translate_services(
	as_cluster* cluster, as_error* err, char* value, const char* node_name, const char* tls_name,
	as_vector* translated, as_error* first_error, bool* has_error
	)
{
	as_vector advertised;
	as_vector_inita(&advertised, sizeof(as_host), 4);

	if (! as_host_parse_addresses(value, &advertised)) {
		as_vector_destroy(&advertised);
		return as_error_update(err, AEROSPIKE_ERR_CLIENT, "Invalid service hosts string: '%s'", value);
	}

	as_endpoint_source source = as_cluster_endpoint_source(cluster);

	for (uint32_t i = 0; i < advertised.size; i++) {
		as_host* host = as_vector_get(&advertised, i);
		as_error terr;
		as_host out;
		as_status status = as_cluster_translate_address(cluster, &terr, node_name, tls_name,
			host->name, host->port, source, &out);

		if (status == AEROSPIKE_OK) {
			as_vector_append(translated, &out);
		}
		else {
			as_log_debug("%s", terr.message);

			if (! *has_error) {
				*first_error = terr;
				*has_error = true;
			}
		}
	}
	as_vector_destroy(&advertised);
	return AEROSPIKE_OK;
}

static as_status
as_switch_to_clear_socket(
	as_cluster* cluster, as_error* err, as_node_info* node_info, uint64_t deadline,
	bool* alternate_missing
	)
{
	// Obtain the node name and non-TLS addresses. The node name is needed to translate addresses.
	bool alt = cluster->use_services_alternate;
	char* command = alt ? "node\nservice-clear-alt\n" : "node\nservice-clear-std\n";
	const char* service_name = alt ? "service-clear-alt" : "service-clear-std";
	char* response = NULL;
	as_status status = as_info_command(err, &node_info->socket, NULL, command, true, deadline, 0, &response);
	as_socket_close(&node_info->socket);

	if (status) {
		return status;
	}

	as_vector values;
	as_vector_inita(&values, sizeof(as_name_value), 2);
	as_info_parse_multi_response(response, &values);

	char* node_name = NULL;
	char* service = NULL;

	for (uint32_t i = 0; i < values.size; i++) {
		as_name_value* nv = as_vector_get(&values, i);

		if (strcmp(nv->name, "node") == 0) {
			node_name = nv->value;
		}
		else if (strcmp(nv->name, service_name) == 0) {
			service = nv->value;
		}
	}

	as_vector hosts;
	as_vector_inita(&hosts, sizeof(as_host), 4);

	as_error first_error;
	bool has_error = false;

	if (! node_name || *node_name == 0 || ! service || *service == 0) {
		if (alt) {
			status = as_lookup_alternate_missing(err, node_name ? node_name : "", "",
				alternate_missing);
		}
		else {
			status = as_error_update(err, AEROSPIKE_ERR_CLIENT, "Invalid service hosts string: '%s'", response);
		}
		goto SWITCH_DONE;
	}

	status = as_lookup_translate_services(cluster, err, service, node_name, node_info->host.tls_name,
		&hosts, &first_error, &has_error);

	if (status) {
		goto SWITCH_DONE;
	}

	as_error error_local;
	as_address_iterator iter;
	as_host* host;
	struct sockaddr* addr;

	// Find first valid non-TLS address.
	for (uint32_t i = 0; i < hosts.size; i++) {
		host = as_vector_get(&hosts, i);
		status = as_lookup_host(&iter, &error_local, host->name, host->port);

		if (status) {
			continue;
		}

		while (as_lookup_next(&iter, &addr)) {
			status = as_socket_create_and_connect(&node_info->socket, &error_local, addr, NULL, NULL, deadline);

			if (status == AEROSPIKE_OK) {
				if (node_info->session) {
					status = as_authenticate(cluster, &error_local, &node_info->socket, NULL,
											 node_info->session, 0, deadline, NULL);

					if (status != AEROSPIKE_OK) {
						as_cluster_add_validation_auth_failure(cluster);
					}
				}

				if (status == AEROSPIKE_OK) {
					// The node keeps its TLS identity (node_info->host.tls_name) even though the
					// socket is clear.
					as_address_copy_storage(addr, &node_info->addr);
					as_lookup_end(&iter);
					goto SWITCH_DONE;
				}

				// Close and try next address.
				as_socket_close(&node_info->socket);
			}
			else {
				as_lookup_record_connect_failure(cluster, &node_info->socket);
			}
		}
		as_lookup_end(&iter);
	}

	// Failed to find non-TLS socket.
	if (hosts.size == 0 && has_error) {
		*err = first_error;
		status = err->code;
	}
	else {
		status = as_error_update(err, AEROSPIKE_ERR_CLIENT, "Invalid service hosts string: '%s'", service);
	}

SWITCH_DONE:
	as_vector_destroy(&values);
	as_lookup_destroy_hosts(&hosts);
	cf_free(response);
	return status;
}

static as_status
as_set_node_address(as_cluster* cluster, as_error* err, char* response, char* tls_name, as_node_info* node_info)
{
	if (! (*response)) {
		// Server does not support service level call (service-clear-std, ...).
		// Load balancer detection is not possible.
		return AEROSPIKE_OK;
	}

	char addr_name[AS_IP_ADDRESS_SIZE];
	as_address_short_name((struct sockaddr*)&node_info->addr, addr_name, sizeof(addr_name));
	uint16_t addr_port = as_address_port((struct sockaddr*)&node_info->addr);

	as_vector hosts;
	as_vector_inita(&hosts, sizeof(as_host), 4);

	as_error first_error;
	bool has_error = false;
	as_status status = as_lookup_translate_services(cluster, err, response, node_info->name,
		tls_name, &hosts, &first_error, &has_error);

	if (status != AEROSPIKE_OK) {
		as_vector_destroy(&hosts);
		return status;
	}

	// Search real hosts for seed. Only a registered translator can change the port, so the
	// implicit static map keeps the legacy host-only comparison.
	as_host* host;
	bool match_port = cluster->translator_registered;

	for (uint32_t i = 0; i < hosts.size; i++) {
		host = as_vector_get(&hosts, i);

		if ((! match_port || host->port == addr_port) && strcmp(host->name, addr_name) == 0) {
			// Found seed which is not a load balancer.
			as_lookup_destroy_hosts(&hosts);
			return AEROSPIKE_OK;
		}
	}

	// Seed not found, so seed is probably a load balancer.
	// Find first valid real host.
	as_socket sock;
	as_address_iterator iter;
	struct sockaddr* addr;
	as_error error_local;

	for (uint32_t i = 0; i < hosts.size; i++) {
		host = as_vector_get(&hosts, i);
		status = as_lookup_host(&iter, &error_local, host->name, host->port);

		if (status != AEROSPIKE_OK) {
			continue;
		}

		while (as_lookup_next(&iter, &addr)) {
			uint64_t deadline = as_socket_deadline(cluster->conn_timeout_ms);
			status = as_socket_create_and_connect(&sock, err, addr, cluster->tls_ctx,
												  tls_name, deadline);

			if (status == AEROSPIKE_OK) {
				if (node_info->session) {
					status = as_authenticate(cluster, &error_local, &sock, NULL,
											 node_info->session, 0, deadline, NULL);

					if (status != AEROSPIKE_OK) {
						as_cluster_add_validation_auth_failure(cluster);
					}
				}

				if (status == AEROSPIKE_OK) {
					// Socket is non-blocking, so must use socket to really tell if connect succeeded.
					char* response2 = 0;
					status = as_info_command(err, &sock, NULL, "node", true, deadline, 0, &response2);

					if (status == AEROSPIKE_OK) {
						// Verify node name.
						char* node_name = 0;
						status = as_info_parse_single_response(response2, &node_name);

						if (status == AEROSPIKE_OK && strcmp(node_name, node_info->name) == 0) {
							// Switch address.
							char new_addr_name[AS_IP_ADDRESS_SIZE];
							as_address_short_name(addr, new_addr_name, sizeof(new_addr_name));
							as_log_info("Switch node address from %s to node's access-address %s", addr_name, new_addr_name);

							as_socket_close(&node_info->socket);
							memcpy(&node_info->socket, &sock, sizeof(as_socket));
							as_address_copy_storage(addr, &node_info->addr);
							as_lookup_end(&iter);
							as_lookup_destroy_hosts(&hosts);
							cf_free(response2);
							return AEROSPIKE_OK;
						}
						cf_free(response2);
					}
				}
				// Close and try next address.
				as_socket_close(&sock);
			}
			else {
				as_lookup_record_connect_failure(cluster, &sock);
			}
		}
		as_lookup_end(&iter);
	}

	// Failed to find a valid address. IP Address is probably internal on the cloud
	// because the server access-address is not configured.  Log warning and continue
	// with original seed.
	as_log_info("Invalid address %s. access-address is probably not configured on server.", response);
	as_lookup_destroy_hosts(&hosts);
	return AEROSPIKE_OK;
}

static struct sockaddr_storage*
as_lookup_find_valid_address(as_cluster* cluster, as_host* host)
{
	as_vector* list = cluster->valid_addresses;

	if (! list) {
		return NULL;
	}

	for (uint32_t i = 0; i < list->size; i++) {
		as_lookup_valid_address* va = as_vector_get(list, i);

		if (va->port == host->port && strcmp(va->name, host->name) == 0) {
			return &va->addr;
		}
	}
	return NULL;
}

static void
as_lookup_set_valid_address(
	as_cluster* cluster, as_host* host, struct sockaddr* addr, const char* node_name
	)
{
	as_vector* list = cluster->valid_addresses;

	if (list) {
		for (uint32_t i = 0; i < list->size; i++) {
			as_lookup_valid_address* va = as_vector_get(list, i);

			if (va->port == host->port && strcmp(va->name, host->name) == 0) {
				as_strncpy(va->node_name, node_name, sizeof(va->node_name));
				as_address_copy_storage(addr, &va->addr);
				return;
			}
		}
	}
	else {
		list = cluster->valid_addresses = as_vector_create(sizeof(as_lookup_valid_address), 4);
	}

	as_lookup_valid_address va;
	as_strncpy(va.name, host->name, sizeof(va.name));
	as_strncpy(va.node_name, node_name, sizeof(va.node_name));
	va.port = host->port;
	as_address_copy_storage(addr, &va.addr);
	as_vector_append(list, &va);
}

static void
as_lookup_set_failure(as_lookup_failure* failure, struct sockaddr* addr, const char* message)
{
	if (! failure->set) {
		as_address_copy_storage(addr, &failure->addr);
		as_strncpy(failure->message, message, sizeof(failure->message));
		failure->set = true;
	}
}

static void
as_lookup_log_failure(as_host* host, struct sockaddr* addr, const char* message)
{
	if (as_log_debug_enabled()) {
		char addr_name[AS_IP_ADDRESS_SIZE];
		as_address_short_name(addr, addr_name, sizeof(addr_name));
		as_log_debug("Address %s %d failed: %s", addr_name, host->port, message);
	}
}

// Wait up to timeout_ms for the open connects of probes and set ready on each that ended,
// connected or failed. Return the number that ended, zero on timeout or -1 on error.
static int
as_lookup_poll(as_lookup_probe* probes, uint32_t count, uint32_t timeout_ms)
{
#if !defined(_MSC_VER)
	struct pollfd* pfds = cf_malloc(count * sizeof(struct pollfd));

	for (uint32_t i = 0; i < count; i++) {
		pfds[i].fd = probes[i].open ? probes[i].fd : -1;
		pfds[i].events = POLLOUT;
		pfds[i].revents = 0;
	}

	int rv = poll(pfds, (nfds_t)count, (int)timeout_ms);

	for (uint32_t i = 0; i < count; i++) {
		probes[i].ready = rv > 0 && probes[i].open && pfds[i].revents != 0;
	}
	cf_free(pfds);
	return rv;
#else
	// A failed connect is reported in the error set, not the write set.
	fd_set writes;
	fd_set errors;
	FD_ZERO(&writes);
	FD_ZERO(&errors);

	for (uint32_t i = 0; i < count; i++) {
		if (probes[i].open) {
			FD_SET(probes[i].fd, &writes);
			FD_SET(probes[i].fd, &errors);
		}
	}

	struct timeval tv;
	tv.tv_sec = timeout_ms / 1000;
	tv.tv_usec = (timeout_ms % 1000) * 1000;

	int rv = select(0, NULL, &writes, &errors, &tv);

	for (uint32_t i = 0; i < count; i++) {
		probes[i].ready = rv > 0 && probes[i].open &&
			(FD_ISSET(probes[i].fd, &writes) || FD_ISSET(probes[i].fd, &errors));
	}
	return rv;
#endif
}

static void
as_lookup_remove_probe(as_lookup_probe* probes, uint32_t* count, uint32_t index)
{
	(*count)--;
	memmove(&probes[index], &probes[index + 1], (*count - index) * sizeof(as_lookup_probe));
}

// Start connects to the addresses of probes one at a time, the next one when delay nanoseconds
// pass or as soon as a connect fails. Return true and move the first address whose connect
// completes from probes to winner, with its socket open, or return false when none completes by
// deadline. Addresses whose connect failed are removed, and the first failure is kept in failure.
// Every other probe socket is closed on return.
static bool
as_lookup_connect_first(
	as_cluster* cluster, as_host* host, as_lookup_probe* probes, uint32_t* count, uint64_t delay,
	uint64_t deadline, as_lookup_probe* winner, as_lookup_failure* failure
	)
{
	int connected = -1;
	uint32_t next = 0;
	uint32_t pending = 0;
	uint64_t start = cf_getns();
	char message[64];

	while (connected < 0) {
		uint64_t now = cf_getns();

		if (now >= deadline) {
			break;
		}

		if (next < *count && now >= start) {
			as_lookup_probe* probe = &probes[next];
			struct sockaddr* addr = (struct sockaddr*)&probe->addr;

			if (as_socket_create_fd(addr->sa_family, &probe->fd) == 0) {
				if (as_socket_connect_fd(probe->fd, addr, as_address_size(addr))) {
					probe->open = true;
					pending++;
					next++;
					start = now + delay;
					continue;
				}
				snprintf(message, sizeof(message), "connect error %d", as_last_error());
				as_close(probe->fd);
			}
			else {
				snprintf(message, sizeof(message), "socket error %d", as_last_error());
			}
			// Counted as as_socket_create_and_connect() failures are.
			as_cluster_add_validation_open_failure(cluster);
			as_lookup_log_failure(host, addr, message);
			as_lookup_set_failure(failure, addr, message);
			as_lookup_remove_probe(probes, count, next);
			continue;
		}

		if (pending == 0) {
			break;
		}

		uint64_t until = (next < *count && start < deadline) ? start : deadline;
		uint64_t ms = (until - now + 999999) / 1000000;
		int rv = as_lookup_poll(probes, next, (uint32_t)ms);

		if (rv < 0) {
#if !defined(_MSC_VER)
			if (errno == EINTR) {
				continue;
			}
#endif
			break;
		}

		// Failed addresses are removed in place, so an earlier winner keeps its index.
		uint32_t i = 0;

		while (i < next) {
			as_lookup_probe* probe = &probes[i];

			if (! probe->ready) {
				i++;
				continue;
			}

			int so_error = 0;
			socklen_t so_len = sizeof(so_error);

			if (getsockopt(probe->fd, SOL_SOCKET, SO_ERROR, (char*)&so_error, &so_len) != 0) {
				so_error = -1;
			}
			probe->open = false;
			probe->ready = false;
			pending--;

			if (so_error == 0) {
				if (connected < 0) {
					// The winner keeps its socket.
					connected = (int)i;
				}
				else {
					as_close(probe->fd);
				}
				i++;
				continue;
			}

			// A refused non-blocking connect is not an open failure, as with a single address,
			// where it surfaces in the first request.
			as_close(probe->fd);
			snprintf(message, sizeof(message), "connect error %d", so_error);
			as_lookup_log_failure(host, (struct sockaddr*)&probe->addr, message);
			as_lookup_set_failure(failure, (struct sockaddr*)&probe->addr, message);
			as_lookup_remove_probe(probes, count, i);
			next--;
			start = cf_getns();
		}
	}

	for (uint32_t i = 0; i < next; i++) {
		if (probes[i].open) {
			as_close(probes[i].fd);
			probes[i].open = false;
		}
	}

	if (connected < 0) {
		return false;
	}
	*winner = probes[connected];
	as_lookup_remove_probe(probes, count, (uint32_t)connected);
	return true;
}

//---------------------------------
// Functions
//---------------------------------

as_status
as_lookup_host(as_address_iterator* iter, as_error* err, const char* hostname, uint16_t port)
{
	iter->hostname_is_alias = true;

	struct addrinfo hints;
	memset(&hints, 0, sizeof(hints));
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_protocol = IPPROTO_TCP;

	// Check if hostname is really an IPv4 address.
	struct in_addr ipv4;
	
	if (inet_pton(AF_INET, hostname, &ipv4) == 1) {
		hints.ai_family = AF_INET;
		hints.ai_flags = AI_NUMERICHOST;
		iter->hostname_is_alias = false;
	}
	else {
		// Check if hostname is really an IPv6 address.
		struct in6_addr ipv6;
		
		if (inet_pton(AF_INET6, hostname, &ipv6) == 1) {
			hints.ai_family = AF_INET6;
			hints.ai_flags = AI_NUMERICHOST;
			iter->hostname_is_alias = false;
		}
	}
	
	int ret = as_lookup_getaddrinfo(hostname, NULL, &hints, &iter->addresses);
	
	if (ret) {
		return as_error_update(err, AEROSPIKE_ERR_INVALID_HOST, "Invalid hostname %s: %s",
							   hostname, gai_strerror(ret));
	}
	
	iter->current = iter->addresses;
	iter->port_be = cf_swap_to_be16(port);
	return AEROSPIKE_OK;
}

void
as_lookup_end(as_address_iterator* iter)
{
	as_lookup_freeaddrinfo(iter->addresses);
}

void
as_lookup_set_resolver(
	as_lookup_getaddrinfo_fn getaddrinfo_fn, as_lookup_freeaddrinfo_fn freeaddrinfo_fn
	)
{
	as_lookup_getaddrinfo = getaddrinfo_fn ? getaddrinfo_fn : as_lookup_default_getaddrinfo;
	as_lookup_freeaddrinfo = freeaddrinfo_fn ? freeaddrinfo_fn : as_lookup_default_freeaddrinfo;
}

as_status
as_lookup_node(
	as_cluster* cluster, as_error* err, as_host* host, struct sockaddr* addr,
	bool detect_load_balancer, as_node_info* node_info, bool* alternate_missing
	)
{
	return as_lookup_node_until(cluster, err, host, addr, detect_load_balancer, node_info,
		alternate_missing, as_socket_deadline(cluster->conn_timeout_ms));
}

// Validate the node on the connected node_info->socket. Every failure closes the socket.
static as_status
as_lookup_node_validate(
	as_cluster* cluster, as_error* err, as_host* host, struct sockaddr* addr,
	bool detect_load_balancer, as_node_info* node_info, bool* alternate_missing,
	uint64_t deadline
	)
{
	if (alternate_missing) {
		*alternate_missing = false;
	}

	bool alt_validated = false;
	as_status status;

	node_info->host = *host;
	as_address_copy_storage(addr, &node_info->addr);
	node_info->session = NULL;

	if (cluster->auth_enabled) {
		deadline = as_socket_deadline(cluster->login_timeout_ms);
		status = as_cluster_login(cluster, err, &node_info->socket, deadline, node_info);

		if (status) {
			as_cluster_add_validation_auth_failure(cluster);
			char str[512];
			snprintf(str, sizeof(str), " from %s:%d", host->name, host->port);
			as_error_append(err, str);
			as_socket_close(&node_info->socket);
			return status;
		}

		if (cluster->tls_ctx && cluster->tls_ctx->for_login_only) {
			// Switch to using non-TLS socket.
			status = as_switch_to_clear_socket(cluster, err, node_info, deadline,
				alternate_missing);

			if (status) {
				cf_free(node_info->session);
				return status;
			}

			// Disable load balancer detection since non-TLS address has already
			// been retrieved via service info command.
			detect_load_balancer = false;

			// The clear service address list was already required to be non-empty.
			alt_validated = true;
		}
	}

	as_string_builder sb;
	as_string_builder_inita(&sb, 256, false);
	as_string_builder_append(&sb, "node\npartition-generation\nbuild\n");
	uint32_t args = 3;

	if (cluster->cluster_name) {
		as_string_builder_append(&sb, "cluster-name\n");
		args++;
	}

	if (detect_load_balancer && as_address_is_local(addr)) {
		// localhost is never a load balancer.
		detect_load_balancer = false;
	}

	// The alternate address list must contain the node itself, even when load balancer detection
	// is not needed.
	bool service = detect_load_balancer || (cluster->use_services_alternate && ! alt_validated);

	if (service) {
		// Seed may be load balancer with changing address. Determine real address.
		const char* address_command = (cluster->tls_ctx) ?
			cluster->use_services_alternate ? "service-tls-alt" : "service-tls-std" :
			cluster->use_services_alternate ? "service-clear-alt" : "service-clear-std";

		as_string_builder_append(&sb, address_command);
		as_string_builder_append_char(&sb, '\n');
		args++;
	}

	char* response = 0;
	status = as_info_command(err, &node_info->socket, NULL, sb.data, true, deadline, 0, &response);
	
	if (status) {
		as_socket_error_append(err, (struct sockaddr*)&node_info->addr);
		as_node_info_destroy(node_info);
		return status;
	}
	
	as_vector values;
	as_vector_inita(&values, sizeof(as_name_value), args);
	
	as_info_parse_multi_response(response, &values);
	
	if ((service && values.size < args - 1) || (!service && values.size != args)) {
		// Vector was probably resized on heap. Destroy vector.
		as_vector_destroy(&values);
		goto Error;
	}

	args = 0;

	// Process node name.
	as_name_value* nv = as_vector_get(&values, args++);
	char* node_name = nv->value;
	
	if (node_name == 0 || *node_name == 0) {
		goto Error;
	}
	as_strncpy(node_info->name, node_name, AS_NODE_NAME_SIZE);

	// Process partition generation.
	nv = as_vector_get(&values, args++);
	uint32_t gen = (uint32_t)strtoul(nv->value, NULL, 10);

	if (gen == (uint32_t)-1) {
		char addr_name[AS_IP_ADDRESS_SIZE];
		as_address_name((struct sockaddr*)&node_info->addr, addr_name, sizeof(addr_name));
		as_error_update(err, AEROSPIKE_ERR_CLIENT, "Node %s %s is not yet fully initialized",
						node_info->name, addr_name);
		cf_free(response);
		as_node_info_destroy(node_info);
		return AEROSPIKE_ERR_CLIENT;
	}

	// Process server build version.
	nv = as_vector_get(&values, args++);

	if (!as_version_from_string(&node_info->version, nv->value)) {
		char addr_name[AS_IP_ADDRESS_SIZE];
		as_address_name((struct sockaddr*)&node_info->addr, addr_name, sizeof(addr_name));
		as_error_update(err, AEROSPIKE_ERR_CLIENT, "Node %s %s version is invalid: %s",
						node_info->name, addr_name, nv->value);
		cf_free(response);
		as_node_info_destroy(node_info);
		return AEROSPIKE_ERR_CLIENT;
	}

	// Process features.
	uint32_t features = 0;
	as_version pscan = {4,9,0,3};

	if (as_version_compare(&node_info->version, &pscan) >= 0) {
		features |= AS_FEATURES_PARTITION_SCAN;
	}
	else {
		// This client requires partition scan support. Partition scans were first
		// supported in server version 4.9. Do not allow any server node into the
		// cluster that is running server version < 4.9.
		char addr_name[AS_IP_ADDRESS_SIZE];
		as_address_name((struct sockaddr*)&node_info->addr, addr_name, sizeof(addr_name));
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
			"Node %s %s version < 4.9. This client requires server version >= 4.9",
			node_info->name, addr_name);
		cf_free(response);
		as_node_info_destroy(node_info);
		return AEROSPIKE_ERR_CLIENT;
	}

	as_version queryshow = {5,7,0,7};

	if (as_version_compare(&node_info->version, &queryshow) >= 0) {
		features |= AS_FEATURES_QUERY_SHOW;
	}

	as_version pquery_batchany = {6,0,0,0};

	if (as_version_compare(&node_info->version, &pquery_batchany) >= 0) {
		features |= AS_FEATURES_PARTITION_QUERY;
		features |= AS_FEATURES_BATCH_ANY;
	}

	as_version query_ops_projection = {8,1,2,0};

	if (as_version_compare(&node_info->version, &query_ops_projection) >= 0) {
		features |= AS_FEATURES_HAS_QUERY_OPS_PROJECTION_EXT;
	}

	if (as_version_compare(&node_info->version, &as_server_version_8_2_1) >= 0) {
		features |= AS_FEATURES_ORDER_BY;
	}

	node_info->features = features;

	// Process cluster name.
	if (cluster->cluster_name) {
		nv = as_vector_get(&values, args++);
		
		if (strcmp(cluster->cluster_name, nv->value) != 0) {
			char addr_name[AS_IP_ADDRESS_SIZE];
			as_address_name((struct sockaddr*)&node_info->addr, addr_name, sizeof(addr_name));
			as_error_update(err, AEROSPIKE_ERR_CLIENT,
					"Invalid node %s %s Expected cluster name '%s' Received '%s'",
					node_info->name, addr_name, cluster->cluster_name, nv->value);
			cf_free(response);
			as_node_info_destroy(node_info);
			return AEROSPIKE_ERR_CLIENT;
		}
	}

	if (service) {
		char* service_value = (args < values.size) ? ((as_name_value*)as_vector_get(&values, args++))->value : NULL;

		if (cluster->use_services_alternate && (! service_value || *service_value == 0)) {
			char addr_name[AS_IP_ADDRESS_SIZE];
			as_address_name((struct sockaddr*)&node_info->addr, addr_name, sizeof(addr_name));
			as_lookup_alternate_missing(err, node_info->name, addr_name, alternate_missing);
			cf_free(response);
			as_node_info_destroy(node_info);
			return err->code;
		}

		if (detect_load_balancer && service_value) {
			status = as_set_node_address(cluster, err, service_value, host->tls_name, node_info);

			if (status != AEROSPIKE_OK) {
				cf_free(response);
				as_node_info_destroy(node_info);
				return status;
			}
		}
	}

	cf_free(response);
	return AEROSPIKE_OK;
	
Error: {
	char addr_name[AS_IP_ADDRESS_SIZE];
	as_address_name((struct sockaddr*)&node_info->addr, addr_name, sizeof(addr_name));
	as_error_update(err, AEROSPIKE_ERR_CLIENT, "Invalid node info response from %s: %s",
					addr_name, response);
	cf_free(response);
	as_node_info_destroy(node_info);
	return AEROSPIKE_ERR_CLIENT;
	}
}

// Validate the node on fd, whose TCP connect to addr completed. The TLS handshake ends at
// deadline in nanoseconds, at least one millisecond from now. When the socket or the handshake
// fails, failure names it and fd is closed.
static as_status
as_lookup_node_connected(
	as_cluster* cluster, as_error* err, as_host* host, struct sockaddr* addr, as_socket_fd fd,
	uint64_t deadline, as_node_info* node_info, bool* alternate_missing, const char** failure
	)
{
	as_socket* sock = &node_info->socket;
	char name[AS_IP_ADDRESS_SIZE];

	*failure = NULL;

	// as_socket_wrap() closes fd when it fails.
	if (! as_socket_wrap(sock, addr->sa_family, fd, cluster->tls_ctx, host->tls_name)) {
		as_cluster_add_validation_open_failure(cluster);
		*failure = "TLS wrap failed";
		as_address_name(addr, name, sizeof(name));
		return as_error_update(err, AEROSPIKE_ERR_CONNECTION, "Socket create failed: %d, %s", -5,
			name);
	}

	if (sock->tls) {
		// Round up so the handshake does not time out before deadline, which would let the
		// caller start another address with no time left.
		uint64_t deadline_ms = (deadline + 999999) / 1000000;
		uint64_t min_ms = cf_getms() + 1;

		if (deadline_ms < min_ms) {
			deadline_ms = min_ms;
		}

		if (as_tls_connect(sock, deadline_ms)) {
			sock->tls_handshake_failed = true;
			as_lookup_record_connect_failure(cluster, sock);
			as_socket_close(sock);
			*failure = "TLS handshake failed";
			as_address_name(addr, name, sizeof(name));
			return as_error_update(err, AEROSPIKE_ERR_CONNECTION, "TLS handshake failed: %s", name);
		}
	}

	return as_lookup_node_validate(cluster, err, host, addr, false, node_info, alternate_missing,
		as_socket_deadline(cluster->conn_timeout_ms));
}

// Run the caller's check on a validated node. A node that is not accepted is destroyed.
static as_lookup_verdict
as_lookup_check(as_lookup_accept_fn accept, void* udata, as_node_info* node_info)
{
	if (! accept) {
		return AS_LOOKUP_ACCEPT;
	}

	as_lookup_verdict verdict = accept(node_info, udata);

	if (verdict != AS_LOOKUP_ACCEPT) {
		as_node_info_destroy(node_info);
	}
	return verdict;
}

static as_status
as_lookup_stopped(as_error* err, as_host* host, struct sockaddr* addr)
{
	char addr_name[AS_IP_ADDRESS_SIZE];
	as_address_short_name(addr, addr_name, sizeof(addr_name));
	return as_error_update(err, AEROSPIKE_ERR_INVALID_NODE,
		"Node of %s %d at %s was rejected", host->name, host->port, addr_name);
}

static as_status
as_lookup_not_accepted(as_error* err, as_host* host)
{
	return as_error_update(err, AEROSPIKE_ERR_INVALID_NODE,
		"No address of %s %d was accepted", host->name, host->port);
}

as_status
as_lookup_node_until(
	as_cluster* cluster, as_error* err, as_host* host, struct sockaddr* addr,
	bool detect_load_balancer, as_node_info* node_info, bool* alternate_missing,
	uint64_t deadline
	)
{
	if (alternate_missing) {
		*alternate_missing = false;
	}

	as_status status = as_socket_create_and_connect(&node_info->socket, err, addr, cluster->tls_ctx,
													host->tls_name, deadline);

	if (status) {
		as_lookup_record_connect_failure(cluster, &node_info->socket);
		return status;
	}

	return as_lookup_node_validate(cluster, err, host, addr, detect_load_balancer, node_info,
		alternate_missing, deadline);
}

as_status
as_lookup_validate_any(
	as_cluster* cluster, as_error* err, as_host* host, as_address_iterator* iter,
	as_lookup_accept_fn accept, void* udata, as_node_info* node_info, bool* alternate_missing
	)
{
	if (alternate_missing) {
		*alternate_missing = false;
	}

	uint32_t n = 0;

	for (struct addrinfo* ai = iter->current; ai; ai = ai->ai_next) {
		n++;
	}

	struct sockaddr* addr;

	if (n <= 1) {
		if (! as_lookup_next(iter, &addr)) {
			return as_error_update(err, AEROSPIKE_ERR_INVALID_HOST,
				"Failed to find addresses for %s %d", host->name, host->port);
		}

		as_status status = as_lookup_node(cluster, err, host, addr, false, node_info,
			alternate_missing);

		if (status != AEROSPIKE_OK) {
			return status;
		}

		switch (as_lookup_check(accept, udata, node_info)) {
			case AS_LOOKUP_ACCEPT:
				return AEROSPIKE_OK;
			case AS_LOOKUP_STOP:
				return as_lookup_stopped(err, host, addr);
			default:
				return as_lookup_not_accepted(err, host);
		}
	}

	as_lookup_probe* probes = cf_malloc(n * sizeof(as_lookup_probe));
	uint32_t count = 0;

	while (count < n && as_lookup_next(iter, &addr)) {
		as_lookup_probe* probe = &probes[count++];
		as_address_copy_storage(addr, &probe->addr);
		probe->open = false;
		probe->ready = false;
	}

	// The address last validated for the host first, then resolver order.
	struct sockaddr_storage* preferred = as_lookup_find_valid_address(cluster, host);

	if (preferred) {
		for (uint32_t i = 1; i < count; i++) {
			if (as_address_equals((struct sockaddr*)&probes[i].addr, (struct sockaddr*)preferred)) {
				as_lookup_probe probe = probes[i];
				memmove(&probes[1], &probes[0], i * sizeof(as_lookup_probe));
				probes[0] = probe;
				break;
			}
		}
	}

	uint32_t bound = (cluster->conn_timeout_ms > 0 && cluster->conn_timeout_ms < cluster->tend_interval) ?
		cluster->conn_timeout_ms : cluster->tend_interval;
	uint32_t delay_ms = bound / n;

	if (delay_ms > AS_LOOKUP_STAGGER_DELAY) {
		delay_ms = AS_LOOKUP_STAGGER_DELAY;
	}

	uint64_t delay = (uint64_t)delay_ms * 1000 * 1000;
	uint64_t deadline = cf_getns() + (uint64_t)bound * 1000 * 1000;
	as_lookup_probe winner;
	as_lookup_failure failure;
	failure.set = false;
	bool has_error = false;
	uint32_t rejected = 0;

	while (as_lookup_connect_first(cluster, host, probes, &count, delay, deadline, &winner,
		&failure)) {
		struct sockaddr* winner_addr = (struct sockaddr*)&winner.addr;
		as_error error_local;
		as_error_init(&error_local);
		bool alt = false;
		const char* connect_failure;
		as_status status = as_lookup_node_connected(cluster, &error_local, host, winner_addr,
			winner.fd, deadline, node_info, &alt, &connect_failure);

		if (status == AEROSPIKE_OK) {
			as_lookup_verdict verdict = as_lookup_check(accept, udata, node_info);

			if (verdict == AS_LOOKUP_ACCEPT) {
				as_lookup_set_valid_address(cluster, host, winner_addr, node_info->name);
				cf_free(probes);
				return AEROSPIKE_OK;
			}

			if (verdict == AS_LOOKUP_STOP) {
				cf_free(probes);
				return has_error ? err->code : as_lookup_stopped(err, host, winner_addr);
			}
			rejected++;
			continue;
		}

		as_lookup_log_failure(host, winner_addr, error_local.message);

		if (connect_failure) {
			as_lookup_set_failure(&failure, winner_addr, connect_failure);
			continue;
		}

		if (! has_error) {
			as_error_copy(err, &error_local);

			if (alternate_missing) {
				*alternate_missing = alt;
			}
			has_error = true;
		}
	}
	cf_free(probes);

	if (has_error) {
		return err->code;
	}

	if (rejected > 0) {
		return as_lookup_not_accepted(err, host);
	}

	char detail[AS_IP_ADDRESS_SIZE + sizeof(failure.message) + 32];
	detail[0] = 0;

	if (failure.set) {
		char addr_name[AS_IP_ADDRESS_SIZE];
		as_address_short_name((struct sockaddr*)&failure.addr, addr_name, sizeof(addr_name));
		snprintf(detail, sizeof(detail), "; first failure: %s %d %s", addr_name, host->port,
			failure.message);
	}

	return as_error_update(err, AEROSPIKE_ERR_CONNECTION,
		"No connection to %s %d completed for %u addresses within %ums%s",
		host->name, host->port, n, bound, detail);
}

void
as_lookup_prune_valid_addresses(as_cluster* cluster, as_nodes* nodes)
{
	as_vector* list = cluster->valid_addresses;

	if (! list) {
		return;
	}

	uint32_t keep = 0;

	for (uint32_t i = 0; i < list->size; i++) {
		as_lookup_valid_address* va = as_vector_get(list, i);
		bool live = false;

		for (uint32_t j = 0; j < nodes->size && ! live; j++) {
			live = strcmp(nodes->array[j]->name, va->node_name) == 0;
		}

		if (live) {
			if (keep != i) {
				as_vector_set(list, keep, va);
			}
			keep++;
		}
	}
	list->size = keep;
}

void
as_lookup_destroy_valid_addresses(as_cluster* cluster)
{
	if (cluster->valid_addresses) {
		as_vector_destroy(cluster->valid_addresses);
		cluster->valid_addresses = NULL;
	}
}
