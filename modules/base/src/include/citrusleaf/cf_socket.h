/*
 * Copyright 2008-2014 Aerospike, Inc.
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
#pragma once

#include <stddef.h>
#include <stdint.h>

#if defined(__linux__) || defined(__APPLE__)
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

// Windows send() and recv() parameter types are different.
#define cf_socket_data_t void
#define cf_socket_size_t size_t

#define cf_close(fd) (close(fd))

//------------------------------------------------
// The API below is not used by the libevent2
// client, so we'll postpone the Windows version.
//
extern int
cf_socket_read_timeout(int fd, uint8_t *buf, size_t buf_len, uint64_t trans_deadline, int attempt_ms);
extern int
cf_socket_write_timeout(int fd, uint8_t *buf, size_t buf_len, uint64_t trans_deadline, int attempt_ms);
extern int
cf_socket_read_forever(int fd, uint8_t *buf, size_t buf_len);
extern int
cf_socket_write_forever(int fd, uint8_t *buf, size_t buf_len);

extern void
cf_print_sockaddr_in(char *prefix, struct sockaddr_in *sa_in);

static inline void
as_socket_address_name(struct sockaddr_in* address, char* name)
{
	inet_ntop(AF_INET, &(address->sin_addr), name, INET_ADDRSTRLEN);
}

#endif

#if defined(__APPLE__)
#define MSG_NOSIGNAL SO_NOSIGPIPE
#endif

#if defined(CF_WINDOWS)
#include <WinSock2.h>
#include <Ws2tcpip.h>

#define cf_socket_data_t char
#define cf_socket_size_t int

#define cf_close(fd) (closesocket(fd))

#define MSG_DONTWAIT	0
#define MSG_NOSIGNAL	0

#define SHUT_RDWR		SD_BOTH
#endif // CF_WINDOWS


extern int
cf_socket_create_nb();
extern int
cf_socket_start_connect_nb(int fd, struct sockaddr_in *sa);
extern int
cf_socket_create_and_connect_nb(struct sockaddr_in *sa);
