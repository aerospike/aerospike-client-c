/*
 *  Citrusleaf Foundation
 *  include/cf_socket.h - Some helpers for IO
 *
 *  Copyright 2008 by Citrusleaf.  All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 */

#pragma once

#include <stddef.h>
#include <stdint.h>


#ifndef CF_WINDOWS
//====================================================================
// Linux
//

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


#else // CF_WINDOWS
//====================================================================
// Windows
//

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
