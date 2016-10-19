/*
 * Copyright 2016 Aerospike, Inc.
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

#include <ctype.h>
#include <pthread.h>

#include <openssl/conf.h>
#include <openssl/crypto.h>
#include <openssl/engine.h>
#include <openssl/err.h>
#include <openssl/ssl.h>

#include <aerospike/as_log_macros.h>
#include <aerospike/as_socket.h>
#include <aerospike/as_tls.h>
#include <citrusleaf/cf_clock.h>

static void* cert_blacklist_read(const char * path);
static bool cert_blacklist_check(void* cert_blacklist,
								 const char* snhex,
								 const char* issuer_name);
static void cert_blacklist_destroy(void* cert_blacklist);

static bool s_tls_inited = false;
static pthread_mutex_t s_tls_init_mutex = PTHREAD_MUTEX_INITIALIZER;
static int s_ex_name_index = -1;
static int s_ex_ctxt_index = -1;

typedef enum as_tls_protocol_e {
	// SSLv2 is always disabled per RFC 6176, we maintain knowledge of
	// it so we can give helpful error messages ...
	
	AS_TLS_PROTOCOL_SSLV2	= (1<<0),
	AS_TLS_PROTOCOL_SSLV3	= (1<<1),
	AS_TLS_PROTOCOL_TLSV1	= (1<<2),
	AS_TLS_PROTOCOL_TLSV1_1	= (1<<3),
	AS_TLS_PROTOCOL_TLSV1_2	= (1<<4),

	AS_TLS_PROTOCOL_NONE	= 0x00,

	AS_TLS_PROTOCOL_ALL		= (AS_TLS_PROTOCOL_SSLV3 |
							   AS_TLS_PROTOCOL_TLSV1 |
							   AS_TLS_PROTOCOL_TLSV1_1 |
							   AS_TLS_PROTOCOL_TLSV1_2),

	AS_TLS_PROTOCOL_DEFAULT	= (AS_TLS_PROTOCOL_TLSV1_2),
	
} as_tls_protocol;

static as_status
protocols_parse(as_config_tls* tlscfg, uint16_t* oprotocols, as_error* errp)
{
	// In case we don't make it all the way through ...
	*oprotocols = AS_TLS_PROTOCOL_NONE;
	
	// If no protocol_spec is provided, use a default value.
	if (! tlscfg->protocols) {
		*oprotocols = AS_TLS_PROTOCOL_DEFAULT;
		return AEROSPIKE_OK;
	}

	uint16_t protocols = AS_TLS_PROTOCOL_NONE;
	
	char const* delim = " \t";
	char* saveptr = NULL;
	for (char* tok = strtok_r(tlscfg->protocols, delim, &saveptr);
		 tok != NULL;
		 tok = strtok_r(NULL, delim, &saveptr)) {
		char act = '\0';
		uint16_t current;

		// Is there a +/- prefix?
		if ((*tok == '+') || (*tok == '-')) {
			act = *tok;
			++tok;
		}
		
		if (strcasecmp(tok, "SSLv2") == 0) {
			return as_error_set_message(errp, AEROSPIKE_ERR_TLS_ERROR,
										"SSLv2 not supported (RFC 6176)");
		}
		else if (strcasecmp(tok, "SSLv3") == 0) {
			current = AS_TLS_PROTOCOL_SSLV3;
		}
		else if (strcasecmp(tok, "TLSv1") == 0) {
			current = AS_TLS_PROTOCOL_TLSV1;
		}
		else if (strcasecmp(tok, "TLSv1.1") == 0) {
			current = AS_TLS_PROTOCOL_TLSV1_1;
		}
		else if (strcasecmp(tok, "TLSv1.2") == 0) {
			current = AS_TLS_PROTOCOL_TLSV1_2;
		}
		else if (strcasecmp(tok, "all") == 0) {
			current = AS_TLS_PROTOCOL_ALL;
		}
		else {
			return as_error_update(errp, AEROSPIKE_ERR_TLS_ERROR,
								   "unknown TLS protocol %s", tok);
		}

		if (act == '-') {
			protocols &= ~current;
        }
        else if (act == '+') {
            protocols |= current;
        }
		else {
			if (protocols != AS_TLS_PROTOCOL_NONE) {
				return as_error_update(errp, AEROSPIKE_ERR_TLS_ERROR,
						   "TLS protocol %s overrides already set parameters."
									   "Check if a +/- prefix is missing ...",
									   tok);
			}
			protocols = current;
		}
	}

	*oprotocols = protocols;
	return AEROSPIKE_OK;
}

#define STACK_LIMIT (16 * 1024)

static inline size_t
as_fdset_size(int fd)
{
	// Roundup fd in increments of FD_SETSIZE and convert to bytes.
	return ((fd / FD_SETSIZE) + 1) * FD_SETSIZE / 8;
}

static inline void
as_fd_set(int fd, fd_set *fdset)
{
	FD_SET(fd%FD_SETSIZE, &fdset[fd/FD_SETSIZE]);
}

static inline int
as_fd_isset(int fd, fd_set *fdset)
{
	return FD_ISSET(fd%FD_SETSIZE, &fdset[fd/FD_SETSIZE]);
}

static int
wait_readable(int fd, uint64_t deadline)
{
	int rv;
	size_t rset_size = as_fdset_size(fd);
	fd_set* rset = (fd_set*)(rset_size > STACK_LIMIT
							 ? cf_malloc(rset_size)
							 : alloca(rset_size));

	while (true) {
		uint64_t now = cf_getms();
		if (now > deadline) {
			rv = 1;
			goto cleanup;
        }
        
		uint64_t ms_left = deadline - now;

		struct timeval tv;
		tv.tv_sec = ms_left / 1000;
		tv.tv_usec = (ms_left % 1000) * 1000;

		memset((void*)rset, 0, rset_size);
		as_fd_set(fd, rset);

		rv = select(fd+1, rset /*readfd*/, 0 /*writefd*/, 0 /*oobfd*/, &tv);

		if (rv > 0 && as_fd_isset(fd, rset)) {
			rv = 0;
			goto cleanup;
		}

		if (rv < 0) {
			goto cleanup;
		}
	}

 cleanup:
	if (rset_size > STACK_LIMIT) {
		cf_free(rset);
	}
	return rv;
}

static int
wait_writable(int fd, uint64_t deadline)
{
	int rv;
	size_t wset_size = as_fdset_size(fd);
	fd_set* wset = (fd_set*)(wset_size > STACK_LIMIT
							 ? cf_malloc(wset_size)
							 : alloca(wset_size));

	while (true) {
		uint64_t now = cf_getms();
		if (now > deadline) {
			rv = 1;
			goto cleanup;
        }
        
		uint64_t ms_left = deadline - now;

		struct timeval tv;
		tv.tv_sec = ms_left / 1000;
		tv.tv_usec = (ms_left % 1000) * 1000;

		memset((void*)wset, 0, wset_size);
		as_fd_set(fd, wset);

		rv = select(fd+1, 0 /*readfd*/, wset /*writefd*/, 0 /*oobfd*/, &tv);

		if (rv > 0 && as_fd_isset(fd, wset)) {
			rv = 0;
			goto cleanup;
		}

		if (rv < 0) {
			goto cleanup;
		}
	}

 cleanup:
	if (wset_size > STACK_LIMIT) {
		cf_free(wset);
	}
	return rv;
}

static pthread_mutex_t *lock_cs;

static void
pthreads_locking_callback(int mode, int type, const char *file, int line)
{
    if (mode & CRYPTO_LOCK) {
        pthread_mutex_lock(&(lock_cs[type]));
    } else {
        pthread_mutex_unlock(&(lock_cs[type]));
    }
}

static void
pthreads_thread_id(CRYPTO_THREADID *tid)
{
    CRYPTO_THREADID_set_numeric(tid, (unsigned long)pthread_self());
}

static void
threading_setup(void)
{
    int i;

    lock_cs = cf_malloc(CRYPTO_num_locks() * sizeof(pthread_mutex_t));
    for (i = 0; i < CRYPTO_num_locks(); i++) {
        pthread_mutex_init(&(lock_cs[i]), NULL);
    }

    CRYPTO_THREADID_set_callback(pthreads_thread_id);
    CRYPTO_set_locking_callback(pthreads_locking_callback);
}

static void
threading_cleanup(void)
{
    int i;

    CRYPTO_set_locking_callback(NULL);
    for (i = 0; i < CRYPTO_num_locks(); i++) {
        pthread_mutex_destroy(&(lock_cs[i]));
    }
    cf_free(lock_cs);
}

void
as_tls_check_init()
{
	// Bail if we've already initialized.
	if (s_tls_inited) {
		return;
	}

	// Acquire the initialization mutex.
	pthread_mutex_lock(&s_tls_init_mutex);

	// Check the flag again, in case we lost a race.
	if (! s_tls_inited) {
		OpenSSL_add_all_algorithms();
		ERR_load_BIO_strings();
		ERR_load_crypto_strings();
		SSL_load_error_strings();
		SSL_library_init();

		threading_setup();

		s_ex_name_index = SSL_get_ex_new_index(0, NULL, NULL, NULL, NULL);
		s_ex_ctxt_index = SSL_get_ex_new_index(0, NULL, NULL, NULL, NULL);
		
		__sync_synchronize();
		
		s_tls_inited = true;

		// Install an atexit handler to cleanup.
		atexit(as_tls_cleanup);
	}

	pthread_mutex_unlock(&s_tls_init_mutex);
}

void
as_tls_cleanup()
{
	// Skip if we were never initialized.
	if (! s_tls_inited) {
		return;
	}

	// Cleanup global OpenSSL state, must be after all other OpenSSL
	// API calls, of course ...
	
	threading_cleanup();

	// https://wiki.openssl.org/index.php/Library_Initialization#Cleanup
	//
	FIPS_mode_set(0);
	ENGINE_cleanup();
	CONF_modules_unload(1);
	EVP_cleanup();
	as_tls_thread_cleanup();
	CRYPTO_cleanup_all_ex_data();
	ERR_free_strings();

	// http://stackoverflow.com/questions/29845527/how-to-properly-uninitialize-openssl
	STACK_OF(SSL_COMP) *ssl_comp_methods = SSL_COMP_get_compression_methods();
	if (ssl_comp_methods != NULL) {
        sk_SSL_COMP_free(ssl_comp_methods);
	}
}

void
as_tls_thread_cleanup()
{
	// Skip if we were never initialized.
	if (! s_tls_inited) {
		return;
	}

	ERR_remove_state(0);
}

static int verify_callback(int preverify_ok, X509_STORE_CTX* ctx)
{
	// If the cert has already failed we're done.
	if (! preverify_ok) {
		return preverify_ok;
	}

    SSL* ssl = X509_STORE_CTX_get_ex_data(
					ctx, SSL_get_ex_data_X509_STORE_CTX_idx());

	// The verify callback is called for each cert in the chain.
	
    X509* current_cert = X509_STORE_CTX_get_current_cert(ctx);

	as_tls_context* asctxt = SSL_get_ex_data(ssl, s_ex_ctxt_index);
	if (! asctxt) {
		as_log_warn("Missing as_tls_context in TLS verify callback");
		return 0;
	}

	if (asctxt->cert_blacklist) {
		// Is this cert blacklisted?
		char name[256];
		X509_NAME* iname = X509_get_issuer_name(current_cert);
		X509_NAME_oneline(iname, name, sizeof(name));
		
		ASN1_INTEGER* sn = X509_get_serialNumber(current_cert);
		BIGNUM* snbn = ASN1_INTEGER_to_BN(sn, NULL);
		char* snhex = BN_bn2hex(snbn);

		as_log_info("CERT: %s %s", snhex, name);

		bool blacklisted =
			cert_blacklist_check(asctxt->cert_blacklist, snhex, name);

		OPENSSL_free(snhex);
		BN_free(snbn);

		if (blacklisted) {
			as_log_warn("CERT: BLACKLISTED");
			return 0;
		}
	}

	// If this is the peer cert, check the name
	if (current_cert == ctx->cert) {
		char * hostname = SSL_get_ex_data(ssl, s_ex_name_index);

		if (! hostname) {
			as_log_warn("Missing hostname in TLS verify callback");
			return 0;
		}

		bool allow_wildcard = true;
		bool matched = as_tls_match_name(ctx->cert, hostname, allow_wildcard);
		as_log_info("NAME: '%s' %s",
					hostname, matched ? "MATCHED" : "NO MATCH");
		return matched ? 1 : 0;
	}

	// If we make it here we are a root or chain cert and are not
	// blacklisted.
	return 1;
}

as_status
as_tls_context_setup(as_config_tls* tlscfg,
					 as_tls_context* octx,
					 as_error* errp)
{
	// Clear the destination, in case we don't make it.
	octx->ssl_ctx = NULL;
	octx->log_session_info = false;
	octx->cert_blacklist = NULL;
	
	if (! tlscfg->enable) {
		return AEROSPIKE_OK;
	}

#if defined(AS_USE_LIBUV)
	return as_error_update(errp, AEROSPIKE_ERR_TLS_ERROR,
		"TLS not supported with libuv");
#endif
	
	// Only initialize OpenSSL if we've enabled TLS.
	as_tls_check_init();
	
	void* cert_blacklist = NULL;
	if (tlscfg->cert_blacklist) {
		cert_blacklist = cert_blacklist_read(tlscfg->cert_blacklist);
		if (! cert_blacklist) {
			return as_error_update(errp, AEROSPIKE_ERR_TLS_ERROR,
								   "Failed to read certificate blacklist: %s",
								   tlscfg->cert_blacklist);
		}
	}
	
	uint16_t protocols = AS_TLS_PROTOCOL_NONE;

	as_status status = protocols_parse(tlscfg, &protocols, errp);
	if (status != AEROSPIKE_OK) {
		cert_blacklist_destroy(cert_blacklist);
		return status;
	}

	const SSL_METHOD* method = NULL;

	// If the selected protocol set is a single protocol we
	// can use a specific method.
	//
	if (protocols == AS_TLS_PROTOCOL_SSLV3) {
		method = SSLv3_client_method();
	}
	else if (protocols == AS_TLS_PROTOCOL_TLSV1) {
		method = TLSv1_client_method();
	}
	else if (protocols == AS_TLS_PROTOCOL_TLSV1_1) {
		method = TLSv1_1_client_method();
	}
	else if (protocols == AS_TLS_PROTOCOL_TLSV1_2) {
		method = TLSv1_2_client_method();
	}
	else {
		// Multiple protocols are enabled, use a flexible method.
		method = SSLv23_client_method();
	}

	SSL_CTX* ctx = SSL_CTX_new(method);
	if (ctx == NULL) {
		cert_blacklist_destroy(cert_blacklist);
		unsigned long errcode = ERR_get_error();
		char errbuf[1024];
		ERR_error_string_n(errcode, errbuf, sizeof(errbuf));
		return as_error_update(errp, AEROSPIKE_ERR_TLS_ERROR,
							   "Failed to create new TLS context: %s", errbuf);
	}

	/* always disable SSLv2, as per RFC 6176 */
    SSL_CTX_set_options(ctx, SSL_OP_NO_SSLv2);

	// Turn off non-enabled protocols.
	if (! (protocols & AS_TLS_PROTOCOL_SSLV3)) {
        SSL_CTX_set_options(ctx, SSL_OP_NO_SSLv3);
    }
	if (! (protocols & AS_TLS_PROTOCOL_TLSV1)) {
        SSL_CTX_set_options(ctx, SSL_OP_NO_TLSv1);
    }
	if (! (protocols & AS_TLS_PROTOCOL_TLSV1_1)) {
        SSL_CTX_set_options(ctx, SSL_OP_NO_TLSv1_1);
    }
	if (! (protocols & AS_TLS_PROTOCOL_TLSV1_2)) {
        SSL_CTX_set_options(ctx, SSL_OP_NO_TLSv1_2);
    }
	
	if (tlscfg->cafile || tlscfg->capath) {
		int rv =
			SSL_CTX_load_verify_locations(ctx, tlscfg->cafile, tlscfg->capath);
		if (rv != 1) {
			cert_blacklist_destroy(cert_blacklist);
			SSL_CTX_free(ctx);
			char errbuf[1024];
			unsigned long errcode = ERR_get_error();
			if (errcode != 0) {
				ERR_error_string_n(errcode, errbuf, sizeof(errbuf));
				return as_error_update(errp, AEROSPIKE_ERR_TLS_ERROR,
									   "Failed to load CAFile: %s", errbuf);
			}
			return as_error_set_message(errp, AEROSPIKE_ERR_TLS_ERROR,
										"Unknown failure loading CAFile");
		}
	}

	if (tlscfg->chainfile) {
		int rv = SSL_CTX_use_certificate_chain_file(ctx, tlscfg->chainfile);
		if (rv != 1) {
			// We seem to be seeing this bug:
			// https://groups.google.com/
			//          forum/#!topic/mailing.openssl.users/nRvRzmKnEQA
			// If the rv is not 1 check the error stack; if it doesn't have an
			// error assume we are OK.
			//
			unsigned long errcode = ERR_peek_error();
			if (errcode != SSL_ERROR_NONE) {
				// There *was* an error after all.
				cert_blacklist_destroy(cert_blacklist);
				SSL_CTX_free(ctx);
				
				unsigned long errcode = ERR_get_error();
				char errbuf[1024];
				ERR_error_string_n(errcode, errbuf, sizeof(errbuf));
				return as_error_update(errp, AEROSPIKE_ERR_TLS_ERROR,
							  "SSL_CTX_use_certificate_chain_file failed: %s",
									   errbuf);
			}
		}
	}

	if (tlscfg->keyfile) {
		int rv = SSL_CTX_use_RSAPrivateKey_file(ctx, tlscfg->keyfile,
											SSL_FILETYPE_PEM);
		if (rv != 1) {
				cert_blacklist_destroy(cert_blacklist);
				SSL_CTX_free(ctx);
				
				unsigned long errcode = ERR_get_error();
				char errbuf[1024];
				ERR_error_string_n(errcode, errbuf, sizeof(errbuf));
				return as_error_update(errp, AEROSPIKE_ERR_TLS_ERROR,
							  "SSL_CTX_use_RSAPrivateKey_file failed: %s",
									   errbuf);
		}
	}

	if (tlscfg->cipher_suite) {
		int rv = SSL_CTX_set_cipher_list(ctx, tlscfg->cipher_suite);
		if (rv != 1) {
			cert_blacklist_destroy(cert_blacklist);
			SSL_CTX_free(ctx);
			return as_error_set_message(errp, AEROSPIKE_ERR_TLS_ERROR,
										"no compatible cipher found");
		}
		// It's bogus that we have to create an SSL just to get the
		// cipher list, but SSL_CTX_get_ciphers doesn't appear to
		// exist ...
		SSL * ssl = SSL_new(ctx);
		for (int prio = 0; true; ++prio) {
			char const * cipherstr = SSL_get_cipher_list(ssl, prio);
			if (!cipherstr) {
				break;
			}
			as_log_info("cipher %d: %s", prio+1, cipherstr);
		}
		SSL_free(ssl);
	}

	if (tlscfg->crl_check || tlscfg->crl_check_all) {
		X509_VERIFY_PARAM* param = X509_VERIFY_PARAM_new();
		unsigned long flags = X509_V_FLAG_CRL_CHECK;
		if (tlscfg->crl_check_all) {
			flags |= X509_V_FLAG_CRL_CHECK_ALL;
		}
		X509_VERIFY_PARAM_set_flags(param, flags);
		SSL_CTX_set1_param(ctx, param);
		X509_VERIFY_PARAM_free(param);
	}

	if (tlscfg->encrypt_only) {
		SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, NULL);
	} else {
		SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, verify_callback);
	}

	octx->ssl_ctx = ctx;
	octx->log_session_info = tlscfg->log_session_info;
	octx->cert_blacklist = cert_blacklist;
	return AEROSPIKE_OK;
}

void
as_tls_context_destroy(as_tls_context* ctx)
{
	if (ctx->cert_blacklist) {
		cert_blacklist_destroy(ctx->cert_blacklist);
	}
	
	if (ctx->ssl_ctx) {
		SSL_CTX_free(ctx->ssl_ctx);
	}
}

int
as_tls_wrap(as_tls_context* ctx, as_socket* sock, const char* tls_name)
{
	sock->ctx = ctx;
	sock->tls_name = tls_name;

	sock->ssl = SSL_new(ctx->ssl_ctx);
	if (sock->ssl == NULL)
		return -1;

	SSL_set_fd(sock->ssl, sock->fd);

	// Note - it's tempting to try and point at the as_socket with the
	// SSL ex_data instead of pointing at it's fields.  It doesn't
	// work because the socket is copied by value (ie memcpy) in
	// multiple places ...
	
	SSL_set_ex_data(sock->ssl, s_ex_name_index, (void*)sock->tls_name);
	SSL_set_ex_data(sock->ssl, s_ex_ctxt_index, ctx);
	
	return 0;
}

void
as_tls_set_name(as_socket* sock, const char* tls_name)
{
	sock->tls_name = tls_name;
	SSL_set_ex_data(sock->ssl, s_ex_name_index, (void*)tls_name);
}

static void
log_session_info(as_socket* sock)
{
	if (! sock->ctx->log_session_info)
		return;
	
	SSL_CIPHER const* cipher = SSL_get_current_cipher(sock->ssl);
	if (cipher) {
		char desc[1024];
		SSL_CIPHER_description(cipher, desc, sizeof(desc));
		size_t len = strlen(desc);
		if (len > 0) {
			desc[len-1] = '\0';	// Trim trailing \n
		}
		as_log_info("TLS cipher: %s", desc);
	}
	else {
		as_log_warn("TLS no current cipher");
	}
}

static void
log_verify_details(as_socket* sock)
{
	long vr = SSL_get_verify_result(sock->ssl);
	if (vr != X509_V_OK) {
		as_log_info("TLS verify result: %s", X509_verify_cert_error_string(vr));
	}
}

int
as_tls_connect_once(as_socket* sock)
{
	int rv = SSL_connect(sock->ssl);
	if (rv == 1) {
		log_session_info(sock);
		return 1;
	}

	int sslerr = SSL_get_error(sock->ssl, rv);
	unsigned long errcode;
	char errbuf[1024];
	switch (sslerr) {
	case SSL_ERROR_WANT_READ:
		return -1;
	case SSL_ERROR_WANT_WRITE:
		return -2;
	case SSL_ERROR_SSL:
		log_verify_details(sock);
		errcode = ERR_get_error();
		ERR_error_string_n(errcode, errbuf, sizeof(errbuf));
		as_log_warn("SSL_connect_once failed: %s", errbuf);
		return -3;
	case SSL_ERROR_SYSCALL:
		errcode = ERR_get_error();
		if (errcode != 0) {
			ERR_error_string_n(errcode, errbuf, sizeof(errbuf));
			as_log_warn("SSL_connect_once I/O error: %s", errbuf);
		}
		else {
			if (rv == 0) {
				as_log_warn("SSL_connect_once I/O error: unexpected EOF");
			}
			else {
				as_log_warn("SSL_connect_once I/O error: %s", strerror(errno));
			}
		}
		return -4;
	default:
		as_log_warn("SSL_connect_once: unexpected ssl error: %d", sslerr);
		return -5;
		break;
	}
}

int
as_tls_connect(as_socket* sock)
{
	uint64_t deadline = cf_getms() + 60000;

	while (true) {
		int rv = SSL_connect(sock->ssl);
		if (rv == 1) {
			log_session_info(sock);
			return 0;
		}

		int sslerr = SSL_get_error(sock->ssl, rv);
		unsigned long errcode;
		char errbuf[1024];
		switch (sslerr) {
		case SSL_ERROR_WANT_READ:
			rv = wait_readable(sock->fd, deadline);
			if (rv != 0) {
				as_log_warn("wait_readable failed: %d", errno);
				return rv;
			}
			// loop back around and retry
			break;
		case SSL_ERROR_WANT_WRITE:
			rv = wait_writable(sock->fd, deadline);
			if (rv != 0) {
				as_log_warn("wait_writables failed: %d", errno);
				return rv;
			}
			// loop back around and retry
			break;
		case SSL_ERROR_SSL:
			log_verify_details(sock);
			errcode = ERR_get_error();
			ERR_error_string_n(errcode, errbuf, sizeof(errbuf));
			as_log_warn("SSL_connect failed: %s", errbuf);
			return -1;
		case SSL_ERROR_SYSCALL:
			errcode = ERR_get_error();
			if (errcode != 0) {
				ERR_error_string_n(errcode, errbuf, sizeof(errbuf));
				as_log_warn("SSL_connect I/O error: %s", errbuf);
			}
			else {
				if (rv == 0) {
					as_log_warn("SSL_connect I/O error: unexpected EOF");
				}
				else {
					as_log_warn("SSL_connect I/O error: %s", strerror(errno));
				}
			}
			return -2;
		default:
			as_log_warn("SSL_connect: unexpected ssl error: %d", sslerr);
			return -3;
			break;
		}
	}
}

int
as_tls_peek(as_socket* sock, void* buf, int num)
{
	uint64_t deadline = cf_getms() + 60000;

	while (true) {
		int rv = SSL_peek(sock->ssl, buf, num);
		if (rv >= 0) {
			return rv;
		}

		int sslerr = SSL_get_error(sock->ssl, rv);
		unsigned long errcode;
		char errbuf[1024];
		switch (sslerr) {
		case SSL_ERROR_WANT_READ:
			// Just return 0, there isn't any data.
			return 0;
		case SSL_ERROR_WANT_WRITE:
			rv = wait_writable(sock->fd, deadline);
			if (rv != 0) {
				return rv;
			}
			// loop back around and retry
			break;
		case SSL_ERROR_SSL:
			log_verify_details(sock);
			errcode = ERR_get_error();
			ERR_error_string_n(errcode, errbuf, sizeof(errbuf));
			as_log_warn("SSL_peek failed: %s", errbuf);
			return -1;
		case SSL_ERROR_SYSCALL:
			errcode = ERR_get_error();
			if (errcode != 0) {
				ERR_error_string_n(errcode, errbuf, sizeof(errbuf));
				as_log_warn("SSL_peek I/O error: %s", errbuf);
			}
			else {
				if (rv == 0) {
					as_log_warn("SSL_peek I/O error: unexpected EOF");
				}
				else {
					as_log_warn("SSL_peek I/O error: %s", strerror(errno));
				}
			}
			return -1;
		default:
			as_log_warn("SSL_peek: unexpected ssl error: %d", sslerr);
			return -1;
			break;
		}
	}
}

int
as_tls_read_pending(as_socket* sock)
{
	// Return the number of pending bytes in the TLS encryption
	// buffer.  If we aren't using TLS return 0.
	//
	return sock->ctx ? SSL_pending(sock->ssl) : 0;
}

int
as_tls_read_once(as_socket* sock, void* buf, size_t len)
{
	int rv = SSL_read(sock->ssl, buf, (int)len);
	if (rv > 0) {
		return rv;
	}
	else {
		int sslerr = SSL_get_error(sock->ssl, rv);
		unsigned long errcode;
		char errbuf[1024];
		switch (sslerr) {
		case SSL_ERROR_WANT_READ:
			return -1;
		case SSL_ERROR_WANT_WRITE:
			return -2;
		case SSL_ERROR_SSL:
			log_verify_details(sock);
			errcode = ERR_get_error();
			ERR_error_string_n(errcode, errbuf, sizeof(errbuf));
			as_log_warn("SSL_read_once failed: %s", errbuf);
			return -3;
		case SSL_ERROR_SYSCALL:
			errcode = ERR_get_error();
			if (errcode != 0) {
				ERR_error_string_n(errcode, errbuf, sizeof(errbuf));
				as_log_warn("SSL_read_once I/O error: %s", errbuf);
			}
			else {
				if (rv == 0) {
					as_log_warn("SSL_read_once I/O error: unexpected EOF");
				}
				else {
					as_log_warn("SSL_read_once I/O error: %s", strerror(errno));
				}
			}
			return -4;
		default:
			as_log_warn("SSL_read_once: unexpected ssl error: %d", sslerr);
			return -5;
		}
	}
}

int
as_tls_read(as_socket* sock, void* bufp, size_t len, uint64_t deadline)
{
	uint8_t* buf = (uint8_t *) bufp;
	size_t pos = 0;

	while (true) {
		int rv = SSL_read(sock->ssl, buf + pos, (int)(len - pos));
		if (rv > 0) {
			pos += rv;
			if (pos >= len) {
				return 0;
			}
		}
		else /* if (rv <= 0) */ {
			int sslerr = SSL_get_error(sock->ssl, rv);
			unsigned long errcode;
			char errbuf[1024];
			switch (sslerr) {
			case SSL_ERROR_WANT_READ:
				rv = wait_readable(sock->fd, deadline);
				if (rv != 0) {
					return rv;
				}
				// loop back around and retry
				break;
			case SSL_ERROR_WANT_WRITE:
				rv = wait_writable(sock->fd, deadline);
				if (rv != 0) {
					return rv;
				}
				// loop back around and retry
				break;
			case SSL_ERROR_SSL:
				log_verify_details(sock);
				errcode = ERR_get_error();
				ERR_error_string_n(errcode, errbuf, sizeof(errbuf));
				as_log_warn("SSL_read failed: %s", errbuf);
				return -1;
			case SSL_ERROR_SYSCALL:
				errcode = ERR_get_error();
				if (errcode != 0) {
					ERR_error_string_n(errcode, errbuf, sizeof(errbuf));
					as_log_warn("SSL_read I/O error: %s", errbuf);
				}
				else {
					if (rv == 0) {
						as_log_warn("SSL_read I/O error: unexpected EOF");
					}
					else {
						as_log_warn("SSL_read I/O error: %s", strerror(errno));
					}
				}
				return -1;
			default:
				as_log_warn("SSL_read: unexpected ssl error: %d", sslerr);
				return -1;
				break;
			}
		}
	}
}

int
as_tls_write_once(as_socket* sock, void* buf, size_t len)
{
	int rv = SSL_write(sock->ssl, buf, (int)len);
	if (rv > 0) {
		return rv;
	}
	else {
		int sslerr = SSL_get_error(sock->ssl, rv);
		unsigned long errcode;
		char errbuf[1024];
		switch (sslerr) {
		case SSL_ERROR_WANT_READ:
			return -1;
		case SSL_ERROR_WANT_WRITE:
			return -2;
		case SSL_ERROR_SSL:
			log_verify_details(sock);
			errcode = ERR_get_error();
			ERR_error_string_n(errcode, errbuf, sizeof(errbuf));
			as_log_warn("SSL_write_once failed: %s", errbuf);
			return -3;
		case SSL_ERROR_SYSCALL:
			errcode = ERR_get_error();
			if (errcode != 0) {
				ERR_error_string_n(errcode, errbuf, sizeof(errbuf));
				as_log_warn("SSL_write_once I/O error: %s", errbuf);
			}
			else {
				if (rv == 0) {
					as_log_warn("SSL_write_once I/O error: unexpected EOF");
				}
				else {
					as_log_warn("SSL_write_once I/O error: %s", strerror(errno));
				}
			}
			return -4;
		default:
			as_log_warn("SSL_write_once: unexpected ssl error: %d", sslerr);
			return -5;
		}
	}
}

int
as_tls_write(as_socket* sock, void* bufp, size_t len, uint64_t deadline)
{
	uint8_t* buf = (uint8_t *) bufp;
	size_t pos = 0;

	while (true) {
		int rv = SSL_write(sock->ssl, buf + pos, (int)(len - pos));
		if (rv > 0) {
			pos += rv;
			if (pos >= len) {
				return 0;
			}
		}
		else /* if (rv <= 0) */ {
			int sslerr = SSL_get_error(sock->ssl, rv);
			unsigned long errcode;
			char errbuf[1024];
			switch (sslerr) {
			case SSL_ERROR_WANT_READ:
				rv = wait_readable(sock->fd, deadline);
				if (rv != 0) {
					return rv;
				}
				// loop back around and retry
				break;
			case SSL_ERROR_WANT_WRITE:
				rv = wait_writable(sock->fd, deadline);
				if (rv != 0) {
					return rv;
				}
				// loop back around and retry
				break;
			case SSL_ERROR_SSL:
				log_verify_details(sock);
				errcode = ERR_get_error();
				ERR_error_string_n(errcode, errbuf, sizeof(errbuf));
				as_log_warn("SSL_write failed: %s", errbuf);
				return -1;
			case SSL_ERROR_SYSCALL:
				errcode = ERR_get_error();
				if (errcode != 0) {
					ERR_error_string_n(errcode, errbuf, sizeof(errbuf));
					as_log_warn("SSL_write I/O error: %s", errbuf);
				}
				else {
					if (rv == 0) {
						as_log_warn("SSL_write I/O error: unexpected EOF");
					}
					else {
						as_log_warn("SSL_write I/O error: %s", strerror(errno));
					}
				}
				return -1;
			default:
				as_log_warn("SSL_write: unexpected ssl error: %d", sslerr);
				return -1;
				break;
			}
		}
	}
}

typedef struct cert_spec_s {
	char const* hex_serial;
	char const* issuer_name;
} cert_spec;

typedef struct cert_blacklist_s {
	size_t ncerts;
	cert_spec certs[];
} cert_blacklist;

static int
cert_spec_compare(const void* ptr1, const void* ptr2)
{
	const cert_spec* csp1 = (const cert_spec*) ptr1;
	const cert_spec* csp2 = (const cert_spec*) ptr2;

	int rv = strcmp(csp1->hex_serial, csp2->hex_serial);
	if (rv != 0) {
		return rv;
	}

	if (csp1->issuer_name == NULL && csp2->issuer_name == NULL) {
		return 0;
	}
	
	if (csp1->issuer_name == NULL) {
		return -1;
	}

	if (csp2->issuer_name == NULL) {
		return 1;
	}

	return strcmp(csp1->issuer_name, csp2->issuer_name);
}

static void* cert_blacklist_read(const char * path)
{
	FILE* fp = fopen(path, "r");
	if (fp == NULL) {
		as_log_warn("Failed to open cert blacklist '%s': %s",
					path, strerror(errno));
		return NULL;
	}

	size_t capacity = 32;
	size_t sz = sizeof(cert_blacklist) + (capacity * sizeof(cert_spec));
	cert_blacklist* cbp = cf_malloc(sz);
	cbp->ncerts = 0;

	char buffer[1024];
	while (true) {
		char* line = fgets(buffer, sizeof(buffer), fp);
		if (! line) {
			break;
		}

		// Lines begining with a '#' are comments.
		if (line[0] == '#') {
			continue;
		}
		
		char* saveptr = NULL;
		char* hex_serial = strtok_r(line, " \t\r\n", &saveptr);
		if (! hex_serial) {
			continue;
		}

		// Skip all additional whitespace.
		while (isspace(*saveptr)) {
			++saveptr;
		}

		// Everything to the end of the line is issuer name.  Note we
		// do not consider whitespace a separator anymore.
		char* issuer_name = strtok_r(NULL, "\r\n", &saveptr);

		// Do we need more room?
		if (cbp->ncerts == capacity) {
			capacity *= 2;
			size_t sz = sizeof(cert_blacklist) + (capacity * sizeof(cert_spec));
			cbp = cf_realloc(cbp, sz);
		}

		cbp->certs[cbp->ncerts].hex_serial = cf_strdup(hex_serial);
		cbp->certs[cbp->ncerts].issuer_name =
			issuer_name ? cf_strdup(issuer_name) : NULL;

		cbp->ncerts++;
	}

	qsort(cbp->certs, cbp->ncerts, sizeof(cert_spec), cert_spec_compare);

	fclose(fp);
	
	return cbp;
}

static bool cert_blacklist_check(void* cbl,
								 const char* hex_serial,
								 const char* issuer_name)
{
	cert_blacklist* cbp = (cert_blacklist*) cbl;

	cert_spec key;

	// First check for just the serial number.
	key.hex_serial = hex_serial;
	key.issuer_name = NULL;
	if (bsearch(&key, cbp->certs,
				cbp->ncerts, sizeof(cert_spec), cert_spec_compare)) {
		return true;
	}

	// Then check for an exact match w/ issuer name as well.
	key.hex_serial = hex_serial;
	key.issuer_name = issuer_name;
	if (bsearch(&key, cbp->certs,
				cbp->ncerts, sizeof(cert_spec), cert_spec_compare)) {
		return true;
	}

	return false;
}

static void cert_blacklist_destroy(void* cbl)
{
	cert_blacklist* cbp = (cert_blacklist*) cbl;
	if (! cbp) {
		return;
	}
	
	for (size_t ii = 0; ii < cbp->ncerts; ++ii) {
		cert_spec* csp = &cbp->certs[ii];
		cf_free((void*) csp->hex_serial);
		if (csp->issuer_name) {
			cf_free((void*) csp->issuer_name);
		}
	}

	cf_free(cbp);
}
