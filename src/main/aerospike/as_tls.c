/*
 * Copyright 2008-2024 Aerospike, Inc.
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
#include <aerospike/as_tls.h>
#include <aerospike/as_atomic.h>
#include <aerospike/as_log_macros.h>
#include <aerospike/as_poll.h>
#include <aerospike/ssl_util.h>
#include <citrusleaf/cf_clock.h>
#include <openssl/conf.h>
#include <openssl/crypto.h>
#include <openssl/engine.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <ctype.h>
#include <pthread.h>
#include <signal.h>

static void* cert_blacklist_read(const char * path);
static bool cert_blacklist_check(void* cert_blacklist,
								 const char* snhex,
								 const char* issuer_name);
static void cert_blacklist_destroy(void* cert_blacklist);

static void manage_sigpipe(void);

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

	AS_TLS_PROTOCOL_ALL		= (AS_TLS_PROTOCOL_TLSV1 |
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
			return as_error_set_message(errp, AEROSPIKE_ERR_TLS_ERROR,
										"SSLv3 not supported");
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

static int
wait_socket(as_socket_fd fd, uint32_t socket_timeout, uint64_t deadline, bool read)
{
	as_poll poll;
	as_poll_init(&poll, fd);

	uint32_t timeout;
	int rv;

	while (true) {
		if (deadline > 0) {
			uint64_t now = cf_getms();

			if (now >= deadline) {
				rv = 1;  // timeout
				break;
			}

			timeout = (uint32_t)(deadline - now);

			if (socket_timeout > 0 && socket_timeout < timeout) {
				timeout = socket_timeout;
			}
		}
		else {
			timeout = socket_timeout;
		}

		rv = as_poll_socket(&poll, fd, timeout, read);

		if (rv > 0) {
			rv = 0;  // success
			break;
		}

		if (rv < 0) {
			break;  // error
		}
		// rv == 0 timeout.  continue in case timed out before real timeout.
	}

	as_poll_destroy(&poll);
	return rv;
}

#if OPENSSL_VERSION_NUMBER < 0x10100000L && !defined USE_XDR
static pthread_mutex_t *lock_cs;

static void
pthreads_locking_callback(int mode, int type, const char *file, int line)
{
	if (mode & CRYPTO_LOCK) {
		pthread_mutex_lock(&(lock_cs[type]));
	}
	else {
		pthread_mutex_unlock(&(lock_cs[type]));
	}
}

static void
pthreads_thread_id(CRYPTO_THREADID *tid)
{
#if !defined(_MSC_VER)
	CRYPTO_THREADID_set_numeric(tid, (unsigned long)pthread_self());
#else
	CRYPTO_THREADID_set_pointer(tid, pthread_self().p);
#endif
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
#endif

void
as_tls_check_init(void)
{
	// Bail if we've already initialized.
	if (s_tls_inited) {
		return;
	}

	// Acquire the initialization mutex.
	pthread_mutex_lock(&s_tls_init_mutex);

	// Check the flag again, in case we lost a race.
	if (! s_tls_inited) {
#if OPENSSL_VERSION_NUMBER < 0x10100000L && !defined USE_XDR
		OpenSSL_add_all_algorithms();
		ERR_load_BIO_strings();
		ERR_load_crypto_strings();
		SSL_load_error_strings();
		SSL_library_init();

		threading_setup();

		// Install an atexit handler to cleanup.
		atexit(as_tls_cleanup);
#endif

		s_ex_name_index = SSL_get_ex_new_index(0, NULL, NULL, NULL, NULL);
		s_ex_ctxt_index = SSL_get_ex_new_index(0, NULL, NULL, NULL, NULL);
				
		s_tls_inited = true;
	}

	pthread_mutex_unlock(&s_tls_init_mutex);
}

void
as_tls_cleanup(void)
{
	// Skip if we were never initialized.
	if (! s_tls_inited) {
		return;
	}

#if !defined USE_XDR
	// Cleanup global OpenSSL state, must be after all other OpenSSL
	// API calls, of course ...

#if OPENSSL_VERSION_NUMBER < 0x10100000L
	threading_cleanup();
#endif

	// https://wiki.openssl.org/index.php/Library_Initialization#Cleanup
	//
#if OPENSSL_VERSION_NUMBER < 0x10100000L
	FIPS_mode_set(0);
#elif OPENSSL_VERSION_NUMBER == 0x10100000L
	FIPS_module_mode_set(0);
#endif
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
#endif
}

void
as_tls_thread_cleanup(void)
{
	// Skip if we were never initialized.
	if (! s_tls_inited) {
		return;
	}
#if OPENSSL_VERSION_NUMBER < 0x10100000L
	ERR_remove_state(0);
#endif
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

	pthread_mutex_lock(&asctxt->lock);

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
			pthread_mutex_unlock(&asctxt->lock);
			return 0;
		}
	}

	pthread_mutex_unlock(&asctxt->lock);

	// If this is the peer cert, check the name
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
	X509* cert = X509_STORE_CTX_get0_cert(ctx);
#else
	X509* cert = ctx->cert;
#endif

	if (current_cert == cert) {
		char * hostname = SSL_get_ex_data(ssl, s_ex_name_index);

		if (! hostname) {
			as_log_warn("Missing hostname in TLS verify callback");
			return 0;
		}

		bool allow_wildcard = true;
		bool matched = as_tls_match_name(cert, hostname, allow_wildcard);

		if (matched) {
			as_log_debug("TLS name '%s' matches", hostname);
		}
		else {
			as_log_warn("TLS name '%s' mismatch", hostname);
		}

		return matched ? 1 : 0;
	}

	// If we make it here we are a root or chain cert and are not
	// blacklisted.
	return 1;
}

static int
password_cb(char* buf, int size, int rwflag, void* udata)
{
	char* pw = udata;

	if (pw == NULL) {
		return 0;
	}

	int pw_len = (int)strlen(pw);

	if (pw_len > size) {
		return 0;
	}

	memcpy(buf, pw, pw_len);
	return pw_len;
}

static bool
as_tls_load_ca_str(SSL_CTX* ctx, char* cert_str)
{
	BIO* cert_bio = BIO_new_mem_buf(cert_str, -1);

	if (cert_bio == NULL) {
		return false;
	}

	X509* cert;
	int count = 0;

	while ((cert = PEM_read_bio_X509(cert_bio, NULL, 0, NULL)) != NULL) {
		X509_STORE* store = SSL_CTX_get_cert_store(ctx);
		int rv = X509_STORE_add_cert(store, cert);

		if (rv == 1) {
			count++;
		}
		else {
			as_log_warn("Failed to add TLS certificate from string");
		}

		X509_free(cert);
	}

	// Above loop always ends with an error - clear it so it doesn't affect
	// subsequent SSL calls in this thread.
	ERR_clear_error();
	BIO_vfree(cert_bio);

	if (count == 0) {
		return false;
	}
	return true;
}

static bool
as_tls_load_cert_chain_str(SSL_CTX* ctx, char* cert_str)
{
	BIO* cert_bio = BIO_new_mem_buf(cert_str, -1);

	if (cert_bio == NULL) {
		return false;
	}

	STACK_OF(X509_INFO)* inf = PEM_X509_INFO_read_bio(cert_bio, NULL, NULL, NULL);

	if (!inf) {
		BIO_free(cert_bio);
		return false;
	}

	/* Iterate over contents of the PEM buffer, and add certs. */
	bool first = true;

	for (int i = 0; i < sk_X509_INFO_num(inf); i++) {
		X509_INFO* itmp = sk_X509_INFO_value(inf, i);

		if (itmp->x509) {
			/* First cert is server cert. Remaining, if any, are intermediate certs. */
			if (first) {
				first = false;

				/*
				 * Set server certificate. Note that this operation increments the
				 * reference count, which means that it is okay for cleanup to free it.
				 */
				if (!SSL_CTX_use_certificate(ctx, itmp->x509)) {
					goto Error;
				}

				if (ERR_peek_error() != 0) {
					goto Error;
				}

				/* Get ready to store intermediate certs, if any. */
				SSL_CTX_clear_chain_certs(ctx);
			}
			else
			{
				/* Add intermediate cert to chain. */
				if (!SSL_CTX_add0_chain_cert(ctx, itmp->x509)) {
					goto Error;
				}

				/*
				 * Above function doesn't increment cert reference count. NULL the info
				 * reference to it in order to prevent it from being freed during cleanup.
				 */
				itmp->x509 = NULL;
			}
		}
	}

	sk_X509_INFO_pop_free(inf, X509_INFO_free);
	BIO_free(cert_bio);
	return true;

Error:
	sk_X509_INFO_pop_free(inf, X509_INFO_free);
	BIO_free(cert_bio);
	return false;
}

static bool
as_tls_load_key_str(SSL_CTX* ctx, char* key_str, const char* key_pw)
{
	BIO* key_bio = BIO_new_mem_buf(key_str, -1);

	if (key_bio == NULL) {
		return false;
	}

	EVP_PKEY* pkey = PEM_read_bio_PrivateKey(key_bio, NULL, password_cb, (void*)key_pw);

	BIO_vfree(key_bio);

	if (pkey == NULL) {
		if (ERR_GET_REASON(ERR_peek_error()) == EVP_R_BAD_DECRYPT) {
			as_log_warn("Invalid password for key string");
		}
		return false;
	}

	int rv = SSL_CTX_use_PrivateKey(ctx, pkey);

	EVP_PKEY_free(pkey);
	return rv == 1;
}

as_status
as_tls_context_setup(as_config_tls* tlscfg, as_tls_context* ctx, as_error* errp)
{
	// Clear the destination, in case we don't make it.
	ctx->ssl_ctx = NULL;
	ctx->pkey = NULL;
	ctx->cert_blacklist = NULL;
	ctx->log_session_info = tlscfg->log_session_info;
	ctx->for_login_only = tlscfg->for_login_only;

	as_tls_check_init();
	pthread_mutex_init(&ctx->lock, NULL);

	if (tlscfg->cert_blacklist) {
		ctx->cert_blacklist = cert_blacklist_read(tlscfg->cert_blacklist);

		if (! ctx->cert_blacklist) {
			// as_tls_context_destroy() will be called in as_cluster_destroy()
			// if an error is returned in this function.
			return as_error_update(errp, AEROSPIKE_ERR_TLS_ERROR,
								   "Failed to read certificate blacklist: %s",
								   tlscfg->cert_blacklist);
		}
	}
	
	uint16_t protocols = AS_TLS_PROTOCOL_NONE;
	as_status status = protocols_parse(tlscfg, &protocols, errp);

	if (status != AEROSPIKE_OK) {
		return status;
	}

	const SSL_METHOD* method = NULL;

	// If the selected protocol set is a single protocol we can use a specific method.
	if (protocols == AS_TLS_PROTOCOL_TLSV1) {
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
		method = TLS_client_method();
#else
		method = TLSv1_client_method();
#endif
	}
	else if (protocols == AS_TLS_PROTOCOL_TLSV1_1) {
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
		method = TLS_client_method();
#else
		method = TLSv1_1_client_method();
#endif
	}
	else if (protocols == AS_TLS_PROTOCOL_TLSV1_2) {
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
		method = TLS_client_method();
#else
		method = TLSv1_2_client_method();
#endif
	}
	else {
		// Multiple protocols are enabled, use a flexible method.
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
		method = TLS_client_method();
#else
		method = SSLv23_client_method();
#endif
	}

	ctx->ssl_ctx = SSL_CTX_new(method);

	if (ctx->ssl_ctx == NULL) {
		unsigned long errcode = ERR_get_error();
		char errbuf[1024];
		ERR_error_string_n(errcode, errbuf, sizeof(errbuf));
		return as_error_update(errp, AEROSPIKE_ERR_TLS_ERROR,
							   "Failed to create new TLS context: %s", errbuf);
	}

	/* always disable SSLv2, as per RFC 6176 */
	SSL_CTX_set_options(ctx->ssl_ctx, SSL_OP_NO_SSLv2);
	SSL_CTX_set_options(ctx->ssl_ctx, SSL_OP_NO_SSLv3);

	// Turn off non-enabled protocols.
	if (! (protocols & AS_TLS_PROTOCOL_TLSV1)) {
		SSL_CTX_set_options(ctx->ssl_ctx, SSL_OP_NO_TLSv1);
	}
	if (! (protocols & AS_TLS_PROTOCOL_TLSV1_1)) {
		SSL_CTX_set_options(ctx->ssl_ctx, SSL_OP_NO_TLSv1_1);
	}
	if (! (protocols & AS_TLS_PROTOCOL_TLSV1_2)) {
		SSL_CTX_set_options(ctx->ssl_ctx, SSL_OP_NO_TLSv1_2);
	}
	
	if (tlscfg->cafile || tlscfg->capath) {
		int rv = SSL_CTX_load_verify_locations(ctx->ssl_ctx, tlscfg->cafile, tlscfg->capath);

		if (rv != 1) {
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
	else if (tlscfg->castring) {
		if (! as_tls_load_ca_str(ctx->ssl_ctx, tlscfg->castring)) {
			return as_error_set_message(errp, AEROSPIKE_ERR_TLS_ERROR,
				"Failed to add any TLS certificates from castring");
		}
	}

	if (tlscfg->certfile) {
		int rv = SSL_CTX_use_certificate_chain_file(ctx->ssl_ctx, tlscfg->certfile);

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
				unsigned long errcode = ERR_get_error();
				char errbuf[1024];
				ERR_error_string_n(errcode, errbuf, sizeof(errbuf));
				return as_error_update(errp, AEROSPIKE_ERR_TLS_ERROR,
									   "SSL_CTX_use_certificate_chain_file failed: %s",
									   errbuf);
			}
		}
	}
	else if (tlscfg->certstring) {
		if (! as_tls_load_cert_chain_str(ctx->ssl_ctx, tlscfg->certstring)) {
			return as_error_set_message(errp, AEROSPIKE_ERR_TLS_ERROR,
				"Failed to add any TLS certificate chains from certstrings");
		}
	}

	if (tlscfg->keyfile) {
		bool ok = false;
		FILE *fh = fopen(tlscfg->keyfile, "r");

		if (fh == NULL) {
			as_error_update(errp, AEROSPIKE_ERR_TLS_ERROR,
					"failed to open key file %s: %s", tlscfg->keyfile,
					strerror(errno));
		}
		else {
			EVP_PKEY *pkey = PEM_read_PrivateKey(fh, NULL, password_cb,
					tlscfg->keyfile_pw);

			if (pkey == NULL) {
				unsigned long errcode = ERR_get_error();

				if (ERR_GET_REASON(errcode) == PEM_R_BAD_PASSWORD_READ) {
					if (tlscfg->keyfile_pw == NULL) {
						as_error_update(errp, AEROSPIKE_ERR_TLS_ERROR,
								"key file %s requires a password",
								tlscfg->keyfile);
					}
					else {
						as_error_update(errp, AEROSPIKE_ERR_TLS_ERROR,
								"password for key file %s too long",
								tlscfg->keyfile);
					}
				}
				else if (ERR_GET_REASON(errcode) == EVP_R_BAD_DECRYPT) {
					as_error_update(errp, AEROSPIKE_ERR_TLS_ERROR,
							"invalid password for key file %s",
							tlscfg->keyfile);
				}
				else {
					char errbuf[1024];
					ERR_error_string_n(errcode, errbuf, sizeof(errbuf));
					as_error_update(errp, AEROSPIKE_ERR_TLS_ERROR,
							"PEM_read_PrivateKey failed: %s", errbuf);
				}
			}
			else {
				ctx->pkey = pkey;
				SSL_CTX_use_PrivateKey(ctx->ssl_ctx, pkey);
				ok = true;
			}

			fclose(fh);
		}

		if (!ok) {
			return AEROSPIKE_ERR_TLS_ERROR;
		}
	}
	else if (tlscfg->keystring) {
		if (! as_tls_load_key_str(ctx->ssl_ctx, tlscfg->keystring, tlscfg->keyfile_pw)) {
			return as_error_set_message(errp, AEROSPIKE_ERR_TLS_ERROR,
				"Failed to load private key from keystring");
		}
	}

	if (tlscfg->cipher_suite) {
		int rv = SSL_CTX_set_cipher_list(ctx->ssl_ctx, tlscfg->cipher_suite);

		if (rv != 1) {
			return as_error_set_message(errp, AEROSPIKE_ERR_TLS_ERROR,
										"no compatible cipher found");
		}
		// It's bogus that we have to create an SSL just to get the
		// cipher list, but SSL_CTX_get_ciphers doesn't appear to
		// exist ...
		SSL* ssl = SSL_new(ctx->ssl_ctx);

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
		SSL_CTX_set1_param(ctx->ssl_ctx, param);
		X509_VERIFY_PARAM_free(param);
	}

	SSL_CTX_set_verify(ctx->ssl_ctx, SSL_VERIFY_PEER, verify_callback);
	manage_sigpipe();
	return AEROSPIKE_OK;
}

void
as_tls_context_destroy(as_tls_context* ctx)
{
	if (ctx->cert_blacklist) {
		cert_blacklist_destroy(ctx->cert_blacklist);
	}

	if (ctx->pkey) {
		EVP_PKEY_free(ctx->pkey);
	}
	
	if (ctx->ssl_ctx) {
		SSL_CTX_free(ctx->ssl_ctx);
	}
	pthread_mutex_destroy(&ctx->lock);
}

as_status
as_tls_config_reload(as_config_tls* tlscfg, as_tls_context* ctx,
		as_error *err)
{
	if (ctx == NULL || ctx->ssl_ctx == NULL) {
		return as_error_update(err, AEROSPIKE_ERR_TLS_ERROR,
				"TLS not enabled");
	}

	pthread_mutex_lock(&ctx->lock);

	if (tlscfg->certfile &&
			SSL_CTX_use_certificate_chain_file(ctx->ssl_ctx,
					tlscfg->certfile) != 1 &&
			ERR_peek_error() != SSL_ERROR_NONE) {
		pthread_mutex_unlock(&ctx->lock);

		char buff[1000];
		ERR_error_string_n(ERR_get_error(), buff, sizeof(buff));

		return as_error_update(err, AEROSPIKE_ERR_TLS_ERROR,
				"Failed to reload certificate file %s: %s",
				tlscfg->certfile, buff);
	}

	if (tlscfg->keyfile &&
#if OPENSSL_VERSION_NUMBER < 0x30000000L
		SSL_CTX_use_RSAPrivateKey_file(ctx->ssl_ctx, tlscfg->keyfile,
									   SSL_FILETYPE_PEM) != 1) {
#else
		SSL_CTX_use_PrivateKey_file(ctx->ssl_ctx, tlscfg->keyfile,
									   SSL_FILETYPE_PEM) != 1) {
#endif
		pthread_mutex_unlock(&ctx->lock);

		char buff[1000];
		ERR_error_string_n(ERR_get_error(), buff, sizeof(buff));

		return as_error_update(err, AEROSPIKE_ERR_TLS_ERROR,
				"Failed to reload private key file %s: %s",
				tlscfg->keyfile, buff);
	}

	if (tlscfg->cert_blacklist) {
		void *new_cbl = cert_blacklist_read(tlscfg->cert_blacklist);

		if (! new_cbl) {
			pthread_mutex_unlock(&ctx->lock);
			return as_error_update(err, AEROSPIKE_ERR_TLS_ERROR,
					"Failed to reload certificate blacklist %s",
					tlscfg->cert_blacklist);
		}

		cert_blacklist_destroy(ctx->cert_blacklist);
		ctx->cert_blacklist = new_cbl;
	}

	pthread_mutex_unlock(&ctx->lock);
	return AEROSPIKE_OK;
}

int
as_tls_wrap(as_tls_context* ctx, as_socket* sock, const char* tls_name)
{
	sock->ctx = ctx;
	sock->tls_name = tls_name;

	pthread_mutex_lock(&ctx->lock);
	sock->ssl = SSL_new(ctx->ssl_ctx);
	pthread_mutex_unlock(&ctx->lock);

	if (sock->ssl == NULL)
		return -1;

	SSL_set_fd(sock->ssl, (int)sock->fd);

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

void
as_tls_set_context_name(struct ssl_st* ssl, as_tls_context* ctx, const char* tls_name)
{
	SSL_set_ex_data(ssl, s_ex_name_index, (char*)tls_name);
	SSL_set_ex_data(ssl, s_ex_ctxt_index, ctx);
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
				as_log_warn("SSL_connect_once I/O error: %d", as_last_error());
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
as_tls_connect(as_socket* sock, uint64_t deadline)
{
	int rv;

#if defined(_MSC_VER)
	// Windows SSL_connect() will fail with SSL_ERROR_SYSCALL if non-blocking
	// socket has not completed TCP connect.  Wait on socket before calling
	// SSL_connect() when on Windows.
	rv = wait_socket(sock->fd, 0, deadline, false);
	if (rv != 0) {
		as_log_warn("wait_writable failed: %d", rv);
		return rv;
	}
#endif

	while (true) {
		rv = SSL_connect(sock->ssl);
		if (rv == 1) {
			log_session_info(sock);
			return 0;
		}

		int sslerr = SSL_get_error(sock->ssl, rv);
		unsigned long errcode;
		char errbuf[1024];
		switch (sslerr) {
		case SSL_ERROR_WANT_READ:
			rv = wait_socket(sock->fd, 0, deadline, true);
			if (rv != 0) {
				as_log_warn("wait_readable failed: %d", rv);
				return rv;
			}
			// loop back around and retry
			break;
		case SSL_ERROR_WANT_WRITE:
			rv = wait_socket(sock->fd, 0, deadline, false);
			if (rv != 0) {
				as_log_warn("wait_writable failed: %d", rv);
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
					as_log_warn("SSL_connect I/O error: %d", as_last_error());
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

/*
This function is too expensive.
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
			rv = wait_writable(sock->fd, 0, deadline);
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
					as_log_warn("SSL_peek I/O error: %d", as_last_error());
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
*/

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
					as_log_warn("SSL_read_once I/O error: %d", as_last_error());
				}
			}
			return -4;
		case SSL_ERROR_ZERO_RETURN:
			as_log_debug("SSL_read_once: server closed connection");
			return -5;
		default:
			as_log_warn("SSL_read_once: unexpected ssl error: %d", sslerr);
			return -6;
		}
	}
}

int
as_tls_read(as_socket* sock, void* bufp, size_t len, uint32_t socket_timeout, uint64_t deadline)
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
			int sslerr;
			// Avoid the expensive call to SSL_get_error() in the most common case.
			BIO* bio = SSL_get_rbio(sock->ssl);
			if (SSL_want_read(sock->ssl) && BIO_should_read(bio) && BIO_should_retry(bio)) {
				sslerr = SSL_ERROR_WANT_READ;
			}
			else {
				sslerr = SSL_get_error(sock->ssl, rv);
			}
			unsigned long errcode;
			char errbuf[1024];
			switch (sslerr) {
			case SSL_ERROR_WANT_READ:
				rv = wait_socket(sock->fd, socket_timeout, deadline, true);
				if (rv != 0) {
					return rv;
				}
				// loop back around and retry
				break;
			case SSL_ERROR_WANT_WRITE:
				rv = wait_socket(sock->fd, socket_timeout, deadline, false);
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
						as_log_warn("SSL_read I/O error: %d", as_last_error());
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
					as_log_warn("SSL_write_once I/O error: %d", as_last_error());
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
as_tls_write(as_socket* sock, void* bufp, size_t len, uint32_t socket_timeout, uint64_t deadline)
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
				rv = wait_socket(sock->fd, socket_timeout, deadline, true);
				if (rv != 0) {
					return rv;
				}
				// loop back around and retry
				break;
			case SSL_ERROR_WANT_WRITE:
				rv = wait_socket(sock->fd, socket_timeout, deadline, false);
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
						as_log_warn("SSL_write I/O error: %d", as_last_error());
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

static void manage_sigpipe(void)
{
#if !defined(_MSC_VER)
	// OpenSSL can encounter a SIGPIPE in the SSL_shutdown sequence.
	// The default behavior is to terminate the program.
	//
	// 1) We can't fix this by calling send with MSG_NOSIGNAL because
	// the call is deep inside OpenSSL and there doesn't appear to be
	// any way to get the flag set there.
	//
	// 2) We can't set SO_NOSIGNAL on the socket because linux doesn't
	// support it.
	//
	// 3) Instead, we specify alternate global signal handling
	// behavior ONLY if the user hasn't already set the SIGPIPE signal
	// handler.  See the accepted answer to the following thread for
	// inspiration:
	// http://stackoverflow.com/questions/25144550/is-it-possible-to-force-openssl-to-not-generate-sigpipe-without-global-signal-ha?rq=1

	struct sigaction old_handler;
	int rv = sigaction(SIGPIPE, NULL, &old_handler);
	if (rv != 0) {
		as_log_warn("sigaction failed to read old handler for SIGPIPE: %s",
					strerror(errno));
		return;
	}

	// Was there already an signal handler installed?
	if (old_handler.sa_handler != SIG_DFL) {
		// Yes, leave it alone ...
		return;
	}

	struct sigaction new_handler;
	memset(&new_handler, 0, sizeof(new_handler));
	new_handler.sa_handler = SIG_IGN;
	rv = sigaction(SIGPIPE, &new_handler, NULL);
	if (rv != 0) {
		as_log_warn("sigaction failed to set SIGPIPE to SIG_IGN: %s",
					strerror(errno));
		return;
	}
#endif
}
