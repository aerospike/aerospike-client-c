/* Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*                      _             _
 *  _ __ ___   ___   __| |    ___ ___| |  mod_ssl
 * | '_ ` _ \ / _ \ / _` |   / __/ __| |  Apache Interface to OpenSSL
 * | | | | | | (_) | (_| |   \__ \__ \ |
 * |_| |_| |_|\___/ \__,_|___|___/___/_|
 *                      |_____|
 *  ssl_util_ssl.c
 *  Additional Utility Functions for OpenSSL
 */

// NOTE - This file is adapted from httpd-2.4.18/modules/ssl/ssl_util_ssl.c
//
// The APR (Apache Portable Runtime) routines are minimally
// implemented at the top of this file in order to change the
// pre-existing ssl specific routines below as little as possible.
//

#include <assert.h>
#include <string.h>
#include <sys/syslog.h>

#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include <aerospike/as_log_macros.h>
#include <aerospike/as_tls.h>
#include "citrusleaf/alloc.h"

#ifndef BOOL
#define BOOL unsigned int
#endif

#ifndef FALSE
#define FALSE 0
#endif

#ifndef TRUE
#define TRUE !FALSE
#endif

#ifndef NUL
#define NUL '\0'
#endif

#define strEQ(s1,s2)     (strcmp(s1,s2)        == 0)

typedef void apr_pool_t;

typedef void server_rec;

typedef int apr_status_t;

typedef struct {
    apr_pool_t * pool;
    int elt_size;
    int nelts;
    int nalloc;
    char * 	elts;
} apr_array_header_t;


typedef struct {
    char * vhost_id;
} SSLSrvConfigRec;

SSLSrvConfigRec s_srv_conf_rec = { "" };

#define mySrvConfig(s) (&s_srv_conf_rec)

void * apr_palloc(apr_pool_t * pp, size_t sz)
{
	return cf_malloc(sz);
}

apr_array_header_t * apr_array_make(apr_pool_t * pp,
                                    size_t nelts,
                                    size_t eltsz)
{
	// This routine isn't prepared for non-zero nelts; extend
	// it if this is needed in the future ...
	assert(nelts == 0);

	apr_array_header_t * arr = cf_malloc(sizeof(apr_array_header_t));
	arr->pool = NULL;
	arr->elt_size = (int)eltsz;
	arr->nelts = (int)nelts;
	arr->nalloc = (int)nelts;
	arr->elts = NULL;
    return arr;
}

void array_destroy(apr_array_header_t * arr)
{
	for (int ii = 0; ii < arr->nelts; ++ii) {
		char ** ptr = (char **) (arr->elts + (ii * arr->elt_size));
		cf_free(*ptr);
	}
	if (arr->elts) {
		cf_free(arr->elts);
	}
	cf_free(arr);
}

void * apr_array_push(apr_array_header_t * arr)
{
	// If we don't have enough space double up.
	if (arr->nelts == arr->nalloc) {
		if (arr->nalloc == 0) {
			arr->nalloc = 16;
		} else {
			arr->nalloc *= 2;
		}
		size_t newsz = arr->nalloc * arr->elt_size;
		arr->elts = cf_realloc(arr->elts, newsz);
	}

	// Point to the first unused location.
	void * ptr = arr->elts + (arr->nelts * arr->elt_size);

	// We'll have one more element now.
	++arr->nelts;
	
    return ptr;
}

#define 	APR_ARRAY_PUSH(ary, type)   (*((type *)apr_array_push(ary)))

int apr_is_empty_array(const apr_array_header_t * arr)
{
	return (arr->nelts == 0);
}

// On non-EBCDIC system, this function is replaced by an empty macro.
#define ap_xlate_proto_from_ascii(buff, len)

const char * ap_strchr_c(const char * str, int c)
{
	return strchr(str, c);
}

#if 0

#include "ssl_private.h"

/*  _________________________________________________________________
**
**  Additional High-Level Functions for OpenSSL
**  _________________________________________________________________
*/

/* we initialize this index at startup time
 * and never write to it at request time,
 * so this static is thread safe.
 * also note that OpenSSL increments at static variable when
 * SSL_get_ex_new_index() is called, so we _must_ do this at startup.
 */
static int app_data2_idx = -1;

void modssl_init_app_data2_idx(void)
{
    int i;

    if (app_data2_idx > -1) {
        return;
    }

    /* we _do_ need to call this twice */
    for (i = 0; i <= 1; i++) {
        app_data2_idx =
            SSL_get_ex_new_index(0,
                                 "Second Application Data for SSL",
                                 NULL, NULL, NULL);
    }
}

void *modssl_get_app_data2(SSL *ssl)
{
    return (void *)SSL_get_ex_data(ssl, app_data2_idx);
}

void modssl_set_app_data2(SSL *ssl, void *arg)
{
    SSL_set_ex_data(ssl, app_data2_idx, (char *)arg);
    return;
}

/*  _________________________________________________________________
**
**  High-Level Private Key Loading
**  _________________________________________________________________
*/

EVP_PKEY *modssl_read_privatekey(const char* filename, EVP_PKEY **key, pem_password_cb *cb, void *s)
{
    EVP_PKEY *rc;
    BIO *bioS;
    BIO *bioF;

    /* 1. try PEM (= DER+Base64+headers) */
    if ((bioS=BIO_new_file(filename, "r")) == NULL)
        return NULL;
    rc = PEM_read_bio_PrivateKey(bioS, key, cb, s);
    BIO_free(bioS);

    if (rc == NULL) {
        /* 2. try DER+Base64 */
        if ((bioS = BIO_new_file(filename, "r")) == NULL)
            return NULL;

        if ((bioF = BIO_new(BIO_f_base64())) == NULL) {
            BIO_free(bioS);
            return NULL;
        }
        bioS = BIO_push(bioF, bioS);
        rc = d2i_PrivateKey_bio(bioS, NULL);
        BIO_free_all(bioS);

        if (rc == NULL) {
            /* 3. try plain DER */
            if ((bioS = BIO_new_file(filename, "r")) == NULL)
                return NULL;
            rc = d2i_PrivateKey_bio(bioS, NULL);
            BIO_free(bioS);
        }
    }
    if (rc != NULL && key != NULL) {
        if (*key != NULL)
            EVP_PKEY_free(*key);
        *key = rc;
    }
    return rc;
}

/*  _________________________________________________________________
**
**  Smart shutdown
**  _________________________________________________________________
*/

int modssl_smart_shutdown(SSL *ssl)
{
    int i;
    int rc;
    int flush;

    /*
     * Repeat the calls, because SSL_shutdown internally dispatches through a
     * little state machine. Usually only one or two interation should be
     * needed, so we restrict the total number of restrictions in order to
     * avoid process hangs in case the client played bad with the socket
     * connection and OpenSSL cannot recognize it.
     */
    rc = 0;
    flush = !(SSL_get_shutdown(ssl) & SSL_SENT_SHUTDOWN);
    for (i = 0; i < 4 /* max 2x pending + 2x data = 4 */; i++) {
        rc = SSL_shutdown(ssl);
        if (rc >= 0 && flush && (SSL_get_shutdown(ssl) & SSL_SENT_SHUTDOWN)) {
            /* Once the close notity is sent through the output filters,
             * ensure it is flushed through the socket.
             */
            if (BIO_flush(SSL_get_wbio(ssl)) <= 0) {
                rc = -1;
                break;
            }
            flush = 0;
        }
        if (rc != 0)
            break;
    }
    return rc;
}

/*  _________________________________________________________________
**
**  Certificate Checks
**  _________________________________________________________________
*/

/* retrieve basic constraints ingredients */
BOOL modssl_X509_getBC(X509 *cert, int *ca, int *pathlen)
{
    BASIC_CONSTRAINTS *bc;
    BIGNUM *bn = NULL;
    char *cp;

    bc = X509_get_ext_d2i(cert, NID_basic_constraints, NULL, NULL);
    if (bc == NULL)
        return FALSE;
    *ca = bc->ca;
    *pathlen = -1 /* unlimited */;
    if (bc->pathlen != NULL) {
        if ((bn = ASN1_INTEGER_to_BN(bc->pathlen, NULL)) == NULL) {
            BASIC_CONSTRAINTS_free(bc);
            return FALSE;
        }
        if ((cp = BN_bn2dec(bn)) == NULL) {
            BN_free(bn);
            BASIC_CONSTRAINTS_free(bc);
            return FALSE;
        }
        *pathlen = atoi(cp);
        OPENSSL_free(cp);
        BN_free(bn);
    }
    BASIC_CONSTRAINTS_free(bc);
    return TRUE;
}

#endif

/* convert an ASN.1 string to a UTF-8 string (escaping control characters) */
static char *asn1_string_to_utf8(apr_pool_t *p, ASN1_STRING *asn1str)
{
    char *result = NULL;
    BIO *bio;
    int len;

    if ((bio = BIO_new(BIO_s_mem())) == NULL)
        return NULL;

    ASN1_STRING_print_ex(bio, asn1str, ASN1_STRFLGS_ESC_CTRL|
                                       ASN1_STRFLGS_UTF8_CONVERT);
    len = BIO_pending(bio);
    if (len > 0) {
        result = apr_palloc(p, len+1);
        len = BIO_read(bio, result, len);
        result[len] = NUL;
    }
    BIO_free(bio);
    return result;
}

/* convert a NAME_ENTRY to UTF8 string */
char *modssl_X509_NAME_ENTRY_to_string(apr_pool_t *p, X509_NAME_ENTRY *xsne)
{
    char *result = asn1_string_to_utf8(p, X509_NAME_ENTRY_get_data(xsne));
    ap_xlate_proto_from_ascii(result, len);
    return result;
}

#if 0

/*
 * convert an X509_NAME to an RFC 2253 formatted string, optionally truncated
 * to maxlen characters (specify a maxlen of 0 for no length limit)
 */
char *modssl_X509_NAME_to_string(apr_pool_t *p, X509_NAME *dn, int maxlen)
{
    char *result = NULL;
    BIO *bio;
    int len;

    if ((bio = BIO_new(BIO_s_mem())) == NULL)
        return NULL;
    X509_NAME_print_ex(bio, dn, 0, XN_FLAG_RFC2253);
    len = BIO_pending(bio);
    if (len > 0) {
        result = apr_palloc(p, (maxlen > 0) ? maxlen+1 : len+1);
        if (maxlen > 0 && maxlen < len) {
            len = BIO_read(bio, result, maxlen);
            if (maxlen > 2) {
                /* insert trailing ellipsis if there's enough space */
                apr_snprintf(result + maxlen - 3, 4, "...");
            }
        } else {
            len = BIO_read(bio, result, len);
        }
        result[len] = NUL;
    }
    BIO_free(bio);

    return result;
}

#endif

static void parse_otherName_value(apr_pool_t *p, ASN1_TYPE *value,
                                  const char *onf, apr_array_header_t **entries)
{
    const char *str;
    int nid = onf ? OBJ_txt2nid(onf) : NID_undef;

    if (!value || (nid == NID_undef) || !*entries)
       return;

    /* 
     * Currently supported otherName forms (values for "onf"):
     * "msUPN" (1.3.6.1.4.1.311.20.2.3): Microsoft User Principal Name
     * "id-on-dnsSRV" (1.3.6.1.5.5.7.8.7): SRVName, as specified in RFC 4985
     */
    if ((nid == NID_ms_upn) && (value->type == V_ASN1_UTF8STRING) &&
        (str = asn1_string_to_utf8(p, value->value.utf8string))) {
        APR_ARRAY_PUSH(*entries, const char *) = str;
    } else if (strEQ(onf, "id-on-dnsSRV") &&
               (value->type == V_ASN1_IA5STRING) &&
               (str = asn1_string_to_utf8(p, value->value.ia5string))) {
        APR_ARRAY_PUSH(*entries, const char *) = str;
    }
}

/* 
 * Return an array of subjectAltName entries of type "type". If idx is -1,
 * return all entries of the given type, otherwise return an array consisting
 * of the n-th occurrence of that type only. Currently supported types:
 * GEN_EMAIL (rfc822Name)
 * GEN_DNS (dNSName)
 * GEN_OTHERNAME (requires the otherName form ["onf"] argument to be supplied,
 *                see parse_otherName_value for the currently supported forms)
 */
BOOL modssl_X509_getSAN(apr_pool_t *p, X509 *x509, int type, const char *onf,
                        int idx, apr_array_header_t **entries)
{
    STACK_OF(GENERAL_NAME) *names;
    int nid = onf ? OBJ_txt2nid(onf) : NID_undef;

    if (!x509 || (type < GEN_OTHERNAME) ||
        ((type == GEN_OTHERNAME) && (nid == NID_undef)) ||
        (type > GEN_RID) || (idx < -1) ||
        !(*entries = apr_array_make(p, 0, sizeof(char *)))) {
        *entries = NULL;
        return FALSE;
    }

    if ((names = X509_get_ext_d2i(x509, NID_subject_alt_name, NULL, NULL))) {
        int i, n = 0;
        GENERAL_NAME *name;
        const char *utf8str;

        for (i = 0; i < sk_GENERAL_NAME_num(names); i++) {
            name = sk_GENERAL_NAME_value(names, i);

            if (name->type != type)
                continue;

            switch (type) {
            case GEN_EMAIL:
            case GEN_DNS:
                if (((idx == -1) || (n == idx)) &&
                    (utf8str = asn1_string_to_utf8(p, name->d.ia5))) {
                    APR_ARRAY_PUSH(*entries, const char *) = utf8str;
                }
                n++;
                break;
            case GEN_OTHERNAME:
                if (OBJ_obj2nid(name->d.otherName->type_id) == nid) {
                    if (((idx == -1) || (n == idx))) {
                        parse_otherName_value(p, name->d.otherName->value,
                                              onf, entries);
                    }
                    n++;
                }
                break;
            default:
                /*
                 * Not implemented right now:
                 * GEN_X400 (x400Address)
                 * GEN_DIRNAME (directoryName)
                 * GEN_EDIPARTY (ediPartyName)
                 * GEN_URI (uniformResourceIdentifier)
                 * GEN_IPADD (iPAddress)
                 * GEN_RID (registeredID)
                 */
                break;
            }

            if ((idx != -1) && (n > idx))
               break;
        }

        sk_GENERAL_NAME_pop_free(names, GENERAL_NAME_free);
    }

    return apr_is_empty_array(*entries) ? FALSE : TRUE;
}

/* return an array of (RFC 6125 coined) DNS-IDs and CN-IDs in a certificate */
static BOOL getIDs(apr_pool_t *p, X509 *x509, apr_array_header_t **ids)
{
    X509_NAME *subj;
    int i = -1;

    /* First, the DNS-IDs (dNSName entries in the subjectAltName extension) */
    if (!x509 ||
        (modssl_X509_getSAN(p, x509, GEN_DNS, NULL, -1, ids) == FALSE && !*ids)) {
        *ids = NULL;
        return FALSE;
    }

    /* Second, the CN-IDs (commonName attributes in the subject DN) */
    subj = X509_get_subject_name(x509);
    while ((i = X509_NAME_get_index_by_NID(subj, NID_commonName, i)) != -1) {
        APR_ARRAY_PUSH(*ids, const char *) = 
            modssl_X509_NAME_ENTRY_to_string(p, X509_NAME_get_entry(subj, i));
    }

    return apr_is_empty_array(*ids) ? FALSE : TRUE;
}

/* 
 * Check if a certificate matches for a particular name, by iterating over its
 * DNS-IDs and CN-IDs (RFC 6125), optionally with basic wildcard matching.
 * If server_rec is non-NULL, some (debug/trace) logging is enabled.
 */
BOOL modssl_X509_match_name(apr_pool_t *p, X509 *x509, const char *name,
                            BOOL allow_wildcard, server_rec *s)
{
    BOOL matched = FALSE;
    apr_array_header_t *ids = NULL;

    /*
     * At some day in the future, this might be replaced with X509_check_host()
     * (available in OpenSSL 1.0.2 and later), but two points should be noted:
     * 1) wildcard matching in X509_check_host() might yield different
     *    results (by default, it supports a broader set of patterns, e.g.
     *    wildcards in non-initial positions);
     * 2) we lose the option of logging each DNS- and CN-ID (until a match
     *    is found).
     */

    if (getIDs(p, x509, &ids)) {
        const char *cp;
        int i;
        char **id = (char **)ids->elts;
        BOOL is_wildcard;

        for (i = 0; i < ids->nelts; i++) {
            if (!id[i])
                continue;

            /*
             * Determine if it is a wildcard ID - we're restrictive
             * in the sense that we require the wildcard character to be
             * THE left-most label (i.e., the ID must start with "*.")
             */
            is_wildcard = (*id[i] == '*' && *(id[i]+1) == '.') ? TRUE : FALSE;

            /*
             * If the ID includes a wildcard character (and the caller is
             * allowing wildcards), check if it matches for the left-most
             * DNS label - i.e., the wildcard character is not allowed
             * to match a dot. Otherwise, try a simple string compare.
             */
            if ((allow_wildcard == TRUE && is_wildcard == TRUE &&
                 (cp = ap_strchr_c(name, '.')) && !strcasecmp(id[i]+1, cp)) ||
                !strcasecmp(id[i], name)) {
                matched = TRUE;
            }

			if (!matched) {
				as_log_info("as_tls_match_name: expecting name '%s', "
							"%smatched by ID '%s'",
							name,
							matched == TRUE ? "" : "NOT ",
							id[i]);
			}

            if (matched == TRUE) {
                break;
            }
        }

    }

	if (!matched) {
		as_log_warn("Cert %s for name '%s'",
					matched == TRUE ? "matches" : "does not match",
					name);
	}

    array_destroy(ids);
    
    return matched;
}

bool as_tls_match_name(X509 *x509, const char *name, bool allow_wildcard)
{
	// We've neutered the actual calls to no need the apr_pool_t or
	// server_rec arguments.
	//
	return modssl_X509_match_name((apr_pool_t *) NULL,
								  x509,
								  name,
								  allow_wildcard,
								  (server_rec *) NULL);
}

#if 0

/*  _________________________________________________________________
**
**  Custom (EC)DH parameter support
**  _________________________________________________________________
*/

DH *ssl_dh_GetParamFromFile(const char *file)
{
    DH *dh = NULL;
    BIO *bio;

    if ((bio = BIO_new_file(file, "r")) == NULL)
        return NULL;
    dh = PEM_read_bio_DHparams(bio, NULL, NULL, NULL);
    BIO_free(bio);
    return (dh);
}

#ifdef HAVE_ECC
EC_GROUP *ssl_ec_GetParamFromFile(const char *file)
{
    EC_GROUP *group = NULL;
    BIO *bio;

    if ((bio = BIO_new_file(file, "r")) == NULL)
        return NULL;
    group = PEM_read_bio_ECPKParameters(bio, NULL, NULL, NULL);
    BIO_free(bio);
    return (group);
}
#endif

/*  _________________________________________________________________
**
**  Session Stuff
**  _________________________________________________________________
*/

char *modssl_SSL_SESSION_id2sz(IDCONST unsigned char *id, int idlen,
                               char *str, int strsize)
{
    if (idlen > SSL_MAX_SSL_SESSION_ID_LENGTH)
        idlen = SSL_MAX_SSL_SESSION_ID_LENGTH;
        
    /* We must ensure not to process more than what would fit in the
     * destination buffer, including terminating NULL */
    if (idlen > (strsize-1) / 2)
        idlen = (strsize-1) / 2;

    ap_bin2hex(id, idlen, str);

    return str;
}

#endif
