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
#pragma once

/**
 * @defgroup as_subcode Error Detail Constants
 * @{
 *
 * Constants for error detail verbosity levels and server subcodes.
 *
 * Subcodes are organized by parent status code. A subcode integer is only
 * meaningful paired with its parent status, dispatch on the (status, subcode)
 * pair.
 */

//---------------------------------
// Error Detail Verbosity Levels
//---------------------------------

/**
 * No error details requested (default).
 * Set on as_policy_base.error_detail_verbosity.
 */
#define AS_ERROR_DETAIL_OFF        0

/**
 * Request subcode only from the server on error responses.
 * Set on as_policy_base.error_detail_verbosity.
 */
#define AS_ERROR_DETAIL_SUBCODE    1

/**
 * Request subcode and human-readable message from the server on error responses.
 * Set on as_policy_base.error_detail_verbosity.
 */
#define AS_ERROR_DETAIL_MESSAGE    2

//---------------------------------
// Subcodes
//---------------------------------

/**
 * No dispatchable subcode. Used when the parent status alone fully identifies
 * the condition. Reserved as 0 across all status families.
 */
#define AS_SUB_NONE                                        0

//---------------------------------
// Subcodes paired with AEROSPIKE_ERR_PARAM (AS_ERR_PARAMETER)
//---------------------------------

/**
 * Per-record TTL exceeds the namespace's max-ttl.
 * App use: clamp the TTL to the namespace max and retry.
 */
#define AS_SUB_PARAM_TTL_INVALID                           1

/**
 * Bit op offset lands past the blob (or above the proto cap).
 * App use: refresh the bin size, recompute the offset, retry.
 */
#define AS_SUB_PARAM_BITS_OFFSET_OUT_OF_RANGE              2

/**
 * Bit op size is out of range (e.g. zero, or too large).
 * App use: clamp the size dimension (vs. offset) and retry.
 */
#define AS_SUB_PARAM_BITS_SIZE_OUT_OF_RANGE                3

/**
 * Blob resize would exceed RECORD_MAX_BLOB_SIZE.
 * App use: backpressure or partition the dynamically-sized blob.
 */
#define AS_SUB_PARAM_BITS_RESIZE_EXCEEDED                  4

/**
 * Write would exceed the per-record bin-count limit.
 * App use: prune least-valuable bins and retry.
 */
#define AS_SUB_PARAM_BIN_COUNT_TOO_LARGE                   5

//---------------------------------
// Subcodes paired with AEROSPIKE_ERR_CLUSTER (AS_ERR_UNAVAILABLE)
//---------------------------------

/**
 * Cluster is still resolving initial partition balance at startup.
 * App use: wait a fixed backoff (~1s) and retry; failing over is
 * pointless since every node is unresolved at once.
 */
#define AS_SUB_UNAVAIL_INITIAL_BALANCE_UNRESOLVED          1

/**
 * A needed replica is unavailable (likely a partition split).
 * App use: an SC reader may downgrade to read-mode=any if safe, or
 * back off longer than for transient unavailability.
 */
#define AS_SUB_UNAVAIL_REPLICA_UNAVAILABLE                 2

//---------------------------------
// Subcodes paired with AEROSPIKE_ERR_UNSUPPORTED_FEATURE
//---------------------------------

/**
 * MRT attempted against a non-SC (AP) namespace.
 * App use: route the MRT to an SC namespace, or use a non-MRT path.
 */
#define AS_SUB_UNSUPP_FEAT_MRT_REQUIRES_STRONG_CONSISTENCY 1

/**
 * Requested feature is unsupported in this context (generic).
 * App use: same dispatch as MRT_REQUIRES_STRONG_CONSISTENCY; kept
 * distinct to preserve the sole live emit (MRT-monitor AP check).
 */
#define AS_SUB_UNSUPP_FEAT_GENERIC                         2

//---------------------------------
// Subcodes paired with AEROSPIKE_ERR_BIN_NOT_FOUND
//---------------------------------

/**
 * HLL op needs an existing bin and can't auto-create one.
 * App use: dispatch a one-time init op with default index_bits,
 * then retry the count/fold.
 */
#define AS_SUB_BIN_NOT_FOUND_HLL_CANNOT_CREATE_WITH_OP     1

//---------------------------------
// Subcodes paired with AEROSPIKE_ERR_BIN_NAME
//---------------------------------

/**
 * Write would exceed the per-record bin-count limit (UDF path).
 * App use: prune least-valuable bins and retry.
 */
#define AS_SUB_BIN_NAME_COUNT_TOO_LARGE                    1

//---------------------------------
// Subcodes paired with AEROSPIKE_ERR_FAIL_FORBIDDEN
//---------------------------------

/**
 * Write bounced by an XDR ship filter at the destination.
 * App use: suppress retry; optionally record the digest for audit.
 */
#define AS_SUB_FORBID_XDR_FILTER_BLOCKED                   1

/**
 * Set-level record-count stop-writes limit reached.
 * App use: route new records to another set, or archive old ones.
 */
#define AS_SUB_FORBID_SET_COUNT_STOP_WRITES                2

/**
 * Set-level size stop-writes limit reached.
 * App use: backpressure or route to a different set (not ns-wide).
 */
#define AS_SUB_FORBID_SET_SIZE_STOP_WRITES                 3

/**
 * Writes stopped due to cluster clock skew.
 * App use: page on-call to investigate NTP / time-source drift.
 */
#define AS_SUB_FORBID_CLOCK_SKEW_STOP_WRITES               4

/**
 * REPLACE / CREATE_OR_REPLACE forbidden while resolving conflicts.
 * App use: back off and retry once the cluster stabilizes.
 */
#define AS_SUB_FORBID_REPLACE_CONFLICT_RESOLVING           5

/**
 * Write forbidden because the set/namespace is mid-truncate.
 * App use: retry shortly after the truncate completes (transient).
 */
#define AS_SUB_FORBID_TRUNCATED                            6

/**
 * Access blocked by a data-masking policy.
 * App use: elevate role / impersonate, or route to an admin queue.
 */
#define AS_SUB_FORBID_MASKING_POLICY_BLOCKED               7

/**
 * Non-durable delete forbidden (would violate durability).
 * App use: upgrade the delete to durable, or skip the shortcut.
 */
#define AS_SUB_FORBID_DURABILITY_VIOLATION                 8

/**
 * Caller's role lacks unmasked access.
 * App use: prompt the user to escalate / switch role (distinct
 * from auth not configured).
 */
#define AS_SUB_FORBID_MASKING_ROLE_VIOLATION               9

//---------------------------------
// Subcodes paired with AEROSPIKE_ERR_OP_NOT_APPLICABLE
//---------------------------------

/**
 * List index is outside the current element range.
 * App use: refresh the cached list size, clamp the index, retry.
 */
#define AS_SUB_OPNOT_CDT_INDEX_OUT_OF_BOUNDS               1

/**
 * Requested rank is past the current population.
 * App use: clamp top-N rank to the element count and retry.
 */
#define AS_SUB_OPNOT_CDT_RANK_OUT_OF_BOUNDS                2

/**
 * Insert would exceed an ordered+bounded list's cap.
 * App use: roll to a fresh bin/key partition, or apply backpressure.
 */
#define AS_SUB_OPNOT_CDT_BOUNDED_LIST_OVERFLOW             3

/**
 * HLL op needs index_bits but the sketch has none set.
 * App use: dispatch a one-time init with default index_bits, retry.
 */
#define AS_SUB_OPNOT_HLL_INDEX_BITS_UNSET                  4

/**
 * Union needs to reduce index_bits but folding isn't allowed.
 * App use: retry with ALLOW_FOLD, or fold sources to the smaller
 * precision first.
 */
#define AS_SUB_OPNOT_HLL_CANNOT_REDUCE_INDEX_BITS          5

/**
 * As above, for the minhash dimension.
 * App use: retry with ALLOW_FOLD, or align sources first.
 */
#define AS_SUB_OPNOT_HLL_CANNOT_REDUCE_MINHASH_BITS        6

/**
 * Fold blocked because the sketch carries minhash bits.
 * App use: switch to a strip-minhash-then-fold path.
 */
#define AS_SUB_OPNOT_HLL_CANNOT_FOLD_MINHASH               7

/**
 * Fold target index_bits >= current (fold can only reduce).
 * App use: clamp target to current-1 and retry, or skip the fold.
 */
#define AS_SUB_OPNOT_HLL_FOLD_INDEX_BITS_TOO_LARGE         8

/**
 * Intersect inputs have mismatched minhash parameters.
 * App use: harmonize sketches (fold/strip minhash) before retry.
 */
#define AS_SUB_OPNOT_HLL_INTERSECT_MINHASH_MISMATCH        9

//---------------------------------
// Subcodes paired with AEROSPIKE_ERR_FILTERED_OUT
//---------------------------------

/**
 * Record filtered out by a metadata-only filter expression.
 * App use: treat as a normal expected miss (no-op).
 */
#define AS_SUB_FILTERED_META                               1

/**
 * Record filtered out by a bin-reading filter expression.
 * App use: as META; split out to meter metadata-vs-bin misses.
 */
#define AS_SUB_FILTERED_BINS                               2

/**
 * A metadata filter expression failed to evaluate.
 * App use: treat as an expression bug - log digest, alert, no retry.
 */
#define AS_SUB_FILTERED_META_EVAL_FAILED                   3

/**
 * A bin filter expression failed to evaluate.
 * App use: as META_EVAL_FAILED.
 */
#define AS_SUB_FILTERED_BINS_EVAL_FAILED                   4

//---------------------------------
// Subcodes paired with AEROSPIKE_ERR_MRT_BLOCKED
//---------------------------------

/**
 * Record is provisionally locked by another MRT.
 * App use: a non-MRT writer backs off with jittered retry until the
 * MRT commits or expires.
 */
#define AS_SUB_MRT_BLOCKED_RECORD_LOCKED                   1

/**
 * Op belongs to a different MRT than the one holding the lock.
 * App use: abort the whole MRT - retrying this op alone can never
 * succeed within the current MRT.
 */
#define AS_SUB_MRT_BLOCKED_ID_MISMATCH                     2

/** @} */
