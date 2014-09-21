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

#include <aerospike/as_error.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_serializer.h>

#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/cl_types.h>
#include <citrusleaf/cl_write.h>

#include <stdint.h>

as_status as_error_fromrc(as_error * err, cl_rv rc);

void askey_from_clkey(as_key * key, const as_namespace ns, const as_set set, cl_object * obj);

void clbin_to_asval(cl_bin * bin, as_serializer * ser, as_val ** val);

void clbin_to_asrecord(cl_bin * bin, as_record * r);

void clbins_to_asrecord(cl_bin * bins, uint32_t nbins, as_record * rec);

void aspolicywrite_to_clwriteparameters(const as_policy_write * policy, const as_record * rec, cl_write_parameters * wp);

void aspolicyoperate_to_clwriteparameters(const as_policy_operate * policy, const as_operations * ops, cl_write_parameters * wp);

void aspolicyremove_to_clwriteparameters(const as_policy_remove * policy, cl_write_parameters * wp);

void asval_to_clobject(as_val * val, cl_object * obj);

void asbinvalue_to_clobject(as_bin_value * val, cl_object * obj);

void asbin_to_clbin(as_bin * as, cl_bin * cl);

void asrecord_to_clbins(as_record * rec, cl_bin * bins, uint32_t nbins);
