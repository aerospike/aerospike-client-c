#pragma once

#include <aerospike/as_error.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_serializer.h>

#include <citrusleaf/citrusleaf.h>
#include <citrusleaf/cl_types.h>
#include <citrusleaf/cl_write.h>

#include <stdint.h>

as_status as_error_fromrc(as_error * err, cl_rv rc);

void clbin_to_asval(cl_bin * bin, as_serializer * ser, as_val ** val);

void clbins_to_asrecord(cl_bin * bins, uint32_t nbins, as_record * rec);

void aspolicywrite_to_clwriteparameters(as_policy_write * policy, as_record * rec, cl_write_parameters * wp);

void aspolicyoperate_to_clwriteparameters(as_policy_operate * policy, cl_write_parameters * wp);

void asval_to_clobject(as_val * val, cl_object * obj);

void asbinvalue_to_clobject(as_bin_value * val, cl_object * obj);

void asbin_to_clbin(as_bin * as, cl_bin * cl);

void asrecord_to_clbins(as_record * rec, cl_bin * bins, uint32_t nbins);
