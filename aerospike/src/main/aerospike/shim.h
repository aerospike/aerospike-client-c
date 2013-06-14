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

void as_record_tobins(as_record * rec, cl_bin * bins, uint32_t nbins);

as_record * as_record_frombins(as_record * rec, cl_bin * bins, uint32_t nbins);

as_val * as_val_frombin(as_serializer * ser, cl_bin * bin);

void as_policy_write_towp(as_policy_write * policy, as_record * rec, cl_write_parameters * wp);

void as_policy_remove_towp(as_policy_remove * policy, cl_write_parameters * wp);
