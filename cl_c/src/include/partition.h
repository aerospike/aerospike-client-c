/******************************************************************************
 * Copyright 2008-2012 by Aerospike.  All rights reserved.
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE.  THE COPYRIGHT NOTICE
 * ABOVE DOES NOT EVIDENCE ANY ACTUAL OR INTENDED PUBLICATION.
 ******************************************************************************/

#pragma once

#include "types.h"

// Partition table calls
// --- all these assume the partition lock is held
extern void cl_partition_table_remove_node( cl_cluster *asc, cl_cluster_node *node );
extern void cl_partition_table_destroy_all(cl_cluster *asc);
extern void cl_partition_table_set( cl_cluster *asc, cl_cluster_node *node, char *ns, cl_partition_id pid, bool write);
extern cl_cluster_node *cl_partition_table_get( cl_cluster *asc, char *ns, cl_partition_id pid, bool write);
