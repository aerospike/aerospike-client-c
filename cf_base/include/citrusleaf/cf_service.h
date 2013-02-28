/*
 *  Citrusleaf Foundation
 *
 *  Copyright 2012 by Citrusleaf. All rights reserved.
 *  THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE. THE COPYRIGHT NOTICE
 *  ABOVE DOES NOT EVIDENCE ANY ACTUALY OR INTENDED PUBLICATION.
 */

#pragma once

#include <sys/types.h>

void cf_process_privsep(uid_t uid, gid_t gid);
void cf_process_daemonize(const char* redirect_file, int *fd_ignore_list, int list_size);
