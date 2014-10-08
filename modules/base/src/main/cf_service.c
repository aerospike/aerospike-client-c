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
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <aerospike/as_log_macros.h>
#include <citrusleaf/cf_service.h>

void
cf_process_privsep(uid_t uid, gid_t gid)
{
    if ((0 != getuid() || ((uid == getuid()) && (gid == getgid()))))
        return;

    /* Drop all auxiliary groups */
    if (0 > setgroups(0, (const gid_t *)0)){
        as_log_error("Could not set groups: %s", strerror(errno));
//        cf_crash(CF_MISC, CF_GLOBAL, CF_CRITICAL, "setgroups: %s", cf_strerror(errno));
        exit(-1);
    }

    /* Change privileges */
    if (0 > setgid(gid)){
    	as_log_error("Could not set gid: %s", strerror(errno));
//        cf_crash(CF_MISC, CF_GLOBAL, CF_CRITICAL, "setgid: %s", cf_strerror(errno));
        exit(-2);
    }
    if (0 > setuid(uid)){
    	as_log_error("Could not set uid: %s", strerror(errno));
//        cf_crash(CF_MISC, CF_GLOBAL, CF_CRITICAL, "setuid: %s", cf_strerror(errno));
        exit(-2);
    }

    return;
}


/* Function to daemonize the server. Fork a new child process and exit the parent process.
 * Close all the file decsriptors opened except the ones specified in the fd_ignore_list.
 * Redirect console messages to a file. */
void
cf_process_daemonize(const char *redirect_file, int *fd_ignore_list, int list_size)
{
    int FD, j;
    char cfile[128];
    pid_t p;
 
    /* Fork ourselves, then let the parent expire */
    if (-1 == (p = fork())){
    	as_log_error("Couldn't fork: %s", strerror(errno));
//      cf_crash(CF_MISC, CF_GLOBAL, CF_CRITICAL, "couldn't fork: %s", cf_strerror(errno));
        exit(-1);
    }
    if (0 != p)
        exit(0);

    /* Get a new session */
    if (-1 == setsid()){
    	as_log_error("Couldn't set session: %s", strerror(errno));
        exit(-2);
//      cf_crash(CF_MISC, CF_GLOBAL, CF_CRITICAL, "couldn't set session: %s", cf_strerror(errno));
    }

    /* Drop all the file descriptors except the ones in fd_ignore_list*/
    for (int i = getdtablesize(); i > 2; i--) {
	for (j = 0; j < list_size; j++) {
	    if (fd_ignore_list[j] == i) {
		break;
	    }
	}
	if(j ==  list_size) {
	    close(i);
	}
    }


    /* Open a temporary file for console message redirection */
    if (redirect_file == NULL){ 
        snprintf(cfile, 128, "/tmp/aerospike-console.%d", getpid());
    }else{
        snprintf(cfile, 128, "%s", redirect_file);
    }
    if (-1 == (FD = open(cfile, O_WRONLY|O_CREAT|O_APPEND,S_IRUSR|S_IWUSR))){
        as_log_error("Couldn't open console redirection file: %s", strerror(errno));
//        cf_crash(CF_MISC, CF_GLOBAL, CF_CRITICAL, "couldn't open console redirection file: %s", cf_strerror(errno));
        exit(-3);
    }
    if (-1 == chmod(cfile, (S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH))){
    	as_log_error("Couldn't set mode on console redirection file: %s", strerror(errno));
//        cf_crash(CF_MISC, CF_GLOBAL, CF_CRITICAL, "couldn't set mode on console redirection file: %s", cf_strerror(errno));
        exit(-4);
    }

    /* Redirect stdout, stderr, and stdin to the console file */
    for (int i = 0; i < 3; i++) {
        if (-1 == dup2(FD, i)){
        	as_log_error("Couldn't duplicate FD: %s", strerror(errno));
//            cf_crash(CF_MISC, CF_GLOBAL, CF_CRITICAL, "couldn't duplicate FD: %s", cf_strerror(errno));
            exit(-5);
        }
    }

     return;
}
