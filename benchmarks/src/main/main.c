/*******************************************************************************
 * Copyright 2008-2020 by Aerospike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/
#include "benchmark.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(_MSC_VER)
#undef _UNICODE  // Use ASCII getopt version on windows.
#endif
#include <getopt.h>

static const char* short_options = "h:p:U:P::n:s:K:k:b:o:Rt:w:z:g:T:dL:SC:N:B:M:Y:Dac:W:u";

static struct option long_options[] = {
	{"hosts",                required_argument, 0, 'h'},
	{"port",                 required_argument, 0, 'p'},
	{"user",                 required_argument, 0, 'U'},
	{"password",             optional_argument, 0, 'P'},
	{"namespace",            required_argument, 0, 'n'},
	{"set",                  required_argument, 0, 's'},
	{"startKey",             required_argument, 0, 'K'},
	{"keys",                 required_argument, 0, 'k'},
	{"bins",                 required_argument, 0, 'b'},
	{"objectSpec",           required_argument, 0, 'o'},
	{"random",               no_argument,       0, 'R'},
	{"transactions",         required_argument, 0, 't'},
	{"workload",             required_argument, 0, 'w'},
	{"threads",              required_argument, 0, 'z'},
	{"throughput",           required_argument, 0, 'g'},
	{"batchSize",            required_argument, 0, '0'},
	{"socketTimeout",        required_argument, 0, '1'},
	{"readSocketTimeout",    required_argument, 0, '2'},
	{"writeSocketTimeout",   required_argument, 0, '3'},
	{"timeout",              required_argument, 0, 'T'},
	{"readTimeout",          required_argument, 0, 'X'},
	{"writeTimeout",         required_argument, 0, 'V'},
	{"maxRetries",           required_argument, 0, 'r'},
	{"debug",                no_argument,       0, 'd'},
	{"latency",              required_argument, 0, 'L'},
	{"shared",               no_argument,       0, 'S'},
	{"replica",              required_argument, 0, 'C'},
	{"readModeAP",           required_argument, 0, 'N'},
	{"readModeSC",           required_argument, 0, 'B'},
	{"commitLevel",          required_argument, 0, 'M'},
	{"connPoolsPerNode",     required_argument, 0, 'Y'},
	{"durableDelete",        no_argument,       0, 'D'},
	{"async",                no_argument,       0, 'a'},
	{"asyncMaxCommands",     required_argument, 0, 'c'},
	{"eventLoops",           required_argument, 0, 'W'},
	{"tlsEnable",            no_argument,       0, 'A'},
	{"tlsCaFile",            required_argument, 0, 'E'},
	{"tlsCaPath",            required_argument, 0, 'F'},
	{"tlsProtocols",         required_argument, 0, 'G'},
	{"tlsCipherSuite",       required_argument, 0, 'H'},
	{"tlsCrlCheck",          no_argument,       0, 'I'},
	{"tlsCrlCheckAll",       no_argument,       0, 'J'},
	{"tlsCertBlackList",     required_argument, 0, 'O'},
	{"tlsLogSessionInfo",    no_argument,       0, 'Q'},
	{"tlsKeyFile",           required_argument, 0, 'Z'},
	{"tlsCertFile",          required_argument, 0, 'y'},
	{"tlsLoginOnly",         no_argument,       0, 'f'},
	{"auth",                 required_argument, 0, 'e'},
	{"usage",                no_argument,       0, 'u'},
	{0, 0, 0, 0}
};

static void
print_usage(const char* program)
{
	blog_line("Usage: %s <options>", program);
	blog_line("options:");
	blog_line("");
	
	blog_line("-h --hosts <host1>[:<tlsname1>][:<port1>],...  # Default: localhost");
	blog_line("   Server seed hostnames or IP addresses.");
	blog_line("   The tlsname is only used when connecting with a secure TLS enabled server.");
	blog_line("   If the port is not specified, the default port is used. Examples:");
	blog_line("");
	blog_line("   host1");
	blog_line("   host1:3000,host2:3000");
	blog_line("   192.168.1.10:cert1:3000,192.168.1.20:cert2:3000");
	blog_line("");
	
	blog_line("-p --port <port> # Default: 3000");
	blog_line("   Server default port.");
	blog_line("");
	
	blog_line("-U --user <user name> # Default: empty");
	blog_line("   User name for Aerospike servers that require authentication.");
	blog_line("");

	blog_line("-P[<password>]  # Default: empty");
	blog_line("   User's password for Aerospike servers that require authentication.");
	blog_line("   If -P is set, the actual password if optional. If the password is not given,");
	blog_line("   the user will be prompted on the command line.");
	blog_line("   If the password is given, it must be provided directly after -P with no");
	blog_line("   intervening space (ie. -Pmypass).");
	blog_line("");

	blog_line("-n --namespace <ns>   # Default: test");
	blog_line("   Aerospike namespace.");
	blog_line("");
	
	blog_line("-s --set <set name>   # Default: testset");
	blog_line("   Aerospike set name.");
	blog_line("");
	
	blog_line("-K --startKey <start> # Default: 0");
	blog_line("   Set the starting value of the working set of keys. If using an");
	blog_line("   'insert' workload, the start_value indicates the first value to");
	blog_line("   write. Otherwise, the start_value indicates the smallest value in");
	blog_line("   the working set of keys.");
	blog_line("");

	blog_line("-k --keys <count>     # Default: 1000000");
	blog_line("   Set the number of keys the client is dealing with. If using an");
	blog_line("   'insert' workload (detailed below), the client will write this");
	blog_line("   number of keys, starting from value = startKey. Otherwise, the");
	blog_line("   client will read and update randomly across the values between");
	blog_line("   startKey and startKey + num_keys.  startKey can be set using");
	blog_line("   '-K' or '--startKey'.");
	blog_line("");
	
	blog_line("-b --bins <count>     # Default: 1");
	blog_line("   Number of bins");
	blog_line("");
	
	blog_line("-o --objectSpec I | B:<size> | S:<size> | L:<size> | M:<size> # Default: I");
	blog_line("   Bin object specification.");
	blog_line("   -o I     : Read/write integer bin.");
	blog_line("   -o B:200 : Read/write byte array bin of length 200.");
	blog_line("   -o S:50  : Read/write string bin of length 50.");
	blog_line("   -o L:50  : Read/write cdt list bin of 50 elements.");
	blog_line("   -o M:50  : Read/write cdt map bin of 50 map entries.");
	blog_line("   -o M:50B : Read/write cdt map bin of ~50 bytes.");
	blog_line("   -o M:50K : Read/write cdt map bin of ~50 kilobytes.");
	blog_line("");

	blog_line("-R --random          # Default: static fixed bin values");
	blog_line("   Use dynamically generated random bin values instead of default static fixed bin values.");
	blog_line("");
	
	blog_line("-t --transactions       # Default: -1 (unlimited)");
	blog_line("    Stop approximately after number of transaction performed in random read/write mode.");
	blog_line("");

	blog_line("-w --workload I,<percent> | RU,<read percent> | DB  # Default: RU,50");
	blog_line("   Desired workload.");
	blog_line("   -w I,60  : Linear 'insert' workload initializing 60%% of the keys.");
	blog_line("   -w RU,80 : Random read/update workload with 80%% reads and 20%% writes.");
	blog_line("   -w DB    : Bin delete workload.");
	blog_line("");
	
	blog_line("-z --threads <count> # Default: 16");
	blog_line("   Load generating thread count.");
	blog_line("");
	
	blog_line("-g --throughput <tps> # Default: 0");
	blog_line("   Throttle transactions per second to a maximum value.");
	blog_line("   If tps is zero, do not throttle throughput.");
	blog_line("   Used in read/write mode only.");
	blog_line("");

	blog_line("--batchSize <size> # Default: 0");
	blog_line("   Enable batch mode with number of records to process in each batch get call.");
	blog_line("   Batch mode is valid only for RU (read update) workloads. Batch mode is disabled by default.");
	blog_line("");

	blog_line("   --socketTimeout <ms> # Default: 30000");
	blog_line("   Read/Write socket timeout in milliseconds.");
	blog_line("");
	
	blog_line("   --readSocketTimeout <ms> # Default: 30000");
	blog_line("   Read socket timeout in milliseconds.");
	blog_line("");
	
	blog_line("   --writeSocketTimeout <ms> # Default: 30000");
	blog_line("   Write socket timeout in milliseconds.");
	blog_line("");

	blog_line("-T --timeout <ms>    # Default: 0");
	blog_line("   Read/Write total timeout in milliseconds.");
	blog_line("");
	
	blog_line("   --readTimeout <ms> # Default: 0");
	blog_line("   Read total timeout in milliseconds.");
	blog_line("");
	
	blog_line("   --writeTimeout <ms> # Default: 0");
	blog_line("   Write total timeout in milliseconds.");
	blog_line("");
	
	blog_line("   --maxRetries <number> # Default: 1");
	blog_line("   Maximum number of retries before aborting the current transaction.");
	blog_line("");
	
    //blog_line("   --sleepBetweenRetries <count>");
	//blog_line("   Milliseconds to sleep between retries if a transaction fails and the timeout was not exceeded.");
	//blog_line("");
	
	blog_line("-d --debug           # Default: debug mode is false.");
	blog_line("   Run benchmarks in debug mode.");
	blog_line("");
	
	blog_line("-L --latency <columns>,<shift>  # Default: latency display is off.");
	blog_line("   Show transaction latency percentages using elapsed time ranges.");
	blog_line("   <columns> Number of elapsed time ranges.");
	blog_line("   <shift>   Power of 2 multiple between each range starting at column 3.");
	blog_line("");
	blog_line("   A latency definition of '--latency 7,1' results in this layout:");
	blog_line("       <=1ms >1ms >2ms >4ms >8ms >16ms >32ms");
	blog_line("          x%%   x%%   x%%   x%%   x%%    x%%    x%%");
	blog_line("");
	blog_line("   A latency definition of '--latency 4,3' results in this layout:");
	blog_line("       <=1ms >1ms >8ms >64ms");
	blog_line("           x%%  x%%   x%%    x%%");
	blog_line("");
	blog_line("   Latency columns are cumulative. If a transaction takes 9ms, it will be");
	blog_line("   included in both the >1ms and >8ms columns.");
	blog_line("");
	
	blog_line("-S --shared          # Default: false");
	blog_line("   Use shared memory cluster tending.");
	blog_line("");

	blog_line("-C --replica {master,any,sequence} # Default: master");
	blog_line("   Which replica to use for reads.");
	blog_line("");

	blog_line("-N --readModeAP {one,all} # Default: one");
	blog_line("   Read mode for AP (availability) namespaces.");
	blog_line("");

	blog_line("-B --readModeSC {session,linearize,allowReplica,allowUnavailable} # Default: session");
	blog_line("   Read mode for SC (strong consistency) namespaces.");
	blog_line("");

	blog_line("-M --commitLevel {all,master} # Default: all");
	blog_line("   Write commit guarantee level.");
	blog_line("");

	blog_line("-Y --connPoolsPerNode <num>  # Default: 1");
	blog_line("   Number of connection pools per node.");
	blog_line("");

	blog_line("-D --durableDelete  # Default: durableDelete mode is false.");
	blog_line("   All transactions will set the durable-delete flag which indicates");
	blog_line("   to the server that if the transaction results in a delete, to generate");
	blog_line("   a tombstone for the deleted record.");
	blog_line("");

	blog_line("-a --async # Default: synchronous mode");
	blog_line("   Enable asynchronous mode.");
	blog_line("");
	
	blog_line("-c --asyncMaxCommands <command count> # Default: 50");
	blog_line("   Maximum number of concurrent asynchronous commands that are active at any point");
	blog_line("   in time.");
	blog_line("");

	blog_line("-W --eventLoops <thread count> # Default: 1");
	blog_line("   Number of event loops (or selector threads) when running in asynchronous mode.");
	blog_line("");

	blog_line("   --tlsEnable         # Default: TLS disabled");
	blog_line("   Enable TLS.");
	blog_line("");

	blog_line("   --tlsCaFile <path>");
	blog_line("   Set the TLS certificate authority file.");
	blog_line("");

	blog_line("   --tlsCaPath <path>");
	blog_line("   Set the TLS certificate authority directory.");
	blog_line("");

	blog_line("   --tlsProtocols <protocols>");
	blog_line("   Set the TLS protocol selection criteria.");
	blog_line("");

	blog_line("   --tlsCipherSuite <suite>");
	blog_line("   Set the TLS cipher selection criteria.");
	blog_line("");

	blog_line("   --tlsCrlCheck");
	blog_line("   Enable CRL checking for leaf certs.");
	blog_line("");

	blog_line("   --tlsCrlCheckAll");
	blog_line("   Enable CRL checking for all certs.");
	blog_line("");

	blog_line("   --tlsCertBlackList <path>");
	blog_line("   Path to a certificate blacklist file.");
	blog_line("");

	blog_line("   --tlsLogSessionInfo");
	blog_line("   Log TLS connected session info.");
	blog_line("");

	blog_line("   --tlsKeyFile <path>");
	blog_line("   Set the TLS client key file for mutual authentication.");
	blog_line("");

	blog_line("   --tlsCertFile <path>");
	blog_line("   Set the TLS client certificate chain file for mutual authentication.");
	blog_line("");

	blog_line("   --tlsLoginOnly");
	blog_line("   Use TLS for node login only.");
	blog_line("");

	blog_line("   --auth {INTERNAL,EXTERNAL,EXTERNAL_SECURE} # Default: INTERNAL");
	blog_line("   Set authentication mode when user/password is defined.");
	blog_line("");

	blog_line("-u --usage           # Default: usage not printed.");
	blog_line("   Display program usage.");
	blog_line("");
}

static const char*
boolstring(bool val)
{
	if (val) {
		return "true";
	}
	else {
		return "false";
	}
}

static void
print_args(arguments* args)
{
	blog_line("hosts:                  %s", args->hosts);
	blog_line("port:                   %d", args->port);
	blog_line("user:                   %s", args->user);
	blog_line("namespace:              %s", args->namespace);
	blog_line("set:                    %s", args->set);
	blog_line("startKey:               %" PRIu64, args->start_key);
	blog_line("keys/records:           %" PRIu64, args->keys);
	blog_line("bins:                   %d", args->numbins);
	blog("object spec:            ");
	
	static const char *units[3] = {"", "b", "k"};

	switch (args->bintype) {
		case 'I':
			blog_line("int");
			break;

		case 'B':
			blog_line("byte[%d]", args->binlen);
			break;

		case 'S':
			blog_line("UTF8 string[%d]", args->binlen);
			break;

		case 'L':
			blog_line("list[%d%s]", args->binlen, units[(int)args->binlen_type]);
			break;

		case 'M':
			blog_line("map[%d%s]", args->binlen, units[(int)args->binlen_type]);
			break;

		default:
			blog_line("");
			break;
	}
	
	blog_line("random values:          %s", boolstring(args->random));

	blog("workload:               ");

	if (args->init) {
		blog_line("initialize %d%% of records", args->init_pct);
	} else if (args->del_bin) {
		blog_line("delete %d bins in %d records", args->numbins, args->keys);
	} else if (args->read_pct) {
		blog_line("read %d%% write %d%%", args->read_pct, 100 - args->read_pct);
		blog_line("stop after:             %" PRIu64 " transactions", args->transactions_limit);
	}
	
	blog_line("threads:                %d", args->threads);
	
	if (args->throughput > 0) {
		blog_line("max throughput:         %d tps", args->throughput);
	}
	else {
		blog_line("max throughput:         unlimited", args->throughput);
	}

	blog_line("batch size:             %d", args->batch_size);
	blog_line("read socket timeout:    %d ms", args->read_socket_timeout);
	blog_line("write socket timeout:   %d ms", args->write_socket_timeout);
	blog_line("read total timeout:     %d ms", args->read_total_timeout);
	blog_line("write total timeout:    %d ms", args->write_total_timeout);
	blog_line("max retries:            %d", args->max_retries);
	blog_line("debug:                  %s", boolstring(args->debug));
	
	if (args->latency) {
		blog_line("latency:                %d columns, shift exponent %d", args->latency_columns, args->latency_shift);
	}
	else {
		blog_line("latency:                false");
	}
	
	blog_line("shared memory:          %s", boolstring(args->use_shm));

	const char* str;
	switch (args->replica) {
		case AS_POLICY_REPLICA_MASTER:
			str = "master";
			break;
		case AS_POLICY_REPLICA_ANY:
			str = "any";
			break;
		case AS_POLICY_REPLICA_SEQUENCE:
			str = "sequence";
			break;
		default:
			str = "unknown";
			break;
	}

	blog_line("read replica:           %s", str);
	blog_line("read mode AP:           %s", (AS_POLICY_READ_MODE_AP_ONE == args->read_mode_ap ? "one" : "all"));

	switch (args->read_mode_sc) {
		case AS_POLICY_READ_MODE_SC_SESSION:
			str = "session";
			break;
		case AS_POLICY_READ_MODE_SC_LINEARIZE:
			str = "linearize";
			break;
		case AS_POLICY_READ_MODE_SC_ALLOW_REPLICA:
			str = "allowReplica";
			break;
		case AS_POLICY_READ_MODE_SC_ALLOW_UNAVAILABLE:
			str = "allowUnavailable";
			break;
		default:
			str = "unknown";
			break;
	}

	blog_line("read mode SC:           %s", str);
	blog_line("write commit level:     %s", (AS_POLICY_COMMIT_LEVEL_ALL == args->write_commit_level ? "all" : "master"));
	blog_line("conn pools per node:    %d", args->conn_pools_per_node);
	blog_line("asynchronous mode:      %s", args->async ? "on" : "off");

	if (args->async) {
		blog_line("async max commands:     %d", args->async_max_commands);
		blog_line("event loops:            %d", args->event_loop_capacity);
	}

	if (args->tls.enable) {
		blog_line("TLS:                    enabled");
		blog_line("TLS cafile:             %s", args->tls.cafile);
		blog_line("TLS capath:             %s", args->tls.capath);
		blog_line("TLS protocols:          %s", args->tls.protocols);
		blog_line("TLS cipher suite:       %s", args->tls.cipher_suite);
		blog_line("TLS crl check:          %s", boolstring(args->tls.crl_check));
		blog_line("TLS crl check all:      %s", boolstring(args->tls.crl_check_all));
		blog_line("TLS cert blacklist:     %s", args->tls.cert_blacklist);
		blog_line("TLS log session info:   %s", boolstring(args->tls.log_session_info));
		blog_line("TLS keyfile:            %s", args->tls.keyfile);
		blog_line("TLS certfile:           %s", args->tls.certfile);
		blog_line("TLS login only:         %s", boolstring(args->tls.for_login_only));
	}

	char* s;
	switch (args->auth_mode) {
		case AS_AUTH_INTERNAL:
			s = "INTERNAL";
			break;
		case AS_AUTH_EXTERNAL:
			s = "EXTERNAL";
			break;
		case AS_AUTH_EXTERNAL_INSECURE:
			s = "EXTERNAL_INSECURE";
			break;
		default:
			s = "unknown";
			break;
	}
	blog_line("auth mode:              %s", s);
}

static int
validate_args(arguments* args)
{
	if (args->start_key == ULLONG_MAX) {
		blog_line("Invalid start key: %" PRIu64, args->start_key);
		return 1;
	}

	if (args->keys == ULLONG_MAX) {
		blog_line("Invalid number of keys: %" PRIu64, args->keys);
		return 1;
	}
	
	if (args->numbins <= 0) {
		blog_line("Invalid number of bins: %d  Valid values: [> 0]", args->keys);
		return 1;
	}
	
	switch (args->bintype) {
		case 'I':
			break;
			
		case 'L':
		case 'M':
		case 'B':
		case 'S':
			if (args->binlen <= 0 || args->binlen > 1000000) {
				blog_line("Invalid bin length: %d  Valid values: [1-1000000]", args->binlen);
				return 1;
			}
			break;
			
		default:
			blog_line("Invalid bin type: %c  Valid values: I|B:<size>|S:<size>", args->bintype);
			return 1;
	}
	
	if (args->init_pct < 0 || args->init_pct > 100) {
		blog_line("Invalid initialize percent: %d  Valid values: [0-100]", args->init_pct);
		return 1;
	}
	
	if (args->read_pct < 0 || args->read_pct > 100) {
		blog_line("Invalid read percent: %d  Valid values: [0-100]", args->read_pct);
		return 1;
	}
	
	if (args->threads <= 0 || args->threads > 10000) {
		blog_line("Invalid number of threads: %d  Valid values: [1-10000]", args->threads);
		return 1;
	}

	if (args->read_socket_timeout < 0) {
		blog_line("Invalid read socket timeout: %d  Valid values: [>= 0]", args->read_socket_timeout);
		return 1;
	}
	
	if (args->write_socket_timeout < 0) {
		blog_line("Invalid write socket timeout: %d  Valid values: [>= 0]", args->write_socket_timeout);
		return 1;
	}

	if (args->read_total_timeout < 0) {
		blog_line("Invalid read total timeout: %d  Valid values: [>= 0]", args->read_total_timeout);
		return 1;
	}
	
	if (args->write_total_timeout < 0) {
		blog_line("Invalid write total timeout: %d  Valid values: [>= 0]", args->write_total_timeout);
		return 1;
	}
	
	if (args->latency_columns < 0 || args->latency_columns > 16) {
		blog_line("Invalid latency columns: %d  Valid values: [1-16]", args->latency_columns);
		return 1;
	}
	
	if (args->latency_shift < 0 || args->latency_shift > 5) {
		blog_line("Invalid latency exponent shift: %d  Valid values: [1-5]", args->latency_shift);
		return 1;
	}
	
	if (args->conn_pools_per_node <= 0 || args->conn_pools_per_node > 1000) {
		blog_line("Invalid connPoolsPerNode: %d  Valid values: [1-1000]", args->conn_pools_per_node);
		return 1;
	}

	if (args->async) {
		if (args->async_max_commands <= 0 || args->async_max_commands > 5000) {
			blog_line("Invalid asyncMaxCommands: %d  Valid values: [1-5000]", args->async_max_commands);
			return 1;
		}
		
		if (args->event_loop_capacity <= 0 || args->event_loop_capacity > 1000) {
			blog_line("Invalid eventLoops: %d  Valid values: [1-1000]", args->event_loop_capacity);
			return 1;
		}
	}
	return 0;
}

static int
set_args(int argc, char * const * argv, arguments* args)
{
	int option_index = 0;
	int c;
	
	while ((c = getopt_long(argc, argv, short_options, long_options, &option_index)) != -1) {
		switch (c) {
			case 'h': {
				free(args->hosts);
				args->hosts = strdup(optarg);
				break;
			}
				
			case 'p':
				args->port = atoi(optarg);
				break;
				
			case 'U':
				args->user = optarg;
				break;
			
			case 'P':
				as_password_acquire(args->password, optarg, AS_PASSWORD_SIZE);
				break;

			case 'n':
				args->namespace = optarg;
				break;
				
			case 's':
				args->set = optarg;
				break;
				
			case 'K':
				args->start_key = strtoull(optarg, NULL, 10);
				break;

			case 'k':
				args->keys = strtoull(optarg, NULL, 10);
				break;
				
			case 'b':
				args->numbins = atoi(optarg);
				break;
				
			case 'o': {
				args->bintype = *optarg;

				if (args->bintype == 'B'
						|| args->bintype == 'S'
						|| args->bintype == 'L'
						|| args->bintype == 'M') {
					char *p = optarg + 1;
					if (*p == ':') {
						args->binlen = atoi(p+1);
						if (args->bintype == 'L' || args->bintype == 'M') {
							switch (p[strlen(p) - 1]) {
							case 'b':
							case 'B':
								args->binlen_type = LEN_TYPE_BYTES;
								break;
							case 'k':
							case 'K':
								args->binlen_type = LEN_TYPE_KBYTES;
								break;
							default:
								break;
							}
						}
					}
					else {
						blog_line("Unspecified bin size.");
						return 1;
					}
				}
				break;
			}

			case 'R':
				args->random = true;
				break;
				
			case 't':
				args->transactions_limit = strtoull(optarg, NULL, 10);
				break;

			case 'w': {
				char* tmp = strdup(optarg);
				char* p = strchr(tmp, ',');
				
				if (strncmp(tmp, "I", 1) == 0) {
					args->init = true;
					if (p) {
						*p = 0;
						args->init_pct = atoi(p + 1);
					}
				} else if (strncmp(tmp, "RU", 2) == 0) {
					if (p) {
						*p = 0;
						args->read_pct = atoi(p + 1);
					}
				} else if (strncmp(tmp, "DB", 2) == 0) {
					args->init = true;
					args->del_bin = true;
				}

				free(tmp);
				break;
			}
								
			case 'z':
				args->threads = atoi(optarg);
				break;
				
			case 'g':
				args->throughput = atoi(optarg);
				break;

			case '0':
				args->batch_size = atoi(optarg);
				break;

			case '1':
				args->read_socket_timeout = atoi(optarg);
				args->write_socket_timeout = args->read_socket_timeout;
				break;

			case '2':
				args->read_socket_timeout = atoi(optarg);
				break;

			case '3':
				args->write_socket_timeout = atoi(optarg);
				break;

			case 'T':
				args->read_total_timeout = atoi(optarg);
				args->write_total_timeout = args->read_total_timeout;
				break;
				
			case 'X':
				args->read_total_timeout = atoi(optarg);
				break;
				
			case 'V':
				args->write_total_timeout = atoi(optarg);
				break;

			case 'r':
				args->max_retries = atoi(optarg);
				break;

			case 'd':
				args->debug = true;
				break;
				
			case 'L': {
				args->latency = true;
				char* tmp = strdup(optarg);
				char* p = strchr(tmp, ',');
				
				if (p) {
					*p = 0;
					args->latency_columns = atoi(tmp);
					args->latency_shift = atoi(p + 1);
				}
				else {
					args->latency_columns = 4;
					args->latency_shift = 3;
				}
				free(tmp);
				break;
			}
				
			case 'S':
				args->use_shm = true;
				break;

			case 'C':
				if (strcmp(optarg, "master") == 0) {
					args->replica = AS_POLICY_REPLICA_MASTER;
				}
				else if (strcmp(optarg, "any") == 0) {
					args->replica = AS_POLICY_REPLICA_ANY;
				}
				else if (strcmp(optarg, "sequence") == 0) {
					args->replica = AS_POLICY_REPLICA_SEQUENCE;
				}
				else {
					blog_line("replica must be master | any | sequence");
					return 1;
				}
				break;

			case 'N':
				if (strcmp(optarg, "one") == 0) {
					args->read_mode_ap = AS_POLICY_READ_MODE_AP_ONE;
				}
				else if (strcmp(optarg, "all") == 0) {
					args->read_mode_ap = AS_POLICY_READ_MODE_AP_ALL;
				}
				else {
					blog_line("readModeAP must be one or all");
					return 1;
				}
				break;

			case 'B':
				if (strcmp(optarg, "session") == 0) {
					args->read_mode_sc = AS_POLICY_READ_MODE_SC_SESSION;
				}
				else if (strcmp(optarg, "linearize") == 0) {
					args->read_mode_sc = AS_POLICY_READ_MODE_SC_LINEARIZE;
				}
				else if (strcmp(optarg, "allowReplica") == 0) {
					args->read_mode_sc = AS_POLICY_READ_MODE_SC_ALLOW_REPLICA;
				}
				else if (strcmp(optarg, "allowUnavailable") == 0) {
					args->read_mode_sc = AS_POLICY_READ_MODE_SC_ALLOW_UNAVAILABLE;
				}
				else {
					blog_line("readModeSC must be session | linearize | allowReplica | allowUnavailable");
					return 1;
				}
				break;

			case 'M':
				if (strcmp(optarg, "all") == 0) {
					args->write_commit_level = AS_POLICY_COMMIT_LEVEL_ALL;
				}
				else if (strcmp(optarg, "master") == 0) {
					args->write_commit_level = AS_POLICY_COMMIT_LEVEL_MASTER;
				}
				else {
					blog_line("commitLevel be all or master");
					return 1;
				}
				break;

			case 'Y':
				args->conn_pools_per_node = atoi(optarg);
				break;

			case 'D':
				args->durable_deletes = true;
				break;

			case 'a':
				args->async = true;
				break;

			case 'c':
				args->async_max_commands = atoi(optarg);
				break;

			case 'W':
				args->event_loop_capacity = atoi(optarg);
				break;

			case 'A':
				args->tls.enable = true;
				break;

			case 'E':
				args->tls.cafile = strdup(optarg);
				break;

			case 'F':
				args->tls.capath = strdup(optarg);
				break;

			case 'G':
				args->tls.protocols = strdup(optarg);
				break;

			case 'H':
				args->tls.cipher_suite = strdup(optarg);
				break;

			case 'I':
				args->tls.crl_check = true;
				break;

			case 'J':
				args->tls.crl_check_all = true;
				break;

			case 'O':
				args->tls.cert_blacklist = strdup(optarg);
				break;

			case 'Q':
				args->tls.log_session_info = true;
				break;

			case 'Z':
				args->tls.keyfile = strdup(optarg);
				break;

			case 'y':
				args->tls.certfile = strdup(optarg);
				break;

			case 'f':
				args->tls.for_login_only = true;
				break;

			case 'e':
				if (! as_auth_mode_from_string(&args->auth_mode, optarg)) {
					blog_line("invalid authentication mode: %s", optarg);
					return 1;
				}
				break;

			case 'u':
			default:
				return 1;
		}
	}
	return validate_args(args);
}

int
main(int argc, char * const * argv)
{
	arguments args;
	args.hosts = strdup("127.0.0.1");
	args.port = 3000;
	args.user = 0;
	args.password[0] = 0;
	args.namespace = "test";
	args.set = "testset";
	args.start_key = 1;
	args.keys = 1000000;
	args.numbins = 1;
	args.bintype = 'I';
	args.binlen = 50;
	args.binlen_type = LEN_TYPE_COUNT;
	args.random = false;
	args.transactions_limit = 0;
	args.init = false;
	args.init_pct = 100;
	args.read_pct = 50;
	args.del_bin = false;
	args.threads = 16;
	args.throughput = 0;
	args.batch_size = 0;
	args.read_socket_timeout = AS_POLICY_SOCKET_TIMEOUT_DEFAULT;
	args.write_socket_timeout = AS_POLICY_SOCKET_TIMEOUT_DEFAULT;
	args.read_total_timeout = AS_POLICY_TOTAL_TIMEOUT_DEFAULT;
	args.write_total_timeout = AS_POLICY_TOTAL_TIMEOUT_DEFAULT;
	args.max_retries = 1;
	args.debug = false;
	args.latency = false;
	args.latency_columns = 4;
	args.latency_shift = 3;
	args.use_shm = false;
	args.replica = AS_POLICY_REPLICA_SEQUENCE;
	args.read_mode_ap = AS_POLICY_READ_MODE_AP_ONE;
	args.read_mode_sc = AS_POLICY_READ_MODE_SC_SESSION;
	args.write_commit_level = AS_POLICY_COMMIT_LEVEL_ALL;
	args.durable_deletes = false;
	args.conn_pools_per_node = 1;
	args.async = false;
	args.async_max_commands = 50;
	args.event_loop_capacity = 1;
	memset(&args.tls, 0, sizeof(as_config_tls));
	args.auth_mode = AS_AUTH_INTERNAL;

	int ret = set_args(argc, argv, &args);
	
	if (ret == 0) {
		print_args(&args);
		run_benchmark(&args);
	}
	else {
		print_usage(argv[0]);
	}
	
	free(args.hosts);
	return ret;
}
