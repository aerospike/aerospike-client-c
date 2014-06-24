/*******************************************************************************
 * Copyright 2008-2013 by Aerospike.
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
 *s
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
#include <getopt.h>

static const char* short_options = "h:p:U:P::n:s:k:o:Rw:z:g:T:dL:u";

static struct option long_options[] = {
	{"hosts",        1, 0, 'h'},
	{"port",         1, 0, 'p'},
	{"user",         1, 0, 'U'},
	{"password",     2, 0, 'P'},
	{"namespace",    1, 0, 'n'},
	{"set",          1, 0, 's'},
	{"keys",         1, 0, 'k'},
	{"objectSpec",   1, 0, 'o'},
	{"random",       0, 0, 'R'},
	{"workload",     1, 0, 'w'},
	{"threads",      1, 0, 'z'},
	{"throughput",   1, 0, 'g'},
	{"timeout",      1, 0, 'T'},
	{"readTimeout",  1, 0, 'X'},
	{"writeTimeout", 1, 0, 'V'},
	{"maxRetries",   1, 0, 'r'},
	{"debug",        0, 0, 'd'},
	{"latency",      1, 0, 'L'},
	{"usage",        0, 0, 'u'},
	{0,              0, 0, 0}
};

static void
print_usage(const char* program)
{
	blog_line("Usage: %s <options>", program);
	blog_line("options:");
	blog_line("");
	
	blog_line("-h --hosts <address1>,<address2>...  # Default: localhost");
	blog_line("   Aerospike server seed hostnames or IP addresses.");
	blog_line("");
	
	blog_line("-p --port <port>      # Default: 3000");
	blog_line("   Aerospike server seed hostname or IP address.");
	blog_line("");
	
	blog_line("-U --user <user name> # Default: empty");
	blog_line("   User name for Aerospike servers that require authentication.");
	blog_line("");

	blog_line("-P[<password>]  # Default: empty");
	blog_line("   User's password for Aerospike servers that require authentication.");
	blog_line("   If -P is set, the actual password if optional. If the password is not given,");
	blog_line("   the user will be prompted on the command line.");
	blog_line("   If the password is given, it must be provided directly after -P with no");
	blog_line("   intrevening space (ie. -Pmypass).");
	blog_line("   ");
	blog_line("");

	blog_line("-n --namespace <ns>   # Default: test");
	blog_line("   Aerospike namespace.");
	blog_line("");
	
	blog_line("-s --set <set name>   # Default: testset");
	blog_line("   Aerospike set name.");
	blog_line("");
	
	blog_line("-k --keys <count>     # Default: 1000000");
	blog_line("   Key/record count or key/record range.");
	blog_line("");
	
	blog_line("-o --objectSpec I| B:<size> | S:<size>  # Default: I");
	blog_line("   Bin object specification.");
	blog_line("   -o I     : Read/write integer bin.");
	blog_line("   -o B:200 : Read/write byte array bin of length 200.");
	blog_line("   -o S:50  : Read/write string bin of length 50.");
	blog_line("");
	
	blog_line("-R --random          # Default: static fixed bin values");
	blog_line("   Use dynamically generated random bin values instead of default static fixed bin values.");
	blog_line("");
	
	blog_line("-w --workload I,<percent> | RU,<read percent>  # Default: RU,50");
	blog_line("   Desired workload.");
	blog_line("   -w I,60  : Linear 'insert' workload initializing 60%% of the keys.");
	blog_line("   -w RU,80 : Random read/update workload with 80%% reads and 20%% writes.");
	blog_line("");
	
	blog_line("-z --threads <count> # Default: 16");
	blog_line("   Load generating thread count.");
	blog_line("");
	
	blog_line("-g --throughput <tps> # Default: 0");
	blog_line("   Throttle transactions per second to a maximum value.");
	blog_line("   If tps is zero, do not throttle throughput.");
	blog_line("   Used in read/write mode only.");
	blog_line("");

	blog_line("-T --timeout <ms>    # Default: 0");
	blog_line("   Read/Write timeout in milliseconds.");
	blog_line("");
	
	blog_line("   --readTimeout <ms> # Default: 0");
	blog_line("   Read timeout in milliseconds.");
	blog_line("");
	
	blog_line("   --writeTimeout <ms> # Default: 0");
	blog_line("   Write timeout in milliseconds.");
	blog_line("");
	
	blog_line("   --maxRetries {0,1}  # Default: 1");
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
	blog("hosts:          ");
	for (int i = 0; i < args->host_count; i++) {
		if (i > 0) {
			blog(", ");
		}
		blog("%s", args->hosts[i]);
	}
	blog_line("");
	
	blog_line("port:           %d", args->port);
	blog_line("user:           %s", args->user);
	blog_line("namespace:      %s", args->namespace);
	blog_line("set:            %s", args->set);
	blog_line("keys/records:   %d", args->keys);
	blog("object spec:    ");
	
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
			
		default:
			blog_line("");
			break;
	}
	
	blog_line("random values:  %s", boolstring(args->random));
	blog("workload:       ");
	
	if (args->init) {
		blog_line("initialize %d%% of records", args->init_pct);
	}
	else {
		blog_line("read %d%% write %d%%", args->read_pct, 100 - args->read_pct);
	}
	
	blog_line("threads:        %d", args->threads);
	
	if (args->throughput > 0) {
		blog_line("max throughput: %d tps", args->throughput);
	}
	else {
		blog_line("max throughput: unlimited", args->throughput);
	}
	blog_line("read timeout:   %d ms", args->read_timeout);
	blog_line("write timeout:  %d ms", args->write_timeout);
	blog_line("max retries:    %d", args->max_retries);
	blog_line("debug:          %s", boolstring(args->debug));
	
	if (args->latency) {
		blog_line("latency:        %d columns, shift exponent %d", args->latency_columns, args->latency_shift);
	}
	else {
		blog_line("latency:        false");
	}
}

static int
validate_args(arguments* args)
{
	if (args->keys <= 0) {
		blog_line("Invalid number of keys: %d  Valid values: [> 0]", args->keys);
		return 1;
	}
	
	switch (args->bintype) {
		case 'I':
			break;
			
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
	
	if (args->read_timeout < 0) {
		
		blog_line("Invalid read timeout: %d  Valid values: [>= 0]", args->read_timeout);
		return 1;
	}
	
	if (args->read_timeout < 0) {
		
		blog_line("Invalid write timeout: %d  Valid values: [>= 0]", args->write_timeout);
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
	return 0;
}

static void
free_hosts(arguments* args)
{
	free(args->hosts);
	free(args->host_string);
}

static int
set_args(int argc, char * const * argv, arguments* args)
{
	int option_index = 0;
	int c;
	
	while ((c = getopt_long(argc, argv, short_options, long_options, &option_index)) != -1) {
		switch (c) {
			case 'h': {
				free_hosts(args);
				args->host_string = strdup(optarg);
				char* p = args->host_string;
				int count = 1;
				
				strsep(&p, ",");
				
				while (p) {
					strsep(&p, ",");
					count++;
				}
				
				args->hosts = malloc(count * sizeof(char*));
				p = args->host_string;
				
				for (int i = 0; i < count; i++) {
					args->hosts[i] = p;
					p += strlen(p) + 1;
				}
				args->host_count = count;
				break;
			}
				
			case 'p':
				args->port = atoi(optarg);
				break;
				
			case 'U':
				args->user = optarg;
				break;
			
			case 'P':
				as_password_prompt_hash(optarg, args->password);
				break;

			case 'n':
				args->namespace = optarg;
				break;
				
			case 's':
				args->set = optarg;
				break;
				
			case 'k':
				args->keys = atoi(optarg);
				break;
				
			case 'o': {
				args->bintype = *optarg;
				
				if (args->bintype == 'B' || args->bintype == 'S') {
					char *p = optarg + 1;
					if (*p == ':') {
						args->binlen = atoi(p+1);
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
				
			case 'w': {
				char* tmp = strdup(optarg);
				char* p = strchr(tmp, ',');
				args->init = (*tmp == 'I');
				
				if (p) {
					*p = 0;
					
					if (args->init) {
						args->init_pct = atoi(p + 1);
					}
					else {
						args->read_pct = atoi(p + 1);
					}
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

			case 'T':
				args->read_timeout = atoi(optarg);
				args->write_timeout = args->read_timeout;
				break;
				
			case 'X':
				args->read_timeout = atoi(optarg);
				break;
				
			case 'V':
				args->write_timeout = atoi(optarg);
				break;

			case 'r':
				args->max_retries = atoi(optarg);
				
				if (args->max_retries > 1) {
					blog_line("maxRetries must be 0 or 1");
					return 1;
				}
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
				
			case 'u':
			default:
				return 1;
		}
	}
	return validate_args(args);
}

static void
cleanup(arguments* args)
{
	free_hosts(args);
}

int
main(int argc, char * const * argv)
{
	arguments args;
	args.host_string = strdup("127.0.0.1");
	args.hosts = malloc(sizeof(char*));
	args.hosts[0] = args.host_string;
	args.host_count = 1;
	args.port = 3000;
	args.user = 0;
	args.password[0] = 0;
	args.namespace = "test";
	args.set = "testset";
	args.keys = 1000000;
	args.bintype = 'I';
	args.binlen = 50;
	args.random = false;
	args.init_pct = 100;
	args.read_pct = 50;
	args.threads = 16;
	args.throughput = 0;
	args.read_timeout = 0;
	args.write_timeout = 0;
	args.max_retries = 1;
	args.debug = false;
	args.latency = false;
	args.latency_columns = 4;
	args.latency_shift = 3;
	
	int ret = set_args(argc, argv, &args);
	
	if (ret == 0) {
		print_args(&args);
		run_benchmark(&args);
	}
	else {
		print_usage(argv[0]);
	}
	cleanup(&args);
	return ret;
}
