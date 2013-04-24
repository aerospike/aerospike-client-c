import sys
import getopt
import os
import zlib

path_py = os.path.abspath(os.path.join(os.path.dirname(__file__), '../python'))
print path_py
if not path_py in sys.path:
	sys.path.insert(1,path_py)
del path_py

#Add path to python_citrusleaf
sys.path.append('../python')

import citrusleaf as cl
import python_citrusleaf
from array import array
from python_citrusleaf import * 

def usage():
        print "Usage:"
        print " -h host (default 127.0.0.1)"
        print " -p port (default 3000)"
        return

#arg processing
try:
        opts, args = getopt.getopt(sys.argv[1:], "h:p:n:", ["host=","port=","namespace="])
except getopt.GetoptError, err:
        print str(err)
        usage()
        sys.exit(-1)
arg_host = "127.0.0.1"
arg_port = 3000
arg_keys = 10000
arg_reads = 10000
arg_writes = 10000
arg_value_length = 100
arg_verbose = False
ns = "test"

for o, a in opts:
        if ((o == "-h") or (o == "--host")):
                arg_host = a
        if (o == "-p" or o == "--port"):
                arg_port = int(a)
        if (o == "-n" or o == "--namespace"):
                ns = a
    
#Test host and port
print arg_host
print arg_port

# Init shm before creating cluster (if using shared memory)
# The first argument should be the maximum number of nodes the cluster 
# can have. The second argument is the shm key.
# If both arguments are zero, default number of nodes, i.e 64 and default key is used
# cl.citrusleaf_use_shm(10,788722985);

#Initialize citrusleaf
cl.citrusleaf_init()

#Initialize async threads.
cl.citrusleaf_async_initialize(5000,1)

#Create citrusleaf cluster
asc=cl.citrusleaf_cluster_create()

#Add host to cluster
rv = cl.citrusleaf_cluster_add_host(asc,arg_host,arg_port,1000)
if(rv!=0): 
	print "Citrusleaf add host failed with "+str(int(rv))
if rv==cl.CITRUSLEAF_OK:
	print "Successfully added host ",arg_host



#CITRUSLEAF_PUT
print "\nCITRUSLEAF PUT sequential"

#define set and write parameters for put
set = ""
cl_wp = cl.cl_write_parameters()
cl.cl_write_parameters_set_default(cl_wp)
cl_wp.timeout_ms = 1000
cl_wp.record_ttl = 100000 #Define record_ttl as part of the write parameters

#Create a cl_bin array of 1 elements
bins=cl.cl_bin_arr(1);
bins[0].binname = "intval"
b = bins[0] 

a = 0
while a < 100: 
	#Initialize the key object
	o_key = cl.cl_object()
	cl.citrusleaf_object_init_int(o_key,a)

	#Initialize value for the bin
	cl.citrusleaf_object_init_int(b.object,a)
	bins[0] = b;
	
	#Putting
	rv = cl.citrusleaf_put(asc,ns,set,o_key,bins,1,cl_wp)
	if rv==cl.CITRUSLEAF_OK:
		print "Citrusleaf put for key",a,"succeeded"
	else:
		print "Citrusleaf put key",a,"failed with ",rv	
	a+=1


#DESTROY CLUSTER
#Cluster destroy
cl.citrusleaf_cluster_destroy(asc);

#CITRUSLEAF SHUTDOWN
cl.citrusleaf_shutdown();

