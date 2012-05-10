from __future__ import with_statement
import sys
from time import time, sleep
import types
import getopt
from random import randrange
import Queue
import thread
import os
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib'))
print path
if not path in sys.path:
	sys.path.insert(1,path)
del path

path_py = os.path.abspath(os.path.join(os.path.dirname(__file__), '../python'))
print path_py
if not path_py in sys.path:
	sys.path.insert(1,path_py)
del path_py

#Add path to the compiled python module
sys.path.append('../lib')

#Add path to the compiled python library (citrusleaf.pyc)
sys.path.append('..')

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
        opts, args = getopt.getopt(sys.argv[1:], "h:p:", ["host=","port="])
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
for o, a in opts:
        if ((o == "-h") or (o == "--host")):
                arg_host = a
        if (o == "-p" or o == "--port"):
                arg_port = int(a)
    
#Test host and port
print arg_host
print arg_port

#Initialize citrusleaf
cl.citrusleaf_init()

#Create citrusleaf cluster
asc=cl.citrusleaf_cluster_create()

#Add host to cluster
rv = cl.citrusleaf_cluster_add_host(asc,arg_host,arg_port,1000)
if(rv!=0): 
	print "Citrusleaf add host failed with "+str(int(rv))
if rv==cl.CITRUSLEAF_OK:
	print "Successfully added host ",arg_host



#CITRUSLEAF_PUT
#Initialize the key object
o_key = cl.cl_object()
cl.citrusleaf_object_init_str(o_key,"key1")

#Create a cl_bin array of 3 elements - can add more to this list and use accordingly
bin_name = ["bin1","bin2","bin3"]
value = ["value1","value2",42949673064]
num_bins = 3
bins=cl.cl_bin_arr(num_bins);

#Fill array with cl_bin structures of type string
for i in xrange(num_bins-1):
	b = cl.cl_bin()
	b.bin_name = bin_name[i]
	cl.citrusleaf_object_init_str(b.object,value[i])
	bins[i] = b

#Add one bin of type int 
b = cl.cl_bin()
b.bin_name = bin_name[2]
cl.citrusleaf_object_init_int(b.object,value[2])
bins[2] = b

#define namespace, set and write parameters for put
ns = "usermap"
set = ""
cl_wp = cl.cl_write_parameters()
cl.cl_write_parameters_set_default(cl_wp)
cl_wp.timeout_ms = 1000
cl_wp.record_ttl = 100 #Define record_ttl as part of the write parameters

#Call citrusleaf put
rv = cl.citrusleaf_put(asc,ns,set,o_key,bins,num_bins,cl_wp)
if rv==cl.CITRUSLEAF_OK:
	print "Citrusleaf put succeeded"
else:
	print "Citrusleaf put failed with ",rv



#CITRUSLEAF GET
#declare an array of cl_bin with num_bins elements
bins_get = cl.cl_bin_arr(num_bins)

#Bin names for the two bins
for i in xrange(num_bins-1):
	b=cl.cl_bin()
	b.bin_name = bin_name[i]
	bins_get[i] = b

#Add bin of object int
b=cl.cl_bin()
b.bin_name = bin_name[2]
bins_get[2] = b

#Create a int * pointer for generation 
gen = cl.new_intp()
#gen = c_int()

#Call citrusleaf_get 
timeout = 1000

rv = cl.citrusleaf_get(asc,ns,set,o_key,bins_get,num_bins,timeout,(gen))

if rv==cl.CITRUSLEAF_OK:
	print "Citrusleaf get succeeded"
	#print the resulting values and the generation
	for i in xrange(num_bins):
		if bins_get[i].object.type == cl.CL_STR:
			print "Bin name: ",bins_get[i].bin_name,"Resulting string: ",bins_get[i].object.u.str
		elif bins_get[i].object.type == cl.CL_INT:
			print "Bin name: ",bins_get[i].bin_name,"Resulting int: ",bins_get[i].object.u.i64
	print "Generation ",cl.intp_value(gen)
else:
	print "Citrusleaf get failed with ",rv

#Free the bins that we recieved
cl.citrusleaf_bins_free(bins_get,num_bins)


#CITRUSLEAF GET ALL
#Declare a int pointer for number of bins
sz = cl.new_intp()

#Declare a ref pointer for cl_bin * 
bins_get_all = cl.new_cl_bin_p()

#Call citrusleaf_get_all with the pointer bins and pointer sz 
rv = cl.citrusleaf_get_all(asc,ns,set,o_key,bins_get_all,sz,1000, gen)
if rv==cl.CITRUSLEAF_OK:
	print "Citrusleaf get all succeeded"
else:
	print "Citrusleaf get all failed with ",rv

#Number of bins returned
n = cl.intp_value(sz)

#Call get_bins with pointer bins and number of bins
arr = get_bins(bins_get_all,n)

#print the value of the bins recieved
for i in xrange(n):
	if(arr[i].object.type)==cl.CL_STR:
		print "Bin name: ",arr[i].bin_name,"Resulting string: ",arr[i].object.u.str
	elif(arr[i].object.type)==cl.CL_INT:
		print "Bin name: ",arr[i].bin_name,"Resulting int: ",arr[i].object.u.i64

#Free the bins that we received 
cl.citrusleaf_bins_free(arr,n)

#CITRUSLEAF DELETE
rv = cl.citrusleaf_delete(asc,ns,set,o_key,cl_wp)
if rv==cl.CITRUSLEAF_OK:
	print "Citrusleaf delete succeeded"
else:
	print "Citrusleaf delete failed with",rv



#CITRUSLEAF_PUT_DIGEST
#Create a pointer to cf_digest
d = cl.new_cf_digest_p()

#Assign the digest to cf_digest manually
cd = "testdigesttestdigest"
d.digest = cd
rv = cl.citrusleaf_put_digest(asc,ns,d,bins,n,cl_wp)
if rv==cl.CITRUSLEAF_OK:
	print "Citrusleaf put digest succeeded"
else:
	 print "Citrusleaf put digest failed with",rv



#CITRUSLEAF GET DIGEST
#declare an array of cl_bin with num_bins elements in which we will get the value with digest cd
bins_gd = cl.cl_bin_arr(num_bins)

#Bin names for the two bins
for i in xrange(num_bins-1):
	b=cl.cl_bin()
	b.bin_name = bin_name[i]
	bins_gd[i] = b

#Add bin of object int
b=cl.cl_bin()
b.bin_name = bin_name[2]
bins_gd[2] = b

#Create a int * pointer for generation 
gen_gd = cl.new_intp()

rv = cl.citrusleaf_get_digest(asc,ns,d,bins_gd,n,1000,gen_gd)
if rv==cl.CITRUSLEAF_OK:
	print "Citrusleaf get digest succeeded"
	#print the resulting values and the generation
	for i in xrange(num_bins):
		if bins_gd[i].object.type == cl.CL_STR:
			print "Bin name: ",bins_gd[i].bin_name,"Resulting string: ",bins_gd[i].object.u.str
		elif bins_gd[i].object.type == cl.CL_INT:
			print "Bin name: ",bins_gd[i].bin_name,"Resulting int: ",bins_gd[i].object.u.i64
	print "Generation ",cl.intp_value(gen_gd)
else:
	print "Citrusleaf get digest failed with ",rv

#Free the bins received
cl.citrusleaf_bins_free(bins_gd,n)

#CITRUSLEAF DELETE DIGEST 
rv = cl.citrusleaf_delete_digest(asc, ns, d, cl_wp);
if rv==cl.CITRUSLEAF_OK:
	print "Citrusleaf delete digest succeeded"
else:
	print "Citrusleaf delete digest failed with", rv


#DESTROY CLUSTER
#Cluster destroy
cl.citrusleaf_cluster_destroy(asc);

#CITRUSLEAF SHUTDOWN
cl.citrusleaf_shutdown();
