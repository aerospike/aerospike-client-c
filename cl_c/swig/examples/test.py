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
print "\nCITRUSLEAF PUT"
o_key = cl.cl_object()
cl.citrusleaf_object_init_str(o_key,"key1")

o_key1 = cl.cl_object()
cl.citrusleaf_object_init_str(o_key1,"key2")

key = [o_key,o_key1]
n_keys = len(key)

#Create a cl_bin array of 4 elements - can add more to this list and use accordingly
bin_name = ["bin1","bin2","bin3", "bin4"]
value = ["value1","value2",42949673064, "some-value-that-is-going-to-be-compressed"]
num_bins = len(bin_name)
string_bins = 2
bins=cl.cl_bin_arr(num_bins);

#Fill array with cl_bin structures of type string
for i in xrange(string_bins):
	b = bins[i] 
	b.bin_name = bin_name[i]
	cl.citrusleaf_object_init_str(b.object,value[i])
	bins[i] = b

#Add one bin of type int 
b = bins[2]
b.bin_name = bin_name[2]
cl.citrusleaf_object_init_int(b.object,value[2])
bins[2] = b

#Add one bin of type blob
b = bins[3]
b.bin_name = bin_name[3]
val = zlib.compress(value[3])
cl.citrusleaf_object_init_blob(b.object,val, len(val))
bins[3] = b

#define namespace, set and write parameters for put
ns = "usermap"
set = ""
cl_wp = cl.cl_write_parameters()
cl.cl_write_parameters_set_default(cl_wp)
cl_wp.timeout_ms = 1000
cl_wp.record_ttl = 100 #Define record_ttl as part of the write parameters

#Call citrusleaf put for two keys
for i in xrange(n_keys):
	rv = cl.citrusleaf_put(asc,ns,set,key[i],bins,num_bins,cl_wp)
	if rv==cl.CITRUSLEAF_OK:
		print "Citrusleaf put succeeded"
	else:
		print "Citrusleaf put failed with ",rv

#CITRUSLEAF GET
print "\nCITRUSLEAF GET"
#declare an array of cl_bin with num_bins elements
bins_get = cl.cl_bin_arr(num_bins)

#Bin names for the bins
for i in xrange(num_bins):
	b = bins_get[i]
	cl.citrusleaf_object_init_null(b.object)
	b.bin_name = bin_name[i]
	bins_get[i] = b

#Create a int * pointer for generation 
gen = cl.new_intp()

#Call citrusleaf_get 
timeout = 1000

rv = cl.citrusleaf_get(asc,ns,set,key[0],bins_get,num_bins,timeout,(gen))

if rv==cl.CITRUSLEAF_OK:
	print "Citrusleaf get succeeded"
	#print the resulting values and the generation
	for i in xrange(num_bins):
		if bins_get[i].object.type == cl.CL_STR:
			print "Bin name: ",bins_get[i].bin_name,"Resulting string: ",bins_get[i].object.u.str
		elif bins_get[i].object.type == cl.CL_INT:
			print "Bin name: ",bins_get[i].bin_name,"Resulting int: ",bins_get[i].object.u.i64
		elif bins_get[i].object.type == cl.CL_BLOB:
                        binary_data = cl.cdata(bins_get[i].object.u.blob, bins_get[i].object.sz)
			print "Bin name: ",bins_get[i].bin_name,"Resulting decompressed blob: ",zlib.decompress(binary_data)
		else:
			print "Bin name: ",bins_get[i].bin_name,"Unknown bin type: ",bins_get[i].object.type
	print "Generation ",cl.intp_value(gen)
else:
	print "Citrusleaf get failed with ",rv

#Free the bins that we recieved
cl.citrusleaf_free_bins(bins_get,num_bins,None)


#CITRUSLEAF GET ALL
print "\nCITRUSLEAF GET ALL"
#Declare a int pointer for number of bins
sz = cl.new_intp()

#Declare a ref pointer for cl_bin * 
bins_get_all = cl.new_cl_bin_p()

#Call citrusleaf_get_all with the pointer bins and pointer sz 
rv = cl.citrusleaf_get_all(asc,ns,set,key[0],bins_get_all,sz,1000, gen)
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
	elif arr[i].object.type == cl.CL_BLOB:
		binary_data = cl.cdata(arr[i].object.u.blob, arr[i].object.sz)
		print "Bin name: ",arr[i].bin_name,"Resulting decompressed blob: ",zlib.decompress(binary_data)
	else:
		print "Bin name: ",arr[i].bin_name,"Unknown bin type: ",arr[i].object.type

#Free the bins that we received 
cl.citrusleaf_free_bins(arr,n,bins_get_all)
cl.delete_intp(sz)
cl.delete_cl_bin_p(bins_get_all)

#CITRUSLEAF BATCH GET - CITRUSLEAF GET MANY DIGESTS
print "\nCITRUSLEAF BATCH GET"

#Calculate the digests and store them in the digest array di
n_digests = n_keys
#Declaring the digest array
di = cl.cf_digest_arr(n_digests)
for i in xrange(n_digests):
	d = cl.cf_digest()
	#Call the C function calculate digest to calculate the digest from key and set
	cl.citrusleaf_calculate_digest(set,key[i],d)
	di[i] = d

#Call wrapper function get many digest
p = cl.citrusleaf_batch_get(asc,ns,di,n_digests,None,0,1)
if p.rv == cl.CITRUSLEAF_OK:
	print "Citrusleaf batch get succeeded"
	sp = p.records
	n_loop = p.index
	for i in xrange(n_loop):
		print "\nFor record ",i
		print "Generation = ",sp[i].gen
		print "Record_ttl = ",sp[i].record_ttl
		n_bins_batch = sp[i].n_bins
		arr_bins = get_bins(sp[i].bin,n_bins_batch)
		for k in xrange(n_bins_batch):
			if arr_bins[k].object.type == cl.CL_STR:
				print "Bin name:",arr_bins[k].bin_name ,"Resulting string: ",arr_bins[k].object.u.str
			elif arr_bins[k].object.type == cl.CL_INT:
				print "Bin name: ",arr_bins[k].bin_name,"Resulting int: ",arr_bins[k].object.u.i64
			elif arr_bins[k].object.type == cl.CL_BLOB:
				binary_data = cl.cdata(arr_bins[k].object.u.blob, arr_bins[k].object.sz)
				print "Bin name: ",arr_bins[k].bin_name,"Resulting decompressed blob: ",zlib.decompress(binary_data)
			else:
				print "Bin name: ",arr_bins[k].bin_name,"Unknown bin type: ",arr_bins[k].object.type
		#Free bins array 
		cl.citrusleaf_free_bins(arr_bins,n_bins_batch,sp[i].bin)
		cl.free(sp[i].bin)
	cl.free(p.records)
else:
	print "Citrusleaf batch get failed with ",p.rv

#CITRUSLEAF DELETE
print "\nCITRUSLEAF DELETE"

for i in xrange(n_keys):
	rv = cl.citrusleaf_delete(asc,ns,set,key[i],cl_wp)
	if rv==cl.CITRUSLEAF_OK:
		print "Citrusleaf delete succeeded"
	else:
		print "Citrusleaf delete failed with",rv



#CITRUSLEAF_PUT_DIGEST
print "\nCITRUSLEAF PUT DIGEST"

#Create a pointer to cf_digest
d = cl.cf_digest()

#Assign the digest to cf_digest manually
#Create a hex string and convert it to a byte string to be passed to the put function
cd = "aefdefaefdefaefdefaefdefaefdefffffffffff"
cd_str = cd.decode("hex")
d.digest = cd_str
rv = cl.citrusleaf_put_digest(asc,ns,d,bins,num_bins,cl_wp)
if rv==cl.CITRUSLEAF_OK:
	print "Citrusleaf put digest succeeded"
else:
	 print "Citrusleaf put digest failed with",rv



#CITRUSLEAF GET DIGEST
print "\nCITRUSLEAF GET DIGEST"
#declare an array of cl_bin with num_bins elements in which we will get the value with digest cd
bins_gd = cl.cl_bin_arr(num_bins)

#Bin names for the two bins
for i in xrange(num_bins):
	b=bins_gd[i] 
	cl.citrusleaf_object_init_null(b.object)
	b.bin_name = bin_name[i]
	bins_gd[i] = b

#Create a int * pointer for generation 
gen_gd = cl.new_intp()

rv = cl.citrusleaf_get_digest(asc,ns,d,bins_gd,n,1000,gen_gd)
if rv==cl.CITRUSLEAF_OK:
	print "Citrusleaf get digest succeeded"
	#print the resulting values and the generation
	for i in xrange(n):
		if bins_gd[i].object.type == cl.CL_STR:
			print "Bin name: ",bins_gd[i].bin_name,"Resulting string: ",bins_gd[i].object.u.str
		elif bins_gd[i].object.type == cl.CL_INT:
			print "Bin name: ",bins_gd[i].bin_name,"Resulting int: ",bins_gd[i].object.u.i64
		elif bins_gd[k].object.type == cl.CL_BLOB:
			binary_data = cl.cdata(bins_gd[k].object.u.blob, bins_gd[k].object.sz)
			print "Bin name: ",bins_gd[k].bin_name,"Resulting decompressed blob: ",zlib.decompress(binary_data)
		else:
			print "Bin name: ",bins_gd[k].bin_name,"Unknown bin type: ",bins_gd[k].object.type

	print "Generation ",cl.intp_value(gen_gd)
else:
	print "Citrusleaf get digest failed with ",rv

#Free the bins received and the generation pointers
cl.citrusleaf_free_bins(bins_gd,num_bins,None)
cl.delete_intp(gen_gd)
cl.delete_intp(gen)

#CITRUSLEAF DELETE DIGEST 
print "\nCITRUSLEAF DELETE DIGEST"
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

