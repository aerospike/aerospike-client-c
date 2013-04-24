#Helper functions 
#Add ./lib to the path of python modules
import os
import sys
import citrusleaf as cl  
#Get bins function for get_all. Send a pointer to cl_bin and return an array of cl_bin
def get_bins(pointer,n):
	#Declare an array of cl_bin with size n
	array = cl.cl_bin_arr(n)
	#Deference the bins pointer returned
	binp = cl.cl_bin_p_value(pointer)
	for i in xrange(n):
		#Define a cl_bin structure 'b'
		b = array[i]
		#get name of ith index of binp
		b.bin_name = cl.get_name(binp,i)
		#get object with name bn	
		b.object = cl.get_object(binp,i)
		#Assigning the newly built struct to ith index of array
		array[i] = b
	return array


