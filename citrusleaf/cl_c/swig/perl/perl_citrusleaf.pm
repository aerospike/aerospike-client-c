#Helper functions 
use citrusleaf;
#Get bins function for get_all. Send a pointer to cl_bin and return an array of cl_bin
package perl_citrusleaf;

sub get_bins{
	$pointer = $_[0];
	$n = $_[1];
	#Declare an array of cl_bin with size n
	$array = new citrusleaf::cl_bin_arr($n);
	#Deference the bins pointer returned
	$binp = citrusleaf::cl_bin_p_value($pointer);
	for ($i = 0; $i < $n; $i++) {
		#Define a cl_bin structure 'b'
		$b = new citrusleaf::cl_bin();
		#get name of ith index of binp
		$b->{bin_name} = citrusleaf::get_name($binp,$i);
		#get object with name bn	
		$b->{object} = citrusleaf::get_object($binp,$i);
		#Assigning the newly built struct to ith index of array
		$array->setitem($i, $b);
		$b->DESTROY();
	}
	return $array;
}


1;
