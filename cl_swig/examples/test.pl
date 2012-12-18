#!/usr/bin/env perl

# Include path to @INC to use citrusleaf.pm
BEGIN{
	use File::Basename;
	use File::Spec;
	$perl_dir = File::Spec->rel2abs(dirname(__FILE__));
	$perl_dir = $perl_dir . "/../perl";
	push @INC, $perl_dir;
}
use citrusleaf;
use perl_citrusleaf;

use Compress::Zlib;

# Default settings
$arg_host = "127.0.0.1";
$arg_port = 3000;
$arg_ns = "test";
$arg_keys = 10000;
$arg_reads = 10000;
$arg_writes = 10000;
$arg_value_length = 100;
$arg_verbose = 0;

sub usage
{
	print "Usage:" . "\n";
	print " -h host (default 127.0.0.1)" . "\n";
	print " -p port (default 3000)" . "\n";
	print " -n namespace (default test)" . "\n";
}

# Get opts
use warnings;
use Getopt::Std;
my %opt;
getopt('hpn', \%opt);

$arg_host = $opt{'h'} if (defined $opt{'h'});
$arg_port = int($opt{'p'}) if (defined $opt{'p'});
$arg_ns = $opt{'n'} if (defined $opt{'n'});
usage(),exit(-1) if $ARGV[0];

#Test host and port
print $arg_host . "\n";
print $arg_port . "\n";
print $arg_ns . "\n";

#Initialize citrusleaf
citrusleaf::citrusleaf_init();

#Create citrusleaf cluster
$asc=citrusleaf::citrusleaf_cluster_create();

#Add host to cluster
$rv = citrusleaf::citrusleaf_cluster_add_host($asc,$arg_host,$arg_port,1000);
if ($rv!=0) {
	print "Citrusleaf add host failed with " , $rv , "\n";
}
if ($rv == citrusleaf::CITRUSLEAF_OK) {
	print "Successfully added host " , $arg_host , "\n";
}


################ CITRUSLEAF_PUT ################
#Initialize the key object
print "\nCITRUSLEAF PUT\n";
$o_key = new citrusleaf::cl_object();
citrusleaf::citrusleaf_object_init_str($o_key,"key1");

$o_key1 = new citrusleaf::cl_object();
citrusleaf::citrusleaf_object_init_str($o_key1,"key2");

@key = ($o_key,$o_key1);
$n_keys = @key;

#Create a cl_bin array of 3 elements - can add more to this list and use accordingly
@bin_name = ( ["bin1_1","bin1_2","bin1_3", "bin1_4"],
			  ["bin2_1","bin2_2","bin2_3", "bin2_4"] );
@value = ( ["value1_1","value1_2",42949673064, "some-value-that-is-going-to-be-compressed_1"], 
		   ["value2_1","value2_2",42949673065, "some-value-that-is-going-to-be-compressed_2"] );

$num_bins = 4;
$string_bins = 2;
$bins_1 = new citrusleaf::cl_bin_arr($num_bins);
$bins_2 = new citrusleaf::cl_bin_arr($num_bins);
@bins = ($bins_1, $bins_2);

#define namespace, set and write parameters for put
$set = "";
$cl_wp = new citrusleaf::cl_write_parameters();
citrusleaf::cl_write_parameters_set_default($cl_wp);
$cl_wp->{timeout_ms} = 1000;
$cl_wp->{record_ttl} = 100; #Define record_ttl as part of the write parameters

@blobData = ();

for ($k = 0; $k < $n_keys; $k++) {
	#Fill array with cl_bin structures of type string
	for ($i = 0; $i < $string_bins; $i++) {
		$b = $bins[$k]->getitem($i);
		$b->{bin_name} = $bin_name[$k][$i];
		citrusleaf::citrusleaf_object_init_str($b->{object},$value[$k][$i]);
		$bins[$k]->setitem($i, $b);
	}
	
	#Add one bin of type int 
	$b = $bins[$k]->getitem($i);
	$b->{bin_name} = $bin_name[$k][2];
	citrusleaf::citrusleaf_object_init_int($b->{object},$value[$k][2]);
	$bins[$k]->setitem(2, $b);

	#Add one bin of type blob
	$b = $bins[$k]->getitem($i);
	$b->{bin_name} = $bin_name[$k][3];
	$val = compress($value[$k][3]);
	push(@blobData, $val);
	citrusleaf::citrusleaf_object_init_blob($b->{object},$blobData[$#blobData], length($val));
	$bins[$k]->setitem(3, $b);

	#Call citrusleaf put for two keys
	$rv = citrusleaf::citrusleaf_put($asc,$arg_ns,$set,$key[$k],$bins[$k],$num_bins,$cl_wp);
	if ($rv == citrusleaf::CITRUSLEAF_OK){
		print "Citrusleaf put succeeded" . "\n";
	} else {
		print "Citrusleaf put failed with " . $rv . "\n";
	}
}

################ CITRUSLEAF_GET ################
print "\nCITRUSLEAF GET\n";

# Declare an array of cl_bin with num_bins elements and set bin names
$bins_get = new citrusleaf::cl_bin_arr($num_bins);
for ($i = 0; $i < $num_bins; $i++) {
	$b = $bins_get->getitem($i);
	citrusleaf::citrusleaf_object_init_null($b->{object});
	$b->{bin_name} = $bin_name[0][$i];
	$bins_get->setitem($i, $b);
}

#Create a int * pointer for generation 
$gen = citrusleaf::new_intp();
$timeout = 1000;
$rv = citrusleaf::citrusleaf_get($asc,$arg_ns,$set,$key[0],$bins_get,$num_bins,$timeout,$gen);

if ($rv == citrusleaf::CITRUSLEAF_OK) {
	print "Citrusleaf get succeeded" . "\n";
	#print the resulting values and the generation
	for ($i = 0; $i < $num_bins; $i++) {
		$bin = $bins_get->getitem($i);
		$bin_name = $bin->{bin_name};
		$type = $bin->{object}->{type};
		if ($type == citrusleaf::CL_STR) {
			print "Bin name: ", $bin_name," Resulting string: ",$bin->{object}->{u}->{str}, "\n";
		}elsif ($type == citrusleaf::CL_INT) {
			print "Bin name: ",$bin_name," Resulting int: ",$bin->{object}->{u}->{i64}, "\n";
		}elsif ($type == citrusleaf::CL_BLOB){
            $binary_data = citrusleaf::cdata($bin->{object}->{u}->{blob}, $bin->{object}->{sz});
			print "Bin name: ",$bin_name," Resulting decompressed blob: ",uncompress($binary_data), "\n";
		}else{
			print "Bin name: ",$bin_name," Unknown bin type: ",$type, "\n";
		}
	}
	print "Generation ",citrusleaf::intp_value($gen), "\n";
} else {
	print "Citrusleaf get failed with ",$rv ,"\n";
}

#Free the bins that we recieved
citrusleaf::citrusleaf_free_bins($bins_get,$num_bins,undef);


################ CITRUSLEAF_GET_ALL ################
print "\nCITRUSLEAF GET ALL\n";

#Declare a int pointer for number of bins
$sz = citrusleaf::new_intp();

#Declare a ref pointer for cl_bin * 
$bins_get_all = citrusleaf::new_cl_bin_p();

#Call citrusleaf_get_all with the pointer bins and pointer sz 
$rv = citrusleaf::citrusleaf_get_all($asc,$arg_ns,$set,$key[0],$bins_get_all,$sz,1000, $gen);
if ($rv==citrusleaf::CITRUSLEAF_OK){
	print "Citrusleaf get all succeeded" , "\n";
}else{
	print "Citrusleaf get all failed with ", $rv, "\n";
}

#Number of bins returned
$n = citrusleaf::intp_value($sz);

#Call get_bins with pointer bins and number of bins
$arr = perl_citrusleaf::get_bins($bins_get_all,$n);

#print the value of the bins recieved
for ($i = 0; $i < $n; $i++) {
	$bin = $arr->getitem($i);
	$bin_name = $bin->{bin_name};
	$type = $bin->{object}->{type};
	if ($type == citrusleaf::CL_STR) {
		print "Bin name: ", $bin_name," Resulting string: ",$bin->{object}->{u}->{str}, "\n";
	}elsif ($type == citrusleaf::CL_INT) {
		print "Bin name: ",$bin_name," Resulting int: ",$bin->{object}->{u}->{i64}, "\n";
	}elsif ($type == citrusleaf::CL_BLOB){
		$binary_data = citrusleaf::cdata($bin->{object}->{u}->{blob}, $bin->{object}->{sz});
		print "Bin name: ",$bin_name," Resulting decompressed blob: ",uncompress($binary_data), "\n";
	}else{
		print "Bin name: ",$bin_name," Unknown bin type: ",$type, "\n";
	}
}

#Free the bins that we received 
citrusleaf::citrusleaf_free_bins($arr,$n,$bins_get_all);
citrusleaf::delete_intp($sz);
citrusleaf::delete_cl_bin_p($bins_get_all);


############## CITRUSLEAF BATCH GET - CITRUSLEAF GET MANY DIGESTS ##############
print "\nCITRUSLEAF BATCH GET\n";

#Calculate the digests and store them in the digest array di
$n_digests = $n_keys;
#Declaring the digest array
$di = new citrusleaf::cf_digest_arr($n_digests);
for ($i = 0; $i < $n_digests; $i++) {
	$d = $di->getitem($i);
	#Call the C function calculate digest to calculate the digest from key and set
	citrusleaf::citrusleaf_calculate_digest($set,$key[$i],$d);
	$di->setitem($i, $d);
}

#Call wrapper function get many digest
$p = citrusleaf::citrusleaf_batch_get($asc,$arg_ns,$di,$n_digests,undef,0,1);

if ($p->{rv} == citrusleaf::CITRUSLEAF_OK) {
	print "Citrusleaf batch get succeeded\n";
	$sp = $p->{records};
	$n_loop = $p->{index};
	
	for ($i = 0; $i < $n_loop; $i++) {
		$spi = $sp->getitem($i);
		print "Generation = ",$spi->{gen}, "\n";
		print "Digest = ",$spi->{digest}->{digest}, "\n";
		print "Record_ttl = ",$spi->{record_ttl}, "\n";
		$n_bins_batch = $spi->{n_bins};
		$array = perl_citrusleaf::get_bins($spi->{bin},$n_bins_batch);
		for ($k = 0; $k < $n_bins_batch; $k++) {
			$bin = $array->getitem($k);
			$bin_name = $bin->{bin_name};
			$type = $bin->{object}->{type};
			if ($type == citrusleaf::CL_STR) {
				print "Bin name: ", $bin_name," Resulting string: ",$bin->{object}->{u}->{str}, "\n";
			}elsif ($type == citrusleaf::CL_INT) {
				print "Bin name: ",$bin_name," Resulting int: ",$bin->{object}->{u}->{i64}, "\n";
			}elsif ($type == citrusleaf::CL_BLOB){
				$binary_data = citrusleaf::cdata($bin->{object}->{u}->{blob}, $bin->{object}->{sz});
				print "Bin name: ",$bin_name," Resulting decompressed blob: ",uncompress($binary_data), "\n";
			}else{
				print "Bin name: ",$bin_name," Unknown bin type: ",$type, "\n";
			}
		}
		#Free bins array 
		citrusleaf::citrusleaf_free_bins($array,$n_bins_batch,$spi->{bin});
		citrusleaf::free($spi->{bin});
	}
	citrusleaf::free($p->{records});
} else{
	print "Citrusleaf batch get failed with ", $p->{rv}, "\n";
}


################ CITRUSLEAF DELETE ################
print "\nCITRUSLEAF DELETE\n";

for ($i = 0; $i < $n_keys; $i++) {
	$rv = citrusleaf::citrusleaf_delete($asc,$arg_ns,$set,$key[$i],$cl_wp);
	if ($rv==citrusleaf::CITRUSLEAF_OK) {
		print "Citrusleaf delete succeeded", "\n";
	}else {
		print "Citrusleaf delete failed with",$rv, "\n";
	}
}


################ CITRUSLEAF PUT DIGEST ################
print "\nCITRUSLEAF PUT DIGEST\n";

#Create a pointer to cf_digest
$d = new citrusleaf::cf_digest();

#Assign the digest to cf_digest manually
$cd = "testdigesttestdigest";
$d->{digest} = $cd;

$rv = citrusleaf::citrusleaf_put_digest($asc,$arg_ns,$d,$bins[0],$n,$cl_wp);
if ($rv == citrusleaf::CITRUSLEAF_OK) {
	print "Citrusleaf put digest succeeded", "\n";
}else{
	 print "Citrusleaf put digest failed with",$rv, "\n";
}


################ CITRUSLEAF GET DIGEST ################
print "\nCITRUSLEAF GET DIGEST\n";
#declare an array of cl_bin with num_bins elements in which we will get the value with digest cd
$bins_gd = new citrusleaf::cl_bin_arr($num_bins);

#Bin names for the two bins
for ($i = 0; $i < $num_bins; $i++) {
	$b = $bins_gd->getitem($i);
	citrusleaf::citrusleaf_object_init_null($b->{object});
	$b->{bin_name} = $bin_name[0][$i];
	$bins_gd->setitem($i, $b);
}

#Create a int * pointer for generation 
$gen_gd = citrusleaf::new_intp();

$rv = citrusleaf::citrusleaf_get_digest($asc,$arg_ns,$d,$bins_gd,$n,1000,$gen_gd);
if ($rv==citrusleaf::CITRUSLEAF_OK) {
	print "Citrusleaf get digest succeeded", "\n";
	#print the resulting values and the generation
	for ($i = 0; $i < $num_bins; $i++) {
		$bin = $bins_gd->getitem($i);
		$bin_name = $bin->{bin_name};
		$type = $bin->{object}->{type};
		if ($type == citrusleaf::CL_STR) {
			print "Bin name: ", $bin_name," Resulting string: ",$bin->{object}->{u}->{str}, "\n";
		}elsif ($type == citrusleaf::CL_INT) {
			print "Bin name: ",$bin_name," Resulting int: ",$bin->{object}->{u}->{i64}, "\n";
		}elsif ($type == citrusleaf::CL_BLOB){
			$binary_data = citrusleaf::cdata($bin->{object}->{u}->{blob}, $bin->{object}->{sz});
			print "Bin name: ",$bin_name," Resulting decompressed blob: ",uncompress($binary_data), "\n";
		}else{
			print "Bin name: ",$bin_name," Unknown bin type: ",$type, "\n";
		}
	}
	print "Generation ",citrusleaf::intp_value($gen_gd), "\n";
}else{
	print "Citrusleaf get digest failed with ",$rv, "\n";
}

#Free the bins received and the generation pointers
citrusleaf::citrusleaf_free_bins($bins_gd,$n,undef);
citrusleaf::delete_intp($gen_gd);
citrusleaf::delete_intp($gen);


################ CITRUSLEAF DELETE DIGEST ################
print "\nCITRUSLEAF DELETE DIGEST\n";
$rv = citrusleaf::citrusleaf_delete_digest($asc, $arg_ns, $d, $cl_wp);
if ($rv==citrusleaf::CITRUSLEAF_OK) {
	print "Citrusleaf delete digest succeeded", "\n";
}else{
	print "Citrusleaf delete digest failed with", $rv, "\n";
}

#DESTROY CLUSTER
#Cluster destroy
citrusleaf::citrusleaf_cluster_destroy($asc);

#CITRUSLEAF SHUTDOWN
citrusleaf::citrusleaf_shutdown();

