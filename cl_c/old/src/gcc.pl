#!/usr/bin/perl

$cc = "";
$ver = "";

# figure out which version of gcc to use, and if it supports the flags we like?

if (-e "/usr/bin/gcc43" ) {
	$cc = "gcc43";
	$ver = "4.3";
}
elsif (-e "/usr/bin/gcc44" ) {
	$cc = "gcc44";
	$ver = "4.4";
}
else {
	$vers = `gcc --version | head -1`;

	# gcc version strings vary a bit between RH and Debian builds.
	# debian seems to be: gcc (Ubuntu 4.3.3-blah) 4.3.3
	# RH seems to be: gcc (GCC) 4.1.1 20080101010 (Red Hat ...)
	# you seem guaranteed that the version is the second thing
	# if I was better at regexp, this would probably be easy
	$cc = "gcc";

	$state=0;
	$version = "";

	foreach $c (split(//, $vers)) {
		if ($c eq ")") {
			$state = 1;
		}
		elsif ($state == 1 && $c eq " ") {
			$state = 2;
		}
		elsif ($state == 2) {
			if ($c eq " " || (ord $c < 17)) {
				$state = 3;
				break;
			}
			else {
				$version = $version . $c;
			}
		}	
	}
	$ver = substr($version, 0, 3);
}

if (@ARGV < 1) {
	print $cc . " " . $ver ;
}
else {
	if ($ARGV[0] eq "v") {
		print $ver ;
	}
	else {
		print $cc;
	}
}
