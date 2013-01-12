#!/usr/bin/python
import sys
import getopt
from subprocess import call

def Usage():
	print ('Helper script to Execute a command against an Aerospike cluster')
	print ('Usage: ascli')
	print ('-l, --location           location of the executables')
	print ('     	                 default: /opt/citrusleaf/bin/commands')
	print ('-u, --usage              usage')
	print ('-v, --verbose            verbose')
	print ('')
	print ('Example: ')
	print ('ascli udf-record-apply test myset mykey test sum 4 5')
	sys.exit(0)

if __name__ == '__main__':

	loc = "/opt/citrusleaf/bin/commands"
	verbose = False
	try:
		opts, args = getopt.getopt(sys.argv[1:], "l:uv")
	except getopt.GetoptError, err:
		print (str(err))
		Usage()
		sys.exit(-1)
	for o, a in opts:
		if ((o == "-l") or (o == "--location")):
			loc = a
		if ((o == "-u") or (o == "--usage")):
			Usage()
		if ((o == "-v") or (o == "--verbose")):
			verbose = True;

	if (len(args)<1):
		Usage()
		sys.exit(-2)		

	args[0] = loc+"/"+args[0]
	if verbose:
		print 'executing: ' + ' '.join(args)

	call(args)

