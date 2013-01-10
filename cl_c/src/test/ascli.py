#!/usr/bin/python
import sys
import getopt

def log(msg):
    fptr.write( "%s, %s\n" %(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),msg))
    fptr.flush()    

if __name__ == '__main__':

    try:
        opts, args = getopt.getopt(sys.argv[1:], "p:h:o:v")
    except getopt.GetoptError, err:
        log (str(err))
        usage()
        sys.exit(-1)
	print opts
	print args
    for o, a in opts:
        if ((o == "-p") or (o == "--port")):
            port = int(a)
        if ((o == "-h") or (o == "--host")):
            host = a
        if ((o == "-o") or (o == "--operand")):
            operand = a
#		if (operand not in ("exists", "get", "put", "quick", "remove", "udf", "udf-get", "udf-list", "udf-put", "udf-record-apply","udf-remove")):
#			raise Usage()

