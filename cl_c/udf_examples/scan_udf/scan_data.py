#!/usr/bin/python
# because of centos 5, run against python 2.4

# the format of a backup file is:
# + n namespace
# + k key
# + g 1   ; generation
# + t 0   ; ttl
# + b 2   ; 2 bins
# - S value 5 string ; a bin called 'value' of type string with length 5 and value string
# - I value2 12345 ; a bin called 'value2' of type Integer with value 12345

# The two parameters are the directory to create them in
# and the number to create

#
# there will be a time called 'last_activity' (integer)
# key will be a unique incrementing integer and stored as user_id
# category will be a string of form cXX,cYY,cZZ where the campains are randomly created

# there will be N objects per file. Files will be named starting at 00000.clb.

# includes
import os, random, time, string
from optparse import OptionParser

#
# f is the file to append to
# ns is the namespace string
# key is the key - integer or whatnot
# timestamp - seconds since epoch
# campain is a string as represented previously

# this one is faster than the other one by 10% end user visible
def write_object( f, ns, user_id, timestamp, campaign, action, asset_id, url ):
	uid_str = str(user_id)
	time_str = str(timestamp)
	c_str = action + ',' + time_str + ',' + asset_id + ',' + url
	
	writelist = [
		"- S ",campaign," ",
		str(len(c_str))," ",c_str,
		"\n"
	]
	f.writelines ( writelist )

g_urls = [ "latimes.com/", "yahoo.co.jp/", "fc2.com/", "ameblo.co.jp/", "rakuten.co.jp/" ]

def random_url():
	s = random.choice(g_urls) + ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(8))
	return s

#
# this function gins up a user object
# and calls the object writer to get it written
	
g_one_month = 60 * 60 * 24 * 30

g_campaigns = []
max_campaigns_per_user = 8

def init_campaigns ( ):
	for i in xrange(1, n_campaigns):
		g_campaigns.append("camp_%d" % i)
#	print "campaigns list is ",g_campaigns
	
def write_user(	f, ns, user_id ):

	# generate a category string
	n_campaigns = ( random.getrandbits(16) % max_campaigns_per_user ) + 1
	# n_campains = random.randint(1,6) # this is slower than the above
	campaigns = random.sample(g_campaigns,n_campaigns)
	campaigns.sort()

	uid_str = str(user_id)
	writelist = [
		"+ n ", ns,
		"\n+ k ", str(user_id),
		"\n+ g 1\n+ t 0"
		"\n+ b ",str(len(campaigns)+1),
		"\n- S user_id ", str(len(uid_str))," ",uid_str,
		"\n"
	]
	f.writelines ( writelist )

	for c in campaigns:
		# generate a last activity time - random in the last month
		# 1 month is ( 60 * 60 * 24 * 30 )                                                                                                                          
		last_activity = long(time.time()) - ( random.getrandbits(32) % g_one_month )
		
		asset_id = "asset_%03d" % random.randint(0,999)
		
		if ( random.randint(0,99) == 0 ): 
			action = "click"
		else:
			action = "imp"

		write_object( f, ns, user_id, last_activity, c, action, asset_id, random_url() )
	
	
#
# PARSE THE ARGUMENTS
#


dirname = "."
users = 1000
users_per_file = 1000000
ns = "test"
n_campaigns = 10

parser = OptionParser()
parser.add_option("-d", "--directory", help="directory to create the files", dest="dirname",default=dirname)
parser.add_option("-u", "--users", help="number of user objects to create",dest="users",default=users)
parser.add_option("-n", "--namespace", help="namespace to insert to",dest="ns",default=ns)
parser.add_option("-c", "--campaigns", help="number of campaigns",dest="n_campaigns",default=n_campaigns)

(options, args) = parser.parse_args()
dirname = options.dirname
users = int(options.users)
ns = options.ns
n_campaigns = options.n_campaigns

if os.path.exists(dirname)==False:
    print "creating directory ", dirname
    os.makedirs(dirname)
    
init_campaigns()

print "ready to create a recommendation set"
print "the directory name is ",dirname," the number of users is ",users, " n_campaigns ",n_campaigns
print "namespace ",ns

f = None
fnumber = 0

initial = True

for user_id in xrange(0,int(users)):
	if initial or user_id % users_per_file == 0 :
		initial = False
#		print "create a new file at user ",user_id
		if f != None:
#			print "close previous file"
			f.flush()
			f.close()
			f = None
		filename = dirname + '/' + str(fnumber) + ".clb"
#		print "opening file ",filename
		f = open(filename, "wb")
		f.write("Version 2.0\n")
#		print "file is ",f
		fnumber = fnumber + 1
	
	write_user(f, ns, user_id )
	
if f != None:
	f.flush()
	f.close()

