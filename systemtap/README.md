
#### Enable ntpd on clients and servers.

Do this first so it can settle while you do the rest ...


#### Dependencies

Install systemtap software:

    # On RedHat/CentOS/Fedora:
    sudo yum install systemtap systemtap-runtime systemtap-sdt-devel

    # On Debian/Ubuntu:
    sudo apt-get install -y systemtap systemtap-runtime systemtap-sdt-dev


Add user to systemtap groups:

    sudo usermod -a -G stapusr,stapsys,stapdev <username>
    # relogin after this to obtain group privileges


#### Building client and test program

    cd aerospike-client-c
    make USE_SYSTEMTAP=1 clean all
    make USE_SYSTEMTAP=1 -C examples/query_examples/simple clean all


#### Generate query trace log file

    cd aerospike-client-c
    stap systemtap/client.stp \
        -o /tmp/example-`hostname`-stap.log \
        -c './examples/query_examples/simple/target/example'


#### Annotate multiple concurrent trace files

    cd aerospike-client-c
    sort -n /tmp/example-*-stap.log | systemtap/annotate 

