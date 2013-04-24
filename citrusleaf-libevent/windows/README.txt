										Citrusleaf windows libevent2 Client

###########################################################################################################################################
Dependencies for 64 bit client:
	libevent2
	openSSL
	pthread

### TO DOWNLOAD AND INSTALL THE DEPENDENCIES  #############################################################################################
### If dependencies are already installed skip to next section ############################################################################

libevent2 --
IF libevent2 is not installed on the system then -
	Download libevent2 : https://github.com/downloads/libevent/libevent/libevent-2.0.21-stable.tar.gz
	Untar the libevent2 package in some folder.
	TO COMPILE :
			Using the Visual Studio Command Prompt - 
				cd libevent-2.0.X-stable\         (here X = 21 )
				nmake -f makefile.nmake
			Or if you prefer the IDE, 
				then File + New + Project, Visual C++,
				General node, pick the Makefile Project template.
				Name = libevent-2.0.X-stable, 
				Location = parent directory (libevent-2.0.X-stable\). 
				OK. 
				Next. 
				Build command = nmake -f makefile.nmake, 
				rest blank.
				Build the project.
OpenSSL -
IF openssl is not installed on the system then -
	Download OpenSSL : http://slproweb.com/download/Win64OpenSSL-1_0_1e.exe
	Double click the exe and follow the instructions to install it on your machine.

pthread --
IF pthread is not installed on the system then -
	Download pthread include, lib and dll folder from : ftp://sourceware.org/pub/pthreads-win32/dll-latest
	Keep them in a folder name as pthread (or anything else).


######### STEPS FOR SUCCESSFUL EXECUTION OF WINDOWS CLIENT ################################################################################

1. ) Link the dependencies to the visual studio project 

	In CitrusleafLibrary project -
	
	For header files
	Edit  CitrusleafLibrary -> Properties -> Configuration Properties -> C/C++ -> General -> Additional Include Directories
	Add 
		path of libevent2_folder\include 
		path of libevent2_folder\WIN32-Code
		path of openSSL_folder\include
		path of pthread_folder\include

	For library -
	Edit  CitrusleafLibrary -> Properties -> Configuration Properties -> Librarian -> Additional Library Directories
	Add 
		path of libevent2_folder\

	In CitrusleafDemo project -
	
	For header files - 
	Edit  CitrusleafDemo -> Properties -> Configuration Properties -> C/C++ -> General -> Additional Include Directories
	Add 
		path of libevent2_folder\include
		path of libevent2_folder\WIN32-Code
		path of openSSL_folder\include
		path of pthread_folder\include

	For library -
	Edit  CitrusleafDemo -> Properties -> Configuration Properties -> Linker -> Additional Library Directories
	Add 
		path of libevent2_folder\
		path of pthread\lib\x64\
		path of openSSL\lib\

2.) Build the solution

3.) Copy pthreadVC2.dll in the location same as of CitrusleafDemo.exe file
	If you are building in Debug mode : 
	Copy the pthread_folder\dll\x64\pthreadVC2.dll into client\cl_libevent2\windows\x64\Debug\

	If you are building in release (Default) mode :
	Copy the pthread_folder\dll\x64\pthreadVC2.dll into client\cl_libevent2\windows\x64\Release\

##########################################################################################################################################

############# STEP BY STEP USAGE #########################################################################################################

1. Boot the cluster up.
2. Unzip the package on your client machine.
3. Open the visual studio solution file located at cl_libevent2\window\ 
4. Build the solution
5. Set config parameters(such as namespace, set, hostname etc)  in the CitrusleafDemo.cpp file in the CitrusleafDemo project.
    You can find it in the set_config function.
6. compile and run.

##########################################################################################################################################
