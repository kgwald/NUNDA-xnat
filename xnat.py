#!/usr/bin/python

import sys, time
import os, fnmatch, urllib, urllib2, base64, gzip

nunda_server="http://nunda.northwestern.edu/nunda/"

RECON_LABEL="Robust"
FUNCTIONAL_RUN_FILES=['snlfunc.nii.gz', '*.pdf', '*.tar.gz']
ANATOMICAL_RUN_FILES=['*.tar.gz','*.pdf']
FUNCTIONAL_DIR_PATTERN='*-func'
ANATOMICAL_DIR_PATTERN='*-anat'
OUTPUT_PROJECT_DIRECTORY='.'

# Note: on a windows box, it might be better to not try to download to the current directory
#OUTPUT_PROJECT_DIRECTORY='C:/Users/reber/Documents/Ongoing Exp/HSeVi/'

class Subject:
    def __init__(self,label,nunda_interface):
        self.label=label
        self.nunda_id=None
        self.experiment_id=None
        self.reconstructions=[]
        self.N=nunda_interface
        return
        
    def add_nunda_id(self,id):
        self.nunda_id=id
        return
    
    def add_exp_id(self,id):
        self.experiment_id=id
        return
    
    def add_reconstruction(self,recon_name,recon_uri):
        self.reconstructions.append([recon_name,recon_uri])
        return
        
    def recon_summary(self,recon_prefix):
        for i in self.reconstructions:
            if i[0][:len(recon_prefix)]==recon_prefix:
                get_runs='data/archive/experiments/%s/reconstructions/%s/out/resources?format=csv' % (self.experiment_id,i[0])
                q=self.N.query_nunda(get_runs)
                runs=self.N.parse_table(q)
                for j in runs[1:]:
                    num_files=0
                    files_size=0
                    get_files='data/archive/experiments/%s/reconstructions/%s/out/resources/%s/files?format=csv' % (self.experiment_id,i[0],j[1])
                    q=self.N.query_nunda(get_files)
                    file_list=self.N.parse_table(q)
                    for k in file_list[1:]:
                        if k[1]!='':
                            files_size=files_size+int(k[1])
                            num_files=num_files+1
                        
                return(num_files,files_size)
        return (0,0)
        
    def recon_files(self,recon_prefix):
        for i in self.reconstructions:
            if i[0][:len(recon_prefix)]==recon_prefix:
                get_runs='data/archive/experiments/%s/reconstructions/%s/out/resources?format=csv' % (self.experiment_id,i[0])
                q=self.N.query_nunda(get_runs)
                runs=self.N.parse_table(q)
                for j in runs[1:]:
                    if fnmatch.fnmatch(j[1],ANATOMICAL_DIR_PATTERN):
                        prefix=j[1].split('-')[0]
                        print "Found anat %s" % prefix
                        get_files='data/archive/experiments/%s/reconstructions/%s/out/resources/%s/files?format=csv' % (self.experiment_id,i[0],j[1])
                        q=self.N.query_nunda(get_files)
                        files_list=self.N.parse_table(q)
                        for k in files_list[1:]:
                            for f in ANATOMICAL_RUN_FILES:
                                if fnmatch.fnmatch(k[0],f):
                                    print "Found %s" % k[0]
                                    file_uri='data/archive/experiments/%s/reconstructions/%s/out/resources/%s/files/%s' % (self.experiment_id,i[0],j[1],k[0])
                                    self.fetch_file(file_uri,prefix,k[0])
                    elif fnmatch.fnmatch(j[1],FUNCTIONAL_DIR_PATTERN):
                        prefix=j[1].split('-')[0]
                        print "Functional run %s" % prefix
                        get_files='data/archive/experiments/%s/reconstructions/%s/out/resources/%s/files?format=csv' % (self.experiment_id,i[0],j[1])
                        q=self.N.query_nunda(get_files)
                        files_list=self.N.parse_table(q)
                        for k in files_list[1:]:
                            for f in FUNCTIONAL_RUN_FILES:
                                if fnmatch.fnmatch(k[0],f):
                                    print "Found %s" % k[0]
                                    file_uri='data/archive/experiments/%s/reconstructions/%s/out/resources/%s/files/%s' % (self.experiment_id,i[0],j[1],k[0])
                                    self.fetch_file(file_uri,prefix,k[0])
                                    # Hack to move functional runs up to higher level directory
                                    if fnmatch.fnmatch(k[0],"*.nii.gz") or fnmatch.fnmatch(k[0],"*.nii"): # BOLD run, move up
                                        self.move_file_up(prefix,k[0])
                return
        print "No match found"
        return None       

    def fetch_file(self,file_uri,run_name,file_name):
        # Check if directory exists
        if not os.path.exists(os.path.join(OUTPUT_PROJECT_DIRECTORY,self.label,run_name)):
            os.makedirs(os.path.join(OUTPUT_PROJECT_DIRECTORY,self.label,run_name))
        local_dest=os.path.join(OUTPUT_PROJECT_DIRECTORY,self.label,run_name,file_name)
        if not os.path.exists(local_dest): # skip if already downloaded
            # urllib.retrieve
            print "Retrieve %s to %s" % (file_uri,local_dest)
            self.N.retrieve_file(file_uri,local_dest)
            
    def move_file_up(self,run_name,file_name):
        print "Move file up %s %s" % (run_name, file_name)
        orig_dest=os.path.join(OUTPUT_PROJECT_DIRECTORY,self.label,run_name,file_name)
        new_dest=os.path.join(OUTPUT_PROJECT_DIRECTORY,self.label,"%s-%s" % (run_name,file_name))
        if os.path.exists(new_dest):
            print "%s exists, no move" % new_dest
            return
        if orig_dest[-3:]=='.gz': # if compressed, uncompress on move
            # Uncompress local file
            uncompressed_output=new_dest[:-3]
            if not os.path.exists(uncompressed_output):
                print "Uncompressing"
                f_in=gzip.open(orig_dest,'rb')
                f_out=open(uncompressed_output,'wb')
                size=16192
                while True:
                    d=f_in.read(size)
                    if d=='':
                        break
                    f_out.write(d)
                f_in.close()
                f_out.close()
        else:
            # move the file up one directory level
            os.rename(orig_dest,new_dest)
        return

# End Subject

class Nunda_Session:
    def __init__(self,user,password):
        self.nunda_jsessionid=None    
        url=nunda_server+"data/JSESSION"
        req = urllib2.Request(url)
        base64string = base64.standard_b64encode('%s:%s' % (user, password))
        req.add_header("Authorization", "Basic %s" % base64string)
        f=urllib2.urlopen(req)
        self.nunda_jsessionid=f.read()
        return
    
    def query_nunda(self,url_string):
        url=nunda_server+url_string
        req = urllib2.Request(url)
        req.add_header("Cookie", "JSESSIONID=%s" % self.nunda_jsessionid)   
        u = urllib2.urlopen(req)
        data = u.read()
        return data
        
    def retrieve_file(self,url_string,local_file):
        url=nunda_server+url_string
        req = urllib2.Request(url)
        req.add_header("Cookie", "JSESSIONID=%s" % self.nunda_jsessionid)   
        u = urllib2.urlopen(req)
        meta = u.info()
        file_size = int(meta.getheaders("Content-Length")[0])
        print "Downloading %s, %d bytes" % (os.path.basename(local_file),file_size)
        f=open(local_file,'wb')
        file_size_dl=0
        block_sz=8192
        while True:
            buffer = u.read(block_sz)
            if not buffer:
                break
            file_size_dl=file_size_dl+len(buffer)
            f.write(buffer)
            #status=r"%12d  [%3.2f%%]" % (file_size_dl, file_size_dl*100./file_size)
            #status=status+chr(13)
            #print status,
        f.close()
        return

    # Helper functions for turning csv formatted data into a list of lists
    def parse_table(self,s):
        lines=s.split('\n')
        table=[]
        for i in lines:
            row=[]
            cols=i.split(',')
            if len(cols)>1:
                for j in cols:
                    row.append(j[1:-1])
                table.append(row)
        return(table)

HELP_MESSAGE="""
xnat.py user:password project_name <options>
	-h or --help	print this message

	user:password	login credentials for NUNDA (sorry to require plaintext)
	project_name	name of the project holding subject sessions on NUNDA

	<options>
	-l or --list	just list the available subjects and return (good for testing)
	all, -a, --all	get all available subjects (note: this is a lot of bandwidth, won't overwrite)

	Variables set in script:
	RECON_LABEL=%s
	FUNCTIONAL_RUN_FILES=%s
	ANATOMICAL_RUN_FILES=%s
	FUNCTIONAL_DIR_PATTERN=%s
	ANATOMICAL_DIR_PATTERN=%s

	The _RUN_FILES are the files that will be downloaded from NUNDA and stored (not all)
	The _DIR_PATTERN is the regular expresssion used to figure out which set of files to get
""" % (RECON_LABEL,FUNCTIONAL_RUN_FILES, ANATOMICAL_RUN_FILES, FUNCTIONAL_DIR_PATTERN, ANATOMICAL_DIR_PATTERN)

def main(a=[]):
    if a==[]:
        args=sys.argv
    else:
        args=a.split()
        print "args", args
    
    if len(args)==1 or args[1]=='-h' or args[1]=='--help':
  	print HELP_MESSAGE
  	return
    
    # user:password should be in argv[1]
    t=args[1].split(':')
    user=t[0]
    password=t[1]
    
    # project name should be in argv[2]
    project_name=args[2]
    
    # Create network connection
    print "Connecting to NUNDA"
    try:
  	N=Nunda_Session(user,password)
    except:
  	print "Unable to connect to NUNDA"
  	return
    
    try:
   	q=N.query_nunda("data/archive/projects/%s/subjects?columns=xnat:subjectData/ID,xnat:subjectData/LABEL&format=csv" % project_name)
    except:
   	print "Unable to retrieve subjects in project %s" % project_name
   	return
    
    # Collects subject and session data from NUNDA
    start_time=time.time()
    subject_table=N.parse_table(q)
    subject_list=[]
    for i in subject_table[1:]:
  	S=Subject(i[1],N)
  	S.add_nunda_id(i[0])
  	q=N.query_nunda("data/archive/projects/%s/subjects/%s/experiments?columns=ID,project&format=csv" % (project_name,S.nunda_id))
  	exp_table=N.parse_table(q)
  	S.add_exp_id(exp_table[1][0])
  	q=N.query_nunda("data/archive/experiments/%s/reconstructions?columns=ID,project&format=csv" % S.experiment_id)
  	recon_table=N.parse_table(q)
  	for r in recon_table[1:]:
  	    S.add_reconstruction(r[1],r[2])
  	subject_list.append(S)
    
    # -l or --list option
    if len(args)>3 and (args[3]=='-l' or args[3]=='--list'):
  	# report what is available
  	for s in subject_list:
 		print "%s, %s reconstructions" % (s.label,len(s.reconstructions))
 		for i in s.reconstructions:
    			print "    %s" % i[0]
  	return
    
    if len(args)==3 or args[3]=='all' or args[3]=='-a' or args[3]=='--all': # defaults to all
   	download_list=subject_list
    else:
  	download_list=[]
  	for i in args[3:]:
  	    for j in subject_list: # find match
    		if j.label==i:
   	   	    print "Adding %s to download list" % i
   	            download_list.append(j)
   	            
    # Get the data
    for s in download_list:
    	print "Getting ",s.label
	s.recon_files(RECON_LABEL)
    
    elapsed_time=time.time()-start_time
    print "Done, %d seconds elapsed" % elapsed_time
    return


#########################

# If running from command line:
# main()

# Running from IDE
print "Starting"
main("xnat.py preber:309cresap HSeVi --list")
             


