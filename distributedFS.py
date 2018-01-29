#!/usr/bin/env python
from __future__ import print_function, absolute_import, division

import data_mngr
import logging,xmlrpclib, pickle, os

from xmlrpclib import Binary
from collections import defaultdict
from errno import ENOENT, ENOTEMPTY
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
from data_mngr import Manager
blk_size = 8 #block size of data in bytes

if not hasattr(__builtins__, 'bytes'):
    bytes = str        #initializes the data dictionary as a dictionary of lists 
    
		
class Memory(LoggingMixIn, Operations):
    'Supports multiple levels of files.'
	
    
    def __init__(self):
        lst = []
        for i in range(dserv_count):
            lst.append(int(argv[3+i]))
    	self.dserv_lst = lst
        self.d_manager = Manager(self.dserv_lst, len(self.dserv_lst))
        self.m_server = xmlrpclib.ServerProxy("http://localhost:"+str(int(argv[2])),allow_none=True)
        self.fd = 0
        now=time()
   
    def open(self, path, flags):        
    	self.fd += 1
        return self.fd
        
    def statfs(self, path):
        return dict(f_bsize=blk_size, f_blocks=4096, f_bavail=2048)
        
    
#methods for meta server

    def chmod(self, path, mode):
        self.m_server.chmod(path,mode)

    def chown(self, path, uid, gid):
        self.m_server.chown(path, uid, gid)

    def create(self, path, mode):
    	self.m_server.create(path,mode)
        self.fd += 1
        return self.fd

    def getattr(self, path, fh=None):
    	val = self.m_server.getattr(path)
    	if not val:
    		raise FuseOSError(ENOENT)
    	
    	return val

    def getxattr(self, path, name, position=0):
        val = self.m_server.getxattr(path,name, 0)
        return val

    def listxattr(self, path):
        val = self.m_server.listxattr(path)
        return val

    def mkdir(self, path, mode):
		self.m_server.mkdir(path,mode)

    def readdir(self, path, fh):
		drct = self.m_server.readdir(path,fh)
		return drct	

    def removexattr(self, path, name):
        self.m_server.removexattr(path,name)
            
    def rmdir(self, path):
        try:
            self.m_server.rmdir(path)
            
        except xmlrpclib.Fault as e:
            if os.strerror(ENOTEMPTY) in e.faultString:
                raise FuseOSError(ENOTEMPTY)
            else:
                raise

    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
        self.m_server.setxattr(path,name,value,options,0)
    
    def unlink(self, path):
        self.m_server.unlink(path)
        self.d_manager.unlink(path)

    def utimens(self, path, times=None):
        self.m_server.utimens(path, None)

#methods for data server

    
    def read(self, path, size, offset, fh):
        return self.d_manager.read(path,size,offset,fh)    #return string starting from offset
    
    def readlink(self, path):
        ls = self.d_manager.readlink(path)
        return ls
    	
    def rename(self, old, new):
    	isdir = self.m_server.rename(old,new)
    	#print(isdir)
    	self.d_manager.rename(old,new,isdir)
    
    def symlink(self, target, source):
        self.m_server.symlink(target, source)
        self.d_manager.symlink(target, source)
        
    def truncate(self, path, length, fh=None):
        self.d_manager.truncate(path,length)
        self.m_server.write(path, length)

    def write(self, path, data, offset, fh):
    	temp = self.d_manager.write(path,data,offset,fh)
        #print('Now I am in FS code')
        print(temp)
        self.m_server.write(path, temp)
        return len(data)


   
   
if __name__ == '__main__':
    if len(argv) > 8:
        print('usage: %s <mountpoint> <meta_server port> <data_server ports>' % argv[0])
        print('Input method supports Max. 5 data servers, although overall code may support more than 4 data servers')
        exit(1)	
	
    print("MetaServer @ http://localhost:"+str(int(argv[2])))

    dserv_count = len(argv)-3
    for i in range(dserv_count):
        print("DataServer"+str(i+1)+" @ http://localhost:"+str(int(argv[3+i])))

    #print("DataServer1 @ http://localhost:"+str(int(argv[3])))
    #print("DataServer2 @ http://localhost:"+str(int(argv[4])))
    #print("DataServer3 @ http://localhost:"+str(int(argv[5])))
    #print("DataServer4 @ http://localhost:"+str(int(argv[6])))

    logging.basicConfig(level=logging.DEBUG)
    fuse = FUSE(Memory(), argv[1], foreground=True)
    
