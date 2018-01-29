#!/usr/bin/env python

import sys, SimpleXMLRPCServer, logging, time, threading, xmlrpclib
from datetime import datetime, timedelta
from xmlrpclib import Binary
from time import time
from sys import argv, exit
from errno import ENOENT, ENOTEMPTY
from stat import S_IFDIR, S_IFLNK, S_IFREG
from collections import defaultdict
from fuse import FuseOSError


# Presents a HT interface
class MetaData:
  def __init__(self):
    self.files = {}
    self.dir = defaultdict(list)
    now = time()
    self.files['/'] = dict(st_mode=(S_IFDIR | 0o755), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2)

  def count(self):
    return len(self.files)
  
  def chmod(self, path, mode):
    self.files[path]['st_mode'] &= 0o770000
    self.files[path]['st_mode'] |= mode        
    return 0
    
  def chown(self, path, uid, gid):
    self.files[path]['st_uid'] = uid
    self.files[path]['st_gid'] = gid
    	
  def readdir(self, path, fh):
    files_existing = ['.','..']
    if path in self.dir:
      for entry in self.dir[path]:
        files_existing.append(entry)
    return files_existing
		
  
  def getattr(self,key):
    rv = {}
    if key in self.files:
      return self.files[key]
    return rv
      
  def getxattr(self, path, name, position):
    attrs = self.files[path].get('attrs', {})
    try:
      return attrs[name]
    except KeyError:
      return ''       # Should return ENOATTR
  
  def listxattr(self, path):
    attrs = self.files[path].get('attrs', {})
    return attrs.keys()

  
  def create(self, path, mode):
    self.files[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())
    parent, name = self.split_path(path)
    self.dir[parent].append(name)
    
    
  def split_path(self,path):
	  sub_path = path.split('/')
	  size = len(sub_path)
	  name = sub_path[size-1]
	  parent = '/'.join(sub_path[:size-1])
	  if parent == '': parent = '/'
	  return parent, name
		
  def mkdir(self, path, mode):
    self.files[path] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                    st_size=0, st_ctime=time(), st_mtime=time(),
                                    st_atime=time())
    parent, name = self.split_path(path)
    self.dir[parent].append(name)
    self.dir[path] = []
    self.files[parent]['st_nlink'] += 1	   
	 
  def rmdir(self,path):
    if not len(self.dir[path]):
      self.files.pop(path)
      parent, name = self.split_path(path)
      self.dir[parent].remove(name)
      self.files[parent]['st_nlink'] -= 1
    else:
      raise FuseOSError(ENOTEMPTY)
     

  def setxattr(self, path, name, value, options, position):
        # Ignore options
    attrs = self.files[path].setdefault('attrs', {})
    attrs[name] = value

  def removexattr(self, path, name):
    attrs = self.files[path].get('attrs', {})
    try:
	  del attrs[name]
    except KeyError:
      pass        # Should return ENOATTR
  
  def symlink(self, target, source):
    self.files[target] = dict(st_mode=(S_IFLNK | 0o777), st_nlink=1,
                                  st_size=len(source))
    parent, name = self.split_path(target)
    self.dir[parent].append(name) 
        
                
  def utimens(self, path, times):
    now = time()
    atime, mtime = times if times else (now, now)
    self.files[path]['st_atime'] = atime
    self.files[path]['st_mtime'] = mtime
    
  def unlink(self, path):
    self.files.pop(path)
    parent, name = self.split_path(path)
    self.dir[parent].remove(name)
    
  def write(self, path, length):
    self.files[path]['st_size'] = length

#This implementation of rename is different from that of fuse file system. 
#This method returns the st_mode of the file/directory which is used by data server 
  
  def rename(self, old, new):
    
    self.files[new] = self.files.pop(old)
    #print(self.files)
    isdir = self.files[new]['st_mode'] & S_IFDIR
    #print(isdir)
    
    parent, name = self.split_path(old)
    new_parent, new_name = self.split_path(new)
    self.dir[parent].remove(name) 
    self.dir[new_parent].append(new_name)
    if (self.files[new]['st_mode'] & S_IFDIR):
      self.dir[new] = self.dir.pop(old)
     
      for x in self.dir:
        if x.startswith(old+'/'):
          string3 = x.replace(old,new,1)
          self.dir[string3] = self.dir.pop(x)
          #print('Just replaced dir')
      for y in self.files:
        if y.startswith(old+'/'):
          string1 = y.replace(old,new,1)
          self.files[string1] = self.files.pop(y)
          #print('Just replaced files')
      
      #self.data_temp_serv.replacedata(old,new)
      #print('I am ready to return st_mode and exit this method')
      return isdir 
    
def main():
  if len(argv) != 2:
    print('usage: %s <meta_server port>' % argv[0])
    exit(1)

  port = int(argv[1])
  try:
    serve(port)
  except:
    print('Pulling down meta server @port:',port)

# Start the xmlrpc server
def serve(port):
  file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', port),allow_none = True)
  file_server.register_introspection_functions()
  sht = MetaData()
  file_server.register_function(sht.getattr)
  file_server.register_function(sht.getxattr)
  file_server.register_function(sht.readdir)
  file_server.register_function(sht.chmod)
  file_server.register_function(sht.chown)
  file_server.register_function(sht.create)
  file_server.register_function(sht.mkdir)
  file_server.register_function(sht.rmdir)
  file_server.register_function(sht.rename)
  file_server.register_function(sht.removexattr)
  file_server.register_function(sht.setxattr)
  file_server.register_function(sht.symlink)
  file_server.register_function(sht.listxattr)
  file_server.register_function(sht.utimens)
  file_server.register_function(sht.unlink)
  file_server.register_function(sht.write)
  print("Meta Server running at port "+ str(port))
  file_server.serve_forever()

if __name__ == "__main__":
  main()
