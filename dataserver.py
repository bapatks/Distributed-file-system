#!/usr/bin/env python

import sys, SimpleXMLRPCServer, logging, time, xmlrpclib, pickle, shelve,random
import os.path
from datetime import datetime, timedelta
from xmlrpclib import Binary
from time import time
from sys import argv, exit
from errno import ENOENT, ENOTEMPTY
from stat import S_IFDIR, S_IFLNK, S_IFREG
from collections import defaultdict

#from fuse import FuseOSError
blk_size = 8 #block size of data in bytes

# Presents a HT interface
class Data:
  def __init__(self):
    self.data = defaultdict(list)
    self.count = 0
    self.serv_ID = int(argv[1])
    self.lost = 0
    self.start(int(argv[1]))

  def start(self,id):
    #self.serv_ID = id
    #print(self.serv_ID)
    store_name = "data_store" + str(id)

    if os.path.isfile(store_name):
      data_store = shelve.open(store_name)
      self.extract(data_store)

    elif not os.path.isfile(store_name):
      data_store = shelve.open(store_name)
      self.lost = 1

    data_store.close()
    #print(self.lost)

  def check_status(self):
    return self.lost

  def extract(self,data_store):
    for key in data_store:
      ext_key = pickle.loads(key)
      self.data[ext_key] = data_store[key]


  def load_serv(self,obj):
    if self.lost==1:
      self.lost=0

    ret_obj = pickle.loads(obj.data)
    self.data = ret_obj
    store_name = "data_store" + str(self.serv_id)
    data_store = shelve.open(store_name)
    #logfile = shelve.open("server_log" + str(self.serv_ID) + ".txt")
    for key in self.data:
      data_store[pickle.dumps(key)] = self.data[key]
      #logfile[pickle.dumps(key)] = self.data[key]

    #logfile.close()
    data_store.close()


  def checksum(self, data):
    checksum_val = ""
    code_sum = 0
    for code in map(ord,data):
      if (code>31) and (code<80):
        encode = code + 29
        code_sum = code_sum + code
        checksum_val = checksum_val + chr(encode)

      elif (code>79) and (code<127):
        encode = code - 29
        code_sum = code_sum + code
        checksum_val = checksum_val + chr(encode)
      else:
        encode = 36
        code_sum = code_sum + code
        checksum_val = checksum_val + chr(encode)

    checksum_val = checksum_val + str(code_sum)
    #print('Checksum for %s is %s'%(data,checksum_val))
    return checksum_val

  def corrupt(self,path):
    if self.lost==1:
      self.lost=0

    count = 0
    for key in sorted(self.data):
      key_path = key[0]
      if key_path==path:
        count = count+1

    #print(count)
    #print('Number of blocks for %s is %d'%(path,count))
    for i in range(1):
      corr_blkn = random.randrange(0,count)
      self.data[(path,corr_blkn)][1] = "#Corrupt"
      #print(self.data)
      print("I corrupted your data! HA HA HA (Evil laughter)")


  def size(self):
    if self.lost==1:
      self.lost=0

    self.count = len(self.data)   
    return self.count

  def find_key(self,path,blk):
    for key in self.data:
      file_path = key[0]
      file_blk = key[1]
      if file_path == path:
        if file_blk == blk:
          return self.data[key]
    return []

  def request_data(self,prev_serv,next_serv):
    new_dict = {}
    if(prev_serv==1 and next_serv==0):
      for key in self.data:
        dat = self.data[key][0]
        checksum = self.data[key][1]
        copyn = self.data[key][2]
        if copyn<2:
          new_dict[key] = [dat,checksum,copyn+1]

    elif(prev_serv==0 and next_serv==1):
      for key in self.data:
        dat = self.data[key][0]
        checksum = self.data[key][1]
        copyn = self.data[key][2]
        if copyn>0:
          new_dict[key] = [dat,checksum,copyn-1]

    return Binary(pickle.dumps(new_dict))

  def getdata(self,path, blkn):
    tup = (path,blkn)
    if tup in self.data:
      return Binary(pickle.dumps(self.data[tup]))
    else:
      return Binary(pickle.dumps([]))

  def putdata(self,path, blkn, data, checksum, copyn):
    #print("I am inside putdata of server",self.serv_ID)
    data_store = shelve.open("data_store" + str(self.serv_ID))
    tup = (path,blkn)
    if (checksum == self.checksum(data)):
      self.data[tup] = [data,checksum,copyn]
      data_store[pickle.dumps(tup)] = [data,checksum,copyn]
      #print(self.data)

    data_store.close()

  def rename(self, old, new, isdir):
    if self.lost==1:
      self.lost = 0

    data_store = shelve.open("data_store" + str(self.serv_ID))
    #logfile = shelve.open("server_log" + str(self.serv_ID) + ".txt")
    if not isdir:
      for key in self.data.keys():
        key_path = key[0]
        key_blkn = key[1]
        if key_path == old:
          self.data[(new,key_blkn)] = self.data.pop(key)
          tup = (new,key_blkn)
          data_store[pickle.dumps(tup)] = data_store.pop(pickle.dumps(key))
          #logfile[pickle.dumps(tup)] = logfile.pop(pickle.dumps(key))
      
    if isdir:
      for key in self.data:
        z = key[0]
        blkn = key[1]
        if z.startswith(old+'/'):
          string2 = z.replace(old,new,1)
          self.data[(string2,blkn)] = self.data.pop(key)
          tup = (string2,blkn)
          data_store[pickle.dumps(tup)] = data_store.pop(pickle.dumps(key))
          #logfile[pickle.dumps(tup)] = logfile.pop(pickle.dumps(key))
          #print('just replaced data')

    #logfile.close()
    data_store.close()
      
  def readlink(self, path):
    if self.lost==1:
      self.lost=0

    data_dict = {}
    for key in self.data:
      key_path = key[0]
      if key_path==path:
        if not key in data_dict:
          data_dict[key] = self.data[key]

    #print("this is the dictionary", data_dict)
    return Binary(pickle.dumps(data_dict))
  
  def read(self, path, size, offset, fh):
    if self.lost==1:
      self.lost=0

    offset_pos = offset%blk_size
    start_blk = offset//blk_size
    last_blk = (offset + size)//blk_size
    data_dict = {}
    for key in self.data:
      key_path = key[0]
      key_blk = key[1]
      if key_path==path:
        if (key_blk>=start_blk) and (key_blk<=last_blk):
          if not key in data_dict:
            data_dict[key] = self.data[key]

    #print("this is the dictionary", data_dict)
    #print(self.data)
    return Binary(pickle.dumps(data_dict))

  def symlink(self, target, blkn, source, checksum_val, copyn):
    #print("entered symlink method on data server")
    if self.lost==1:
      self.lost=0
    data_store = shelve.open("data_store" + str(self.serv_ID))
    #logfile = shelve.open("server_log" + str(self.serv_ID) + ".txt")
    #if checksum_val == self.checksum(source):
    self.data[(target,blkn)] = [source, checksum_val, copyn]
    tup = (target,blkn)
    data_store[pickle.dumps(tup)] = [source, checksum_val, copyn]
    #logfile[pickle.dumps(tup)] = [source, checksum_val, copyn]
    #print(self.data)
    #logfile.close()
    data_store.close()

  def truncate(self, path, length, fh=None):
    if self.lost==1:
      self.lost=0

    block = length//blk_size
    offset_pos = length%blk_size
    data_store = shelve.open("data_store" + str(self.serv_ID))
    #logfile = shelve.open("server_log" + str(self.serv_ID) + ".txt")
    for key in self.data.keys():
      key_path = key[0]
      key_blk = key[1]
      if key_path == path:
        if(key_blk > block):
          self.data.pop(key)
          data_store.pop(pickle.dumps(key))
          #logfile.pop(pickle.dumps(key))

        elif key_blk==block:
          new_str = self.data[key][0]
          self.data[key][0] = new_str[:offset_pos]
          self.data[key][1] = self.checksum(self.data[key][0])
          #update data_store
          data_store[pickle.dumps(key)] = [new_str[:offset_pos], self.checksum(self.data[key][0]), self.data[key][2]]
          #logfile[pickle.dumps(key)] = [new_str[:offset_pos], self.checksum(self.data[key][0]), self.data[key][2]]

    self.size()
    #logfile.close()
    data_store.close()
    #print('I just truncated the file')
    #print(self.data)
    
          
  def unlink(self,path):
    if self.lost==1:
      self.lost = 0

    data_store = shelve.open("data_store" + str(self.serv_ID))
    #logfile = shelve.open("server_log" + str(self.serv_ID) + ".txt")
    for key in self.data.keys():
      key_path = key[0]
      if key_path == path:
        self.data.pop(key)
        data_store.pop(pickle.dumps(key))
        #logfile.pop(pickle.dumps(key))
        
    #print(self.data)
    #logfile.close()
    data_store.close()

  #write method definition is different from the one in fuse file system
  #1) the offset argument is not actually the offset from where data is to be written in the file,
  #   but, it gives the position of cursor in file after previous writes to different data servers
  #2) CHECKSUM is used for checking for data corruption, it is an extra argument added in this implementation
  def write(self, path, data, checksum, offset, copyn, fh):
    #print('now I am in dataserver')
    if self.lost==1:
      self.lost=0

    block = offset//blk_size
    offset_pos = offset%blk_size
    data_store = shelve.open("data_store" + str(self.serv_ID))
    #logfile = shelve.open("server_log" + str(self.serv_ID) + ".txt")
    value = self.find_key(path, block)
    #print('value is',value)
    if (value==[]):
      value.append("")
      value.append("")
      value.append(0)
      self.data[(path,block)] = ["","",0]
    #print('value is',value)
    data_string = value[0]
    #print(data_string)
    #if (checksum==self.checksum(data)):
      #print('I entered checksum block')
    self.data[(path,block)][0] = data_string + data
    self.data[(path,block)][1] = self.checksum(self.data[(path,block)][0])
    self.data[(path,block)][2] = copyn
      #update data_store
    tup = (path,block)
    data_store[pickle.dumps(tup)] = [data_string + data, self.checksum(self.data[(path,block)][0]), copyn]
    #logfile[pickle.dumps(tup)] = [data_string + data, self.checksum(self.data[(path,block)][0]), copyn]
    
    self.size()
    #logfile.close()
    data_store.close()
    #print(self.data)
        
def main():
  if len(argv) > 7:
    print('Usage: %s <server_number> <data_server ports>' % argv[0])
    print('Input method supports Max. 5 data servers, although overall code may support more than 4 data servers')
    exit(1)

  serv_num = int(argv[1])
  #dserv_count = len(argv)-2
  port = int(argv[serv_num+2])
  
  try:
    serve(port)
  except KeyboardInterrupt:
    print('Pulling down server @port:',port)
  

# Start the xmlrpc server
def serve(port):
  file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', port),allow_none = True)
  file_server.register_introspection_functions()
  sht = Data()
  file_server.register_function(sht.readlink)
  file_server.register_function(sht.read)
  file_server.register_function(sht.rename)
  file_server.register_function(sht.unlink)
  file_server.register_function(sht.symlink)
  file_server.register_function(sht.truncate)
  file_server.register_function(sht.write)
  file_server.register_function(sht.size)
  file_server.register_function(sht.start)
  file_server.register_function(sht.check_status)
  file_server.register_function(sht.request_data)
  file_server.register_function(sht.getdata)
  file_server.register_function(sht.putdata)
  file_server.register_function(sht.load_serv)
  file_server.register_function(sht.corrupt)
  print("Data server running at port "+str(port))
  file_server.serve_forever()

if __name__ == "__main__":
  main()
