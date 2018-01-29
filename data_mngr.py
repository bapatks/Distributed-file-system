#!/usr/bin/env python
from __future__ import print_function, absolute_import, division

import logging,xmlrpclib, os, pickle, shelve, random

from collections import defaultdict
from errno import ENOENT, ENOTEMPTY
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time
from time import sleep
from xmlrpclib import Binary
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

blk_size = 8 #block size of data in bytes
#server_count = 4
if not hasattr(__builtins__, 'bytes'):
    bytes = str        #initializes the data dictionary as a dictionary of lists

class Manager:
	def __init__(self,ports,serv_count):
		#self.count = 0
		self.d_server = []
		self.server_ports = ports
		self.server_count = serv_count
		print(self.server_ports)
		for i in range(self.server_count):
			self.d_server.append(xmlrpclib.ServerProxy("http://localhost:"+str(self.server_ports[i]),allow_none=True))
			#self.d_server[i].start(i,0) #fault_bit = 0
		
		#print(self.d_server)
		self.hash_table = {}

	#checksum method works on ASCII values of characters
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
		#print('Checksum for data %s is %s'%(data,checksum_val))
		return checksum_val

	def list2str(self, ls):
		string = "".join(ls)
		return string

	def build_data(self,serv_id):
		#print('Flag is 1')
		x,y = self.find_adj_serv(serv_id)
		#print('here server x=',x)
		#print('and server y=',y)
		new_store = {}
		ret_obj = self.d_server[x].request_data(1,0)
		stored = pickle.loads(ret_obj.data)
		new_store.update(stored)

		ret_obj = self.d_server[y].request_data(0,1)
		stored = pickle.loads(ret_obj.data)
		new_store.update(stored)

		self.d_server[serv_id].load_serv(Binary(pickle.dumps(new_store)))
		#print(new_store)

	def find_adj_serv(self, serv_id):
		if serv_id==0:
			x = self.server_count-1
			y = serv_id + 1
		elif serv_id==self.server_count-1:
			x = serv_id-1
			y = 0
		else:
			x = serv_id-1
			y = serv_id+1

		return x,y

	def hash(self,path):
		if path in self.hash_table:
			#print("I was too lazy to calculate, hash table = " + str(self.hash_table))
			return self.hash_table[path]
		else:
			size = 0
			for i in range(self.server_count):
				try:
					if (i==0):
						size = self.d_server[i].size()
						index = i
					else:
						data_size = self.d_server[i].size()
						if (data_size < size):
							size = data_size
							index = i

				except:
					while True:
						print("Data Server",i,"is currently down. Cannot proceed with operation")
						try:
							absent = self.d_server[i].check_status()
							#Does not build data if absent=0
							#print("value of absent is",absent)
							if absent==1:
								self.build_data(i)
								#print('Coming out of build_data')

							if (i==0):
								size = self.d_server[i].size()
								index = i
							else:
								data_size = self.d_server[i].size()
								if (data_size < size):
									size = data_size
									index = i
							break

						except:
							pass
						sleep(5)
				
			self.hash_table[path] = index
			#print("I did some work, hash table = " + str(self.hash_table))
			return index
	
	def determine_server(self, first, num):
		return (first + num)%(self.server_count)

	#Server methods start from here
	def unlink(self, path):
		for i in range(self.server_count):
			try:
				#print('now going to server :',i)
				self.d_server[i].unlink(path)
			except:
				while True:
					print("Data Server",i,"is currently down. Cannot proceed with truncate operation")
					try:
						absent = self.d_server[i].check_status()
						#Does not build data if absent=0
						if absent==1:
							self.build_data(i)
						
						self.d_server[i].unlink(path)
						break
					except:
						pass
					sleep(5)


	def read(self, path, size, offset, fh):
		#index = self.hash(path)
		data = {}
		data_string = ""
		#count = 0
		start_blk = offset//blk_size
		last_blk = (offset+size)//blk_size
		offset_pos = offset%blk_size
		
		#offset_number = offset
		for i in range(self.server_count):
			try:
				#if(i==1):
					#self.d_server[i].corrupt(path)
				#elif(i==3):
					#self.d_server[i].corrupt(path)

				ret_obj = self.d_server[i].read(path,size,offset,fh)
				data_dict = pickle.loads(ret_obj.data)
			except:
				continue
			#print("the server is %d"%i)
			#print("dictionary returned from server is",data_dict)
			#print('server selected was',i)
			

			for key in data_dict.keys():
				value = data_dict.get(key)
				block_data = value[0]
				checksum_data = value[1]
				copyn = value[2]
				if (checksum_data != self.checksum(block_data)):
					#print("Found an error in checksum for data '%s' at server %d"%(block_data,i))
					key_path = key[0]
					key_blk = key[1]
					data_dict.pop(key)
					while True:
						k = random.randrange(0,self.server_count)
						#print("Server %d is the randomly chosen one for correcting"%k)
						if (k!=i):
							try:
								ret_obj = self.d_server[k].getdata(key_path,key_blk)
								ret_data = pickle.loads(ret_obj.data)
								#print("data returned from server",k,"is",ret_data)
								if (ret_data!=[]):
									blkdat = ret_data[0]
									checksum_data = ret_data[1]
									if (checksum_data == self.checksum(blkdat)):
										#print("Entered code to rectify corrupted value")
										self.d_server[i].putdata(key_path,key_blk,blkdat,checksum_data,copyn)
										break

							except:
								pass

			data.update(data_dict)
			#print(data)

		#logic for concatenating different blocks into one string
		for key in sorted(data):
			value = data.get(key)
			if((key[1]>=start_blk) and (key[1]<=last_blk)):
				if(key[1] == start_blk):
					data_string = data_string + value[0][offset_pos:]
				else:
					data_string = data_string + value[0]
		
		return data_string

	def truncate(self, path, length, fh=None):
		#print("I am truncate of manager")
		index = self.hash(path)
		for i in range(self.server_count):
			try:
				self.d_server[i].truncate(path,length)
			except:
				while True:
					print("Data Server",i,"is currently down. Cannot proceed with truncate operation")
					try:
						absent = self.d_server[i].check_status()
						#Does not build data if absent=0
						if absent==1:
							self.build_data(i)
						
						self.d_server[i].truncate(path,length)
						break
					except:
						pass
					sleep(5)
		

	def rename(self, old, new, isdir):
		for i in range(self.server_count):
			try:
				self.d_server[i].rename(old,new,isdir)
			except:
				while True:
					print("Data Server",i,"is currently down. Cannot proceed with rename operation")
					try:
						absent = self.d_server[i].check_status()
						#Does not build data if absent=0
						if absent==1:
							self.build_data(i)
						
						self.d_server[i].rename(old,new,isdir)
						break
					except:
						pass
					sleep(5)


	def write(self, path, data, offset, fh):
		#print("I am in manager")
		store_data = ""
		index = self.hash(path)
		offset_pos = offset%blk_size
		adj_size = blk_size - offset_pos
		offset_number = offset

		sel_serv = self.determine_server(index, offset_number//blk_size)
		sel_redndt1 = self.determine_server(sel_serv, 1)
		sel_redndt2 = self.determine_server(sel_serv, 2)
		store_data = store_data + data[:adj_size]
		checksum_val = self.checksum(store_data)
		#print('checksum is',checksum_val)
		#print('selected server no.',sel_serv)
		try:
			#store block in assigned server
			self.d_server[sel_serv].write(path,store_data,checksum_val,offset_number,0,fh)
		except:
			while True:
				print("Data Server",sel_serv,"is currently down. Cannot proceed with write operation")
				try:
					#store block in assigned server
					absent = self.d_server[sel_serv].check_status()
					#Does not build data if absent=0
					if absent==1:
						self.build_data(sel_serv)
					
					self.d_server[sel_serv].write(path,store_data,checksum_val,offset_number,0,fh)
					break
				except:
					pass
				sleep(5)

		try:
			#store 1st redundant copy 
			self.d_server[sel_redndt1].write(path,store_data,checksum_val,offset_number,1,fh)
		except:
			while True:
				print("Data Server",sel_redndt1,"is currently down. Cannot proceed with write operation")
				try:
					#store 1st redundant copy
					absent = self.d_server[sel_redndt1].check_status()
					#Does not build data if absent=0
					if absent==1:
						self.build_data(sel_redndt1)
					
					self.d_server[sel_redndt1].write(path,store_data,checksum_val,offset_number,1,fh)
					break
				except:
					pass
				sleep(5)

		try:
			#store 2nd redundant copy 
			self.d_server[sel_redndt2].write(path,store_data,checksum_val,offset_number,2,fh)
		except:
			while True:
				print("Data Server",sel_redndt2,"is currently down. Cannot proceed with write operation")
				try:
					#store 2nd redundant copy 
					absent = self.d_server[sel_redndt2].check_status()
					#Does not build data if absent=0
					if absent==1:
						self.build_data(sel_redndt2)
					
					self.d_server[sel_redndt2].write(path,store_data,checksum_val,offset_number,2,fh)
					break
				except:
					pass
				sleep(5)

		offset_number = offset + len(store_data)
		#print('lenth of data stored',len(store_data))

		while(offset_number - offset < len(data)):
			sel_serv = self.determine_server(index, offset_number//blk_size)
			sel_redndt1 = self.determine_server(sel_serv, 1)
			sel_redndt2 = self.determine_server(sel_serv, 2)
			if (len(data[adj_size:]) < blk_size):
				store_data = data[adj_size:]
				checksum_val = self.checksum(store_data)
				#print(store_data)
				
			elif (len(data[adj_size:]) >= blk_size):
				store_data = data[adj_size:adj_size+blk_size]
				adj_size = adj_size + blk_size
				checksum_val = self.checksum(store_data)
				#print(store_data)

			try:
				#store block in assigned server
				self.d_server[sel_serv].write(path,store_data,checksum_val,offset_number,0,fh)
			except:
				while True:
					print("Data Server",sel_serv,"is currently down. Cannot proceed with write operation")
					try:
						#store block in assigned server
						absent = self.d_server[sel_serv].check_status()
						#Does not build data if absent=0
						if absent==1:
							self.build_data(sel_serv)
						
						self.d_server[sel_serv].write(path,store_data,checksum_val,offset_number,0,fh)
						break
					except:
						pass
					sleep(5)

			try:
				#store 1st redundant copy 
				self.d_server[sel_redndt1].write(path,store_data,checksum_val,offset_number,1,fh)
			except:
				while True:
					print("Data Server",sel_redndt1,"is currently down. Cannot proceed with write operation")
					try:
						#store 1st redundant copy
						absent = self.d_server[sel_redndt1].check_status()
						#Does not build data if absent=0
						if absent==1:
							self.build_data(sel_redndt1)
						
						self.d_server[sel_redndt1].write(path,store_data,checksum_val,offset_number,1,fh)
						break
					except:
						pass
					sleep(5)

			try:
				#store 2nd redundant copy 
				self.d_server[sel_redndt2].write(path,store_data,checksum_val,offset_number,2,fh)
			except:
				while True:
					print("Data Server",sel_redndt2,"is currently down. Cannot proceed with write operation")
					try:
						#store 2nd redundant copy 
						absent = self.d_server[sel_redndt2].check_status()
						#Does not build data if absent=0
						if absent==1:
							self.build_data(sel_redndt2)
						
						self.d_server[sel_redndt2].write(path,store_data,checksum_val,offset_number,2,fh)
						break
					except:
						pass
					sleep(5)

			offset_number = offset_number + len(store_data)
			

		temp = offset_number
		return temp

	def symlink(self, target, source):
		index = self.hash(target)
		offset_number = 0
		last_blk = len(source)//blk_size
		blk_num = 0

		while(blk_num <= last_blk):
			sel_serv = self.determine_server(index, offset_number//blk_size)
			sel_redndt1 = self.determine_server(sel_serv, 1)
			sel_redndt2 = self.determine_server(sel_serv, 2)
			start_pos = blk_num*blk_size

			if blk_num!=last_blk:
				store_data = source[start_pos:start_pos+blk_size]
				checksum_val = self.checksum(store_data)
				#print("in condition of not last block store_data is",store_data)
			elif blk_num==last_blk:
				store_data = source[start_pos:]
				checksum_val = self.checksum(store_data)
				#print("in condition of last block store_data is",store_data)

			try:
				self.d_server[sel_serv].symlink(target, blk_num, store_data, checksum_val, 0)
			except:
				while True:
					print("Data Server",sel_serv,"is currently down. Cannot proceed with creating symlink")
					try:
						#store block in assigned server
						absent = self.d_server[sel_serv].check_status()
						#Does not build data if absent=0
						if absent==1:
							self.build_data(sel_serv)
						
						self.d_server[sel_serv].symlink(target, blk_num, store_data, checksum_val, 0)
						break
					except:
						pass
					sleep(5)

			try:
				#store 1st redundant copy 
				self.d_server[sel_redndt1].symlink(target, blk_num, store_data, checksum_val, 1)
			except:
				while True:
					print("Data Server",sel_redndt1,"is currently down. Cannot proceed with creating symlink")
					try:
						#store 1st redundant copy
						absent = self.d_server[sel_redndt1].check_status()
						#Does not build data if absent=0
						if absent==1:
							self.build_data(sel_redndt1)
						
						self.d_server[sel_redndt1].symlink(target, blk_num, store_data, checksum_val, 1)
						break
					except:
						pass
					sleep(5)

			try:
				#store 2nd redundant copy 
				self.d_server[sel_redndt2].symlink(target, blk_num, store_data, checksum_val, 2)
			except:
				while True:
					print("Data Server",sel_redndt2,"is currently down. Cannot proceed with creating symlink")
					try:
						#store 2nd redundant copy 
						absent = self.d_server[sel_redndt2].check_status()
						#Does not build data if absent=0
						if absent==1:
							self.build_data(sel_redndt2)
						
						self.d_server[sel_redndt2].symlink(target, blk_num, store_data, checksum_val, 2)
						break
					except:
						pass
					sleep(5)
			
			offset_number = offset_number + len(store_data)
			blk_num = blk_num+1
		
		

	def readlink(self, path):
		#index = self.hash(path)
		data = {}
		data_string = ""
		#print("in readlink path is",path)
		#offset_number = offset
		for i in range(self.server_count):
			try:
				ret_obj = self.d_server[i].readlink(path)
				data_dict = pickle.loads(ret_obj.data)
			except:
				continue
			
			#print("dictionary returned to manager is",data_dict)
			#print('server selected was',i)
			for key in data_dict.keys():
				value = data_dict.get(key)
				block_data = value[0]
				checksum_data = value[1]
				copyn = value[2]
				if (checksum_data != self.checksum(block_data)):
					key_path = key[0]
					key_blk = key[1]
					data_dict.pop(key)
					while True:
						k = random.randrange(0,self.server_count)
						#print("Server %d is the randomly chosen one for correcting"%k)
						if (k!=i):
							try:
								ret_obj = self.d_server[k].getdata(key_path,key_blk)
								ret_data = pickle.loads(ret_obj.data)
								#print("data returned from server",k,"is",ret_data)
								if (ret_data!=[]):
									blkdat = ret_data[0]
									checksum_data = ret_data[1]
									if (checksum_data == self.checksum(blkdat)):
										#print("Entered code to rectify corrupted value")
										self.d_server[i].putdata(key_path,key_blk,blkdat,checksum_data,copyn)
										break

							except:
								pass

			data.update(data_dict)
			#print(data)

		for key in sorted(data):
			value = data.get(key)
			data_string = data_string + value[0]
		
		return data_string





if __name__ =='__main__':
	print("Hello I am in data_mngr")
	#mngr = Manager()
