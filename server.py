#!/usr/bin/env python3

import os
import socket
import sys
import select
import threading
import math
import struct
import queue
import scrypt

q = queue.Queue()

sludge_bucket = []
waste_bucket = []

class Header:
	def __init__(self, type1, size, custom):
		self.type1 = type1
		self.size = size
		self.custom = custom

	def serialize(self):
		return struct.pack("!HHL", self.type1, self.size, self.custom)
		
class Bucket:
	def __init__(self):
		self.data = None
	
	def add(self, data):
		sludge_bucket.append(data)
		
	def add_waste(self, data):
		waste_bucket.append(data)
		
def is_prime(n):
	if n==2 or n==3: 
		return True
	if n%2==0 or n<2: 
		return False
	for i in range(3,int(n**0.5)+1,2):   # only odd numbers
		if n%i==0:
			return False    

	return True

#Found at http://codegolf.stackexchange.com/questions/3134/undulant-numbers?page=1&tab=votes#tab-top
def is_undulant(n):
	if n == 0:
		return False
	digits = [int(i) for i in list(str(n))]
	diffs = []
	for i in range(len(digits) - 1):
		diffs.append((digits[i] > digits[i+1]) - (digits[i] < digits[i+1]))
	if len(diffs) == 1:
	   if diffs[0] == 0:
		   return False
	else:
	   for i in range(len(diffs) - 1):
		   if diffs[i] * diffs[i+1] != -1:
		       return False
	return True

def molecules(packet):
	pull = 8
	(type, size, custom) = struct.unpack("!HHL", packet[:pull])
	molecules = [""]
	while pull < len(packet):
		(data, left, right) = struct.unpack("!LHH", packet[pull:pull+8])
		molecules.append((left, right, data))
		pull += 8
	
	ret_list = generate_mols(molecules)
	return ret_list

def generate_mols(molecules):
	m_list = []
	while True:
		x = 1
		md = {}
		while True:
			if x in m_list:
				x += 1
				continue
			elif x + 1 > len(molecules):
				break
			if molecules[x][0] or molecules[x][1]:
				md[x] = (molecules[x][0], molecules[x][1])
				break
			else:
				x += 1
		conversion(md, molecules)
		for i in md:
			m_list.append(i)
		m_list.append("")
		if x + 1 > len(molecules):
			break
	ret_list = []
	i_list = [(0,0,0) for x in range(len(molecules))]
	added = 0
	for i in m_list:
		if i:
			added = 1
			i_list[i] = molecules[i]
			if molecules[i][0] > len(i_list) or molecules[i][1] > len(i_list):
				i_list[0] = 1
		else:
			if added:
				ret_list.append(i_list)
				added = 0
			i_list = [(0,0,0) for x in range(len(molecules))]
	return ret_list
	
def conversion(md, a):
	i = 1
	while True:
		if i > len(a) - 1:
			return
		if i not in md.keys():
			if a[i][0] in md.keys():
				md[i] = (a[i][0], a[i][1])
			i += 1
			continue
		if md[i][0] not in md.keys():
			try:
				md[md[i][0]] = (a[md[i][0]][0], a[md[i][0]][1])
			except:
				pass
		if md[i][1] not in md.keys():
			try:
				md[md[i][1]] = (a[md[i][1]][0], a[md[i][1]][1])
			except:
				pass
		i += 1
	
#https://docs.python.org/3/library/queue.html
def worker():
	while True:
		item = q.get()
		if item is None:
			break
		parser(item)
		q.task_done()

def parser(item):
	bucket = Bucket()
	sludge_outgoing = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sludge_outgoing.connect(('localhost', 40000))
	
	waste_outgoing = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	waste_outgoing.connect(('localhost', 40003))


	functions = {"1": debris, "2": mercury, "3": selenium,
	        "4": feces, "5": ammonia, "6": deaeration, "7": phosphates,
			"8":chlorine, "9":lead
			}

	mol = []
	p_l = []

	mol = molecules(item)

	print(mol)

	for link in mol:
		p_l = functions["1"](link)
		if p_l:
			header = Header(1, 8 + 8*len(p_l), 0)
			outgoing = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			outgoing.connect(("localhost", 3334))
			outgoing.send(header.serialize())
			for i in p_l:
				h1 = struct.pack("!LHH", i[2], i[0], i[1])
				outgoing.send(h1)
			outgoing.close()
			return 0
		
		p_l = functions["2"](link, bucket)
		if p_l:
			link = p_l
			
		p_l = functions["3"](link, bucket)
		if p_l:
			link = p_l
						
		p_l = functions["4"](link, bucket)
		if p_l:
			link = p_l
			
		p_l = functions["5"](link, bucket)
		if p_l:
			link = p_l
			
		#p_l = functions["6"](link, bucket)
		#if p_l:
		#	link = p_l
			
		p_l = functions["7"](link, bucket)
		if p_l:
			link = p_l

		"""
		p_l = functions["8"](mol)
		if p_l:
			mol = p_l
		"""
		p_l = functions["9"](link, bucket)
		if p_l:
			link = p_l
		
	temp_list = []		
	temp_list = str(sludge_bucket)
	if len(sludge_bucket) in range(1, 10):
		sludge_outgoing.send(bytes(','.join(sludge_bucket),'utf-8'))
		sludge_bucket[:] = []

	temp_list2 = []		
	temp_list2 = str(waste_bucket)
	if len(waste_bucket) in range(1, 10):
		waste_outgoing.send(bytes(','.join(waste_bucket),'utf-8'))
		waste_bucket[:] = []


	water_outgoing = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	water_outgoing.connect(('localhost', 40005))		
	if p_l:
		header = Header(0, 8 + 8*len(p_l), 0)
		water_outgoing.send(header.serialize())
		for i in p_l:
				h1 = struct.pack("!LHH", i[2], i[0], i[1])
				water_outgoing.send(h1)
		water_outgoing.close()
		
	waste_outgoing.close()
		
	sludge_outgoing.close()

#derived from primmm at https://github.com/dsprimm/Final_capstone
def clean_debris(p_list):
	list_len = len(p_list)
	ret_list = []
	for mol in p_list[1:]:
		left = mol[0]
		right = mol[1]
		data = mol[2]
		if not data:
			continue
		if left > list_len:
			left = 0xFFFF
		if right > list_len:
			right = 0xFFFF
		ret_list.append((left, right, data))
	return ret_list

def debris(p_list):
	list_len = len(p_list)
	for mol in p_list[1:]:
		left = mol[0]
		right = mol[1]
		data = mol[2]
		if left > list_len:
			p_list = clean_debris(p_list)
			return p_list
		elif right > list_len:
			p_list = clean_debris(p_list)
			return p_list
	return 0

def mercury(p_list, bucket):
	double_l = []
	for mol in p_list:
		left = mol[0]
		right = mol[1]
		data = mol[2]
		double_l.append((left, right, data))
	change = 0
	while True:
		if merc_check(double_l, bucket) <= 1:
			break
		change = 1
	if change:
		return double_l
	return double_l

#found with help from Primm
def merc_check(double_l, bucket):
	dic = {}
	m_set = set()
	for i in range(1, len(double_l)):
		if double_l[i] != (0,0,0):
			dic[i] = double_l[i]
			m_set.add(double_l[i][0])
			m_set.add(double_l[i][1])
	point = 0
	if 0 in m_set:
		m_set.remove(0)
	for i in dic:
		if i not in m_set:
			point += 1
	if point > 1:
		clean(double_l, dic, m_set, bucket)
	return point

def clean(double_l, dic, m_set, bucket):
	small = 0
	trash = None
	for i in dic:
		if i in m_set:
			continue
		else:
			if not trash:
				small = double_l[i][2] 
				trash = i
			elif small > double_l[i][2]:
				small = double_l[i][2] 
				trash = i
	bucket.add_waste(str(double_l[trash][2]))
	double_l[trash] = (0,0,0)

#found with help from Primm
def selenium(p_list, bucket):
	double_l = [('')]
	s_child = 0
	for mol in p_list:
		left = mol[0]
		right = mol[1]
		data = mol[2]
		if left == 0 and right == 0:
			continue
		double_l.append((left, right, data))
	m_dict = {}
	for i in range(1, len(double_l)):
		m_dict[i] = (double_l[i][0], double_l[i][1], double_l[i][2])
	chain = 0
	dbl = 0
	for i in m_dict: 
		if m_dict[i][0] == 0 and m_dict[i][1] or m_dict[i][1] == 0 and m_dict[i][0] or m_dict[i][0] == m_dict[i][1]:
			if dbl:
				return 0  # this cannot be selenium
			chain = 1
		if m_dict[i][0] == 0 and m_dict[i][0] == 0 and m_dict[i][2]:
 			return 0
		else:
			if chain:
				return 0  # this cannot be selenium
			dbl = 1
	
	if chain:
		p_list = clean_chain(m_dict, bucket)
		return p_list
	else:
		p_list = clean_dbl(m_dict, bucket)
		return p_list		

#found with help from Primm
def clean_dbl(double_l, bucket):
	big = 0
	ret_list = []
	last = 0
	for i in double_l:
		if big < double_l[i][2]:
			big = double_l[i][2]
			trash = i
	for i in double_l:
		if not last:
			last = i
		print(last)
		if i in double_l[double_l[i][0]] and i in double_l[double_l[i][1]]:
			pass
		else:
			return 0
		if i == trash:
			ret_list.append((double_l[i][0], double_l[i][1], 0))
		elif trash == double_l[i][0]:
			if last == double_l[i][1]:
				ret_list.append((0, 0, double_l[i][2]))
			else:
				ret_list.append((0, double_l[i][1], double_l[i][2]))
		elif trash == double_l[i][1]:
			if last == double_l[i][0]:
				ret_list.append((0, 0, double_l[i][2]))
			else:
				ret_list.append((double_l[i][0], 0, double_l[i][2]))
		else:
			if i in [double_l[i][0]]:
				ret_list.append((double_l[i][0], 0, double_l[i][2]))
			else:
				ret_list.append((double_l[i][1], 0, double_l[i][2]))
		last = i
	bucket.add_waste(str(double_l[trash][2]))
	send_list = []
	for item in ret_list:
		send_list.append((item[0], item[1], item[2]))
	return send_list

#found with help from Primm
def clean_chain(double_l, bucket):
	last = 0
	big = 0
	for i in double_l:
		if type(i) == int:
			if double_l[i][2] > big:
				big = double_l[i][2]
				trash = i
	ret_list = []
	for i in double_l:
		if double_l[i][0] == i:
			if double_l[i][1] not in double_l.keys():
				return 0
		if double_l[i][1] == i:
			if double_l[i][0] not in double_l.keys():
				return 0
	for i in double_l:
		print(i, trash, double_l[i])
		if i == trash:
			ret_list.append((double_l[i][0], double_l[i][1], 0))
		elif trash == double_l[i][0]:
			if trash == double_l[i][1]:
				ret_list.append((0, 0, double_l[i][2]))
			else:
				ret_list.append((0, double_l[i][1], double_l[i][2]))
		elif trash == double_l[i][1]:
			ret_list.append((double_l[i][0], 0, double_l[i][2]))
		else:
			ret_list.append(double_l[i])
	bucket.add_waste(str(double_l[trash][2]))
	send_list = []
	for item in ret_list:
		send_list.append((item[0], item[1], item[2]))
	return send_list

def feces(p_list, bucket):
	ret_list = []
	poo = 0
	for mol in p_list:
		left = mol[0]
		right = mol[1]
		data = mol[2]
		if is_prime(data):
			bucket.add(str(data))
			data = 0
			poo = 1
		else:
			pass
		ret_list.append((left, right, data))	

	if poo:
		return ret_list
	else:
		return 0

def ammonia(p_list, bucket):
	ret_list = []
	poo = 0
	for mol in p_list:
		left = mol[0]
		right = mol[1]
		data = mol[2]
		if is_undulant(data):
			bucket.add(str(data))
			data = 0
			pee = 1
		else:
			pass
		ret_list.append((left, right, data))	

	if pee:
		return ret_list
	else:
		return 0

def deaeration(mol):
	pass

#found with help from Primm
def phosphates(p_list, bucket):
	dll = [""]
	s_child = 0
	for mol in p_list:
		left = mol[0]
		right = mol[1]
		data = mol[2]
		if left == 0 or right == 0:
			s_child += 1
		dll.append((left, right, data))
		
	m_dict = {0: None}
	
	for i in range(1, len(dll)):
		if dll[i] != (0,0,0):
			m_dict[i] = (dll[i][0], dll[i][1], dll[i][2])
			
	print(m_dict)
	
	for i in m_dict:
		if i:
			if m_dict[i][0] and m_dict[i][0] in m_dict.keys():
				if i not in m_dict[m_dict[i][0]] and m_dict[m_dict[i][0]]:
					return 0
			elif m_dict[i][1] and m_dict[i][1] in m_dict.keys():
				if i not in m_dict[m_dict[i][1]] and m_dict[m_dict[i][1]]:
					return 0
	p_list = clean_phosphates(m_dict, bucket)
	return p_list

#found with help from Primm
def clean_phosphates(dll, bucket):
	print(dll)
	ret_list = []
	ends = []
	for i in dll:
		if i:
			if 0 == dll[i][0] or 0 == dll[i][1]:
				ends.append(dll[i])
				
	print(ends)
	if len(ends) == 1:
		return(ends)
		
	if ends[0][2] >= ends[1][2]:
		head = ends[0]
	else:
		head = ends[1]
		
	for i in dll:
		if i:
			if head == dll[i]:
				if head[0]:
					dll[i] = (head[0], head[0], head[2])
				else:
					dll[i] = (head[1], head[1], head[2])
				head = i
				break
	last = head
	while head:
		if dll[head][0] == 0 or dll[head][1] == 0:
			dll[head] = (0, 0, dll[head][2])
		elif dll[head][0] != last:
			dll[head] = (dll[head][0], dll[head][0], dll[head][2])
		else:
			dll[head] = (dll[head][1], dll[head][1], dll[head][2])
		last = head
		head = dll[head][0]

	for item in double_l:
		ret_list.append((item[0], item[1], item[2]))
	return ret_list
	
def chlorine(p_list):
	pass
	
def is_triangle(n):
	n = int(n)
	if n <= 0:
		return False
	
	#found at mathforum.org/library/drmath/view/57162.html
	x = (n*(n+1)/2)
		
	n = (math.sqrt(1+(8*x) - 1)/2)
	
	if (x).is_integer():
		return True
	return False
	
def lead(p_list, bucket):
	ret_list = []
	lead = 0
	for mol in p_list:
		left = mol[0]
		right = mol[1]
		data = mol[2]
		if is_triangle(data):
			bucket.add_waste(str(data))
			data = 0
			lead = 1
		else:
			pass

		ret_list.append((left, right, data))
		
	if lead:	
		return ret_list
	else:
		return 0

#pulled from https://docs.python.org/3/library/queue.html
def main():
	num_worker_threads = 2

	server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

	server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

	server.bind(('localhost', 1132))
	
	server.listen(num_worker_threads)

	threads = []
	for i in range(num_worker_threads):
		t = threading.Thread(target=worker)
		t.start()
		threads.append(t)

	recieved = []	

	while server:
		try:
			#put check to make sure we have same number of threads we started with
			if len(threads) < num_worker_threads:
				t = threading.Thread(target=worker)
				t.start()
				threads.append(t)
				
			try:
				conn, addr = server.accept()
			except Exception:
				break
				
			if conn:
				try:
					conn.settimeout(5)
					val = conn.recv(1024)
					recieved.append(val)
				except socket.timeout:
					pass

			for data in recieved:
				q.put(data)
				recieved.remove(data)
		except KeyboardInterrupt:
			print("Caught")

			# block until all tasks are done
			q.join()

			# stop workers
			for i in range(num_worker_threads):
				q.put(None)
			for t in threads:
				t.join()
				
			server.close()
	
if __name__ == "__main__":
	main()
