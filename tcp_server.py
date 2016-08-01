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
water_bucket = []

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

	def add_water(self, data):
		water_bucket.append(data)
		
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

#found in coordination with Primm at
#https://github.com/dsprimm/Final_capstone/blob/master/molecular_separation.py
#***
def molecules(packet):
	pull = 8
	(type, size, custom) = struct.unpack("!HHL", packet[:pull])
	molecules = [""]
	while pull < len(packet):
		(data, left, right) = struct.unpack("!LHH", packet[pull:pull+8])
		#print((data, left, right))
		molecules.append((left, right, data))
		pull += 8
	#print("Molecules {}".format(molecules))
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
				#print("Xo {}, X1 {}".format(molecules[x][0], molecules[x][1]))
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
	#print("I_list: {}, M_list {}".format(i_list, m_list))
	for i in m_list:
		if i:
			added = 1
			i_list[i] = molecules[i]
			if molecules[i][0] > len(molecules)-1 or molecules[i][1] > len(molecules)-1:
				i_list[0] = 1
		else:
			if added:
				ret_list.append(i_list)
				added = 0
			i_list = [(0,0,0) for x in range(len(molecules))]
	return ret_list
	
def conversion(md, a):
	i = 1
	#print("Md {}".format(md))
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
#***
	
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
	sludge_outgoing.connect(('', 40000))
	
	waste_outgoing = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	waste_outgoing.connect(('', 40003))


	functions = {"0": bacteria, "1": debris, "2": mercury, "3":lead, "4": selenium,
	        "5": feces, "6": ammonia, "7": deaeration, "8": phosphates,
			"9":chlorinate
			}

	mol = []
	p_l = []

	mol = molecules(item)

	#mol = set(mol)

	#print("mol {}".format(mol))

	linker = []

	for link in mol:
		p_l = functions["0"](link, bucket)
		if p_l:
			link = p_l

		p_l = functions["1"](link)
		if p_l:
			outgoing = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

			h2 = b''
			
			debris_l = functions["3"](p_l, bucket)
			
			if debris_l:
				print("Lead cleared from debris")
				p_l = debris_l
			else:
				print("No lead found")
			print("Len of p_l {} : p_l {}".format(len(p_l), p_l))	
			header = Header(1, 8+8*len(p_l), 0)
			h2 += header.serialize()
	
			for i in p_l:
				h2 += struct.pack("!LHH", i[2], i[0], i[1])

			not_sent = 1
			while not_sent:
				try:
					outgoing.connect(("downstream", 2222))
					#header = Header(1, 8 + 8*len(p_l), 0)
					#outgoing.send(header.serialize())
					outgoing.send(h2)
					not_sent = 0
					print("Debris sent")
				except Exception:
					continue
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
			
		p_l = functions["6"](link, bucket)
		if p_l:
			link = p_l
			
		p_l = functions["7"](link)
		if p_l:
			link = p_l

		#print("Link {}".format(link))

		if link:
			linker = link
		
	if len(sludge_bucket) in range(50, 75):
		print("Sending to sludge server")
		sludge_outgoing.send(bytes(','.join(sludge_bucket),'utf-8'))
		sludge_bucket[:] = []

	if len(waste_bucket) in range(50, 75):
		print("Sending to waste server")
		waste_outgoing.send(bytes(','.join(waste_bucket),'utf-8'))
		waste_bucket[:] = []


	h1 = b''
	water = []
	if linker:
		for i in linker:
			if i[2]:
				bucket.add_water(i)
		print("Len of water bucket {}".format(len(water_bucket)))
		if len(water_bucket) > 200:
			print("Water sending")
			water = functions["9"](water_bucket)
			
			header = Header(0, 8+8*len(water_bucket), 0)
			h1 += header.serialize()
			tmp_b = fix_water(water_bucket)
			
			print("Tmp_b {}".format(tmp_b))
			
			for i in tmp_b:
				h1 += struct.pack("!LHH", i[2], i[0], i[1])
			
			not_sent = 1
			water_outgoing = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			while not_sent:
				try:
					water_outgoing.connect(("downstream", 1111))
					#header = Header(0, 8 + 8*len(linker), 0)
					#water_outgoing.send(header.serialize())
					water_outgoing.send(h1)
					not_sent = 0
					water_bucket[:] = []
					print("Water sent correctly")
				except Exception:
					continue
			water_outgoing.close()
		
	waste_outgoing.close()
		
	sludge_outgoing.close()

def phosphates():
	pass


def fix_water(bucket):
	end_numb = len(bucket)-11
	number = 0
	ret_list = []
	#print("Bucket {}".format(bucket))
	for mol in bucket:
			left = mol[0]
			right = mol[1]
			data = mol[2]
			
			#bucket.remove(mol)
	
			if number > end_numb:
				#print("Here")
				ret_list.append((left, right, data))
				continue

			if number == 0:
				left = 0
				right = number + 1
			else:
				left = 0
				right = number+1
			
			number += 1

			ret_list.append((left, right, data))
	return ret_list

#found in coordination with Primm at
#https://github.com/dsprimm/Final_capstone/blob/master/debris.py
#***
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
#***

#found in coordination with Primm at
#https://github.com/dsprimm/Final_capstone/blob/master/mercury.py
#***
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
		print("Found merc")
		clean_merc(double_l, dic, m_set, bucket)
	return point
	
def clean_merc(double_l, dic, m_set, bucket):
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
#***

#found in coordination with Primm at
#https://github.com/dsprimm/Final_capstone/blob/master/selenium.py
#***
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
	
	fp = open("test.txt", "w")

	if chain:
		print("Before change clean chain{}".format(p_list), file=fp)
		p_list = clean_chain(m_dict, bucket)
		print("After change clean chain{}".format(p_list), file=fp)
		return p_list
	else:
		print("Before change clean double: {}".format(p_list), file=fp)
		p_list = clean_dbl(m_dict, bucket)
		print("After change clean double: {}".format(p_list), file=fp)
		return p_list		

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
		if i in double_l[[i][0]] and i in double_l[[i][1]]:
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
	print("Cleaning double l")
	bucket.add_waste(str(double_l[trash][2]))
	send_list = []
	for item in ret_list:
		send_list.append((item[0], item[1], item[2]))
	return send_list

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
	print("Cleaning chain l")
	bucket.add_waste(str(double_l[trash][2]))
	send_list = []
	for item in ret_list:
		send_list.append((item[0], item[1], item[2]))
	return send_list
#***

def feces(p_list, bucket):
	ret_list = []
	poo = 0
	for mol in p_list:
		left = mol[0]
		right = mol[1]
		data = mol[2]
		if is_prime(data):
			print("Feces found")
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
	pee = 0
	for mol in p_list:
		left = mol[0]
		right = mol[1]
		data = mol[2]
		if is_undulant(data):
			print("Ammonia found")
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

#found in coordination with Primm at
#https://github.com/dsprimm/Final_capstone/blob/master/deaeration.py
#***
def deaeration(p_list):
	dll = []
	for mol in p_list:
		left = mol[0]
		right = mol[1]
		data = mol[2]
		dll.append((left, right, data))
	change = 0
	while True:
		if air_check(dll) == 0:
			break
		change = 1
	if change:
		return dll
	return 0

def air_check(d_list):
	dic = {}
	for i in range(1, len(d_list)):
		if d_list[i][2] == 0:
			clean_all_zero(d_list)
			return 1
	return 0

def clean_all_zero(d_list):
	remove = None
	for i in range(1, len(d_list)):
		if d_list[i][2] == 0:
			remove = i
			break
	for i in range(1, len(d_list)):
		if i == remove:
			continue
		if d_list[i][0] == remove:
			d_list[i] = (d_list[d_list[i][0]][0], d_list[i][1],d_list[i][2])
		if d_list[i][1] == remove:
			d_list[i] = (d_list[i][0], d_list[d_list[i][1]][1],d_list[i][2])
		if d_list[i][0] >= remove:
			d_list[i] = (d_list[i][0]-1, d_list[i][1],d_list[i][2])
		if d_list[i][1] >= remove:
			d_list[i] = (d_list[i][0], d_list[i][1]-1,d_list[i][2])
	d_list.pop(remove)
#***
	
#found at hubpages.com/education/How-to-Tell-If-a-Number-is-Triangular
def is_triangle(num):
	if num == 0:
		return False

	number = .5*math.sqrt(8*num + 1) - .5
	
	if number.is_integer():
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
			print("Lead found")
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



#found at https://technobeans.wordpress.com/2012/04/16/5-ways-of-fibonacci-in-python/	
def is_fib(data):
	if data == 0:
		return False		
	a, b = 1,1
	fib_list = []
	for i in range(45):
		a, b = b, a+b
		if a == data:
			return True
	return False
		
def bacteria(p_list, bucket):
	ret_list = []
	bac = 0
	for mol in p_list:
		if type(mol) == int:
			continue
		left = mol[0]
		right = mol[1]
		data = mol[2]
		if is_fib(data):
			print("Fungus/Bacteria fund")
			bucket.add_waste(str(data))
			data = 0
			bac = 1
		else:
			pass
	
	if bac:
		return ret_list
	else:
		return 0		


def chlorinate(bucket):
	#print("This is bucket {}".format(bucket))
	number = len(bucket)-9
	for mol in bucket[0:8]:
		left = mol[0]
		right = mol[1]
		data = mol[2]
		"""
		if left:
			right = left
		else:
			left = right
		"""
		left = number+2
		right = number+2
	
		bucket.append((left, right, data))

		number += 1	

	bucket.append((0,0,0))
	bucket.append((0,0,0))

	for i in bucket[0:8]:
		bucket.remove(i)

	#print("This is bucket after cleaning {}".format(bucket))

	return bucket

#pulled from https://docs.python.org/3/library/queue.html
def main():
	num_worker_threads = 20

	server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

	server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

	server.bind(('', 1111))
	
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
					val = conn.recv(4096)
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
