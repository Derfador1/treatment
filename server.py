#!/usr/bin/ python3

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

class Header:
	def __init__(self, type1, size, custom):
		self.type1 = type1
		self.size = size
		self.custom = custom

	def serialize(self):
		return struct.pack("!HHL", self.type1, self.size, self.custom)

def is_prime(n):
	if n==2 or n==3: return True
	if n%2==0 or n<2: return False
	for i in range(3,int(n**0.5)+1,2):   # only odd numbers
		if n%i==0:
			return False    

	return True

#Found at http://codegolf.stackexchange.com/questions/3134/undulant-numbers?page=1&tab=votes#tab-top
def is_undulant(n):
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
	# todo return multiple lists, one for each found structure
		# with multiple structures pad empty space with junk air
		# [LL, LL, LL, LL, 0, 0 SE, SE, SE] would be converted to
		# [
		# [LL, LL, LL, LL, 0, 0, 0, 0 ,0]
		# [0, 0, 0, 0, 0, 0, SE, SE, SE]
		# ]
	pull = 8
	(type, size, custom) = struct.unpack("!HHL", packet[:pull])
	molecules = [""]
	while pull < len(packet):
		(data, left, right) = struct.unpack("!LHH", packet[pull:pull+8])
		molecules.append((left, right, data))
		pull += 8
	
	ret_list = generate_mols(molecules)
	return ret_list
	
def rip(md, a):
	i = 1
	while True:
		if i > len(a):
			return
		if i not in md.keys():
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
		rip(md, molecules)
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
		else:
			if added:
				ret_list.append(i_list)
				added = 0
			i_list = [(0,0,0) for x in range(len(molecules))]
	return ret_list

#pulled from https://github.com/dsprimm/Final_capstone
def worker():
	while True:
		item = q.get()
		if item is None:
		    break
		parser(item)
		q.task_done()

def parser(item):
	sludge_outgoing = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sludge_outgoing.connect(("localhost", 40000))

	functions = {"1": debris, "2": mercury, "3": selenium,
	         "4": feces, "5": ammonia, "6": deaeration, "7": phosphates,
			}
	#"8": chlorine

	mol = []
	p_l = []
	temp_l = []

	mol = molecules(item)
	
	temp_l = mol

	for i in mol:
		p_l = functions["1"](i)
		if p_l:
			header = Header(1, 8 + 8*len(p_l), 0)
			outgoing = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			outgoing.connect(("localhost", 3333))
			outgoing.send(header.serialize())
			outgoing.send(b''.join(p_l))
			outgoing.close()
			return 0
		"""
		p_l = functions["2"](mol)
		if p_l:
			mol = p_l
		"""
			
		p_l = functions["3"](i)
		if p_l:
			#goes to waste functions
			"""
			print("Here")
			header = Header(1, 8 + 8*len(p_l), 0)
			outgoing1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			outgoing1.connect(("localhost", 33356))
			outgoing1.send(header.serialize())
			data = b""
			for i in p_l:
				data += struct.pack("!LHH", i[2], i[0], i[1])
			outgoing1.send(data)
			outgoing1.close()
			"""
			i = p_l
			
		p_l = functions["4"](i, sludge_outgoing)
		if p_l:
			i = p_l

		"""
		p_l = functions["7"](mol)
		print(p_l)
		if p_l:
			header = Header(1, 8 + 8*len(p_l), 0)
			outgoing1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			outgoing1.connect(("localhost", 33355))
			outgoing1.send(header.serialize())
			outgoing1.send(b''.join(p_l))
			outgoing1.close()
			mol = p_l
		
		p_l = functions["8"](mol)
		if p_l:
			mol = p_l
		"""
	sludge_outgoing.close()

#derived from primmm at https://github.com/dsprimm/Final_capstone
def clean(p_list):
	lenlen = len(p_list)
	ret_list = []
	for mol in p_list:
		left = mol[0]
		right = mol[1]
		data = mol[2]
		#(data, left, right) = struct.unpack("!LHH", mol)
		if not data:
			continue
		if left > lenlen:
			left = 0xFFFF
		if right > lenlen:
			right = 0xFFFF
		ret_list.append((left, right, data))
	return ret_list

#derived from primmm at https://github.com/dsprimm/Final_capstone
def debris(p_list):
	lenlen = len(p_list)
	for mol in p_list:
		left = mol[0]
		right = mol[1]
		data = mol[2]
		#(data, left, right) = struct.unpack("!LHH", mol)
		if left > lenlen:
			p_list = clean(p_list)
			return p_list
		elif right > lenlen:
			p_list = clean(p_list)
			return p_list
	return 0

def mercury(mol):
	pass

def selenium(p_list):
	lenlen = len(p_list)
	dll = [('')]
	s_child = 0
	for mol in p_list:
		#(data, left, right) = struct.unpack("!LHH", mol)
		left = mol[0]
		right = mol[1]
		data = mol[2]
		if left == 0 and right == 0:
			continue
		dll.append((left, right, data))
	l = 0
	r = 0
	m_dict = {}
	for i in range(1, len(dll)):
		m_dict[i] = (dll[i][0], dll[i][1], dll[i][2])
	# checking for dbl circle
	chain = 0
	dbl = 0
	for i in m_dict:
		if m_dict[i][0] == 0 and m_dict[i][1] or m_dict[i][1] == 0 and m_dict[i][0] or m_dict[i][0] == m_dict[i][1]:
			if dbl:
				return 0  # this cannot be selenium
			chain = 1
		else:
			if chain:
				return 0  # this cannot be selenium
			dbl = 1
	
	if chain:
		p_list = clean_chain(m_dict)
		return p_list
	else:
		print(dll)
		p_list = clean_dbl(m_dict)
		return p_list		

	
def clean_dbl(dll):
	big = 0
	ret_list = []
	last = 0
	for i in dll:
		if big < dll[i][2]:
			big = dll[i][2]
			trash = i
	for i in dll:
		if not last:
			last = i
		print(last)
		if i in dll[dll[i][0]] and i in dll[dll[i][1]]:
			pass
		else:
			return 0
		if i == trash:
			ret_list.append((dll[i][0], dll[i][1], 0))
		elif trash == dll[i][0]:
			if last == dll[i][1]:
				ret_list.append((0, 0, dll[i][2]))
			else:
				ret_list.append((0, dll[i][1], dll[i][2]))
		elif trash == dll[i][1]:
			if last == dll[i][0]:
				ret_list.append((0, 0, dll[i][2]))
			else:
				ret_list.append((dll[i][0], 0, dll[i][2]))
		else:
			if i in [dll[i][0]]:
				ret_list.append((dll[i][0], 0, dll[i][2]))
			else:
				ret_list.append((dll[i][1], 0, dll[i][2]))
		last = i
	#waste(dll[trash][2])
	send_list = []
	for item in ret_list:
		send_list.append((item[0], item[1], item[2]))
	return send_list

def clean_chain(dll):
	last = 0
	big = 0
	for i in dll:
		if type(i) == int:
			if dll[i][2] > big:
				big = dll[i][2]
				trash = i
	ret_list = []
	for i in dll:
		if dll[i][0] == i:
			if dll[i][1] not in dll.keys():
				return 0
		if dll[i][1] == i:
			if dll[i][0] not in dll.keys():
				return 0
	for i in dll:
		print(i, trash, dll[i])
		if i == trash:
			ret_list.append((dll[i][0], dll[i][1], 0))
		elif trash == dll[i][0]:
			if trash == dll[i][1]:
				ret_list.append((0, 0, dll[i][2]))
			else:
				ret_list.append((0, dll[i][1], dll[i][2]))
		elif trash == dll[i][1]:
			ret_list.append((dll[i][0], 0, dll[i][2]))
		else:
			ret_list.append(dll[i])
	#waste(dll[trash][2])
	send_list = []
	for item in ret_list:
		send_list.append((item[0], item[1], item[2]))
	return send_list

#Found scrypt methods at https://pypi.python.org/pypi/scrypt/
"""
def sludger(data):
	salt  = "I Hate Liam Echlin"
	h1 = scrypt.hash(data, salt, N = 2048, r = 4, p = 4)
	header = Header(2, 8 + len(h1), 0)
	outgoing = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	outgoing.connect(("localhost", 1234))
	outgoing.send(header.serialize())
	outgoing.send(h1)
	outgoing.close()
"""

def feces(p_list, outgoing):
	listo = []
	poo = 0
	for mol in p_list:
		#(data, left, right) = struct.unpack("!LHH", mol)
		print(mol)
		left = mol[0]
		right = mol[1]
		data = mol[2]
		if is_prime(data):
			#sludger(str(data))
			outgoing.send(bytes(str(data),'utf-8'))
			data = 0
			poo = 1
		else:
			pass
		#listo.append(struct.pack("!LHH", data, left, right))
		listo.append((left, right, data))	

	if poo:
		return listo
	else:
		return 0

def ammonia(mol):
	pass

def deaeration(mol):
	pass

def phosphates(p_list):
	lenlen = len(p_list)
	dll = [('')]
	s_child = 0
	for mol in p_list:
		#(data, left, right) = struct.unpack("!LHH", mol)
		data = mol[0]
		left = mol[1]
		right = mol[2]
		if left == 0 and right == 0:
			continue
		if left == 0 or right == 0:
			s_child += 1
		dll.append((left, right, data))
	for i in range(1, len(dll)):
		if dll[i][0]:
			try:
				if i not in dll[dll[i][0]] and dll[i][0] != dll[i][1]:
					return 5  # this is not phosphates
			except Exception:
				pass
		if dll[i][1]:
			try:
				if i not in dll[dll[i][1]] and dll[i][0] != dll[i][1]:
					return 8  # this is not phosphates
			except Exception:
				pass
	if s_child != 2:  # if there are not only 2 nodes with data and only 1 child
		return 2  # this is not phosphates
	p_list = clean_phosphates(dll)
	return p_list
	
def clean_phosphates(dll):
	ret_list = []
	ends = []
	for i in range(1, len(dll)):
		if 0 == dll[i][0] or 0 == dll[i][1]:
			ends.append(dll[i])
	if ends[0][2] >= ends[1][2]:
		# rewire all nodes to chain link, pointed to the head, ends[0] is the new head
		head = ends[0]
	else:
		# rewire all nodes to chain link, pointed to the head, ends[1] is the new head
		head = ends[1]
	for i in range(1, len(dll)):
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
	dll.pop(0)
	#for item in dll:
	#	ret_list.append(struct.pack("!LHH", item[2], item[0], item[1]))
	return ret_list
"""
def chlorine(p_list):
	for i in p_list:
		(data, left, right) = struct.unpack("!LHH", mol)
		if left == right and left:
			right = 0
			mol = struct.pack("!LHH", data, left, right)
			return p_list
	return 0
"""

#pulled from https://github.com/dsprimm/Final_capstone
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
		#put check to make sure we have same number of threads we started with
		"""
		if len(threads) < num_worker_threads:
			t = threading.Thread(target=worker)
			t.start()
			threads.append(t)
		"""
		
		conn, addr = server.accept()
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
	
		#if len(recieved) == 0:
		#	break
			
		#server.close()

	# block until all tasks are done
	q.join()

	# stop workers
	for i in range(num_worker_threads):
		q.put(None)
	for t in threads:
		t.join()
	
if __name__ == "__main__":
	main()
