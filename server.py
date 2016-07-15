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

def molecules(code):
	pull = 8
	(type, size, coustom) = struct.unpack("!HHL", code[:pull])
	molecules = []
	while pull < len(code):
		molecules.append(code[pull:pull+8])
		pull += 8
	return molecules

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
			"8": chlorine}

	mol = []
	p_l = []
	temp_l = []

	mol = molecules(item)
	
	temp_l = mol
	
	p_l = functions["1"](mol)
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
		
	p_l = functions["3"](mol)
	if p_l:
		mol = p_l
		
	p_l = functions["4"](mol, sludge_outgoing)
	if p_l:
		mol = p_l

	"""
	p_l = functions["7"](mol)
	if p_l:
		mol = p_l

	p_l = functions["8"](mol)
	if p_l:
		mol = p_l

	elif i == '2':
		pass
	elif i == '3':
		pass
	elif i == '4':
		p_l = functions[i](mol)
		p_list = p_l
	elif i == '5':
		pass
	elif i == '6':
		pass
	elif i == '7':
		pass
	elif i == '8':
		pass
	else:
		functions[i](mol)
	"""
	sludge_outgoing.close()

#derived from primmm at https://github.com/dsprimm/Final_capstone
def clean(p_list):
	lenlen = len(p_list)
	ret_list = []
	for mol in p_list:
		(data, left, right) = struct.unpack("!LHH", mol)
		if not data:
			continue
		if left > lenlen:
			left = 0xFFFF
		if right > lenlen:
			right = 0xFFFF
		ret_list.append(struct.pack("!LHH", data, left, right))
	return ret_list

#derived from primmm at https://github.com/dsprimm/Final_capstone
def debris(p_list):
	lenlen = len(p_list)
	for mol in p_list:
		(data, left, right) = struct.unpack("!LHH", mol)
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
		(data, left, right) = struct.unpack("!LHH", mol)
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
		p_list = clean_chain(dll)
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
		send_list.append(struct.pack("!LHH", item[2], item[0], item[1]))
	return send_list

def clean_chain(dll):
	last = 0
	big = 0
	for i in range(1, len(dll)):
		if dll[i][2] > big:
			big = dll[i][2]
			trash = i
	ret_list = []
	for i in range(1, len(dll)):
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
		send_list.append(struct.pack("!LHH", item[2], item[0], item[1]))
	return send_list

#Found scrypt methods at https://pypi.python.org/pypi/scrypt/
def sludger(data):
	salt  = "I Hate Liam Echlin"
	h1 = scrypt.hash(data, salt, N = 2048, r = 4, p = 4)
	header = Header(2, 8 + len(h1), 0)
	outgoing = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	outgoing.connect(("localhost", 1234))
	outgoing.send(header.serialize())
	outgoing.send(h1)
	outgoing.close()

def feces(p_list, outgoing):
	listo = []
	poo = 0
	for mol in p_list:
		(data, left, right) = struct.unpack("!LHH", mol)
		if is_prime(data):
			#sludger(str(data))
			outgoing.send(bytes(str(data),'utf-8'))
			data = 0
			poo = 1
		else:
			pass
		listo.append(struct.pack("!LHH", data, left, right))	

	if poo:
		return listo
	else:
		return 0

def ammonia(mol):
	pass

def deaeration(mol):
	pass

def phosphates(mol):
	pass

def chlorine(p_list):
	for i in p_list:
		(data, left, right) = struct.unpack("!LHH", mol)
		if left == right and left:
			right = 0
			mol = struct.pack("!LHH", data, left, right)
			return p_list
	return 0

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
