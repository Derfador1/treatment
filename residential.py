#!/usr/bin/ python3

import os
import socket
import sys
import select
import threading
import math
import struct

#updated from https://github.com/dsprimm/fdr/blob/master/fdr.py

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setblocking(0)

server.bind(('localhost', 1111))

server.listen(100)

workers = []  # list to store worker threads in

#updated from https://github.com/dsprimm/fdr/blob/master/fdr.py
class Worker(threading.Thread):
	"""
	worker holds the basic information for the threaded worker to do its job
	"""
	def __init__(self, sock, func, inbound, lock, lock_l):
		super().__init__()
		self.sock = sock
		self.running = [1]  # python black magic
		self.func = func
		self.inbound = inbound
		self.lock = lock
		self.lock_l = lock_l

	def run(self):
		"""
		calls out to the threads starting function, this enters
		the provided function
		"""
		self.func(self.sock, self.running, self.inbound, self.lock, self.lock_l)
		# allows pointer pointer manipulation

#updated from https://github.com/dsprimm/fdr/blob/master/fdr.py
def dispatcher(sock, running, inbound, lock, lock_l):
	"""
	dispatcher(None, list, list, semaphore)
	dispatcher spawns up threads for each of the functions, popping from
	the inbound list, sock is not used in this function
	"""

	worker = None
	while running[0]:
		if len(inbound) > 0:
			lock.acquire()  # checks if the semaphore is avaliable
			item = inbound.pop(0)
			worker.append(Worker(item, parser, code, lock, lock_l))
			lock.release()  # frees semaphore so the thread can take it
			#for i in functions.keys():
			    #worker = Worker(item, parser, code[1:], lock)
			    #worker.start()
				#print(code)
			#functions[i](code)
			#Will be for something
			#else:
			    #item[0].sendto(b"invalid request, must be F|D|R<data>\n\0", item[1][1])

def parser(code):
	functions = {"1": debris, "2": mercury, "3": selenium,
		         "4": feces, "5": ammonia, "6": phosphates,
				"7": chlorine}
				
	mol = molecules(code)
	
	for i in function.keys():
		for d in molecules:
			functions[i](d)
		
def molecules(code):
	molecules = []
	
	pull = 8
	(type, size, custom) = struct.unpack("!HHL", packet[:pull])
	molecules = []
	while pull + 8 < len(packet):
		molecules.append(packet[pull:pull+8])
		pull += 8
	return molecules

#Derived from MSG Simpson
def debris(code):
	if len(code) % 8 != 0:
		send_report("Error: Bad trash payload size")
		return False
	print("Processing %d tidbit" %(len(code)/8))
	while code:
		data, left_i, right_i = struct.unpack('!IHH', code[0:8])
		print ("    Recieved Trash nugget:%x left:%d right:%d"% (data, left_i, right_i))
		code = code[8:]

def mercury(code):
	pass

def selenium(code):
	pass

def feces(code):
	if len(code) % 8 != 0:
		send_report("Error: Bad trash payload size")
		return False
	print("Processing %d tidbit" %(len(code)/8))
	while code:
		data, left_i, right_i = struct.unpack('!IHH', code[0:8])
		if is_prime(data):
			print("poopy")
			break
		print ("    Recieved Trash nugget:%x left:%d right:%d"% (data, left_i, right_i))
		code = code[8:]

def ammonia(code):
	pass

def phosphates(code):
	pass

def chlorine(code):
	pass 

#updated from https://github.com/dsprimm/fdr/blob/master/fdr.py
def poller(sock, running, inbound, lock, lock_l):
	"""
	poller(socket, list)
	this function was mostly copied from https://pymotw.com/2/select/
	i do not know all the usage for select.poll, however this works
	"""
	# shamelessly copied from https://pymotw.com/2/select/
	READ = select.POLLIN | select.POLLPRI | select.POLLHUP | select.POLLERR
	TIMEOUT = 200  # polls 5 times a second
	poll = select.poll()
	poll.register(sock, READ)
	while running[0]:
		if lock_l.acquire(blocking=True, timeout=-1):
			# Wait for at least one of the sockets to be ready for processing
			events = poll.poll(TIMEOUT)
			for fd, flag in events:
				# Retrieve the actual socket from its file descriptor
				if flag & (select.POLLIN):
					receive(sock, inbound)
			lock_l.release()
	# end shame

def receive(sock, inbound):
	"""
	receive(socket)
	receives any data that is in the pipe
	"""	
	conn, addr = sock.accept()
	recv_val = conn.recv(1024)  # bad magic number, look into chnageing
	inbound.append((sock, recv_val))
	#socket1.close()
	#conn.close() #or disconnect

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

def main():
	inbound = []  # pointer pointer black magic
	lock = threading.Semaphore(value=6)  # allows for 6 workers
	
	lock_l = threading.Semaphore(value=1)

	workers.append(Worker(server, poller, inbound, lock, lock_l))
	workers.append(Worker(server, poller, inbound, lock, lock_l))
	workers.append(Worker(server, poller, inbound, lock, lock_l))
	workers.append(Worker(server, poller, inbound, lock, lock_l))
	workers.append(Worker(server, poller, inbound, lock, lock_l))
	
	workers.append(Worker(None, dispatcher, inbound, lock, lock_l))
	
	for worker in workers:
		worker.start()

	while True:
		try:
			user_inp = input("Server is running\n/q or ctrl-c to quit\n")
			if user_inp == '/q':
				for worker in workers:
					worker.running[0] = 0  # pointer pointer magic
					worker.join()
					try:
						sock.close()  # there is one sock with a None
					except:
						pass
				print()
				break
		except KeyboardInterrupt:
			for worker in workers:
				worker.running[0] = 0  # pointer pointer magic
				worker.join()
				try:
				    worker.sock.close()  # there is one sock with a None
				except:
				    pass
			print()
			break

if __name__ == "__main__":
	main()
