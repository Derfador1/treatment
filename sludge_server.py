#!/usr/bin/env python3

import os
import socket
import sys
import struct
import queue
import scrypt
import threading

q = queue.Queue()

class Header:
	def __init__(self, type1, size, custom):
		self.type1 = type1
		self.size = size
		self.custom = custom

	def serialize(self):
		return struct.pack("!HHL", self.type1, self.size, self.custom)
	
#Found scrypt methods at https://pypi.python.org/pypi/scrypt/		
def sludger(data):
	if data:
		outgoing = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		#outgoing.connect(("downstream", 4444))
		list1 = []
		list1 = data.split(',')
		#print("str", list1)
		#header = Header(2, 8 + len(list1)*64, 0)
		#outgoing.send(header.serialize())
		h1 = b''
		for i in list1:
			i = str(i)
			salt  = "I Hate Liam Echlin"
			h1 += scrypt.hash(i, salt, N = 2048, r = 4, p = 4)
		not_sent = 1
		while not_sent:
			try:
				outgoing.connect(("downstream", 4444))
				header = Header(2, 8 + len(list1)*64, 0)
				outgoing.send(header.serialize())
				outgoing.send(h1)
				not_sent = 0
			except Exception:
				continue
		outgoing.close()
	
def worker():
	while True:
		item = q.get()
		if item is None:
		    break
		sludger(item)
		q.task_done()

def main():
	num_worker_threads = 2	

	server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

	server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

	server.bind(('', 40000))
	
	server.listen(num_worker_threads)
	
	threads = []
	for i in range(num_worker_threads):
		t = threading.Thread(target=worker)
		t.start()
		threads.append(t)
	
	recieved = []

	while server:
		try:
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
					val = conn.recv(4096).decode('utf-8')
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
