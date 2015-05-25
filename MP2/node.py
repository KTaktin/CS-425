import socket   #for sockets
import sys      #for exit
import threading
from threading import Thread
import Queue
import time
import random
import os
import math
import operator

#declaring global variables
UDP_IP = socket.gethostname()
UDP_IP = ''
base_port = 9100
m = 8
maxKey = 2**m

#node class creates node object
class Node(object):
	#init function
	def __init__(self, nodeNum):
		self.id = int(nodeNum)
		self.finger = [self.id]*m
		self.port = int(base_port+self.id)
		self.keys = list()
		self.initFuncQueues()
		self.predecessor = int
		self.msg_counter = 0

		#binding porting for UDP communication
		self.s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		self.s.bind((UDP_IP, self.port))

		listening = threading.Thread(target=self.listen)
		listening.daemon = True
		listening.start()
		
		self.join(self.id)

	#func used to remove node from system
	def leave(self):
		#print 'node ' + str(self.id) + ' is leaving'
		# On a leave command, transfer all keys to successor and set its predecessor
		self.send_data(self.id, self.finger[0], 'transfer_keys', self.predecessor)
		toSend = str(self.id) + ' ' + str(self.finger[0]) + ' ' + str((self.id-2**(m-1)-1)%maxKey) + ' ' + str(2**(m-1))
		# Remove fingers from predecessors
		self.send_data(self.id, self.predecessor, 'remove_finger', toSend)
		#Close the node
		self.send_return(self.id, self.id, 'exit', 0)
		self.s.close()
		del self.s
		
	#func removes given node from finger table
	def remove_finger(self, removed_node, replace_node, lower_bound, count):
		#print 'removing finger' + str(self.id)
		# If the node is not the removed node or count is leq 0, we don't need to check it
		if self.id != removed_node and self.id != self.predecessor and count > 0:
			# Check all fingers and change them to the removed node's successor
			for i in range(m):
				if removed_node == self.finger[i]:
					self.finger[i] = replace_node
			toSend = str(removed_node) + ' ' + str(replace_node) + ' ' + str(lower_bound) + ' ' + str(count-1)
			# send message to predecessor
			self.send_data(self.id, self.predecessor, 'remove_finger', toSend)
		return 0

	#func initializes our function queues
	def initFuncQueues(self):
		# Queues for incoming commands
		self.funcQueues = dict()
		self.funcQueues['finished_find_successor'] = Queue.Queue()
		self.funcQueues['finished_find_predecessor'] = Queue.Queue()
		self.funcQueues['finished_closest_preceding_finger'] = Queue.Queue()
		self.funcQueues['finished_init_finger_table'] = Queue.Queue()
		self.funcQueues['finished_update_others'] = Queue.Queue()
		self.funcQueues['finished_update_finger_table'] = Queue.Queue()
		self.funcQueues['finished_transfer_keys'] = Queue.Queue()

	#func dequeues
	def getFuncQueue(self, key):
		# Dequeue, block if nothing there
		if key not in self.funcQueues:
			self.funcQueues[key] = Queue.Queue()
		return self.funcQueues[key].get(block=True)

	#func enqueus
	def putFuncQueue(self, key, message):
		# Enqueue
		if key not in self.funcQueues:
			self.funcQueues[key] = Queue.Queue()
		self.funcQueues[key].put(message)

	#function listens for incoming messages
	def listen(self):
		#keep looping to receive messages
		while 1:
			#get data from port
			data, addr = self.s.recvfrom(1024)
			dataArray = data.split()

			#parse data
			srcNode = dataArray[0]
			destNode = dataArray[1]
			command = dataArray[2]
			message = ' '.join(dataArray[3:])

			#parse command and respond correctly
			if command == 'exit':
				break
			elif command.startswith('finished_'):
				self.putFuncQueue(command, dataArray)
			else:
				execute = threading.Thread(target=self.execute, args=(data,))
				execute.daemon = True
				execute.start()

	#func executes command
	def execute(self, data):
		#print 'execute'
		#parse data
		dataArray = data.split()
		srcNode = dataArray[0]
		destNode = dataArray[1]
		command = dataArray[2]
		message = ' '.join(dataArray[3:])

		#execute commands
		if command == 'find_successor':
			returnMsg = self.find_successor(int(dataArray[3]))
		elif command == 'find_predecessor':
			returnMsg = self.find_predecessor(int(dataArray[3]))
		elif command == 'closest_preceding_finger':
			returnMsg = self.closest_preceding_finger(int(dataArray[3]))
		elif command == 'init_finger_table':
			returnMsg = self.init_finger_table(int(dataArray[3]))
		elif command == 'update_others':
			returnMsg = self.update_others()
		elif command == 'update_finger_table':
			returnMsg = self.update_finger_table(int(dataArray[3]), int(dataArray[4]))
		elif command == 'transfer_keys':
			returnMsg = self.transfer_keys(int(dataArray[3]))
		elif command == 'successor':
			returnMsg = self.finger[0]
		elif command == 'remove_finger':
			returnMsg = self.remove_finger(int(dataArray[3]), int(dataArray[4]), int(dataArray[5]), int(dataArray[6]))
		else:
			return

		# Return value to sender
		self.send_return(self.id, srcNode, 'finished_' + command, returnMsg)

	#func used to send return messages for commands
	def send_return(self, srcNode, destNode, command, message):
		self.msg_counter += 1
		toSend = str(srcNode) + ' ' + str(destNode) + ' ' + str(command) + ' ' + str(message)
		self.s.sendto(toSend, (UDP_IP, base_port + int(destNode)))

	#func used to send command to other nodes
	def send_data(self, srcNode, destNode, command, message):
		self.msg_counter += 1
		toSend = str(srcNode) + ' ' + str(destNode) + ' ' + str(command) + ' ' + str(message)
		self.s.sendto(toSend, (UDP_IP, base_port + int(destNode)))
		dataArray = self.getFuncQueue('finished_' + command)
		return int(dataArray[3])

	#func finds successor for given key
	def find_successor(self,nodeNum):
		#print 'find successor'
		# Key with the same value as node has it as successor
		if self.id == nodeNum:
			return nodeNum
		# Otherwise we ask its predecessor to find the successor for it
		pred = self.find_predecessor(nodeNum)
		return self.send_data(self.id, pred, 'successor', nodeNum)

	#func finds predecessor for node number
	def find_predecessor(self,nodeNum):
		#print 'find predecessor'
		cur_node = self.id
		successor = self.finger[0]
		# Check if the target node is within the current node and its successor
		# If yes, then the current node is the predecessor of target
		while not (self.checkIfInRange(nodeNum, cur_node, successor) or nodeNum == successor):
			# Else get the closest finger preceding target
			cur_node = self.send_data(self.id, cur_node, 'closest_preceding_finger', nodeNum)
			if cur_node == self.id:
				return cur_node
			successor = self.send_data(self.id, cur_node, 'successor', nodeNum)
		return cur_node
	
	#func returns closest finger preceding id
	def closest_preceding_finger(self,nodeNum):
		#print 'closest preceding finger'
		# Find largest finger smaller than target
		for i in range(m-1,-1,-1):
			if self.checkIfInRange(self.finger[i], self.id, nodeNum):
				return self.finger[i]
		return self.id

	#func joins node n to system
	def join(self, nodeNum):
		# Just make a finger table, update other finger tables, and get keys
		if nodeNum != 0:
			self.init_finger_table(0)
			self.update_others()
			for i in range(self.predecessor+1, self.id+1):
				self.keys.append(i)
		# If its the first node (0), we just get all the keys and nothing else
		else:
			for i in range(0, maxKey):
				self.keys.append(i)
			self.predecessor = nodeNum
			for i in range(0, m):
				self.finger[i] = nodeNum
		return 0

	#func initializes finger table of local node
	def init_finger_table(self,nodeNum):
		#print 'init finger table'
		lb = (self.id + 1) % maxKey
		# Get first value in finger table
		successor = self.send_data(self.id, nodeNum, 'find_successor', lb)
		self.finger[0] = int(successor)
		self.predecessor = self.send_data(self.id, nodeNum, 'find_predecessor', self.finger[0])
		self.send_data(self.id, int(successor), 'transfer_keys', self.id)

		for i in range(m-1):
			start = (self.id + 2**(i+1)) % maxKey
			# If the finger is within self and its successor, assign successor node to it
			if self.checkIfInRange(start, self.id, self.finger[i]) or start == self.id: 
				self.finger[i+1] = self.finger[i]
			# Else we ask another node to find the successor of that finger
			else:
				self.finger[i+1] = self.send_data(self.id, nodeNum, 'find_successor', start)
		return 0

	#func updates all nodes whose finger table should refer to node n
	def update_others(self):
		#print 'update others'
		for i in range(0, m):
			p = self.find_predecessor(((self.id - 2**i) % maxKey) + 1)
			self.send_data(self.id, p, 'update_finger_table', str(self.id) + ' ' + str(i))
		return 0

	#func if s is the ith finger of n, update n's finger table with s
	def update_finger_table(self,s,i):
		#print 'update finger table'
		lb = (self.id + 2**i) % maxKey
		if (s == lb or self.checkIfInRange(s,lb,self.finger[i])) and (self.finger[i] != lb):
			self.finger[i] = s
			p = self.predecessor
			self.send_data(self.id, p, 'update_finger_table', str(s) + ' ' + str(i))
		return 0

	#func transfers keys
	def transfer_keys(self, newNodeNum):
		#print 'transfer keys'
		# Mainly used to update a the predecessor of a successor and to delete the sucessors keys
		self.predecessor = newNodeNum
		self.keys[:] = []
		if self.id < self.predecessor:
			for i in range(self.predecessor+1, self.id+maxKey+1):
				self.keys.append(i%maxKey)
		elif self.id == 0 and self.predecessor == 0:
			for i in range(0, maxKey):
				self.keys.append(i)
		else:
			for i in range(self.predecessor+1, self.id+1):
				self.keys.append(i)
		return 0

	#func checks if num is in the range given
	def checkIfInRange(self, num, lb, ub):
		# Used to check if num is between lower bound and upper bound, good for wraparound situations
		if ub <= lb:
			if (num >= 0 and num < ub) or (num > lb and num <= 255):
				return True
			return False
		else:
			if num > lb and num < ub:
				return True
			return False
