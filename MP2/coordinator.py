import socket   #for sockets
import sys      #for exit
import threading
from threading import Thread
import Queue
import time
import random
import os
import math
from node import Node

#declaring global variables
m = 8
maxKey = 2**m
nodes = [None]*maxKey
nodeList = list()

#function used to parse commands from terminal
def read_command():
	#read command from terminal
	command = raw_input()
	args = command.split()

	global nodes

	#parse command and initiate action
	#quit
	if args[0] == "q":
		exit(1)
	#join node
	if args[0] == "join":
		nodeNum = int(args[1])
		if nodes[nodeNum] == None:
			#print 'joining ' + str(nodeNum)
			nodes[nodeNum] = Node(nodeNum)
			nodeList.append(nodeNum)
	#find key by asking node
	elif args[0] == "find":
		#print 'find'
		nodeNum = int(args[1])
		key = int(args[2])
		node = nodes[nodeNum]
		if node is not None:
			print node.find_successor(key)
	#leave node
	elif args[0] == "leave":
		#print 'leave'
		nodeNum = int(args[1])
		node = nodes[nodeNum]
		if node is not None:
			node.leave()
			nodes[nodeNum] = None
			nodeList.remove(nodeNum)
	#show keys for node
	elif args[0] == "show":
		nodeNum = int(args[1])
		node = nodes[nodeNum]
		if node is not None:
			data = str(node.id)
			for key in sorted(node.keys):
				data += ' ' + str(key)
			print data
	#show all keys
	elif args[0] == "show-all":
		for i in range (0,maxKey):
			node = nodes[i]
			if node is not None:
				data = str(node.id)
				for key in sorted(node.keys):
					data += ' ' + str(key)
				print data
	#command for phase 1
	elif args[0] == "phase1":
		nodeNum = int(args[1])
		for i in range (0, nodeNum):
			randomNum = random.randint(1,maxKey-1)
			if nodes[randomNum] == None:
				nodes[randomNum] = Node(randomNum)
				nodeList.append(randomNum)
			else:
				i -= 1
			time.sleep(0.01)
		count_msgs_passed(1)
	#command for phase 2
	elif args[0] == "phase2":
		for i in range(0, random.randint(64, maxKey)):
			p = random.randint(0, len(nodeList)-1)
			node = nodes[nodeList[p]]
			k = random.randint(0, maxKey-1)
			if node is not None:
				print 'find ' + str(node.id) + ' ' + str(k) + ': ' + str(node.find_successor(k))
		count_msgs_passed(2)

 	#show fingers for node FOR TESTING
	elif args[0] == "finger":
		nodeNum = int(args[1])
		node = nodes[nodeNum]
		if node is not None:
			print node.finger
	#else command entered was incorrect
	else:
		print 'Cannot process command'
	sys.stdout.flush()

def count_msgs_passed(phaseNum):
	global nodes
	total_count = 0
	for i in range (0,maxKey):
		node = nodes[i]
		if node is not None:
			total_count += node.msg_counter
			node.msg_counter = 0
	print 'The total number of messages for Phase ' + str(phaseNum) + ' is ' + str(total_count)

#main function
def main(argv):
	#if file name was given then output to file
	try:
		if str(argv[0]) == '-g':
			sys.stdout = open(argv[1], 'w')
	except:
		print 'This is the Coordinator'
		pass

	#initialize node 0
	global nodes
	nodes[0] = Node(0)
	nodeList.append(0)

	#main thread used to read commands
	while 1:
		read_command()

if __name__ == "__main__":
	main(sys.argv[1:])
