import sys      #for exit
import threading
from threading import Thread, Lock
import Queue
import time
import random
import os
import math
import operator

msg_queue = list()
request_queue = list()
running = True
mutex = list()
print_mutex = Lock()

option = 1
nodes_entered = 0
cs_int = -1
next_req = -1
tot_exec_time = -1

nodes = [None]*9

class Node(object):
    def __init__(self, nodeNum):
        self.id = int(nodeNum)
        self.state = 'released'
        self.voted = False
        self.inquiring = False
        self.recv_failed = False
        self.vs = self.get_vs()
        self.replies_received = 0
        self.voted_for = ('none', -1)
        self.votes_gotten = list()
        self.tenative_vote = ('none', -1)
        self.recent_voted = -1
        self.my_time = 0

        #print "%d nodes entered\n" % nodes_entered

        t = threading.Thread(target=self.node_thread)
        t.daemon = True
        t.start()		

    def node_thread(self):
        msg = ('', 0)
        global nodes_entered, running, print_mutex
        nodes_entered += 1
        #wait for all the nodes to enter
        while nodes_entered < 9:
            pass

        #introduce some randomness to start times
        start = time.time()
        while (time.time() - start) < random.randint(500, 1500)/1000:
            pass

        print ('Time: ' + '{0:.22f}'.format(time.time()) + ' node: %d\n') % self.id
        listener = threading.Thread(target=self.receive_thread)
        listener.daemon = True
        listener.start()
        
        start = time.time()
        print '%d ' % tot_exec_time
        while int(time.time()) - int(start) < tot_exec_time:
                #set self to wanted and send requests to voting set
                self.state = 'wanted'
                self.send_request()
                #wait until we get all votes
                while len(self.votes_gotten) < 5:
                    pass
                self.replies_received = 0
                self.state = 'held'
                print_mutex.acquire()
                print ("Entering CS " + '{0:.16f}'.format(time.time()) + "; %d; " + str(self.votes_gotten) + '\n') % self.id
              
                print_mutex.release()
                #go into CS
                enter_time = float(time.time())
                while (float(time.time()) - enter_time) < cs_int/1000:
                    pass
                
                print_mutex.acquire()
                print ( "Leaving CS " + '{0:.16f}'.format(time.time()) + "; %d; " + str(self.votes_gotten) + '\n') % self.id
                print_mutex.release()
                #clear votes, set self to release and wait for a bit until we ask again for requests
                self.votes_gotten = []
                self.state = 'released'
                self.send_release()
                
                exit_time = float(time.time())
                while (float(time.time()) - exit_time) < next_req/1000:
                    pass
                self.recv_failed = False
        
        running = False
        print_mutex.acquire()
        print 'Exiting\n'
        print_mutex.release()
        return

    def receive_thread(self):
        global msg_queue, request_queue, running, print_mutex
        msg = ('',0, '')
        while running:
            msg = self.check_msg()

            if msg[0] == 'request':
                if self.state == 'held':
                    request_queue[self.id].put((msg[2], msg[1]))
                    self.send_fail(msg[1])
                elif self.voted == True:
                    request_queue[self.id].put((msg[2], msg[1]))
                    #new message came earlier, try to rescind vote
                    if self.voted_for[0] > msg[2]:
                        self.send_inquire(self.voted_for[1])
                    #tiebreaker scenario
                    elif self.voted_for[0] == msg[2] and self.voted_for[1] > msg[1]:
                        self.send_inquire(self.voted_for[1])
                    #not high enough priority, report fail
                    else:
                        self.send_fail(msg[1])
                #no message to compare with, so just send reply
                else:
                    self.send_reply(msg[1])
                    self.voted_for = (msg[2], msg[1])
                    self.voted = True

            if msg[0] == 'reply':
                #add it to our set of gotten replies
                if msg[1] not in self.votes_gotten:
                    self.votes_gotten.append(msg[1])

            if msg[0] == 'release':
                if not request_queue[self.id].empty():
                    #get next message in request queue and send a reply
                    new_msg = request_queue[self.id].get()
                    self.send_reply(new_msg[1])
                    self.voted_for = (new_msg[0], new_msg[1])
                    self.voted = True
                else:
                    self.voted = False
                    self.voted_for = ('none', -1)
                    
            if msg[0] == 'inquire':
                #if its been reported that one of our voting set members rejected us and we get a receive,
                #happily give up our vote
                if len(self.votes_gotten) in range(0,5) and msg[1] in self.votes_gotten and self.recv_failed:
                    self.votes_gotten.remove(msg[1])
                    self.send_relinquish(msg[1])

            #rejected
            if msg[0] == 'fail':
                self.recv_failed = True

            #our target relinquished our vote, so we will grant permission to our top priority message
            if msg[0] == 'relinquish':            
                top_request = request_queue[self.id].get()
                self.send_reply(top_request[1])
                request_queue[self.id].put((self.voted_for))
                
                self.voted_for = top_request
                
            if option == 1:
                print_mutex.acquire()
                print ('{0:.8f}'.format(time.time()) + "; Thread %d; From %s; " + msg[0] + '\n') % (self.id, msg[1])
                print_mutex.release()
        return

    def check_msg(self):
        global msg_queue, running
        start = time.time()
        while msg_queue[self.id].empty():
            if time.time() - start > 2 and running:
                #a time out clause, where if stuff really goes wrong we'll rebroadcast our request
                msg = request_queue[self.id].get()
                return ('request', msg[1], msg[0])
            pass
        mutex[self.id].acquire()
        rtn = msg_queue[self.id].get()
        mutex[self.id].release()
        return rtn

## Message sending functions, sends a string for the message, the node id, and the time the message
## was sent if applicable

        #send requests to voting set, using current time
    def send_request(self):
        global msg_queue
        self.my_time = '{0:.16f}'.format(time.time())
    
        for node in self.vs:
            mutex[node].acquire()
            msg_queue[node].put(('request', self.id, self.my_time))
            mutex[node].release()
        return

        #send requests to voting set using old time
    def resend_request(self):
        global msg_queue

        for node in self.vs:
            mutex[node].acquire()
            msg_queue[node].put(('request', self.id, self.my_time))
            mutex[node].release()
        return
    
    def send_relinquish(self, dest_num):
        global msg_queue
        mutex[dest_num].acquire()
        msg_queue[dest_num].put(('relinquish', self.id, ''))
        mutex[dest_num].release()
        return

    def send_deny(self, dest_num):
        global msg_queue
        msg_queue[dest_num].put(('deny', self.id, ''))
        return
    
    def send_inquire(self, dest_num):
        global msg_queue
        mutex[dest_num].acquire()
        msg_queue[dest_num].put(('inquire', self.id, ''))
        mutex[dest_num].release()
        return

    def send_release(self):
        global msg_queue
        for node in self.vs:
            mutex[node].acquire()
            msg_queue[node].put(('release', self.id, ''))
            mutex[node].release()
        return

    def send_reply(self, dest_num):
        global msg_queue
        mutex[dest_num].acquire()
        msg_queue[dest_num].put(('reply', self.id, ''))
        mutex[dest_num].release()
        return

    def send_fail(self, dest_num):
        global msg_queue
        mutex[dest_num].acquire()
        msg_queue[dest_num].put(('fail', self.id, ''))
        mutex[dest_num].release()
        return
     
    def get_vs(self):
        vs = list()
        for i in range (1, 3):
            vs.append((self.id + 3*i)%9)
        if self.id in range(0, 3):
            for i in range(0, 3):
                vs.append(i)
        elif self.id in range(3, 6):
            for i in range(3, 6):
                vs.append(i)
        elif self.id in range(6, 9):
            for i in range(6, 9):
                vs.append(i)
        return vs

def init_queues():
    global msg_queue, running
    for i in range(0, 9):
        msg_queue.append(Queue.Queue())
        request_queue.append(Queue.PriorityQueue())
        mutex.append(Lock())
    return
	
#main function
def main(argv):
    global cs_int, next_req, tot_exec_time, option
    #if file name was given then output to file
    cs_int = int(argv[0])
    next_req = int(argv[1])
    tot_exec_time = int(argv[2])
    option = int(argv[3])

    print '%s %s %s %s' % (cs_int, next_req, tot_exec_time, option)

    init_queues()
    global nodes, running, nodes_entered
    
    for i in range (0, 9):
        time.sleep(.05)
        nodes[i] = Node(i)
    #print "%d " % nodes_entered

    #main thread used to read commands
    while running:
        pass

if __name__ == "__main__":
	#main(('5', '7', '20', '1'))
	main(sys.argv[1:])

