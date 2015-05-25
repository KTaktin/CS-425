/*
** server.c -- a stream socket server demo
*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <sys/wait.h>
#include <signal.h>
#include <pthread.h>

#define BACKLOG 10	 // how many pending connections queue will hold
#define DELAY 3  // Max is actually this value +1

int talk_socks[4]; //Sockets use for sends
int hear_socks[4]; //Socks for recvs
int listen_socks[4];
int seq_num[4]; //Which number we're expecting next
int ack_flag = 0;
int ack_count = 0;
int seq_num_all = 0; // a cumalitive sequence num
int seq_num_tbp = 0; // num of next sequence to be processed

int connect_flag;

time_t rawtime;
struct tm* time_info;
char hostname[1024];

void sigchld_handler(int s)
{
	while(waitpid(-1, NULL, WNOHANG) > 0);
}

// get sockaddr, IPv4 or IPv6:
void *get_in_addr(struct sockaddr *sa)
{
	if (sa->sa_family == AF_INET) {
		return &(((struct sockaddr_in*)sa)->sin_addr);
	}

	return &(((struct sockaddr_in6*)sa)->sin6_addr);
}

unsigned short get_in_port(struct sockaddr *sa)
{
	if (sa->sa_family == AF_INET) {
		return ((struct sockaddr_in*)sa)->sin_port;
	}

	return ((struct sockaddr_in6*)sa)->sin6_port;
}

// Sets a set of sockets for select
int get_socket_set(fd_set* set){
	FD_ZERO(set);
	FD_SET(hear_socks[0], set);
	FD_SET(hear_socks[1], set);
	FD_SET(hear_socks[2], set);
	FD_SET(hear_socks[3], set);
	return 0;
}

// Gets a socket for two servers
int make_connection(char* argv, char* port){
	printf("Connecting to port %s...\n", port);	
	int sockfd, numbytes;  
	struct addrinfo hints, *servinfo, *p;
	int rv;
	char s[INET6_ADDRSTRLEN];

	memset(&hints, 0, sizeof hints);
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	if ((rv = getaddrinfo(argv, port, &hints, &servinfo)) != 0) {
		fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
		return 1;
	}

	// loop through all the results and connect to the first we can
	for(p = servinfo; p != NULL; p = p->ai_next) {
		if ((sockfd = socket(p->ai_family, p->ai_socktype,
				p->ai_protocol)) == -1) {
			perror("client: socket");
			continue;
		}

		if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
			close(sockfd);
			perror("client: connect");
			continue;
		}

		break;
	}

	if (p == NULL) {
		fprintf(stderr, "client: failed to connect\n");
		return 2;
	}

	inet_ntop(p->ai_family, get_in_addr((struct sockaddr *)p->ai_addr),
			s, sizeof s);
	printf("client: connecting to %s\n", s);

	freeaddrinfo(servinfo); // all done with this structure

	return sockfd;
}

// targeted send thread
void *msg_thread(void* arg){
	int rand_val = 0;
	char *msg;
	msg=(char*)arg;
	
	/*
		1 - delete
		2 - get
		3 - insert
		4 - update
	*/
	int dest = (int)msg[99] - 49;
	
	if(dest > 3 || dest < 0){
		free(arg);
		return NULL;
	}	

	rand_val = rand() % 6 + 1;
	time(&rawtime);
	time_info = localtime(&rawtime);


	// ack stuff taken out for now
		
	while(msg[98] > seq_num[dest]);

	printf("Sending message with %d seconds delay at %s to %d\n", rand_val,
		asctime(time_info), dest + 1);
	sleep(rand_val);
	send(talk_socks[dest], msg, 100, 0);
	if(msg[98] == seq_num[dest]) seq_num[dest]++;

	free(arg);	

	return NULL;
}

// Thread to send one message to all of the servers
void *msgall_thread(void* arg){
	int rand_val = 0;
	int my_seq = seq_num_all++;
	char *msg;
	msg=(char*)arg;
	
	/*
		1 - delete
		2 - get
		3 - insert
		4 - update
	*/
	int dest = (int)msg[99] - 49;
	
	
	printf("inc seq: %d, local seq: %d\n", msg[98], seq_num[dest]);
	printf("Source: %d\n", dest);
	if(dest > 3 || dest < 0){
		free(arg);
		return NULL;
	}	

	rand_val = rand() % DELAY + 1;
	time(&rawtime);
	time_info = localtime(&rawtime);

	printf("Sending message with %d seconds delay at %s to %d\n", rand_val,
			asctime(time_info), dest + 1);
	sleep(rand_val);

	// some ack stuff taken out for now
	while(msg[98] > seq_num[dest] && my_seq > seq_num_tbp);
	/*if(msg[97] == '1' || msg[97] == '3' || msg[97] == '4'){
		ack_flag = 1;
	}*/


	send(talk_socks[0], msg, 100, 0);
	send(talk_socks[1], msg, 100, 0);
	send(talk_socks[2], msg, 100, 0);
	send(talk_socks[3], msg, 100, 0);	

	if(msg[98] == seq_num[dest]) seq_num[dest]++;
	
	free(arg);	

	return NULL;
}

// Connects to all of the outside servers
void connect_all(){ 
	talk_socks[0] = make_connection(hostname, "3500");
	talk_socks[1] = make_connection(hostname, "3501");
	talk_socks[2] = make_connection(hostname, "3502");
	talk_socks[3] = make_connection(hostname, "3503");
}

/* Thread for getting commands, mainly used for connect */
void *cmd_thread(void* arg)
{
	struct sockaddr_storage their_addr; // connector's address information
	socklen_t sin_size = sizeof their_addr;	
	char s[INET6_ADDRSTRLEN];
	char str[50];
	char cmd[15];
	char msg[50];
	char dest[1];
	int i = 0;
	pthread_t pth;
	int temp_sock;

	while(1){
		fgets(str, 111, stdin);
		sscanf(str, "%s %s %s", cmd, msg, dest);	
		// Connects all the servers and stores sockets for receiving and sending
		if(strcmp("connect", cmd) == 0){
			// First the central server connects to the other servers 
			// and stores the sending sockets
			connect_all();
			// then it accepts incoming connections, and stores receiving sockets
			for(i = 0; i < 4; i++){
				 hear_socks[i] = accept(listen_socks[i], (struct sockaddr *)&their_addr, 						&sin_size);
				if (temp_sock == -1) {
					perror("accept");
					exit(1);
				}

				inet_ntop(their_addr.ss_family,
				get_in_addr((struct sockaddr *)&their_addr),
						s, sizeof s);
				printf("server: got connection from %s:%d\n", s, 3900 + i);
			}
			connect_flag = 1;
		}
		else if(strcmp("quit", cmd) == 0){
			for(i = 0; i < 4; i++){
				close(hear_socks[i]);
				close(talk_socks[i]);
			}
			exit(1);
		}
		else{
			printf("Not a valid command\n");
		}
	}

	return NULL;
}

int main(void)
{
	int new_fd;  // listen on sock_fd, new connection on new_fd
	pthread_t send_pth, cmd_pth; //thread for sends
	struct addrinfo hints, *servinfo, *p;
	struct sigaction sa;
	int yes=1;
	char s[INET6_ADDRSTRLEN];
	int rv;
	char** ports[4];
	hostname[1023] = '\0';
	gethostname(hostname, 1023);
	connect_flag = 0;

	// Early on in the devel process, I had trouble discerning the identity of
	// connecting servers. For a quick fix, I set 4 ports dedicated to one server
	// each. This is obviously not practical for more servers, and results in unused
	// sockets. 
	ports[0] = "3490";
	ports[1] = "3491";
	ports[2] = "3492";
	ports[3] = "3493";


	// Setting up sockets for each of the ports
	int i;
	for(i = 0; i < 4; i++){
		memset(&hints, 0, sizeof hints);
		memset(&servinfo, 0, sizeof servinfo);
		hints.ai_family = AF_UNSPEC;
		hints.ai_socktype = SOCK_STREAM;
		hints.ai_flags = AI_PASSIVE; // use my IP
		if ((rv = getaddrinfo(NULL, ports[i], &hints, &servinfo)) != 0) {
			fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
			return 1;
		}
		// loop through all the results and bind to the first we can
		for(p = servinfo; p != NULL; p = p->ai_next) {
			if ((listen_socks[i] = socket(p->ai_family, p->ai_socktype,
					p->ai_protocol)) == -1) {
				perror("server: socket");
				continue;
			}
			if (setsockopt(listen_socks[i], SOL_SOCKET, SO_REUSEADDR, &yes,
					sizeof(int)) == -1) {
				perror("setsockopt");
				exit(1);
			}
			if (bind(listen_socks[i], p->ai_addr, p->ai_addrlen) == -1) {
				close(listen_socks[i]);
				perror("server: bind");
				continue;
			}
			break;
		}
		if (p == NULL)  {
			fprintf(stderr, "server: failed to bind\n");
			return 2;
		}
	}

	freeaddrinfo(servinfo); // all done with this structure


	// All sockets are listening
	for(i = 0; i < 4; i++){
		if (listen(listen_socks[i], BACKLOG) == -1) {
			perror("listen");
			exit(1);
		}	
	}

	sa.sa_handler = sigchld_handler; // reap all dead processes
	sigemptyset(&sa.sa_mask);
	sa.sa_flags = SA_RESTART;
	if (sigaction(SIGCHLD, &sa, NULL) == -1) {
		perror("sigaction");
		exit(1);
	}

	// There's a command for connecting, so that the other servers can be set up
	// before we connect to all of them
	printf("server: waiting for connections...\n");

	// Variables for select
	char buf[100];
	struct timeval tv;
	// We're essentially using polling as opposed to interrupts to check for
	// new messages
	tv.tv_sec = 0;
	tv.tv_usec = 100000;
	fd_set readfds;
	pthread_create(&cmd_pth, NULL, cmd_thread, "blah");

	while(!connect_flag); // wait until connected before continuing to receive loop

	while(1) {   
		// Reset buffer
		memset(buf, '\0', 100);	
		// additional buffer for inputs, since buf can change during execution of
		// message threads		
		char* temp_msg = malloc(100);
		
		// Local time
		time(&rawtime);
		time_info = localtime(&rawtime);
		
		get_socket_set(&readfds);	
		// Select and check if we have an incoming message
		rv = select(hear_socks[3] + 1, &readfds, NULL, NULL, &tv);
	
		if (rv == -1) {
			perror("select"); // error occurred in select()
		} else if (rv == 0) {
			//printf("Timeout occurred!  No data after .1 seconds.\n");
		} else {
			// one or more of the descriptors have data
			if (FD_ISSET(hear_socks[0], &readfds)) {
				recv(hear_socks[0], buf, sizeof buf, 0);
				printf("client: received '%s' from port 3500 at %s\n", 
						buf, asctime(time_info));
			}
			else if (FD_ISSET(hear_socks[1], &readfds)) {
				recv(hear_socks[1], buf, sizeof buf, 0);
				printf("client: received '%s' from port 3501 at %s\n", 
						buf, asctime(time_info));
			}		
			else if (FD_ISSET(hear_socks[2], &readfds)) {
				recv(hear_socks[2], buf, sizeof buf, 0);
				printf("client: received '%s' from port 3502 at %s\n", 
						buf, asctime(time_info));
			}
			else if (FD_ISSET(hear_socks[3], &readfds)) {
				recv(hear_socks[3], buf, sizeof buf, 0);
				printf("client: received '%s' from port 3503 at %s\n", 
						buf, asctime(time_info));
			}

			FD_ZERO(&readfds);
			
			// Get copy of message thread dedicated for msg threads
			memcpy(temp_msg, buf, 100);
			
			// 3 cases: ack, val from get, and a cmd			
			//printf("message: %s\n", temp_msg);			

			// In the case we get an ack
			if(strcmp("ack", temp_msg) == 0){
				ack_count++;		
				// go to the next process waiting
				if(ack_count%4 == 0){
					printf("Got 4 acks\n");
					seq_num_tbp++;
					pthread_create(&send_pth, NULL, msg_thread, temp_msg);
					ack_flag = 0;
				}
			}

			//printf("msg: %d, %d\n", temp_msg[98], temp_msg[99]);
			// regular send		
			else if(temp_msg[97] == '0'){ // targeted send
				pthread_create(&send_pth, NULL, msg_thread, temp_msg);
			}
			
			// In the case we get a command
			else if(temp_msg[97] == '1' || temp_msg[97] == '2' || temp_msg[97] == '3' 						|| temp_msg[97] == '4'){ // server cmd, send to all
				//printf("cmd\n");
				pthread_create(&send_pth, NULL, msgall_thread, temp_msg);
			}
			
			// returning k, v
			else if(temp_msg[97] == '5'){
				pthread_create(&send_pth, NULL, msg_thread, temp_msg);
			}
		}

	}

	return 0;
}

