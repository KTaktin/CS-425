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

#define PORT "3490"  // the port users will be connecting to

#define BACKLOG 10	 // how many pending connections queue will hold


int hear_sock, talk_sock;

time_t rawtime;
struct tm* time_info;
char hostname[1024];
int values[1024];
char* port;
int src;

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

// Gets a socket for two servers
int make_connection(char* argv, char* in_port){
	int newsock;  
	struct addrinfo hints, *servinfo, *p;
	int rv;
	char s[INET6_ADDRSTRLEN];

	memset(&hints, 0, sizeof hints);
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;

	if ((rv = getaddrinfo(argv, in_port, &hints, &servinfo)) != 0) {
		fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
		return 1;
	}

	// loop through all the results and connect to the first we can
	for(p = servinfo; p != NULL; p = p->ai_next) {
		if ((newsock = socket(p->ai_family, p->ai_socktype,
				p->ai_protocol)) == -1) {
			perror("client: socket");
			continue;
		}

		if (connect(newsock, p->ai_addr, p->ai_addrlen) == -1) {
			close(newsock);
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

	// return our new descriptor
	return newsock;
}

// a dumb function to change the message to an ack
void change_ack(char* str){
	memset(str, '\0', 97);
	*str = 'a';
	memset(&str[1], 'c', 1);
	memset(&str[2], 'k', 1);
}	

// thread called to send a message
void *msg_thread(void* arg){
	//int rand_val = 0;
	char *msg;
	msg=(char*)arg;

	//rand_val = rand() % 5 + 1;
	//time(&rawtime);
	time_info = localtime(&rawtime);

	printf("Sending message with max 6 seconds delay at %s\n", asctime(time_info));
	//sleep(rand_val);
	send(talk_sock, msg, 100, 0);

	// Since we're always passing in malloc'd temp messages, we free those up after
	// we're done with them
	free(arg);

	return NULL;
}

/* Server thread */
void *cmd_thread(void* arg)
{
	char* str[50];
	char* cmd[15];
	char* dest[1]; // takes on the dest for send, or the source for others
	int seq = 0;
	pthread_t pth;

	//message has the following structure:
	// bits 0-96: data (0 key, 49 value if needed)
	// bit 97: command
	// bit 98: sequence number originally used for FIFO, now depreciated
	// bit 99: message's sender 
	while(1){
		char* msg = malloc(100);
		
		// keyboard input
		fgets(str, 50, stdin);
			
		memset(msg, '\0', strlen(msg));
		memset(dest, '\0', strlen(dest));
		

		// Storing input values
		sscanf(str, "%s %s %s", cmd, msg, &msg[49]);
		sscanf(str, "%s %s %s", cmd, msg, dest);
		// Store sender
		msg[99] = src;
		
		// get the command, put relevant data in message, and send to central
		if(strcmp("send", cmd) == 0 && dest[0] != NULL){
			msg[97] = '0';
			msg[98] = (char)seq++;
			msg[99] = dest[0];
			pthread_create(&pth, NULL, msg_thread, msg);
		}
		else if(strcmp("delete", cmd) == 0 && msg != NULL){
			msg[97] = '1';
			msg[98] = (char)seq++;
			msg[99] = src + 49;
			pthread_create(&pth, NULL, msg_thread, msg);
		}
		else if(strcmp("get", cmd) == 0 && msg != NULL && msg[49] != NULL){
			if(msg[49] == '1'){
				msg[97] = '2';
				msg[98] = (char)seq++;
				msg[99] = src + 49;
				pthread_create(&pth, NULL, msg_thread, msg);
			}
 			else{
				int index;
				sscanf(msg, "%d", &index);
				if(values[index] == -1) printf("Does not exist!\n");
				else printf("Value at %d is %d\n", index, values[index]);
			}
		}
		else if(strcmp("update", cmd) == 0 && dest[0] != NULL){
			msg[97] = '3';
			msg[98] = (char)seq++;
			msg[99] = src + 49;
			pthread_create(&pth, NULL, msg_thread, msg);
		}
		else if(strcmp("insert", cmd) == 0 && dest[0] != NULL){
			msg[97] = '4';
			msg[98] = (char)seq++;
			msg[99] = src + 49;
			pthread_create(&pth, NULL, msg_thread, msg);
		}
		else if(strcmp("show-all", cmd) == 0){
			int i;
			for(i = 0; i < 1024; i++){
				if(values[i] != -1) printf("(%d, %d)\n", i, values[i]);
			}
		}
		else if(strcmp("delay", cmd) == 0 && msg != NULL){
			int sec, ms;
			sscanf(msg, "%d.%d", &sec, &ms);
			usleep(1000000*sec + 100000*ms);
		}
		else if(strcmp("search", cmd) == 0 && msg != NULL){
			int index;
			sscanf(msg, "%d", &index);
			if(values[index] != -1) printf("ALL OF THEM\n");
			else printf("NONE OF THEM\n");
		}
		else if(strcmp("quit", cmd) == 0){
			close(talk_sock);
			close(hear_sock);
			exit(1);
		}
		else{
			printf("Not a valid command\n");
		}
		memset(str, '\0', strlen(str));
		memset(cmd, '\0', strlen(cmd));
	}

	return NULL;
}

int main(int argc, char* argv[])
{
	int rv, sockfd;
	struct addrinfo hints, *servinfo, *p;
	pthread_t send_pth, ack_pth; //thread for sends
	struct sockaddr_storage their_addr; // connector's address information
	char s[INET6_ADDRSTRLEN];
	char *central_port;
	socklen_t sin_size;
	struct sigaction sa;
	int yes=1;
	hostname[1023] = '\0';
	gethostname(hostname, 1023);
	memset(values, -1, 1024 * sizeof(int));
	int read_val[4]; // Values for get function
	int read_cnt = 0;
	int read_key = 0;

	// Server initialization and port getting things
	if(argc == 1){
		printf("Specify a server number (1-4), exiting...\n");
		exit(1);
	}

	if(!strcmp("1", argv[1])){
		printf("Port 3500\n");
		port = "3500";
		src = 0;
		central_port = "3490";
	}
	else if(!strcmp("2", argv[1])){
		printf("Port 3501\n");
		port = "3501";
		src = 1;
		central_port = "3491";
	}	
	else if(!strcmp("3", argv[1])){
		printf("Port 3502\n");
		port = "3502";
		src = 2;	
		central_port = "3492";
	}	
	else if(!strcmp("4", argv[1])){
		printf("Port 3503\n");
		port = "3503";
		src = 3;		
		central_port = "3493";
	}	
	else{
		printf("Need servers 1-4");
		exit(1);
	}

	// Getting our listening socket
	memset(&hints, 0, sizeof hints);
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE; //use my IP
	printf("check");

	if ((rv = getaddrinfo(NULL, port, &hints, &servinfo)) != 0) {
		fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
		return 1;
	}

	// loop through all the results and bind to the first we can
	for(p = servinfo; p != NULL; p = p->ai_next) {
		if ((sockfd = socket(p->ai_family, p->ai_socktype,
				p->ai_protocol)) == -1) {
			perror("server: socket");
			continue;
		}

		if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes,
				sizeof(int)) == -1) {
			perror("setsockopt");
			exit(1);
		}

		if (bind(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
			close(sockfd);
			perror("server: bind");
			continue;
		}

		break;
	}
	
	if (p == NULL)  {
		fprintf(stderr, "server: failed to bind\n");
		return 2;
	}

	freeaddrinfo(servinfo); // all done with this structure

	// Start listening
	if (listen(sockfd, BACKLOG) == -1) {
		perror("listen");
		exit(1);
	}

	sa.sa_handler = sigchld_handler; // reap all dead processes
	sigemptyset(&sa.sa_mask);
	sa.sa_flags = SA_RESTART;
	if (sigaction(SIGCHLD, &sa, NULL) == -1) {
		perror("sigaction");
		exit(1);
	}

	printf("server: waiting for connections...\n");
	
	// Wait for the central server to connect to us
	hear_sock = accept(sockfd, (struct sockaddr *)&their_addr, &sin_size);
	if (hear_sock == -1) {
		perror("accept");
		exit(1);
	}
	inet_ntop(their_addr.ss_family,
		get_in_addr((struct sockaddr *)&their_addr),
		s, sizeof s);
	printf("server: got connection from %s\n", s);

	// After we've gotten a receiving socket, it's time to get a sending one
	talk_sock = make_connection(hostname, central_port);

	pthread_create(&send_pth, NULL, cmd_thread, "blah");

	char buf[100];

	
	while(1) {  // client loop loop
		int key, value;
		char* temp_msg = malloc(100);
		
		if (recv(hear_sock, buf, 100, 0) == -1) {
			perror("recv");
			exit(1);
		}

		buf[100] = '\0';
		memcpy(temp_msg, buf, 100);

		// Local time
		time(&rawtime);
		time_info = localtime(&rawtime);

		printf("received new message at %s\n", asctime(time_info));	
		//printf("message: %s, %c\n", temp_msg, temp_msg[97]);

		if(strcmp("ack", temp_msg) == 0){
			printf("got ack\n");
		}


		// delete, delete and send ack
		else if(buf[97] == '1'){
			sscanf(temp_msg, "%d", &key);
			sscanf(&temp_msg[49], "%d", &value);
			values[key] = -1;
			change_ack(temp_msg);
			pthread_create(&ack_pth, NULL, msg_thread, temp_msg);
		}	
		// get, modify the data with the value and send it back
		else if(buf[97] == '2'){
			sscanf(temp_msg, "%d", &key);
			sprintf(&temp_msg[49], "%d", values[key]);
			temp_msg[97] = '5';			
			pthread_create(&ack_pth, NULL, msg_thread, temp_msg);
		}
		// update, update and send ack
		else if(buf[97] == '3'){
			sscanf(buf, "%d", &key);
			sscanf(&buf[49], "%d", &value);
			if(values[key] != -1){
				values[key] = value;
				printf("Key %d does not exist!\n", key);
			}
			else printf("Updated value %d at key %d\n", value, key);
			change_ack(temp_msg);
			pthread_create(&ack_pth, NULL, msg_thread, temp_msg);
		}
		// insert, insert and send ack
		else if(buf[97] == '4'){
			sscanf(buf, "%d", &key);
			sscanf(&buf[49], "%d", &value);
			values[key] = value;
			printf("Inserted value %d at key %d\n", value, key);
			change_ack(temp_msg);
			printf("Ack message\n");
			pthread_create(&ack_pth, NULL, msg_thread, temp_msg);
		}
		else if(buf[97] == '5'){
			if(read_cnt < 4){
				sscanf(buf, "%d", &read_key);
				sscanf(&buf[49], "%d", &read_val[read_cnt++]);				
				//read_cnt++;			
			}
			if(read_cnt == 4){
				int i;
				printf("Servers returned values: ");
				for(i = 0; i < 4; i++){
					if(read_val[i] == -1) printf("DNE");
					else printf("(%d, %d)", read_key, read_val[i]);
					if(i != 3) printf(", ");
				}
				printf("\n");
				read_cnt = 0;
			}
		}
	}

	return 0;
}
