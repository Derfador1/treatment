#define _XOPEN_SOURCE 600

//happy

#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
//#include <syslog.h>
#include <unistd.h>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>

void extract_ip_string(struct sockaddr *address, char *dst, size_t len);

int main(int argc, char *argv[])
{
	struct addrinfo *results, *downstream, hints = {0};
	int errcode = 0;

	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_DGRAM;

	errcode = getaddrinfo("downstream", "1111", &hints, &downstream);
	if(errcode) {
		fprintf(stderr, "Could not get info of port 1111: %s",
				gai_strerror(errcode));
		return -1;
	}

	int out = socket(downstream->ai_family, downstream->ai_socktype, 0);
	if(out < 0) {
		perror("Could not create socket");
		freeaddrinfo(downstream);
		return -2;
	}

	errcode = getaddrinfo("stormdrain", "1111", &hints, &results);
	if(errcode) {
		fprintf(stderr, "Could not get info of port 1111: %s",
				gai_strerror(errcode));
		return -1;
	}
	// First one is 127.0.1.1
	results = results->ai_next;

	int in = socket(results->ai_family, results->ai_socktype, 0);
	if(in < 0) {
		return -in;
	}

	errcode = bind(in, results->ai_addr, results->ai_addrlen);

	while(1) {
		struct sockaddr_storage remote = {0};
		socklen_t remote_sz = sizeof(remote);

		char *buffer = calloc(1, 65535);

		ssize_t size = recvfrom(in, buffer, 65536, 0, (struct sockaddr *)&remote, &remote_sz);

		//extract_ip_string((struct sockaddr *)&remote, ip_as_str,
				//sizeof(ip_as_str));
		//syslog(LOG_INFO, "%s is raining", ip_as_str);

		// 
		sendto(out, buffer, size, 0, downstream->ai_addr, downstream->ai_addrlen);

		free(buffer);
	}

	close(in);
	close(out);
}

void extract_ip_string(struct sockaddr *address, char *dst, size_t len)
{
	void *addr;

	if(address->sa_family == AF_INET) {
		// IPv6 Land
		addr = &((struct sockaddr_in6 *)address)->sin6_addr;
	} else {
		// IPv4 Land
		addr = &(((struct sockaddr_in *)address)->sin_addr);
	}
	inet_ntop(address->sa_family, addr, dst, len);
}


