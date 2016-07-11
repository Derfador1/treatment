#define _XOPEN_SOURCE 600

#include <netdb.h>
#include <stdio.h>
#include <unistd.h>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>

#include "networking.h"

int listen_incoming(const char *port)
{
	struct addrinfo *localhost, hints = {0};

	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE;


	int errcode = getaddrinfo(NULL, port, &hints, &localhost);
	if(errcode) {
		fprintf(stderr, "Could not get info of localhost:%s: %s",
				port, gai_strerror(errcode));
		return -1;
	}

	int out = socket(localhost->ai_family, localhost->ai_socktype, 0);
	if(out < 0) {
		perror("Could not create socket");
		freeaddrinfo(localhost);
	}

	errcode = bind(out, localhost->ai_addr, localhost->ai_addrlen);
	if(errcode) {
		perror("Unable to bind to localhost");
		close(out);
		freeaddrinfo(localhost);
		return -1;
	}

	freeaddrinfo(localhost);

	errcode = listen(out, 10);
	if(errcode) {
		perror("Unable to listen to localhost");
		close(out);
		return -1;
	}

	return out;
}

int accept_incoming(int sd)
{
	struct sockaddr_storage remote = {0};
	socklen_t sz = sizeof(remote);

	int conn = accept(sd, (struct sockaddr *)&remote, &sz);
	if(conn < 0) {
		perror("Could not accept connection");
	}

	return conn;
}

int connect_outgoing(const char *host, const char *port)
{
	struct addrinfo *downstream, hints = {0};

	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_CANONNAME;

	int errcode = getaddrinfo(host, port, &hints, &downstream);
	if(errcode) {
		fprintf(stderr, "Could not get info of %s:%s: %s",
				host, port, gai_strerror(errcode));
		return -1;
	}

	int out = socket(downstream->ai_family, downstream->ai_socktype, 0);
	if(out < 0) {
		perror("Could not create socket");
		freeaddrinfo(downstream);
	}

	errcode = connect(out, downstream->ai_addr, downstream->ai_addrlen);
	if(errcode) {
		perror("Unable to connect downstream");
		close(out);
		freeaddrinfo(downstream);
		return -1;
	}

	return out;
}
