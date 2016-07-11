
#ifndef NETWORKING_H
 #define NETWORKING_H


int listen_incoming(const char *port);
int accept_incoming(int sd);

int connect_outgoing(const char *host, const char *port);

#endif
