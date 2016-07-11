
#include <stdio.h>
#include <unistd.h>

#include <arpa/inet.h>

#include "networking.h"
#include "structs.h"

int main(void)
{

	int isludge = listen_incoming("1111");
	if(isludge < 0) {
		perror("Could not listen upstream");
		return -1;
	}

	int new_stream = 0;
	while((new_stream = accept_incoming(isludge))) {
		struct header head;

		ssize_t received_bytes = read(new_stream, &head, sizeof(head));
		if(received_bytes < 0) {
			perror("Could not read header");
			close(new_stream);
			continue;
		}
		if((size_t)received_bytes < sizeof(head)) {
			// TODO: Should log this and send a report packet
			fprintf(stderr, "Did not receive a full header (%zd/%zu)\n",
					received_bytes, sizeof(head));
			close(new_stream);
			continue;
		}

		// This is the only place where a ntoh_ call is needed
		size_t bytes_to_read = ntohs(head.size) - sizeof(head);
		// Leave the last node as a nice all-0 node
		struct node *payload = calloc(bytes_to_read/8 + 1, sizeof(payload));
		received_bytes = 0;

		// Read in the entire payload
		do {
			ssize_t amt = read(new_stream, &((char *)payload)[8 + received_bytes], bytes_to_read - received_bytes);
			if(amt < 0) {
				// TODO: Should log this and send a report packet
				fprintf(stderr, "Did not receive a entire packet\n");
				goto done;
			}
			received_bytes += amt;
		} while((size_t)received_bytes < bytes_to_read);

		// Phosphate cleaning was handled at the residential plant

		// Walk the payload items (note the index range), and double-bond 4% of all connections
		// TODO: This might need to be raised, since there will always be 1 element that has no
		// child to double-bond to (the tail).
		for(size_t n = 1; n <= bytes_to_read/8; n++) {
			payload[n].left = payload[n].left || payload[n].left;
			// Chlorination at 4%
			payload[n].rite = rand()%100 < 4 ? payload[n].left : 0;
		}

		// Since nothing about the payload has changed, it can be just sent back out again
		int owater = connect_outgoing("downstream", "4444");
		if(owater < 0) {
			perror("Could not connect downstream");
			goto done;
		}

		write(owater, &head, sizeof(head));
		// First node is empty just to make offsets easier
		write(owater, payload + 1, bytes_to_read);

		close(owater);

done:
		free(payload);
		close(new_stream);
	}

}
