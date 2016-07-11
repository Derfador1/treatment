
#include <libscrypt.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <arpa/inet.h>

#include "networking.h"
#include "structs.h"

int main(void)
{
	int isludge = listen_incoming("4444");
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
		size_t items = (ntohs(head.size) - sizeof(head))/8;
		// Leave the last node as a nice all-0 node
		struct node *payload = calloc(items+1, sizeof(payload));
		received_bytes = 0;

		// Read in the entire payload
		do {
			ssize_t amt = read(new_stream, &((char *)payload)[8 + received_bytes], items*8 - received_bytes);
			if(amt < 0) {
				// TODO: Should log this and send a report packet
				fprintf(stderr, "Did not receive a entire packet\n");
				goto done;
			}
			received_bytes += amt;
		} while((size_t)received_bytes < items*8);


		struct hash *hashes = calloc(items, sizeof(*hashes));

		for(size_t n = 0; n < items; ++n) {
			char buf[16];
			snprintf(buf, sizeof(buf), "%u", ntohl(payload[n+1].data));
printf("'%s'\n", buf);
			libscrypt_scrypt((const uint8_t *)buf, strlen(buf),
					(const uint8_t *)salt, strlen(salt),
					2048, 4, 4,
					hashes + n, sizeof(*hashes));
		}
		// TODO: This may be too big if there were lots of items
		// to bake
		head.size = htons(sizeof(head) + items*sizeof(*hashes));

		int osludge = connect_outgoing("downstream", "4444");
		if(osludge < 0) {
			perror("Could not connect downstream");
			goto done;
		}

		write(osludge, &head, sizeof(head));
		write(osludge, hashes, items * sizeof(*hashes));

		close(osludge);

done:
		free(payload);
		close(new_stream);
	}

}
