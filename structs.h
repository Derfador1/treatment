
#ifndef STRUCTS_H
 #define STRUCTS_H

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

extern const char *salt;

struct node {
	uint32_t data;
	uint16_t left;
	uint16_t rite;
};

struct header {
	uint16_t type;
	uint16_t size;
	char custom[4];
};

struct hash {
	uint8_t code[64];
};

struct sludge {
	struct header *header;
	struct hash *hashes;
};

struct liquid {
	struct header *header;
	struct node *nodes;
};

bool serialize(const struct node *n, const size_t nmemb, char *buf, const size_t sz);

// N nodes will require N+1 0 values
// This will stop when a node is full; 0s at the end may be ommitted
struct liquid *deserialize(const char *buf, size_t *nmemb);


#endif
