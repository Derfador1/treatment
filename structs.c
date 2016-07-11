
#include "structs.h"

#include <inttypes.h>
#include <stdio.h>

const char *salt = "I Hate Liam Echlin";

/*
static char *buf_ptr;
static size_t buf_sz;
void _serialize(uint32_t val) {
	int amt = snprintf(buf_ptr, buf_sz, "%" PRIu32 " ", val);
	buf_ptr += amt;
	buf_sz -= amt;
}

bool serialize(const struct node *t, char *buf, const size_t sz)
{
	if(!t || !buf) {
		return false;
	}

	buf_ptr = buf;
	buf_sz = sz;
	node_preorder(t, _serialize);

	return true;
}

void _deserialize(struct node **t)
{
	uint32_t value = 0;
	char *word = strtok(NULL, " ");
	if(!word) {
		return;
	}
	sscanf(word, "%" PRIu32, &value);
	if(value) {
		*t = node_create(value);
		_deserialize(&((*t)->left));
		_deserialize(&((*t)->rite));
	}
}

struct node *deserialize(const char *buf)
{
	if(!buf) {
		return false;
	}
	char *word = strtok((char *)buf, " ");

	uint32_t value = 0;
	sscanf(word, "%" PRIu32, &value);
	if(value) {
		struct node *node = node_create(value);
		_deserialize(&(node->left));
		_deserialize(&(node->rite));
		return node;
	}

	return NULL;
}

void node_preorder(const struct node *t,
		void (*func)(uint32_t))
{
	if(!t) {
		return;
	}

	func(t->data);
	node_preorder(t->left, func);
	node_preorder(t->rite, func);
}
*/

