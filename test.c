#include <stdio.h>
#include <string.h>
#include <libscrypt.h>

/*

This is just a demo of the scrypt system that is used for drying sludge.
Note that the output is binary, so it should be piped to a hexdump-like
application.

*/

int main(void)
{
	unsigned char buf[64] = {'\0'};
	char *input  = "67";
	char *salt  = "I Hate Liam Echlin";
	int ret = libscrypt_scrypt(
		input, strlen(input),
		salt, strlen(salt),
		2048, 4, 4,
		buf, sizeof(buf));
	
	fwrite(buf, sizeof(buf), 1, stdout);
}

