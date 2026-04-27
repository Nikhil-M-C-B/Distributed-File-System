#ifndef COMMON_H
#define COMMON_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <pthread.h>
#include <time.h>

#define MAX_BUF 4096
#define NM_PORT 9000
#define SS_PORT 9001

typedef struct {
    char name[128];
    char owner[64];
    char created[64];
    char last_access[64];
    int  words;
    int  chars;
} Meta;

void get_timestamp(char *buf);
void trim_newline(char *s);

// Lock management functions
int create_lock(const char *fname, int sindex);
int release_lock(const char *fname, int sindex);
int is_locked(const char *fname, int sindex);

// Access control functions
int check_access(const char *fname, const char *user, const char *owner);
void add_access(const char *fname, const char *user, const char *perm);
void remove_access(const char *fname, const char *user);
void get_access_list(const char *fname, char *output, int maxlen);

#endif
