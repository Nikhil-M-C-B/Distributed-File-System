#ifndef LOGGER_H
#define LOGGER_H
#include "common.h"
void init_logger(const char *path);
void log_event(const char *tag,const char *msg);
#endif
