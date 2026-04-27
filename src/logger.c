#include "../include/logger.h"
static FILE *logf=NULL;
void init_logger(const char *path){
    logf=fopen(path,"a");
    if(!logf){perror("log");exit(1);}
}
void log_event(const char *tag,const char *msg){
    char ts[64];
    get_timestamp(ts);
    fprintf(logf,"[%s] [%s] %s\n",ts,tag,msg);
    fflush(logf);
    printf("[%s] [%s] %s\n",ts,tag,msg);
}