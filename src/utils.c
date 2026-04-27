#include "../include/common.h"
void get_timestamp(char *buf){
    time_t t=time(NULL);
    strftime(buf,64,"%Y-%m-%d %H:%M:%S",localtime(&t));
}
void trim_newline(char *s){ s[strcspn(s,"\n")]=0; }

#include <sys/stat.h>
#include <fcntl.h>

int create_lock(const char *fname, int sindex){
    char dir[256]; sprintf(dir,"data/locks");
    mkdir(dir,0755);
    char lockpath[512]; sprintf(lockpath,"%s/%s.%d.lock",dir,fname,sindex);
    int fd = open(lockpath, O_CREAT | O_EXCL, 0644);
    if(fd==-1) return 0;
    close(fd);
    return 1;
}
int release_lock(const char *fname, int sindex){
    char lockpath[512]; sprintf(lockpath,"data/locks/%s.%d.lock",fname,sindex);
    if(remove(lockpath)==0) return 1;
    return 0;
}
int is_locked(const char *fname, int sindex){
    char lockpath[512]; sprintf(lockpath,"data/locks/%s.%d.lock",fname,sindex);
    return access(lockpath, F_OK)==0;
}

// Access control functions
// Returns: 0=no access, 1=read, 2=write (implies read)
int check_access(const char *fname, const char *user, const char *owner){
    // Owner always has full access
    if(strcmp(user, owner) == 0) return 2;
    
    FILE *fp = fopen("data/access.txt", "r");
    if(!fp) return 0;
    
    char file[128], usr[64], perm[8];
    while(fscanf(fp, "%127s %63s %7s", file, usr, perm) == 3){
        if(strcmp(file, fname) == 0 && strcmp(usr, user) == 0){
            fclose(fp);
            if(strcmp(perm, "RW") == 0) return 2;
            if(strcmp(perm, "R") == 0) return 1;
        }
    }
    fclose(fp);
    return 0;
}

void remove_access(const char *fname, const char *user){
    FILE *fp = fopen("data/access.txt", "r");
    if(!fp) return;
    
    char temp[256]; sprintf(temp, "data/access.txt.tmp");
    FILE *out = fopen(temp, "w");
    if(!out){ fclose(fp); return; }
    
    char file[128], usr[64], perm[8];
    while(fscanf(fp, "%127s %63s %7s", file, usr, perm) == 3){
        // Skip the entry we want to remove
        if(!(strcmp(file, fname) == 0 && strcmp(usr, user) == 0)){
            fprintf(out, "%s %s %s\n", file, usr, perm);
        }
    }
    fclose(fp);
    fclose(out);
    rename(temp, "data/access.txt");
}

void add_access(const char *fname, const char *user, const char *perm){
    // First, remove any existing access for this user on this file
    remove_access(fname, user);
    
    // Then add the new access
    FILE *fp = fopen("data/access.txt", "a");
    if(fp){
        fprintf(fp, "%s %s %s\n", fname, user, perm);
        fclose(fp);
    }
}

void get_access_list(const char *fname, char *output, int maxlen){
    FILE *meta = fopen("data/metadata.txt", "r");
    char owner[64] = "system";
    if(meta){
        char f[128], o[64];
        while(fscanf(meta, "%127s %63s", f, o) == 2){
            if(strcmp(f, fname) == 0){
                strncpy(owner, o, 63);
                owner[63] = '\0';
                break;
            }
        }
        fclose(meta);
    }
    
    // Start with owner
    snprintf(output, maxlen, "%s (RW)", owner);
    
    // Add other users with access
    FILE *fp = fopen("data/access.txt", "r");
    if(!fp) return;
    
    char file[128], usr[64], perm[8];
    while(fscanf(fp, "%127s %63s %7s", file, usr, perm) == 3){
        if(strcmp(file, fname) == 0){
            char temp[128];
            snprintf(temp, sizeof(temp), ", %s (%s)", usr, perm);
            strncat(output, temp, maxlen - strlen(output) - 1);
        }
    }
    fclose(fp);
}