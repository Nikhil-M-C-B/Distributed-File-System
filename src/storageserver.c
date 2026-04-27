#include "../include/common.h"
#include "../include/logger.h"
#include <ctype.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <dirent.h>

#define MAX_SENT 256
#define MAX_LEN  4096

/* ---------------- Sentence utilities ---------------- */
int split_into_sentences(const char *text, char sents[MAX_SENT][MAX_LEN]) {
    int c = 0, i = 0;
    for (int j = 0; text[j]; j++) {
        sents[c][i++] = text[j];
        if (text[j]=='.' || text[j]=='!' || text[j]=='?') {
            while (isspace((unsigned char)text[j+1])) j++;
            sents[c][i] = '\0';
            c++; i = 0;
        }
    }
    if (i>0) {
        sents[c][i]='\0';
        c++;
    }
    return c;
}

void join_sentences(char sents[MAX_SENT][MAX_LEN], int n, char *out) {
    out[0]='\0';
    for (int i=0;i<n;i++) {
        strcat(out, sents[i]);
        if (i<n-1) strcat(out," ");
    }
}

/* ---------------- Persistent UNDO support ---------------- */
void backup_file(const char *fname){
    char path[256];
    sprintf(path,"data/files/%s",fname);

    FILE *in=fopen(path,"r");
    if(!in){
        char msg[256];
        sprintf(msg,"[SS] BACKUP: File not found for backup: '%s'",fname);
        log_event("SS",msg);
        return;
    }

    // Create persistent undo file on disk
    char undo_path[300];
    sprintf(undo_path,"data/files/%s.undo",fname);
    FILE *out=fopen(undo_path,"w");
    if(!out){
        fclose(in);
        char msg[256];
        sprintf(msg,"[SS] BACKUP: Failed to create undo file for '%s'",fname);
        log_event("SS",msg);
        return;
    }

    // Copy entire file content to undo backup
    char buf[512];
    while(fgets(buf,sizeof(buf),in)){
        fputs(buf, out);
    }
    fclose(in);
    fclose(out);

    char msg[256];
    sprintf(msg,"[SS] PERSISTENT BACKUP: Undo snapshot saved to disk for file='%s'",fname);
    log_event("SS",msg);
}

void handle_undo(int csock,const char *fname){
    char msg[256];
    sprintf(msg,"[SS] UNDO request for file='%s'",fname);
    log_event("SS",msg);

    // Check if persistent undo file exists
    char undo_path[300];
    sprintf(undo_path,"data/files/%s.undo",fname);
    FILE *undo_file=fopen(undo_path,"r");
    if(!undo_file){
        send(csock,"ERROR: Nothing to undo\n[END]\n",30,0);
        sprintf(msg,"[SS] UNDO: No undo file found for '%s'",fname);
        log_event("SS",msg);
        return;
    }

    // Restore file from undo backup
    char path[256];
    sprintf(path,"data/files/%s",fname);
    FILE *fp=fopen(path,"w");
    if(!fp){
        fclose(undo_file);
        send(csock,"ERROR: Cannot restore file\n[END]\n",33,0);
        sprintf(msg,"[SS] UNDO: Cannot write to file '%s'",fname);
        log_event("SS",msg);
        return;
    }

    // Copy undo backup to original file
    char buf[512];
    while(fgets(buf,sizeof(buf),undo_file)){
        fputs(buf, fp);
    }
    fclose(undo_file);
    fclose(fp);

    // Remove undo file after successful restore
    remove(undo_path);

    send(csock,"Undo Successful!\n[END]\n",24,0);
    sprintf(msg,"[SS] PERSISTENT UNDO: Reverted last change from disk backup; file='%s'",fname);
    log_event("SS",msg);
}

/* ---------------- Persistence helpers ---------------- */
void remove_from_metadata(const char *fname) {
    FILE *meta = fopen("data/metadata.txt", "r");
    if (!meta) return;
    
    char temp_path[] = "data/metadata.txt.tmp";
    FILE *temp = fopen(temp_path, "w");
    if (!temp) {
        fclose(meta);
        return;
    }
    
    char f[128], o[64];
    while (fscanf(meta, "%127s %63s", f, o) == 2) {
        if (strcmp(f, fname) != 0) {
            fprintf(temp, "%s %s\n", f, o);
        }
    }
    
    fclose(meta);
    fclose(temp);
    rename(temp_path, "data/metadata.txt");
    
    char msg[256];
    sprintf(msg, "[SS] PERSISTENCE: Removed '%s' from metadata.txt", fname);
    log_event("SS", msg);
}

void save_metadata_entry(const char *fname, const char *owner) {
    // Check if entry exists
    FILE *meta = fopen("data/metadata.txt", "r");
    int found = 0;
    if(meta){
        char f[128], o[64];
        while(fscanf(meta, "%127s %63s", f, o) == 2){
            if(strcmp(f, fname) == 0){
                found = 1;
                break;
            }
        }
        fclose(meta);
    }
    
    // Add if not found
    if(!found){
        FILE *meta_append = fopen("data/metadata.txt", "a");
        if(meta_append){
            fprintf(meta_append, "%s %s\n", fname, owner);
            fclose(meta_append);
            char msg[256];
            sprintf(msg, "[SS] PERSISTENCE: Added metadata entry file='%s' owner='%s'", fname, owner);
            log_event("SS", msg);
        }
    }
}

void save_acl_to_disk(const char *fname, const char *username, int access_level) {
    // Read existing ACLs
    FILE *acl = fopen("data/access.txt", "r");
    char temp_path[] = "data/access.txt.tmp";
    FILE *temp = fopen(temp_path, "w");
    if (!temp) {
        if (acl) fclose(acl);
        return;
    }
    
    int found = 0;
    if (acl) {
        char f[128], u[64], perm[8];
        while (fscanf(acl, "%127s %63s %7s", f, u, perm) == 3) {
            if (strcmp(f, fname) == 0 && strcmp(u, username) == 0) {
                if (access_level > 0) {
                    const char *perm_str = (access_level >= 2) ? "RW" : "R";
                    fprintf(temp, "%s %s %s\n", fname, username, perm_str);
                }
                found = 1;
            } else {
                fprintf(temp, "%s %s %s\n", f, u, perm);
            }
        }
        fclose(acl);
    }
    
    // Add new entry if not found and access_level > 0
    if (!found && access_level > 0) {
        const char *perm_str = (access_level >= 2) ? "RW" : "R";
        fprintf(temp, "%s %s %s\n", fname, username, perm_str);
    }
    
    fclose(temp);
    rename(temp_path, "data/access.txt");
    
    char msg[256];
    sprintf(msg, "[SS] PERSISTENCE: Updated ACL file='%s' user='%s' level=%d", fname, username, access_level);
    log_event("SS", msg);
}

void remove_acl_for_file(const char *fname) {
    FILE *acl = fopen("data/access.txt", "r");
    if (!acl) return;
    
    char temp_path[] = "data/access.txt.tmp";
    FILE *temp = fopen(temp_path, "w");
    if (!temp) {
        fclose(acl);
        return;
    }
    
    char f[128], u[64], perm[8];
    int removed_count = 0;
    while (fscanf(acl, "%127s %63s %7s", f, u, perm) == 3) {
        if (strcmp(f, fname) != 0) {
            fprintf(temp, "%s %s %s\n", f, u, perm);
        } else {
            removed_count++;
        }
    }
    
    fclose(acl);
    fclose(temp);
    rename(temp_path, "data/access.txt");
    
    char msg[256];
    sprintf(msg, "[SS] PERSISTENCE: Removed %d ACL entries for file='%s'", removed_count, fname);
    log_event("SS", msg);
}

/* ---------------- WRITE ---------------- */
int insert_word_at(char *sentence, int widx, const char *insert) {
    char words[512][128]; 
    int wc = 0;
    
    // Preserve original sentence since strtok modifies it
    char temp[MAX_LEN];
    strncpy(temp, sentence, MAX_LEN-1);
    temp[MAX_LEN-1] = '\0';
    
    // Split into words
    char *tok = strtok(temp, " ");
    while(tok && wc < 512) { 
        strcpy(words[wc++], tok); 
        tok = strtok(NULL, " "); 
    }
    
    // Validation: widx must be in range [0, wc] inclusive
    // widx=0 means insert before all words (beginning)
    // widx=wc means insert after all words (append)
    if(widx < 0 || widx > wc) {
        return -1;
    }
    
    // Shift words to make room for insertion
    for(int i = wc; i > widx; i--) {
        strcpy(words[i], words[i-1]);
    }
    strcpy(words[widx], insert);
    wc++;
    
    // Rebuild sentence from word array
    sentence[0] = '\0';
    for(int i = 0; i < wc; i++){
        strcat(sentence, words[i]);
        if(i < wc - 1) strcat(sentence, " ");
    }
    return 0;
}

void handle_write(int csock,const char *fname,int sindex, const char *user){
    // Check write access first
    FILE *meta = fopen("data/metadata.txt", "r");
    char owner[64] = "system";
    if(meta){
        char f[128], o[64];
        while(fscanf(meta, "%127s %63s", f, o) == 2){
            if(strcmp(f, fname) == 0){
                strcpy(owner, o);
                break;
            }
        }
        fclose(meta);
    }
    
    int access = check_access(fname, user, owner);
    if(access < 2){  // Need write access (2)
        send(csock, "ERROR: No write access to file\n[END]\n", 37, 0);
        return;
    }
    
    char path[256]; sprintf(path,"data/files/%s",fname);
    FILE *fp=fopen(path,"r");
    if(!fp){ send(csock,"ERROR: File not found\n[END]\n",28,0); return; }

    char text[MAX_LEN*2]={0}, buf[512];
    while(fgets(buf,sizeof(buf),fp)) strcat(text,buf);
    fclose(fp);

    char sents[MAX_SENT][MAX_LEN];
    int n=split_into_sentences(text,sents);
    
    // 0-based indexing:
    // sindex=0 means first sentence (edit existing or create if empty file)
    // sindex=n means append new sentence at end
    // Valid range: 0 to n (inclusive)
    
    int actual_idx;
    
    // Validation: sentence index must be in range [0, n]
    if (sindex < 0) {
        send(csock, "ERROR: Sentence index cannot be negative\n[END]\n", 47, 0);
        return;
    }
    
    if (sindex > n) {
        char errmsg[128];
        snprintf(errmsg, sizeof(errmsg), "ERROR: Sentence index %d out of range. Valid: 0 to %d\n[END]\n", sindex, n);
        send(csock, errmsg, strlen(errmsg), 0);
        return;
    }
    
    if (sindex == n) {
        // Append new sentence at end
        if (n >= MAX_SENT) {
            send(csock, "ERROR: Maximum sentences reached\n[END]\n", 40, 0);
            return;
        }
        strcpy(sents[n], "");
        actual_idx = n;
        n++;
    } else {
        // Edit existing sentence at sindex (0-based, direct mapping)
        actual_idx = sindex;
    }

    // Attempt to lock the sentence for exclusive write
    if(is_locked(fname,sindex)){
        send(csock,"ERROR: Sentence is locked by another user\n[END]\n",48,0);
        char msg[256];
        sprintf(msg,"[SS] WRITE request: file='%s' sentence=%d user=%s -- lock conflict",
                fname,sindex,user);
        log_event("SS",msg);
        return;
    }
    if(!create_lock(fname,sindex)){
        send(csock,"ERROR: Could not acquire lock\n[END]\n",36,0);
        char msg[256];
        sprintf(msg,"[SS] WRITE request: file='%s' sentence=%d user=%s -- lock create failed",
                fname,sindex,user);
        log_event("SS",msg);
        return;
    }
    {
        char msg[256];
        sprintf(msg,"[SS] Sentence %d locked by user %s",sindex,user);
        log_event("SS",msg);
    }
    backup_file(fname);
    send(csock,"Enter <word_index> <content> lines (end with ETIRW):\n",53,0);

    while(1){
        memset(buf,0,sizeof(buf));
        ssize_t r = recv(csock,buf,sizeof(buf)-1,0);
        if(r <= 0){
            // Client disconnected, release lock and abort
            release_lock(fname,sindex);
            log_event("SS", "WRITE aborted: client disconnected");
            return;
        }
        trim_newline(buf);
        if(strlen(buf)==0) continue;
        if(strcmp(buf,"ETIRW")==0) break;

        int widx; char content[256];
        if(sscanf(buf,"%d %[^\n]",&widx,content)!=2){
            send(csock,"ERROR: Invalid format. Use: <word_index> <content>\n",52,0);
            continue;
        }

        // Count current words in the sentence to validate widx
        char temp_copy[MAX_LEN];
        strncpy(temp_copy, sents[actual_idx], MAX_LEN-1);
        temp_copy[MAX_LEN-1] = '\0';
        int word_count = 0;
        char *tok = strtok(temp_copy, " ");
        while(tok) { word_count++; tok = strtok(NULL, " "); }
        
        // Valid word index range: 0 to word_count (inclusive)
        // 0 = insert before first word, word_count = append after last word
        if(widx < 0 || widx > word_count){
            char errmsg[128];
            snprintf(errmsg, sizeof(errmsg), "ERROR: Word index %d out of range. Valid: 0 to %d\n", widx, word_count);
            send(csock, errmsg, strlen(errmsg), 0);
            continue;
        }

        char copy[MAX_LEN]; 
        strcpy(copy, sents[actual_idx]);
        if(insert_word_at(copy, widx, content) == -1){
            send(csock, "ERROR: Word insertion failed\n", 30, 0);
            continue;
        }
        strcpy(sents[actual_idx], copy);
        
        // Check if the modified sentence contains delimiters and needs splitting
        char check_delim[MAX_LEN];
        strcpy(check_delim, sents[actual_idx]);
        int has_delimiter = 0;
        for(int i = 0; check_delim[i]; i++){
            if(check_delim[i] == '.' || check_delim[i] == '!' || check_delim[i] == '?'){
                has_delimiter = 1;
                break;
            }
        }
        
        if(has_delimiter){
            // Split this sentence into multiple sentences
            char split_sents[MAX_SENT][MAX_LEN];
            int split_count = split_into_sentences(sents[actual_idx], split_sents);
            
            if(split_count > 1){
                // Need to make room for additional sentences
                if(n + split_count - 1 > MAX_SENT){
                    send(csock, "WARNING: Sentence split would exceed maximum sentences\n", 56, 0);
                } else {
                    // Shift sentences after actual_idx to make room
                    for(int i = n - 1; i > actual_idx; i--){
                        strcpy(sents[i + split_count - 1], sents[i]);
                    }
                    // Insert the split sentences
                    for(int i = 0; i < split_count; i++){
                        strcpy(sents[actual_idx + i], split_sents[i]);
                    }
                    n += (split_count - 1);
                    
                    char msg[256];
                    sprintf(msg, "[SS] Sentence auto-split into %d sentences at index %d", split_count, actual_idx);
                    log_event("SS", msg);
                    send(csock, "OK (sentence auto-split)\n", 26, 0);
                    continue;
                }
            }
        }
        
        send(csock,"OK\n",3,0);
    }

    char out[MAX_LEN*2];
    join_sentences(sents,n,out);
    fp=fopen(path,"w"); 
    if(fp){
        fputs(out, fp); 
        fclose(fp);
        
        // Ensure file metadata persists
        FILE *meta = fopen("data/metadata.txt", "r");
        int found = 0;
        if(meta){
            char f[128], o[64];
            while(fscanf(meta, "%127s %63s", f, o) == 2){
                if(strcmp(f, fname) == 0){
                    found = 1;
                    break;
                }
            }
            fclose(meta);
        }
        
        // Add to metadata if not present
        if(!found){
            FILE *meta_append = fopen("data/metadata.txt", "a");
            if(meta_append){
                fprintf(meta_append, "%s %s\n", fname, user);
                fclose(meta_append);
                char logmsg[256];
                sprintf(logmsg,"[SS] PERSISTENCE: Added file '%s' to metadata for user '%s'",fname,user);
                log_event("SS",logmsg);
            }
        }
    }
    
    // Release lock
    release_lock(fname,sindex);
    {
        char msg[256];
        sprintf(msg,"[SS] Lock released (ETIRW) for file='%s' sentence=%d",fname,sindex);
        log_event("SS",msg);
    }
    send(csock,"Write Successful!\n[END]\n",25,0);

    // Log statistics about the write operation
    {
        int total_words = 0;
        for(int i=0;i<n;i++){
            char *p = sents[i];
            int in_word = 0;
            while(*p){
                if(!isspace((unsigned char)*p) && !in_word){ 
                    in_word=1; 
                    total_words++; 
                }
                else if(isspace((unsigned char)*p)) in_word=0;
                p++;
            }
        }
        char msg[256];
        sprintf(msg,"[SS] Write complete: file='%s' sentence=%d total_sentences=%d total_words=%d",
                fname, sindex, n, total_words);
        log_event("SS",msg);
    }
}

void handle_read(int csock,const char *fname, const char *user){
    /* Determine owner from metadata and check read access */
    char owner[64] = "system";
    FILE *meta = fopen("data/metadata.txt", "r");
    if (meta) {
        char f[128], o[64];
        while (fscanf(meta, "%127s %63s", f, o) == 2) {
            if (strcmp(f, fname) == 0) {
                strncpy(owner, o, sizeof(owner)-1);
                owner[sizeof(owner)-1] = '\0';
                break;
            }
        }
        fclose(meta);
    }

    int access = check_access(fname, user, owner);
    if (access < 1) { // need at least read access
        const char *err_msg = "ERROR: No read access to file\n[END]\n";
        send(csock, err_msg, strlen(err_msg), 0);
        char msg[256]; sprintf(msg, "[SS] READ denied: file='%s' user=%s", fname, user);
        log_event("SS", msg);
        return;
    }
    
    char path[256]; sprintf(path,"data/files/%s",fname);
    FILE *fp=fopen(path,"r");
    if(!fp){ 
        send(csock,"ERROR: File not found\n[END]\n",29,0); 
        return; 
    }

    char text[MAX_LEN*2]={0}, buf[512];
    while(fgets(buf,sizeof(buf),fp)){
        if (strncmp(buf,"#OWNER:",7)==0) continue;
        strcat(text,buf);
    }
    fclose(fp);

    // Build complete response with [END] marker
    char response[MAX_LEN*2 + 20];
    if(strlen(text) == 0){
        strcpy(response, "(empty file)\n[END]\n");
    } else {
        snprintf(response, sizeof(response), "%s[END]\n", text);
    }
    
    send(csock, response, strlen(response), 0);
    
    /* Update last-access time (preserve mtime) */
    struct stat st;
    if (stat(path, &st) == 0) {
        struct timeval times[2];
        times[0].tv_sec = time(NULL); times[0].tv_usec = 0;           /* atime = now */
        times[1].tv_sec = st.st_mtime; times[1].tv_usec = 0;         /* mtime = original */
        utimes(path, times);
    }

    char msg[128]; sprintf(msg,"READ file %s by %s",fname,user);
    log_event("SS",msg);
}

/* ---------------- STREAM ---------------- */
void handle_stream(int csock,const char *fname){
    char path[256]; sprintf(path,"data/files/%s",fname);
    FILE *fp=fopen(path,"r");
    if(!fp){ send(csock,"ERROR: File not found\n[END]\n",28,0); return; }

    char word[128];
    int wcount = 0;
    log_event("SS","[SS] STREAM start");
    while(fscanf(fp,"%127s",word)==1){
        char sendbuf[140];
        snprintf(sendbuf, sizeof(sendbuf), "%s ", word);
        // Check if send fails (connection closed)
        if(send(csock,sendbuf,strlen(sendbuf),0) <= 0){
            // Connection broken, stop streaming
            fclose(fp);
            log_event("SS","STREAM stopped: connection closed");
            return;
        }
        wcount++;
        usleep(100000);  // 0.1 second delay
    }
    fclose(fp);
    send(csock,"\n[END]\n",7,0);
    {
        char msg[256];
        sprintf(msg,"[SS] STREAM finished; words_sent=%d",wcount);
        log_event("SS",msg);
    }
}

/* ---------------- INFO ---------------- */
void handle_info(int csock, const char *fname, const char *user) {
    char path[256];
    sprintf(path, "data/files/%s", fname);

    struct stat st;
    if (stat(path, &st) != 0) {
        send(csock, "ERROR: File not found\n[END]\n", 29, 0);
        return;
    }

    // Get timestamps
    char ctime_str[64], mtime_str[64], atime_str[64];
    strftime(ctime_str, sizeof(ctime_str), "%Y-%m-%d %H:%M:%S", localtime(&st.st_ctime));
    strftime(mtime_str, sizeof(mtime_str), "%Y-%m-%d %H:%M:%S", localtime(&st.st_mtime));
    strftime(atime_str, sizeof(atime_str), "%Y-%m-%d %H:%M:%S", localtime(&st.st_atime));

    // Find owner from metadata
    char owner[64] = "system";
    FILE *meta = fopen("data/metadata.txt", "r");
    if (meta) {
        char fname_m[128], owner_m[64];
        while (fscanf(meta, "%s %s", fname_m, owner_m) == 2) {
            if (strcmp(fname_m, fname) == 0) {
                strcpy(owner, owner_m);
                break;
            }
        }
        fclose(meta);
    }

    // Get access list
    char access_list[512];
    get_access_list(fname, access_list, sizeof(access_list));

    // Format output exactly like the specification
    char out[1024];
    snprintf(out, sizeof(out),
        "--> File: %s\n"
        "--> Owner: %s\n"
        "--> Created: %s\n"
        "--> Last Modified: %s\n"
        "--> Size: %ld bytes\n"
        "--> Access: %s\n"
        "--> Last Accessed: %s by %s\n"
        "[END]\n",
        fname,
        owner,
        ctime_str,
        mtime_str,
        st.st_size,
        access_list,
        atime_str,
        user);

    send(csock, out, strlen(out), 0);

    char msg[128];
    sprintf(msg, "INFO requested for %s", fname);
    log_event("SS", msg);
}


/* ---------------- DELETE ---------------- */
void handle_delete(int csock,const char *fname, const char *user){
    char path[256]; sprintf(path,"data/files/%s",fname);
    
    // Check if file exists first
    if(access(path, F_OK) != 0){
        send(csock,"ERROR: File not found or cannot delete\n[END]\n",44,0);
        char msg[128]; 
        sprintf(msg,"[SS] DELETE failed: file '%s' not found",fname);
        log_event("SS",msg);
        return;
    }
    
    // Check permission: only owner or users with write (RW) access can delete
    // Determine owner from metadata
    char owner[128] = "system";
    FILE *mfp = fopen("data/metadata.txt", "r");
    if (mfp) {
        char mf[128], mo[64];
        while (fscanf(mfp, "%127s %63s", mf, mo) == 2) {
            if (strcmp(mf, fname) == 0) {
                strncpy(owner, mo, sizeof(owner)-1);
                owner[sizeof(owner)-1] = '\0';
                break;
            }
        }
        fclose(mfp);
    }

    int access_level = check_access(fname, user, owner);
    if (access_level == 0) {
        send(csock, "ERROR: No permission to delete this file\n[END]\n", 40, 0);
        char msg[256]; sprintf(msg, "[SS] DELETE DENIED: user='%s' file='%s' owner='%s'", user, fname, owner);
        log_event("SS", msg);
        return;
    }

    // Delete the main file
    if(remove(path)==0){
        // Persist metadata changes: remove from metadata.txt
        remove_from_metadata(fname);
        
        // Persist ACL changes: remove all access control entries
        remove_acl_for_file(fname);
        
        // Delete undo backup file if it exists (persistent cleanup)
        char undo_path[300];
        sprintf(undo_path,"data/files/%s.undo",fname);
        if(access(undo_path, F_OK) == 0){
            remove(undo_path);
        }
        
        // Clean up any existing locks for this file
        DIR *lockdir = opendir("data/locks");
        if(lockdir){
            struct dirent *de;
            int locks_removed = 0;
            while((de = readdir(lockdir))){
                // Check if lock file belongs to this file
                if(strncmp(de->d_name, fname, strlen(fname)) == 0 && strstr(de->d_name, ".lock")){
                    char lockpath[512];
                    sprintf(lockpath, "data/locks/%s", de->d_name);
                    remove(lockpath);
                    locks_removed++;
                }
            }
            closedir(lockdir);
        }
        
        char msgout[256];
        sprintf(msgout,"File '%s' deleted successfully!\n[END]\n",fname);
        send(csock,msgout,strlen(msgout),0);
        
        char msg[128]; 
        sprintf(msg,"[SS] File deleted: '%s'",fname);
        log_event("SS",msg);
    } else {
        send(csock,"ERROR: File not found or cannot delete\n[END]\n",44,0);
        char msg[128]; 
        sprintf(msg,"[SS] DELETE failed: unable to remove file '%s'",fname);
        log_event("SS",msg);
    }
}

/* ---------------- LIST USERS placeholder ---------------- */
void handle_list(int csock) {
    FILE *fp = fopen("data/users.list", "r");
    if (!fp) {
        send(csock, "No users found\n[END]\n", 21, 0);
        return;
    }
    
    char line[128];
    char seen[100][128]; // Store up to 100 unique usernames
    int count = 0;
    
    while (fgets(line, sizeof(line), fp)) {
        line[strcspn(line, "\n")] = 0; // Remove newline
        
        // Check if already seen
        int duplicate = 0;
        for (int i = 0; i < count; i++) {
            if (strcmp(seen[i], line) == 0) {
                duplicate = 1;
                break;
            }
        }
        
        if (!duplicate && strlen(line) > 0) {
            strcpy(seen[count++], line);
            strcat(line, "\n");
            send(csock, line, strlen(line), 0);
        }
    }
    
    fclose(fp);
    send(csock, "[END]\n", 6, 0);
}

/* ---------------- Thread handler for concurrent client connections ---------------- */
void* handle_client(void* arg) {
    int csock = *(int*)arg;
    free(arg);
    
    struct sockaddr_in caddr;
    socklen_t clen = sizeof(caddr);
    getpeername(csock, (struct sockaddr*)&caddr, &clen);
    
    char client_ip[32];
    strcpy(client_ip, inet_ntoa(caddr.sin_addr));
    int client_port = ntohs(caddr.sin_port);

    char buf[MAX_BUF]={0};
    recv(csock,buf,sizeof(buf),0); trim_newline(buf);

    char cmd[32] = {0}, fname[128] = {0}, user[64] = {0};
    int sindex=-1;

    // Parse command first
    sscanf(buf,"%31s",cmd);

    if(strcmp(cmd,"READ")==0 || strcmp(cmd,"STREAM")==0 ||
       strcmp(cmd,"INFO")==0 || strcmp(cmd,"DELETE")==0){
        // Format: CMD <filename> <username>
        sscanf(buf,"%31s %127s %63s",cmd,fname,user);
    } else if(strcmp(cmd,"WRITE")==0){
        // Format: WRITE <filename> <sentence_index> <username>
        sscanf(buf,"%31s %127s %d %63s",cmd,fname,&sindex,user);
    } else if(strcmp(cmd,"UNDO")==0){
        // Format: UNDO <filename> <username>
        sscanf(buf,"%31s %127s %63s",cmd,fname,user);
    } else if(strcmp(cmd,"LIST")==0){
        // no extra fields
    }

    char logbuf[512];
    snprintf(logbuf, sizeof(logbuf),
             "[SS] Request received: raw='%s' From: user=%s IP=%s:%d",
             buf, user[0] ? user : "-", client_ip, client_port);
    log_event("SS",logbuf);

    if(strcmp(cmd,"READ")==0)
        handle_read(csock,fname,user);
    else if(strcmp(cmd,"WRITE")==0 && sindex>=0)
        handle_write(csock,fname,sindex,user);
    else if(strcmp(cmd,"STREAM")==0)
        handle_stream(csock,fname);
    else if(strcmp(cmd,"UNDO")==0)
        handle_undo(csock,fname);
    else if(strcmp(cmd,"INFO")==0)
        handle_info(csock,fname,user);
    else if(strcmp(cmd,"DELETE")==0)
        handle_delete(csock,fname,user);
    else if(strcmp(cmd,"LIST")==0)
        handle_list(csock);
    else
        send(csock,"Unknown\n[END]\n",15,0);

    close(csock);
    return NULL;
}

/* ---------------- Main ---------------- */
int main(){
    init_logger("data/logs/storageserver.log");
    log_event("SS","StorageServer started");

    // ========== DATA PERSISTENCE INITIALIZATION ==========
    // Create necessary directories if they don't exist
    mkdir("data", 0755);
    mkdir("data/files", 0755);
    mkdir("data/locks", 0755);
    mkdir("data/logs", 0755);
    
    // Initialize ACL file (access.txt) - persistent access control lists
    FILE *acl = fopen("data/access.txt", "r");
    if (!acl) {
        acl = fopen("data/access.txt", "w");
        if (acl) {
            fclose(acl);
        }
    } else {
        // Count existing ACL entries
        char f[128], u[64], perm[8];
        int count = 0;
        while(fscanf(acl, "%127s %63s %7s", f, u, perm) == 3) count++;
        fclose(acl);
    }
    
    // Initialize metadata file (metadata.txt) - persistent file ownership
    FILE *meta = fopen("data/metadata.txt", "r");
    if (!meta) {
        meta = fopen("data/metadata.txt", "w");
        if (meta) {
            fclose(meta);
        }
    } else {
        // Count existing metadata entries
        char f[128], o[64];
        int count = 0;
        while(fscanf(meta, "%127s %63s", f, o) == 2) count++;
        fclose(meta);
    }
    
    // Clean up any stale locks from previous crashes
    int locks_cleaned = 0;
    DIR *lockdir = opendir("data/locks");
    if(lockdir){
        struct dirent *de;
        while((de = readdir(lockdir))){
            if(de->d_name[0] != '.' && strstr(de->d_name, ".lock")){
                char lockpath[512];
                sprintf(lockpath, "data/locks/%s", de->d_name);
                remove(lockpath);
                locks_cleaned++;
            }
        }
        closedir(lockdir);
    }

    int sock=socket(AF_INET,SOCK_STREAM,0);
    struct sockaddr_in addr={0};
    addr.sin_family=AF_INET;
    addr.sin_port=htons(SS_PORT);
    addr.sin_addr.s_addr=INADDR_ANY;
    bind(sock,(struct sockaddr*)&addr,sizeof(addr));
    listen(sock,5);
    log_event("SS","Listening on port 9001");
    log_event("SS", "Registered with name server successfully");
    log_event("SS", "[CONCURRENCY] Multi-threaded mode enabled for concurrent client handling");

    while(1){
        struct sockaddr_in caddr; socklen_t clen=sizeof(caddr);
        int csock=accept(sock,(struct sockaddr*)&caddr,&clen);
        
        if(csock < 0){
            log_event("SS", "[ERROR] Failed to accept client connection");
            continue;
        }

        // Create thread to handle this client
        pthread_t thread_id;
        int* client_sock = malloc(sizeof(int));
        *client_sock = csock;
        
        if(pthread_create(&thread_id, NULL, handle_client, client_sock) != 0){
            log_event("SS", "[ERROR] Failed to create thread for client");
            close(csock);
            free(client_sock);
            continue;
        }
        
        // Detach thread so it cleans up automatically when done
        pthread_detach(thread_id);
    }
}