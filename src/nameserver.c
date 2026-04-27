#include "../include/common.h"
#include "../include/logger.h"
#include <dirent.h>
#include <sys/stat.h>

Meta files[256];
int fcount = 0;

void add_file_to_list(const char *fname, const char *owner,
                      const char *created, const char *last_access) 
{
    strcpy(files[fcount].name, fname);
    strcpy(files[fcount].owner, owner);
    strcpy(files[fcount].created, created);
    strcpy(files[fcount].last_access, last_access);

    // Count words & chars
    char path[256];
    sprintf(path, "data/files/%s", fname);

    FILE *fp = fopen(path, "r");
    int w = 0, c = 0;
    char word[128];

    while (fp && fscanf(fp, "%127s", word) == 1) {
        w++;
        c += strlen(word) + 1;
    }

    if (fp) fclose(fp);

    files[fcount].words = w;
    files[fcount].chars = c;
    fcount++;
}


void refresh_files_for_user(const char *current_user) {
    fcount = 0;

    // Load ownership metadata
    char owners[256][2][128];
    int owner_count = 0;

    FILE *meta = fopen("data/metadata.txt", "r");
    if (meta) {
        while (fscanf(meta, "%s %s", owners[owner_count][0], owners[owner_count][1]) == 2)
            owner_count++;
        fclose(meta);
    }

    DIR *d = opendir("data/files");
    if (!d) return;

    struct dirent *de;
    while ((de = readdir(d))) {
        if (de->d_name[0] == '.')
            continue;

        // Only include .txt files
        int len = strlen(de->d_name);
        if (len < 4 || strcmp(de->d_name + len - 4, ".txt") != 0)
            continue;

        char path[256];
        sprintf(path, "data/files/%s", de->d_name);

        struct stat st;
        stat(path, &st);

        char cbuf[64], abuf[64];
        strftime(cbuf, 64, "%Y-%m-%d %H:%M:%S", localtime(&st.st_ctime));
        strftime(abuf, 64, "%Y-%m-%d %H:%M:%S", localtime(&st.st_atime));

        // Find owner from metadata
        char owner[64] = "system";
        int found_owner = 0;
        for (int i = 0; i < owner_count; i++) {
            if (strcmp(owners[i][0], de->d_name) == 0) {
                strcpy(owner, owners[i][1]);
                found_owner = 1;
                break;
            }
        }

        // If file has no owner in metadata, claim it for current_user
        if (!found_owner && current_user && current_user[0] != '\0') {
            strncpy(owner, current_user, sizeof(owner) - 1);
            owner[sizeof(owner) - 1] = '\0';

            FILE *meta_append = fopen("data/metadata.txt", "a");
            if (meta_append) {
                fprintf(meta_append, "%s %s\n", de->d_name, owner);
                fclose(meta_append);
            }
        }

        add_file_to_list(de->d_name, owner, cbuf, abuf);
    }

    closedir(d);
}


int create_file(const char *fname, const char *user) {
    char fpath[256];
    sprintf(fpath, "data/files/%s", fname);

    FILE *fp = fopen(fpath, "r");
    if (fp) {
        fclose(fp);
        return 0; // failure: already exists
    }

    // Create empty file
    fp = fopen(fpath, "w");
    if (!fp) {
        perror("create");
        return 0;
    }
    fclose(fp);

    // Add metadata entry to disk
    FILE *meta = fopen("data/metadata.txt", "a");
    if (meta) {
        fprintf(meta, "%s %s\n", fname, user);
        fclose(meta);
    }

    // Add to in-memory file list for VIEW operations
    char ts[64];
    get_timestamp(ts);
    add_file_to_list(fname, user, ts, ts);

    char msg[256];
    sprintf(msg, "Created file '%s' by user '%s'", fname, user);
    log_event("NM", msg);

    return 1; // success
}



int main() {
    init_logger("data/logs/nameserver.log");
    log_event("NM", "NameServer started");

    // ========== DATA PERSISTENCE INITIALIZATION ==========
    // Create necessary directories if they don't exist
    mkdir("data", 0755);
    mkdir("data/files", 0755);
    mkdir("data/locks", 0755);
    mkdir("data/logs", 0755);
    
    // Initialize and verify metadata file
    FILE *meta_check = fopen("data/metadata.txt", "r");
    if (!meta_check) {
        meta_check = fopen("data/metadata.txt", "w");
        if (meta_check) {
            fclose(meta_check);
        }
    } else {
        int meta_count = 0;
        char f[128], o[64];
        while (fscanf(meta_check, "%127s %63s", f, o) == 2) {
            meta_count++;
        }
        fclose(meta_check);
    }
    
    // Initialize and verify access control file
    FILE *acl_check = fopen("data/access.txt", "r");
    if (!acl_check) {
        acl_check = fopen("data/access.txt", "w");
        if (acl_check) {
            fclose(acl_check);
        }
    } else {
        int acl_count = 0;
        char f[128], u[64], perm[8];
        while (fscanf(acl_check, "%127s %63s %7s", f, u, perm) == 3) {
            acl_count++;
        }
        fclose(acl_check);
    }
    
    // Verify data/files directory and count existing files
    DIR *files_dir = opendir("data/files");
    if (files_dir) {
        struct dirent *de;
        int file_count = 0;
        while ((de = readdir(files_dir))) {
            if (de->d_name[0] != '.') {
                file_count++;
            }
        }
        closedir(files_dir);
    }

    int sock = socket(AF_INET, SOCK_STREAM, 0);

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(NM_PORT);
    addr.sin_addr.s_addr = INADDR_ANY;

    bind(sock, (struct sockaddr *)&addr, sizeof(addr));
    listen(sock, 5);

    log_event("NM", "Listening on port 9000");
    log_event("NM", "Storage server registered successfully");

    while (1) {
        struct sockaddr_in caddr;
        socklen_t clen = sizeof(caddr);
        int csock = accept(sock, (struct sockaddr *)&caddr, &clen);

        char client_ip[32];
        strcpy(client_ip, inet_ntoa(caddr.sin_addr));
        int client_port = ntohs(caddr.sin_port);

        char buf[MAX_BUF] = {0};
        recv(csock, buf, sizeof(buf), 0);
        trim_newline(buf);

        char cmd[32] = {0}, arg1[128] = {0}, user[64] = {0};
        sscanf(buf, "%31s %127s %63s", cmd, arg1, user);

        char logbuf[512];
        snprintf(logbuf, sizeof(logbuf),
                 "[NS] Request received: raw='%s' From: user=%s IP=%s:%d",
                 buf, user[0] ? user : "-", client_ip, client_port);
        log_event("NS", logbuf);

        if (strcmp(cmd, "CREATE") == 0) {

            // check if filename exists in input
            char fname[128], username[64];
            int n = sscanf(buf, "%*s %127s %63s", fname, username);

            if (n < 1) {
                send(csock, "ERROR: Missing filename for CREATE\n[END]\n", 41, 0);
                continue;
            }

            // empty string check
            if (strlen(fname) == 0 || fname[0] == '\n' || fname[0] == ' ') {
                send(csock, "ERROR: Invalid or empty filename\n[END]\n", 40, 0);
                continue;
            }

            // second argument (username) must be present
            if (n < 2) {
                send(csock, "ERROR: Internal: Missing username\n[END]\n", 40, 0);
                continue;
            }

            int ok = create_file(fname, username);

            if (ok) {
                send(csock, "File Created Successfully!\n[END]\n", 33, 0);
                snprintf(logbuf, sizeof(logbuf),
                         "[NS] CREATE SUCCESS file='%s' user=%s IP=%s:%d",
                         fname, username, client_ip, client_port);
                log_event("NS", logbuf);
            } else {
                send(csock, "ERROR: File already exists\n[END]\n", 33, 0);
                snprintf(logbuf, sizeof(logbuf),
                         "[NS] CREATE FAILED (ERR_FILE_EXISTS) file='%s' user=%s IP=%s:%d",
                         fname, username, client_ip, client_port);
                log_event("NS", logbuf);
            }
        }


        else if (strncmp(cmd, "VIEW", 4) == 0) {

            // For VIEW, the client always sends: "VIEW <username>"
            // so if user field is empty, treat arg1 as username.
            if (user[0] == '\0' && arg1[0] != '\0') {
                strncpy(user, arg1, sizeof(user) - 1);
                user[sizeof(user) - 1] = '\0';
            }

            // Refresh files and ensure any existing unowned files
            // get assigned to this user for future sessions
            refresh_files_for_user(user);

            int detailed = (strstr(buf, "-l") != NULL);
            int all = (strstr(buf, "-a") != NULL);

            // detect -al / -la
            if (strstr(buf, "-al") || strstr(buf, "-la")) {
                detailed = 1;
                all = 1;
            }

            char out[MAX_BUF * 4];
            out[0] = '\0';

            /* ---------------- SIMPLE VIEW ---------------- */
            if (!detailed) {
                for (int i = 0; i < fcount; i++) {

                    if (!all) {
                        // OWNER ALWAYS SEES FILE
                        if (strcmp(files[i].owner, user) != 0) {
                            // Not owner → check if this user has access
                            if (!check_access(files[i].name, user, files[i].owner))
                                continue;
                        }
                    }

                    char line[256];
                    sprintf(line, "--> %s\n", files[i].name);
                    strcat(out, line);
                }
            }

            /* ---------------- DETAILED VIEW ---------------- */
            else {
                strcat(out, "---------------------------------------------------------\n");
                strcat(out, "|  Filename  | Words | Chars | Last Access Time | Owner |\n");
                strcat(out, "|------------|-------|-------|------------------|-------|\n");

                for (int i = 0; i < fcount; i++) {

                    if (!all) {
                        // Show file if user is owner OR has access
                        int is_owner = (strcmp(files[i].owner, user) == 0);
                        int has_access = check_access(files[i].name, user, files[i].owner);
                        
                        if (!is_owner && !has_access) {
                            continue;  // Skip file - no ownership and no access
                        }
                    }

                    char line[256];
                    sprintf(line, "| %-10s | %5d | %5d | %-16s | %-8s |\n",
                            files[i].name,
                            files[i].words,
                            files[i].chars,
                            files[i].last_access,
                            files[i].owner);
                    strcat(out, line);
                }

                strcat(out, "---------------------------------------------------------\n");
            }

            if (strlen(out) == 0)
                strcat(out, "(no files found)\n");

            strcat(out, "[END]\n");
            send(csock, out, strlen(out), 0);

            snprintf(logbuf, sizeof(logbuf),
                     "[NS] VIEW%s%s SUCCESS user=%s IP=%s:%d",
                     detailed ? " -l" : "",
                     all ? "a" : "",
                     user, client_ip, client_port);
            log_event("NS", logbuf);
        }


        else if (strcmp(cmd, "ADDACCESS") == 0) {
                char flag[8] = "", fname[128] = "", target_user[64] = "", owner[64] = "";
                int n = sscanf(buf, "%*s %7s %127s %63s %63s", flag, fname, target_user, user);

                if (n < 4 || strlen(flag) == 0 || strlen(fname) == 0 || strlen(target_user) == 0 || strlen(user) == 0) {
                    send(csock, "ERROR: Invalid ADDACCESS format. Usage: ADDACCESS <flag> <filename> <target_user> <your_username>\n[END]\n", 98, 0);
                    snprintf(logbuf, sizeof(logbuf),
                             "[NS] ADDACCESS DENIED Reason=ERR_INVALID_FORMAT IP=%s:%d", client_ip, client_port);
                    log_event("NS", logbuf);
                    // Do not return, continue to check file existence for more specific error
                }

                FILE *meta = fopen("data/metadata.txt", "r");
                if (meta) {
                    char f[128], o[64];
                    while (fscanf(meta, "%127s %63s", f, o) == 2) {
                        if (strcmp(f, fname) == 0) {
                            strcpy(owner, o);
                            break;
                        }
                    }
                    fclose(meta);
                }

                if (strlen(owner) == 0) {
                    send(csock, "ERROR: File not found for access control\n[END]\n", 48, 0);
                    snprintf(logbuf, sizeof(logbuf),
                             "[NS] ADDACCESS DENIED file='%s' Reason=ERR_FILE_NOT_FOUND IP=%s:%d",
                             fname, client_ip, client_port);
                    log_event("NS", logbuf);
                }

                if (strcmp(owner, user) != 0) {
                    send(csock, "ERROR: Only owner can grant access\n[END]\n", 41, 0);
                    snprintf(logbuf, sizeof(logbuf),
                             "[NS] ADDACCESS DENIED file='%s' target=%s by=%s Reason=ERR_NOT_OWNER IP=%s:%d",
                             fname, target_user, user, client_ip, client_port);
                    log_event("NS", logbuf);
                } else {
                    const char *perm = (strcmp(flag, "-W") == 0) ? "RW" : "R";
                    add_access(fname, target_user, perm);
                    send(csock, "Access granted successfully!\n[END]\n", 35, 0);
                    snprintf(logbuf, sizeof(logbuf),
                             "[NS] ADDACCESS perm=%s file='%s' target=%s by=%s IP=%s:%d",
                             perm, fname, target_user, user, client_ip, client_port);
                    log_event("NS", logbuf);
                }
        }

        else if (strcmp(cmd, "REMACCESS") == 0) {
                char fname[128] = "", target_user[64] = "", owner[64] = "";
                int n = sscanf(buf, "%*s %127s %63s %63s", fname, target_user, user);

                if (n < 3 || strlen(fname) == 0 || strlen(target_user) == 0 || strlen(user) == 0) {
                    send(csock, "ERROR: Invalid REMACCESS format. Usage: REMACCESS <filename> <target_user> <your_username>\n[END]\n", 97, 0);
                    snprintf(logbuf, sizeof(logbuf),
                             "[NS] REMACCESS DENIED Reason=ERR_INVALID_FORMAT IP=%s:%d", client_ip, client_port);
                    log_event("NS", logbuf);
                }

                FILE *meta = fopen("data/metadata.txt", "r");
                if (meta) {
                    char f[128], o[64];
                    while (fscanf(meta, "%127s %63s", f, o) == 2) {
                        if (strcmp(f, fname) == 0) {
                            strcpy(owner, o);
                            break;
                        }
                    }
                    fclose(meta);
                }

                if (strlen(owner) == 0) {
                    send(csock, "ERROR: File not found for access control\n[END]\n", 48, 0);
                    snprintf(logbuf, sizeof(logbuf),
                             "[NS] REMACCESS DENIED file='%s' Reason=ERR_FILE_NOT_FOUND IP=%s:%d",
                             fname, client_ip, client_port);
                    log_event("NS", logbuf);
                }

                if (strcmp(owner, user) != 0) {
                    send(csock, "ERROR: Only owner can remove access\n[END]\n", 42, 0);
                    snprintf(logbuf, sizeof(logbuf),
                             "[NS] REMACCESS DENIED file='%s' target=%s by=%s Reason=ERR_NOT_OWNER IP=%s:%d",
                             fname, target_user, user, client_ip, client_port);
                    log_event("NS", logbuf);
                } else {
                    remove_access(fname, target_user);
                    send(csock, "Access removed successfully!\n[END]\n", 35, 0);
                    snprintf(logbuf, sizeof(logbuf),
                             "[NS] REMACCESS file='%s' target=%s by=%s IP=%s:%d",
                             fname, target_user, user, client_ip, client_port);
                    log_event("NS", logbuf);
                }
        }

        else if (strcmp(cmd, "EXEC") == 0) {
            char fname[128];
            sscanf(buf, "%*s %127s %63s", fname, user);

            FILE *meta = fopen("data/metadata.txt", "r");
            char owner[64] = "system";

            if (meta) {
                char f[128], o[64];
                while (fscanf(meta, "%127s %63s", f, o) == 2) {
                    if (strcmp(f, fname) == 0) {
                        strcpy(owner, o);
                        break;
                    }
                }
                fclose(meta);
            }

            int access = check_access(fname, user, owner);
            if (access == 0) {
                send(csock, "ERROR: No read access to file\n[END]\n", 36, 0);
                snprintf(logbuf, sizeof(logbuf),
                         "[NS] EXEC DENIED file='%s' user=%s Reason=ERR_ACCESS_DENIED IP=%s:%d",
                         fname, user, client_ip, client_port);
                log_event("NS", logbuf);
            } else {
                char path[256];
                sprintf(path, "data/files/%s", fname);

                FILE *fp = fopen(path, "r");
                if (!fp) {
                    send(csock, "ERROR: File not found\n[END]\n", 28, 0);
                } else {
                    char script[MAX_BUF];
                    int pos = 0;
                    char line[512];

                    while (fgets(line, sizeof(line), fp) && pos < MAX_BUF - 1) {
                        int len = strlen(line);
                        if (pos + len < MAX_BUF) {
                            strcpy(script + pos, line);
                            pos += len;
                        }
                    }

                    fclose(fp);
                    script[pos] = '\0';

                    FILE *pipe = popen(script, "r");
                    if (pipe) {
                        char output[MAX_BUF];
                        while (fgets(output, sizeof(output), pipe)) {
                            send(csock, output, strlen(output), 0);
                        }
                        pclose(pipe);
                    }

                    send(csock, "[END]\n", 6, 0);

                    snprintf(logbuf, sizeof(logbuf),
                             "[NS] EXEC SUCCESS file='%s' user=%s IP=%s:%d",
                             fname, user, client_ip, client_port);
                    log_event("NS", logbuf);
                }
            }
        }

        else {
            send(csock, "Unknown command\n[END]\n", 22, 0);
        }

        close(csock);
    }
}
