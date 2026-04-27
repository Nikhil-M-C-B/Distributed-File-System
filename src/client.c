#include "../include/common.h"
#include "../include/logger.h"

void talk(const char *ip, int port, const char *msg, int is_write) {
    int s = socket(AF_INET, SOCK_STREAM, 0);
    if (s < 0) {
        perror("socket");
        return;
    }

    // Add socket timeout to prevent infinite blocking
    struct timeval timeout;
    timeout.tv_sec = 30;  // 30 second timeout
    timeout.tv_usec = 0;
    setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, ip, &addr.sin_addr);
    
    if (connect(s, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(s);
        return;
    }

    // Send the command
    if (send(s, msg, strlen(msg), 0) < 0) {
        perror("send");
        close(s);
        return;
    }

    char buf[MAX_BUF];
    ssize_t n;

    // Handle WRITE operation
    if (is_write) {
        // Receive initial response from server
        n = recv(s, buf, sizeof(buf) - 1, 0);
        if (n <= 0) {
            if (n == 0) {
                printf("ERROR: Server closed connection\n");
            } else {
                perror("recv");
            }
            close(s);
            return;
        }
        
        buf[n] = '\0';
        printf("%s", buf);
        fflush(stdout);
        
        // Check if server sent an error or [END] - if so, don't enter write loop
        if (strstr(buf, "ERROR") != NULL || strstr(buf, "[END]") != NULL) {
            close(s);
            return;
        }
        
        // Enter interactive word editing mode
        char line[512];
        while (1) {
            if (!fgets(line, sizeof(line), stdin)) {
                break;
            }
            
            // Send the line to server
            if (send(s, line, strlen(line), 0) < 0) {
                perror("send");
                break;
            }
            
            // Check if user wants to end
            if (strncmp(line, "ETIRW", 5) == 0) {
                break;
            }
            
            // Receive response for each word insertion
            n = recv(s, buf, sizeof(buf) - 1, 0);
            if (n <= 0) {
                if (n == 0) {
                    printf("\nERROR: Server closed connection\n");
                } else {
                    perror("recv");
                }
                close(s);
                return;
            }
            
            buf[n] = '\0';
            printf("%s", buf);
            fflush(stdout);
        }
        
        // After ETIRW, receive final confirmation message
        n = recv(s, buf, sizeof(buf) - 1, 0);
        if (n > 0) {
            buf[n] = '\0';
            printf("%s", buf);
            fflush(stdout);
        }
        
        close(s);
        return;
    }

    // Handle READ/STREAM and other operations
    int is_stream = (strncmp(msg, "STREAM", 6) == 0);
    int got_end_marker = 0;
    char accumulated[MAX_BUF * 4] = {0};  // Buffer to accumulate data
    int acc_len = 0;
    
    while ((n = recv(s, buf, sizeof(buf) - 1, 0)) > 0) {
        buf[n] = '\0';
        
        // Accumulate data to handle split packets
        if (acc_len + n < sizeof(accumulated) - 1) {
            strcat(accumulated, buf);
            acc_len += n;
        }
        
        // Check for [END] marker in accumulated data
        if (strstr(accumulated, "[END]") != NULL) {
            got_end_marker = 1;
        }

        if (is_stream) {
            // For streaming, process word by word
            char *end_marker = strstr(buf, "[END]");
            if (end_marker) {
                *end_marker = '\0';
                got_end_marker = 1;
            }

            char *saveptr = NULL;
            char *tok = strtok_r(buf, " \n", &saveptr);
            while (tok) {
                printf("%s ", tok);
                fflush(stdout);
                usleep(100000); // 0.1 sec per word
                tok = strtok_r(NULL, " \n", &saveptr);
            }

            if (got_end_marker) {
                printf("\n");
                fflush(stdout);
                break;
            }
        } else {
            // For normal operations, print as received
            printf("%s", buf);
            fflush(stdout);
            
            if (got_end_marker) {
                break;
            }
        }
    }
    
    // Check if connection was lost before receiving [END], but only print unavailable error if no ERROR message was received
    if (n < 0) {
        perror("recv");
    } else if (n == 0 && !got_end_marker) {
        // If accumulated contains 'ERROR', do not print unavailable error
        if (strstr(accumulated, "ERROR") == NULL) {
            printf("\n\nERROR: Connection lost - Storage Server unavailable\n");
            fflush(stdout);
        }
    }
    
    close(s);
}

int main(int argc, char *argv[]) {
    if (argc != 5) {
        fprintf(stderr, "Usage: %s <NM_IP> <NM_PORT> <SS_IP> <SS_PORT>\n", argv[0]);
        return 1;
    }

    const char *nm_ip = argv[1];
    int nm_port = atoi(argv[2]);
    const char *ss_ip = argv[3];
    int ss_port = atoi(argv[4]);

    init_logger("data/logs/client.log");
    log_event("CLIENT", "Client started");

    char user[64];
    printf("Enter username: ");
    fflush(stdout);
    if (scanf("%63s", user) != 1) {
        fprintf(stderr, "Error reading username\n");
        return 1;
    }
    getchar(); // consume newline

    // Check if user already exists to avoid duplicates
    FILE *ufp = fopen("data/users.list", "a+");
    if (ufp) {
        rewind(ufp);
        char line[128];
        int found = 0;
        while (fgets(line, sizeof(line), ufp)) {
            trim_newline(line);
            if (strcmp(line, user) == 0) {
                found = 1;
                break;
            }
        }
        if (!found) {
            fprintf(ufp, "%s\n", user);
            fflush(ufp);
        }
        fclose(ufp);
    }

    char cmd[256];
    while (1) {
        printf("\n%s> ", user);
        fflush(stdout);
        
        if (!fgets(cmd, sizeof(cmd), stdin)) {
            break;
        }
        
        trim_newline(cmd);
        
        if (strlen(cmd) == 0) {
            continue;
        }
        
        if (strcmp(cmd, "EXIT") == 0) {
            break;
        }

        char full[512];
        snprintf(full, sizeof(full), "%s %s", cmd, user);

        // Route commands to appropriate server
        if (strncmp(cmd, "CREATE", 6) == 0 || strncmp(cmd, "VIEW", 4) == 0) {
            talk(nm_ip, nm_port, full, 0);
        }
        else if (strncmp(cmd, "ADDACCESS", 9) == 0 || strncmp(cmd, "REMACCESS", 9) == 0 || strncmp(cmd, "EXEC", 4) == 0) {
            talk(nm_ip, nm_port, full, 0);
        }
        else if (strncmp(cmd, "READ", 4) == 0) {
            talk(ss_ip, ss_port, full, 0);
        }
        else if (strncmp(cmd, "WRITE", 5) == 0) {
            int idx = -1;
            char fname[128];
            if (sscanf(cmd, "WRITE %127s %d", fname, &idx) != 2) {
                printf("Usage: WRITE <filename> <sentence_index>\n");
                continue;
            }
            if (idx < 0) {
                printf("ERROR: Sentence index cannot be negative\n");
                continue;
            }
            talk(ss_ip, ss_port, full, 1);
        }
        else if (strncmp(cmd, "UNDO", 4) == 0) {
            talk(ss_ip, ss_port, full, 0);
        }
        else if (strncmp(cmd, "STREAM", 6) == 0) {
            talk(ss_ip, ss_port, full, 0);
        }
        else if (strncmp(cmd, "DELETE", 6) == 0) {
            talk(ss_ip, ss_port, full, 0);
        }
        else if (strncmp(cmd, "INFO", 4) == 0) {
            talk(ss_ip, ss_port, full, 0);
        }
        else if (strncmp(cmd, "LIST", 4) == 0) {
            talk(ss_ip, ss_port, full, 0);
        }
        else {
            printf("Unknown command\n");
        }
    }

    log_event("CLIENT", "Client terminated");
    return 0;
}