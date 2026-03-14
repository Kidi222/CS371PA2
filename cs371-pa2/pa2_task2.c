#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <fcntl.h>
#include <pthread.h>

#define MAX_EVENTS 64
#define PAYLOAD_SIZE 16
#define DEFAULT_CLIENT_THREADS 4
#define DEFAULT_WINDOW_SIZE 64
#define DEFAULT_TIMEOUT_MS 100
#define MAX_WINDOW_SIZE 65536
#define MAX_SERVER_CLIENTS 65536

#define MSG_TYPE_DATA 1U
#define MSG_TYPE_ACK  2U

typedef struct {
    uint32_t type;
    uint32_t seq;
    uint32_t ack;
    char payload[PAYLOAD_SIZE];
} gbn_msg_t;

typedef struct {
    uint32_t seq;
    int valid;
    long long first_sent_us;
    long long last_sent_us;
} window_slot_t;

typedef struct {
    int epoll_fd;
    int socket_fd;
    long long total_rtt_us;
    long long elapsed_us;
    long tx_cnt;
    long rx_cnt;
    long retransmit_cnt;
    double request_rate;
    struct sockaddr_in server_addr;
} client_thread_data_t;

typedef struct {
    int used;
    struct sockaddr_in addr;
    uint32_t expected_seq;
} server_client_state_t;

static char *server_ip = (char *)"127.0.0.1";
static int server_port = 12345;
static int num_client_threads = DEFAULT_CLIENT_THREADS;
static int num_requests = 100000;
static int window_size = DEFAULT_WINDOW_SIZE;
static int timeout_ms = DEFAULT_TIMEOUT_MS;

static long long now_us(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (long long)tv.tv_sec * 1000000LL + (long long)tv.tv_usec;
}

static void die_perror(const char *msg) {
    perror(msg);
    exit(EXIT_FAILURE);
}

static void set_nonblocking_or_die(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        die_perror("fcntl(F_GETFL)");
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        die_perror("fcntl(F_SETFL)");
    }
}

static int create_client_socket_or_die(const struct sockaddr_in *server_addr) {
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) {
        die_perror("socket");
    }

    if (connect(fd, (const struct sockaddr *)server_addr, sizeof(*server_addr)) < 0) {
        close(fd);
        die_perror("connect");
    }

    set_nonblocking_or_die(fd);
    return fd;
}

static void fill_payload(char payload[PAYLOAD_SIZE], uint32_t seq) {
    snprintf(payload, PAYLOAD_SIZE, "SEQ%011u", seq % 1000000000U);
}

static int send_data_packet(int socket_fd, uint32_t seq) {
    gbn_msg_t msg;
    ssize_t sent_bytes;

    memset(&msg, 0, sizeof(msg));
    msg.type = MSG_TYPE_DATA;
    msg.seq = seq;
    fill_payload(msg.payload, seq);

    sent_bytes = send(socket_fd, &msg, sizeof(msg), 0);
    if (sent_bytes < 0) {
        return -1;
    }
    if ((size_t)sent_bytes != sizeof(msg)) {
        errno = EIO;
        return -1;
    }
    return 0;
}

static void *client_thread_func(void *arg) {
    client_thread_data_t *data = (client_thread_data_t *)arg;
    struct epoll_event event;
    struct epoll_event events[MAX_EVENTS];
    window_slot_t *window = NULL;
    long long thread_start_us = now_us();
    long long timeout_us = (long long)timeout_ms * 1000LL;
    uint32_t base = 0;
    uint32_t next_seq = 0;

    window = (window_slot_t *)calloc((size_t)window_size, sizeof(window_slot_t));
    if (window == NULL) {
        fprintf(stderr, "calloc failed in client thread\n");
        return NULL;
    }

    data->epoll_fd = -1;
    data->socket_fd = create_client_socket_or_die(&data->server_addr);
    data->total_rtt_us = 0;
    data->elapsed_us = 0;
    data->tx_cnt = 0;
    data->rx_cnt = 0;
    data->retransmit_cnt = 0;
    data->request_rate = 0.0;

    data->epoll_fd = epoll_create1(0);
    if (data->epoll_fd < 0) {
        close(data->socket_fd);
        free(window);
        die_perror("epoll_create1");
    }

    memset(&event, 0, sizeof(event));
    event.events = EPOLLIN;
    event.data.fd = data->socket_fd;
    if (epoll_ctl(data->epoll_fd, EPOLL_CTL_ADD, data->socket_fd, &event) < 0) {
        close(data->epoll_fd);
        close(data->socket_fd);
        free(window);
        die_perror("epoll_ctl add client socket");
    }

    while (base < (uint32_t)num_requests) {
        while (next_seq < (uint32_t)num_requests && next_seq < base + (uint32_t)window_size) {
            int slot = (int)(next_seq % (uint32_t)window_size);
            long long send_time_us;

            if (send_data_packet(data->socket_fd, next_seq) < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    break;
                }
                perror("send data");
                continue;
            }

            send_time_us = now_us();
            window[slot].seq = next_seq;
            window[slot].valid = 1;
            window[slot].first_sent_us = send_time_us;
            window[slot].last_sent_us = send_time_us;
            data->tx_cnt++;
            next_seq++;
        }

        int wait_ms = timeout_ms;
        if (base < next_seq) {
            int base_slot = (int)(base % (uint32_t)window_size);
            long long age_us = now_us() - window[base_slot].last_sent_us;
            long long remain_us = timeout_us - age_us;
            wait_ms = (remain_us <= 0) ? 0 : (int)((remain_us + 999LL) / 1000LL);
        }

        int n_events = epoll_wait(data->epoll_fd, events, MAX_EVENTS, wait_ms);
        if (n_events < 0) {
            if (errno == EINTR) {
                continue;
            }
            die_perror("epoll_wait client");
        }

        for (int i = 0; i < n_events; i++) {
            if (events[i].data.fd != data->socket_fd) {
                continue;
            }

            while (1) {
                gbn_msg_t ack_msg;
                ssize_t recv_bytes = recv(data->socket_fd, &ack_msg, sizeof(ack_msg), 0);
                if (recv_bytes < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        break;
                    }
                    perror("recv ack");
                    break;
                }
                if ((size_t)recv_bytes != sizeof(ack_msg)) {
                    continue;
                }
                if (ack_msg.type != MSG_TYPE_ACK) {
                    continue;
                }
                if (ack_msg.ack < base || ack_msg.ack >= next_seq) {
                    continue;
                }

                long long ack_time_us = now_us();
                for (uint32_t seq = base; seq <= ack_msg.ack; seq++) {
                    int slot = (int)(seq % (uint32_t)window_size);
                    if (window[slot].valid && window[slot].seq == seq) {
                        data->total_rtt_us += ack_time_us - window[slot].first_sent_us;
                        data->rx_cnt++;
                        window[slot].valid = 0;
                    }
                }
                base = ack_msg.ack + 1;
            }
        }

        if (base < next_seq) {
            int base_slot = (int)(base % (uint32_t)window_size);
            long long age_us = now_us() - window[base_slot].last_sent_us;
            if (window[base_slot].valid && age_us >= timeout_us) {
                long long resend_time_us = now_us();
                for (uint32_t seq = base; seq < next_seq; seq++) {
                    int slot = (int)(seq % (uint32_t)window_size);
                    if (!window[slot].valid || window[slot].seq != seq) {
                        continue;
                    }
                    if (send_data_packet(data->socket_fd, seq) == 0) {
                        window[slot].last_sent_us = resend_time_us;
                        data->retransmit_cnt++;
                    } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
                        perror("retransmit");
                    }
                }
            }
        }
    }

    data->elapsed_us = now_us() - thread_start_us;
    if (data->elapsed_us > 0) {
        data->request_rate = ((double)data->rx_cnt * 1000000.0) / (double)data->elapsed_us;
    }

    free(window);
    close(data->socket_fd);
    close(data->epoll_fd);
    return NULL;
}

static int same_client(const struct sockaddr_in *a, const struct sockaddr_in *b) {
    return a->sin_family == b->sin_family &&
           a->sin_port == b->sin_port &&
           a->sin_addr.s_addr == b->sin_addr.s_addr;
}

static server_client_state_t *get_client_state(server_client_state_t *clients,
                                               size_t client_count,
                                               const struct sockaddr_in *addr) {
    server_client_state_t *free_slot = NULL;

    for (size_t i = 0; i < client_count; i++) {
        if (clients[i].used) {
            if (same_client(&clients[i].addr, addr)) {
                return &clients[i];
            }
        } else if (free_slot == NULL) {
            free_slot = &clients[i];
        }
    }

    if (free_slot != NULL) {
        free_slot->used = 1;
        free_slot->addr = *addr;
        free_slot->expected_seq = 0;
        return free_slot;
    }

    return NULL;
}

static void run_client(void) {
    pthread_t *threads = NULL;
    client_thread_data_t *thread_data = NULL;
    struct sockaddr_in server_addr;
    long long total_rtt_us = 0;
    long long total_elapsed_us = 0;
    long total_tx = 0;
    long total_rx = 0;
    long total_retransmit = 0;

    threads = (pthread_t *)calloc((size_t)num_client_threads, sizeof(pthread_t));
    thread_data = (client_thread_data_t *)calloc((size_t)num_client_threads, sizeof(client_thread_data_t));
    if (threads == NULL || thread_data == NULL) {
        fprintf(stderr, "calloc failed for client threads\n");
        free(threads);
        free(thread_data);
        exit(EXIT_FAILURE);
    }

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons((uint16_t)server_port);
    if (inet_pton(AF_INET, server_ip, &server_addr.sin_addr) != 1) {
        fprintf(stderr, "invalid server IP: %s\n", server_ip);
        free(threads);
        free(thread_data);
        exit(EXIT_FAILURE);
    }

    for (int i = 0; i < num_client_threads; i++) {
        thread_data[i].server_addr = server_addr;
        if (pthread_create(&threads[i], NULL, client_thread_func, &thread_data[i]) != 0) {
            fprintf(stderr, "pthread_create failed\n");
            free(threads);
            free(thread_data);
            exit(EXIT_FAILURE);
        }
    }

    for (int i = 0; i < num_client_threads; i++) {
        pthread_join(threads[i], NULL);
        total_rtt_us += thread_data[i].total_rtt_us;
        total_elapsed_us += thread_data[i].elapsed_us;
        total_tx += thread_data[i].tx_cnt;
        total_rx += thread_data[i].rx_cnt;
        total_retransmit += thread_data[i].retransmit_cnt;
    }

    printf("Task 2 UDP Go-Back-N client\n");
    printf("threads=%d requests_per_thread=%d window=%d timeout_ms=%d\n",
           num_client_threads, num_requests, window_size, timeout_ms);
    printf("tx_cnt=%ld\n", total_tx);
    printf("rx_cnt=%ld\n", total_rx);
    printf("retransmit_cnt=%ld\n", total_retransmit);
    printf("delivery_ok=%s\n", (total_tx == total_rx) ? "YES" : "NO");
    if (total_rx > 0) {
        printf("average_completion_us=%lld\n", total_rtt_us / total_rx);
    } else {
        printf("average_completion_us=0\n");
    }
    if (total_elapsed_us > 0) {
        printf("aggregate_goodput=%.2f msg/s\n",
               ((double)total_rx * 1000000.0) / (double)total_elapsed_us);
    } else {
        printf("aggregate_goodput=0.00 msg/s\n");
    }

    free(threads);
    free(thread_data);
}

static void run_server(void) {
    int server_fd;
    int epoll_fd;
    struct sockaddr_in server_addr;
    struct epoll_event event;
    struct epoll_event events[MAX_EVENTS];
    server_client_state_t *clients = NULL;
    int optval = 1;

    clients = (server_client_state_t *)calloc(MAX_SERVER_CLIENTS, sizeof(server_client_state_t));
    if (clients == NULL) {
        fprintf(stderr, "calloc failed for server client table\n");
        exit(EXIT_FAILURE);
    }

    server_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (server_fd < 0) {
        free(clients);
        die_perror("socket");
    }

    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) < 0) {
        close(server_fd);
        free(clients);
        die_perror("setsockopt SO_REUSEADDR");
    }

    set_nonblocking_or_die(server_fd);

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons((uint16_t)server_port);
    if (strcmp(server_ip, "0.0.0.0") == 0) {
        server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    } else if (inet_pton(AF_INET, server_ip, &server_addr.sin_addr) != 1) {
        close(server_fd);
        free(clients);
        fprintf(stderr, "invalid bind IP: %s\n", server_ip);
        exit(EXIT_FAILURE);
    }

    if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        close(server_fd);
        free(clients);
        die_perror("bind");
    }

    epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) {
        close(server_fd);
        free(clients);
        die_perror("epoll_create1");
    }

    memset(&event, 0, sizeof(event));
    event.events = EPOLLIN;
    event.data.fd = server_fd;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &event) < 0) {
        close(epoll_fd);
        close(server_fd);
        free(clients);
        die_perror("epoll_ctl add server fd");
    }

    printf("Task 2 UDP Go-Back-N server listening on %s:%d\n", server_ip, server_port);

    while (1) {
        int n_events = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        if (n_events < 0) {
            if (errno == EINTR) {
                continue;
            }
            die_perror("epoll_wait server");
        }

        for (int i = 0; i < n_events; i++) {
            if (events[i].data.fd != server_fd) {
                continue;
            }

            while (1) {
                gbn_msg_t msg;
                gbn_msg_t ack_msg;
                struct sockaddr_in client_addr;
                socklen_t client_len = sizeof(client_addr);
                ssize_t recv_bytes;
                server_client_state_t *client_state;
                uint32_t cumulative_ack;

                recv_bytes = recvfrom(server_fd, &msg, sizeof(msg), 0,
                                      (struct sockaddr *)&client_addr, &client_len);
                if (recv_bytes < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        break;
                    }
                    perror("recvfrom");
                    break;
                }
                if ((size_t)recv_bytes != sizeof(msg)) {
                    continue;
                }
                if (msg.type != MSG_TYPE_DATA) {
                    continue;
                }

                client_state = get_client_state(clients, MAX_SERVER_CLIENTS, &client_addr);
                if (client_state == NULL) {
                    fprintf(stderr, "server client table full\n");
                    continue;
                }

                if (msg.seq == client_state->expected_seq) {
                    client_state->expected_seq++;
                }

                cumulative_ack = (client_state->expected_seq == 0) ? 0 : (client_state->expected_seq - 1U);

                memset(&ack_msg, 0, sizeof(ack_msg));
                ack_msg.type = MSG_TYPE_ACK;
                ack_msg.ack = cumulative_ack;

                if (sendto(server_fd, &ack_msg, sizeof(ack_msg), 0,
                           (struct sockaddr *)&client_addr, client_len) < 0) {
                    if (errno != EAGAIN && errno != EWOULDBLOCK) {
                        perror("sendto ack");
                    }
                }
            }
        }
    }
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr,
                "Usage:\n"
                "  %s server [server_ip server_port]\n"
                "  %s client [server_ip server_port num_client_threads num_requests window_size timeout_ms]\n",
                argv[0], argv[0]);
        return EXIT_FAILURE;
    }

    if (argc > 2) {
        server_ip = argv[2];
    }
    if (argc > 3) {
        server_port = atoi(argv[3]);
    }
    if (argc > 4) {
        num_client_threads = atoi(argv[4]);
    }
    if (argc > 5) {
        num_requests = atoi(argv[5]);
    }
    if (argc > 6) {
        window_size = atoi(argv[6]);
    }
    if (argc > 7) {
        timeout_ms = atoi(argv[7]);
    }

    if (server_port <= 0 || server_port > 65535) {
        fprintf(stderr, "server_port must be between 1 and 65535\n");
        return EXIT_FAILURE;
    }
    if (num_client_threads <= 0) {
        fprintf(stderr, "num_client_threads must be > 0\n");
        return EXIT_FAILURE;
    }
    if (num_requests <= 0) {
        fprintf(stderr, "num_requests must be > 0\n");
        return EXIT_FAILURE;
    }
    if (window_size <= 0 || window_size > MAX_WINDOW_SIZE) {
        fprintf(stderr, "window_size must be between 1 and %d\n", MAX_WINDOW_SIZE);
        return EXIT_FAILURE;
    }
    if (timeout_ms <= 0) {
        fprintf(stderr, "timeout_ms must be > 0\n");
        return EXIT_FAILURE;
    }

    if (strcmp(argv[1], "server") == 0) {
        run_server();
    } else if (strcmp(argv[1], "client") == 0) {
        run_client();
    } else {
        fprintf(stderr, "first argument must be server or client\n");
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
