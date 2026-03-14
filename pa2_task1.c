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

typedef struct {
    uint32_t request_id;
    char payload[PAYLOAD_SIZE];
} udp_echo_msg_t;

typedef struct {
    uint32_t request_id;
    int active;
    long long sent_at_us;
} inflight_pkt_t;

typedef struct {
    int epoll_fd;
    int socket_fd;
    long long total_rtt_us;
    long long elapsed_us;
    long tx_cnt;
    long rx_cnt;
    long lost_cnt;
    long timeout_cnt;
    double request_rate;
    struct sockaddr_in server_addr;
} client_thread_data_t;

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

static void fill_payload(char payload[PAYLOAD_SIZE], uint32_t request_id) {
    snprintf(payload, PAYLOAD_SIZE, "REQ%011u", request_id % 1000000000U);
}

static void *client_thread_func(void *arg) {
    client_thread_data_t *data = (client_thread_data_t *)arg;
    struct epoll_event event;
    struct epoll_event events[MAX_EVENTS];
    inflight_pkt_t *inflight = NULL;
    long long thread_start_us = now_us();
    int inflight_count = 0;
    uint32_t next_request_id = 0;
    long long timeout_us = (long long)timeout_ms * 1000LL;

    inflight = (inflight_pkt_t *)calloc((size_t)window_size, sizeof(inflight_pkt_t));
    if (inflight == NULL) {
        fprintf(stderr, "calloc failed in client thread\n");
        return NULL;
    }

    data->epoll_fd = -1;
    data->socket_fd = create_client_socket_or_die(&data->server_addr);
    data->total_rtt_us = 0;
    data->elapsed_us = 0;
    data->tx_cnt = 0;
    data->rx_cnt = 0;
    data->lost_cnt = 0;
    data->timeout_cnt = 0;
    data->request_rate = 0.0;

    data->epoll_fd = epoll_create1(0);
    if (data->epoll_fd < 0) {
        close(data->socket_fd);
        free(inflight);
        die_perror("epoll_create1");
    }

    memset(&event, 0, sizeof(event));
    event.events = EPOLLIN;
    event.data.fd = data->socket_fd;
    if (epoll_ctl(data->epoll_fd, EPOLL_CTL_ADD, data->socket_fd, &event) < 0) {
        close(data->epoll_fd);
        close(data->socket_fd);
        free(inflight);
        die_perror("epoll_ctl add client socket");
    }

    while ((data->rx_cnt + data->lost_cnt) < num_requests) {
        while (next_request_id < (uint32_t)num_requests && inflight_count < window_size) {
            udp_echo_msg_t msg;
            ssize_t sent_bytes;
            int slot = -1;

            for (int i = 0; i < window_size; i++) {
                if (!inflight[i].active) {
                    slot = i;
                    break;
                }
            }
            if (slot < 0) {
                break;
            }

            memset(&msg, 0, sizeof(msg));
            msg.request_id = next_request_id;
            fill_payload(msg.payload, next_request_id);

            sent_bytes = send(data->socket_fd, &msg, sizeof(msg), 0);
            if (sent_bytes < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    break;
                }
                perror("send");
                continue;
            }
            if ((size_t)sent_bytes != sizeof(msg)) {
                fprintf(stderr, "partial UDP send detected\n");
                continue;
            }

            inflight[slot].request_id = next_request_id;
            inflight[slot].active = 1;
            inflight[slot].sent_at_us = now_us();
            inflight_count++;
            data->tx_cnt++;
            next_request_id++;
        }

        long long current_us = now_us();
        int wait_ms = timeout_ms;

        for (int i = 0; i < window_size; i++) {
            if (inflight[i].active) {
                long long age_us = current_us - inflight[i].sent_at_us;
                long long remain_us = timeout_us - age_us;
                int candidate_ms = (remain_us <= 0) ? 0 : (int)((remain_us + 999LL) / 1000LL);
                if (candidate_ms < wait_ms) {
                    wait_ms = candidate_ms;
                }
            }
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
                udp_echo_msg_t reply;
                ssize_t recv_bytes = recv(data->socket_fd, &reply, sizeof(reply), 0);
                if (recv_bytes < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        break;
                    }
                    perror("recv");
                    break;
                }
                if ((size_t)recv_bytes != sizeof(reply)) {
                    continue;
                }

                for (int j = 0; j < window_size; j++) {
                    if (inflight[j].active && inflight[j].request_id == reply.request_id) {
                        long long rtt_us = now_us() - inflight[j].sent_at_us;
                        inflight[j].active = 0;
                        inflight_count--;
                        data->rx_cnt++;
                        data->total_rtt_us += rtt_us;
                        break;
                    }
                }
            }
        }

        current_us = now_us();
        for (int i = 0; i < window_size; i++) {
            if (inflight[i].active && (current_us - inflight[i].sent_at_us) >= timeout_us) {
                inflight[i].active = 0;
                inflight_count--;
                data->lost_cnt++;
                data->timeout_cnt++;
            }
        }
    }

    data->elapsed_us = now_us() - thread_start_us;
    if (data->elapsed_us > 0) {
        data->request_rate = ((double)data->rx_cnt * 1000000.0) / (double)data->elapsed_us;
    }

    free(inflight);
    close(data->socket_fd);
    close(data->epoll_fd);
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
    long total_lost = 0;
    long total_timeouts = 0;

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
        total_lost += thread_data[i].lost_cnt;
        total_timeouts += thread_data[i].timeout_cnt;
    }

    printf("Task 1 UDP pipelined client\n");
    printf("threads=%d requests_per_thread=%d window=%d timeout_ms=%d\n",
           num_client_threads, num_requests, window_size, timeout_ms);
    printf("tx_cnt=%ld\n", total_tx);
    printf("rx_cnt=%ld\n", total_rx);
    printf("lost_pkt_cnt=%ld\n", total_lost);
    printf("timeout_cnt=%ld\n", total_timeouts);
    if (total_rx > 0) {
        printf("average_rtt_us=%lld\n", total_rtt_us / total_rx);
    } else {
        printf("average_rtt_us=0\n");
    }
    if (total_elapsed_us > 0) {
        printf("aggregate_rx_rate=%.2f msg/s\n",
               ((double)total_rx * 1000000.0) / (double)total_elapsed_us);
    } else {
        printf("aggregate_rx_rate=0.00 msg/s\n");
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
    int optval = 1;

    server_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (server_fd < 0) {
        die_perror("socket");
    }

    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) < 0) {
        close(server_fd);
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
        fprintf(stderr, "invalid bind IP: %s\n", server_ip);
        exit(EXIT_FAILURE);
    }

    if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        close(server_fd);
        die_perror("bind");
    }

    epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) {
        close(server_fd);
        die_perror("epoll_create1");
    }

    memset(&event, 0, sizeof(event));
    event.events = EPOLLIN;
    event.data.fd = server_fd;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &event) < 0) {
        close(epoll_fd);
        close(server_fd);
        die_perror("epoll_ctl add server fd");
    }

    printf("Task 1 UDP server listening on %s:%d\n", server_ip, server_port);

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
                udp_echo_msg_t msg;
                struct sockaddr_in client_addr;
                socklen_t client_len = sizeof(client_addr);
                ssize_t recv_bytes = recvfrom(server_fd, &msg, sizeof(msg), 0,
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

                if (sendto(server_fd, &msg, sizeof(msg), 0,
                           (struct sockaddr *)&client_addr, client_len) < 0) {
                    if (errno != EAGAIN && errno != EWOULDBLOCK) {
                        perror("sendto");
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
