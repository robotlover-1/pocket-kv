#include "kvstore/kvstore.h"
#include <ctype.h>
#include <strings.h>
#include <sys/select.h>

typedef struct {
    char host[128];
    int port;
} sentinel_addr_t;

typedef struct {
    int role_master;
    char master_host[128];
    int master_port;
    int master_link_up;
} sentinel_info_t;

static void trim_right(char *s) {
    if (!s) return;
    size_t n = strlen(s);
    while (n > 0 && (s[n - 1] == '\r' || s[n - 1] == '\n' || isspace((unsigned char)s[n - 1]))) {
        s[n - 1] = '\0';
        --n;
    }
}

static int tcp_connect_timeout(const char *host, int port, int timeout_ms) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        close(fd);
        return -1;
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        close(fd);
        return -1;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, host, &addr.sin_addr) <= 0) {
        close(fd);
        return -1;
    }

    int rc = connect(fd, (struct sockaddr *)&addr, sizeof(addr));
    if (rc == 0) {
        (void)fcntl(fd, F_SETFL, flags);
        return fd;
    }
    if (errno != EINPROGRESS) {
        close(fd);
        return -1;
    }

    fd_set wfds;
    FD_ZERO(&wfds);
    FD_SET(fd, &wfds);

    struct timeval tv;
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;

    rc = select(fd + 1, NULL, &wfds, NULL, &tv);
    if (rc <= 0) {
        close(fd);
        return -1;
    }

    int err = 0;
    socklen_t errlen = sizeof(err);
    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &errlen) != 0 || err != 0) {
        close(fd);
        return -1;
    }

    if (fcntl(fd, F_SETFL, flags) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static int wait_fd_readable(int fd, int timeout_ms) {
    fd_set rfds;
    FD_ZERO(&rfds);
    FD_SET(fd, &rfds);

    struct timeval tv;
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;

    for (;;) {
        int rc = select(fd + 1, &rfds, NULL, NULL, &tv);
        if (rc < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        return rc > 0 ? 0 : -1;
    }
}

static int send_all(int fd, const unsigned char *buf, size_t len) {
    size_t off = 0;
    while (off < len) {
        ssize_t n = send(fd, buf + off, len - off, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return -1;
        off += (size_t)n;
    }
    return 0;
}

static int read_line_timeout(int fd, char *buf, size_t cap, int timeout_ms) {
    size_t used = 0;
    if (!buf || cap < 3) return -1;

    while (used + 1 < cap) {
        char ch;
        if (wait_fd_readable(fd, timeout_ms) != 0) return -1;

        ssize_t n = recv(fd, &ch, 1, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return -1;

        buf[used++] = ch;
        buf[used] = '\0';

        if (used >= 2 && buf[used - 2] == '\r' && buf[used - 1] == '\n') {
            return (int)used;
        }
    }
    return -1;
}

static int recv_exact_timeout(int fd, char *buf, size_t len, int timeout_ms) {
    size_t off = 0;
    while (off < len) {
        if (wait_fd_readable(fd, timeout_ms) != 0) return -1;
        ssize_t n = recv(fd, buf + off, len - off, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return -1;
        off += (size_t)n;
    }
    return 0;
}

static int read_simple_reply(int fd, char *reply, size_t cap, int timeout_ms) {
    int n = read_line_timeout(fd, reply, cap, timeout_ms);
    if (n < 0) return -1;
    trim_right(reply);
    return 0;
}

static int send_inline_cmd_simple(const char *host, int port, const char *cmd,
                                  char *reply, size_t reply_cap, int timeout_ms) {
    int fd = tcp_connect_timeout(host, port, timeout_ms);
    if (fd < 0) return -1;

    int rc = -1;
    if (send_all(fd, (const unsigned char *)cmd, strlen(cmd)) == 0) {
        rc = read_simple_reply(fd, reply, reply_cap, timeout_ms);
    }

    close(fd);
    return rc;
}

static int ping_node(const char *host, int port, int timeout_ms) {
    char reply[256];
    if (send_inline_cmd_simple(host, port, "PING\r\n", reply, sizeof(reply), timeout_ms) != 0) return -1;
    return (!strcmp(reply, "+PONG") || !strcmp(reply, "+OK")) ? 0 : -1;
}

static int fetch_info(const char *host, int port, sentinel_info_t *out) {
    int fd = -1;
    char line[256];
    char body[8192];
    char *saveptr = NULL;

    if (!out) return -1;
    memset(out, 0, sizeof(*out));

    fd = tcp_connect_timeout(host, port, 1000);
    if (fd < 0) return -1;

    if (send_all(fd, (const unsigned char *)"INFO\r\n", 6) != 0) {
        close(fd);
        return -1;
    }

    if (read_line_timeout(fd, line, sizeof(line), 1000) < 0) {
        close(fd);
        return -1;
    }
    trim_right(line);

    if (line[0] == '$') {
        long bulk_len = atol(line + 1);
        if (bulk_len <= 0 || (size_t)bulk_len + 3 > sizeof(body)) {
            close(fd);
            return -1;
        }

        if (recv_exact_timeout(fd, body, (size_t)bulk_len + 2, 1000) != 0) {
            close(fd);
            return -1;
        }
        body[bulk_len] = '\0';
    } else {
        snprintf(body, sizeof(body), "%s", line);
    }

    close(fd);

    for (char *ln = strtok_r(body, "\n", &saveptr); ln; ln = strtok_r(NULL, "\n", &saveptr)) {
        trim_right(ln);

        if (!strncmp(ln, "role:", 5)) {
            const char *v = ln + 5;
            out->role_master = !strcmp(v, "master") ? 1 : 0;
        } else if (!strncmp(ln, "master_host:", 12)) {
            snprintf(out->master_host, sizeof(out->master_host), "%s", ln + 12);
        } else if (!strncmp(ln, "master_port:", 12)) {
            out->master_port = atoi(ln + 12);
        } else if (!strncmp(ln, "master_link:", 12)) {
            out->master_link_up = !strcmp(ln + 12, "up") ? 1 : 0;
        }
    }

    return 0;
}

static int issue_slaveof_no_one(const char *host, int port) {
    char reply[512];
    if (send_inline_cmd_simple(host, port, "SLAVEOF NO ONE\r\n", reply, sizeof(reply), 1500) != 0) return -1;
    return (reply[0] == '+' || reply[0] == ':') ? 0 : -1;
}

static int issue_slaveof(const char *host, int port, const char *master_host, int master_port) {
    char cmd[1024];
    char reply[512];
    int n;

    n = snprintf(cmd, sizeof(cmd), "SLAVEOF %s %d\r\n", master_host, master_port);
    if (n < 0 || (size_t)n >= sizeof(cmd)) {
        /* 截断或错误，但缓冲区足够大（1024），不应发生 */
        return -1;
    }
    if (send_inline_cmd_simple(host, port, cmd, reply, sizeof(reply), 1500) != 0) return -1;
    return (reply[0] == '+' || reply[0] == ':') ? 0 : -1;
}

static int parse_addr_list(const char *spec, sentinel_addr_t *nodes, int max_nodes) {
    if (!spec || !*spec || !nodes || max_nodes <= 0) return 0;

    char tmp[1024];
    snprintf(tmp, sizeof(tmp), "%s", spec);

    int count = 0;
    char *saveptr = NULL;
    for (char *tok = strtok_r(tmp, ",", &saveptr); tok && count < max_nodes; tok = strtok_r(NULL, ",", &saveptr)) {
        trim_right(tok);
        while (*tok && isspace((unsigned char)*tok)) ++tok;
        if (!*tok) continue;

        char *colon = strrchr(tok, ':');
        if (!colon) continue;
        *colon = '\0';

        snprintf(nodes[count].host, sizeof(nodes[count].host), "%s", tok);
        nodes[count].port = atoi(colon + 1);
        if (nodes[count].port > 0) count++;
    }
    return count;
}

static int same_addr(const char *h1, int p1, const char *h2, int p2) {
    if (!h1 || !h2) return 0;
    return p1 == p2 && strcmp(h1, h2) == 0;
}

static int choose_promotion_target(sentinel_addr_t *nodes, int count, const char *failed_host, int failed_port) {
    for (int i = 0; i < count; ++i) {
        if (same_addr(nodes[i].host, nodes[i].port, failed_host, failed_port)) continue;

        sentinel_info_t info;
        if (fetch_info(nodes[i].host, nodes[i].port, &info) != 0) {
            fprintf(stderr, "[sentinel] skip candidate %s:%d reason=fetch_info_failed\n",
                    nodes[i].host, nodes[i].port);
            continue;
        }

        fprintf(stderr,
                "[sentinel] candidate %s:%d role=%s master=%s:%d master_link=%s\n",
                nodes[i].host,
                nodes[i].port,
                info.role_master ? "master" : "slave",
                info.master_host[0] ? info.master_host : "unknown",
                info.master_port,
                info.master_link_up ? "up" : "down");

        if (info.role_master) return i;

        if (!info.role_master) {
            /* 理想情况：host+port 都匹配旧主 */
            if (same_addr(info.master_host, info.master_port, failed_host, failed_port)) {
                return i;
            }

            /* 兼容当前项目：host 可能为空，但 port 仍然指向旧主 */
            if ((!info.master_host[0]) && info.master_port == failed_port) {
                return i;
            }
        }
    }
    return -1;
}

static void redirect_other_slaves(sentinel_addr_t *nodes, int count,
                                  const char *new_master_host, int new_master_port,
                                  const char *old_master_host, int old_master_port) {
    for (int i = 0; i < count; ++i) {
        if (same_addr(nodes[i].host, nodes[i].port, new_master_host, new_master_port)) continue;
        if (same_addr(nodes[i].host, nodes[i].port, old_master_host, old_master_port)) continue;

        sentinel_info_t info;
        if (fetch_info(nodes[i].host, nodes[i].port, &info) != 0) continue;
        if (info.role_master) continue;

        if ((info.master_host[0] && same_addr(info.master_host, info.master_port, old_master_host, old_master_port)) ||
            (!info.master_host[0] && info.master_port == old_master_port) ||
            !same_addr(info.master_host, info.master_port, new_master_host, new_master_port)) {
            if (issue_slaveof(nodes[i].host, nodes[i].port, new_master_host, new_master_port) == 0) {
                fprintf(stderr, "[sentinel] reconfigured slave %s:%d -> %s:%d\n",
                        nodes[i].host, nodes[i].port, new_master_host, new_master_port);
            }
        }
    }
}

static int wait_until_role_master(const char *host, int port, int timeout_ms) {
    long long start = kvs_now_ms();
    while (kvs_now_ms() - start < timeout_ms) {
        sentinel_info_t info;
        if (fetch_info(host, port, &info) == 0 && info.role_master) return 0;
        usleep(200 * 1000);
    }
    return -1;
}

int sentinel_start(void) {
    sentinel_addr_t nodes[32];
    int node_count = parse_addr_list(g_cfg.sentinel_known_slaves, nodes, 32);
    int sdown = 0;
    long long sdown_since = 0;
    long long last_failover_ms = 0;

    fprintf(stderr,
        "[sentinel] monitoring name=%s master=%s:%d slaves=%s down_after=%d failover_timeout=%d quorum=%d\n",
        g_cfg.sentinel_master_name,
        g_cfg.sentinel_monitor_host,
        g_cfg.sentinel_monitor_port,
        g_cfg.sentinel_known_slaves,
        g_cfg.sentinel_down_after_ms,
        g_cfg.sentinel_failover_timeout_ms,
        g_cfg.sentinel_quorum);

    for (;;) {
        int master_ok = (ping_node(g_cfg.sentinel_monitor_host, g_cfg.sentinel_monitor_port, 1000) == 0);

        if (master_ok) {
            if (sdown) {
                fprintf(stderr, "[sentinel] master recovered before failover %s:%d\n",
                        g_cfg.sentinel_monitor_host, g_cfg.sentinel_monitor_port);
            }
            sdown = 0;
            sdown_since = 0;
        } else {
            long long now = kvs_now_ms();
            if (!sdown) {
                sdown = 1;
                sdown_since = now;
                fprintf(stderr, "[sentinel] sdown candidate master=%s:%d\n",
                        g_cfg.sentinel_monitor_host, g_cfg.sentinel_monitor_port);
            }

            if (now - sdown_since >= g_cfg.sentinel_down_after_ms &&
                now - last_failover_ms >= g_cfg.sentinel_failover_timeout_ms) {

                char failed_host[128];
                int failed_port = g_cfg.sentinel_monitor_port;
                snprintf(failed_host, sizeof(failed_host), "%s", g_cfg.sentinel_monitor_host);

                int idx = choose_promotion_target(nodes, node_count, failed_host, failed_port);
                if (idx < 0) {
                    fprintf(stderr, "[sentinel] failover aborted: no promotable slave\n");
                    usleep(500 * 1000);
                    continue;
                }

                fprintf(stderr, "[sentinel] failover start old_master=%s:%d promote=%s:%d\n",
                        failed_host, failed_port, nodes[idx].host, nodes[idx].port);

                if (issue_slaveof_no_one(nodes[idx].host, nodes[idx].port) != 0) {
                    fprintf(stderr, "[sentinel] failover aborted: promote command failed target=%s:%d\n",
                            nodes[idx].host, nodes[idx].port);
                    last_failover_ms = now;
                    usleep(500 * 1000);
                    continue;
                }

                if (wait_until_role_master(nodes[idx].host, nodes[idx].port, 5000) != 0) {
                    fprintf(stderr, "[sentinel] failover aborted: promoted node did not become master target=%s:%d\n",
                            nodes[idx].host, nodes[idx].port);
                    last_failover_ms = now;
                    usleep(500 * 1000);
                    continue;
                }

                redirect_other_slaves(nodes, node_count,
                                      nodes[idx].host, nodes[idx].port,
                                      failed_host, failed_port);

                strncpy(g_cfg.sentinel_monitor_host, nodes[idx].host, sizeof(g_cfg.sentinel_monitor_host) - 1);
                g_cfg.sentinel_monitor_host[sizeof(g_cfg.sentinel_monitor_host) - 1] = '\0';
                g_cfg.sentinel_monitor_port = nodes[idx].port;
                last_failover_ms = now;
                sdown = 0;
                sdown_since = 0;

                fprintf(stderr, "[sentinel] failover done new_master=%s:%d\n",
                        g_cfg.sentinel_monitor_host, g_cfg.sentinel_monitor_port);
            }
        }

        usleep(500 * 1000);
    }

    return 0;
}
