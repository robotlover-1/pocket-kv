/*
 * tc_clone_receiver.c — AF_PACKET receiver for cloned TCP packets via VXLAN
 *
 * Listens on vxlan100 interface for VXLAN-encapsulated TCP packets.
 * Strips VXLAN → Ethernet → IP → TCP headers, extracts payload, counts.
 *
 * Build:
 *   gcc -O2 -Wall -o tc_clone_receiver tc_clone_receiver.c
 *
 * Usage:
 *   sudo ./tc_clone_receiver --iface vxlan100 --port 15800
 */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <getopt.h>
#include <linux/if_packet.h>
#include <net/ethernet.h>
#include <net/if.h>
#include <netinet/ip.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <unistd.h>

static volatile int g_running = 1;
static void sig_handler(int sig) { (void)sig; g_running = 0; }

int main(int argc, char **argv) {
    const char *iface = "vxlan100";
    int port = 15800;

    struct option long_opts[] = {
        {"iface", required_argument, 0, 'i'},
        {"port",  required_argument, 0, 'p'},
        {"help",  no_argument, 0, 'h'},
        {0, 0, 0, 0}};

    int opt;
    while ((opt = getopt_long(argc, argv, "i:p:h", long_opts, NULL)) != -1) {
        switch (opt) {
        case 'i': iface = optarg; break;
        case 'p': port = atoi(optarg); break;
        case 'h':
            printf("Usage: %s [--iface vxlan100] [--port 15800]\n", argv[0]);
            return 0;
        }
    }

    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);

    /* Raw socket with ETH_P_ALL to capture ALL frames on vxlan100 */
    int fd = socket(AF_PACKET, SOCK_RAW, htons(ETH_P_ALL));
    if (fd < 0) { perror("socket AF_PACKET"); return 1; }

    /* Bind to vxlan100 */
    struct sockaddr_ll sll = {0};
    sll.sll_family = AF_PACKET;
    sll.sll_protocol = htons(ETH_P_ALL);
    sll.sll_ifindex = if_nametoindex(iface);
    if (!sll.sll_ifindex) { perror("if_nametoindex"); return 1; }

    if (bind(fd, (struct sockaddr *)&sll, sizeof(sll)) < 0) {
        perror("bind"); return 1;
    }

    printf("[tc-rx] listening on %s (ifindex=%d), port=%d\n",
           iface, sll.sll_ifindex, port);
    fflush(stdout);

    long long total_msgs = 0;
    long long total_bytes = 0;
    unsigned char buf[65536];

    while (g_running) {
        ssize_t n = recv(fd, buf, sizeof(buf), 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            perror("recv"); break;
        }

        /* On vxlan100, we get the INNER Ethernet frame directly
         * (kernel already stripped the VXLAN/outer UDP/IP headers).
         *
         * Frame layout:
         *   [14B inner Ethernet] [20B IP] [20B TCP] [payload...]
         */
        if (n < 54) continue; /* min: eth(14) + ip(20) + tcp(20) */

        struct ethhdr *eth = (struct ethhdr *)buf;
        struct iphdr *ip = (struct iphdr *)(eth + 1);

        /* Skip non-IP */
        if (eth->h_proto != htons(ETH_P_IP)) continue;
        if (ip->protocol != IPPROTO_TCP) continue;

        int ip_hdr_len = ip->ihl * 4;
        if (n < 14 + ip_hdr_len + 20) continue;

        struct tcphdr *tcp = (struct tcphdr *)((char *)ip + ip_hdr_len);
        int tcp_hdr_len = tcp->doff * 4;

        /* Filter by dst port */
        if (ntohs(tcp->dest) != port) continue;

        /* Payload */
        unsigned char *payload = (unsigned char *)tcp + tcp_hdr_len;
        int payload_len = (int)n - 14 - ip_hdr_len - tcp_hdr_len;
        if (payload_len <= 0) continue;

        /* Each TCP segment = one "message" */
        total_msgs++;
        total_bytes += payload_len;
    }

    close(fd);
    printf("[tc-rx] done. total_msgs=%lld total_bytes=%lld (%.2f MB)\n",
           total_msgs, total_bytes, (double)total_bytes / (1024.0 * 1024.0));
    return 0;
}
