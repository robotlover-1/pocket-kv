#ifndef __SERVER_H__
#define __SERVER_H__

#include "kvstore.h"

#define CONNECTION_SIZE 1024

typedef int (*RCALLBACK)(int fd);

struct conn {
    int fd;
    RCALLBACK send_callback;
    union {
        RCALLBACK recv_callback;
        RCALLBACK accept_callback;
    } r_action;
    kvs_stream_t stream;
};

#endif
