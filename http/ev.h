typedef void (*ev_callback)(struct epoll_event *evt);

struct ev_header{
    int epfd;
    int fd;
    ev_callback callback;
};

static inline void ev_init_header(struct ev_header *hdr,int epfd,int fd,
	ev_callback callback){

    hdr->epfd = epfd;
    hdr->fd = fd;
    hdr->callback = callback;
}
