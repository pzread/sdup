#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<errno.h>
#include<sys/types.h>
#include<sys/epoll.h>
#include<sys/socket.h>
#include<netinet/in.h>
#include<http_parser.h>

#include"ev.h"

#define EV_MAX 4096

struct httpsrv_data{
    struct ev_header hdr;
};
struct httpcli_data{
    struct ev_header hdr;

    char buf[4096];
    struct http_parser hp;
    struct http_parser_settings hp_set;
};

static void httpcli_callback(struct epoll_event *evt);
static int httpreq_complete_callback(struct http_parser *hp);
static void httpsrv_callback(struct epoll_event *evt);
int httpsrv_init(int epfd,int skfd);

int ev_add(struct ev_header *data,int evts);

static void httpsrv_callback(struct epoll_event *evt){
    struct httpsrv_data *data;
    struct httpcli_data *cli_data;
    int skfd;

    if(evt->events & EPOLLIN){
	data = (struct httpsrv_data*)evt->data.ptr;
	while((skfd = accept4(data->hdr.fd,NULL,0,SOCK_NONBLOCK)) != -1){
	    cli_data = malloc(sizeof(*cli_data));

	    ev_init_header(&cli_data->hdr,data->hdr.epfd,skfd,httpcli_callback);
	    http_parser_init(&cli_data->hp,HTTP_REQUEST);
	    cli_data->hp.data = cli_data;
	    memset(&cli_data->hp_set,0,sizeof(cli_data->hp_set));
	    cli_data->hp_set.on_message_complete = httpreq_complete_callback;

	    ev_add(&cli_data->hdr,EPOLLIN | EPOLLOUT | EPOLLET);
	}
    }
}
int httpsrv_init(int epfd,int skfd){
    struct httpsrv_data *data;
    struct epoll_event evt;

    data = malloc(sizeof(*data));
    ev_init_header(&data->hdr,epfd,skfd,httpsrv_callback);

    evt.events = EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET;
    evt.data.ptr = data;

    return 0;
}

static void httpcli_callback(struct epoll_event *evt){
    int ret;

    struct httpcli_data *data;
    int readc;

    data = (struct httpcli_data*)evt->data.ptr;

    if(evt->events & EPOLLIN){
	while((readc = read(data->hdr.fd,data->buf,4096)) > 0){
	    http_parser_execute(&data->hp,&data->hp_set,data->buf,readc);
	}
    }
    if(evt->events & EPOLLRDHUP){
	http_parser_execute(&data->hp,&data->hp_set,NULL,0);
    }
}
static int httpreq_header_callback(struct http_parser *hp,const char *at,size_t len){

    return 0;
}
static int httpreq_complete_callback(struct http_parser *hp){
    struct httpcli_data *data;

    printf("test\n");

    data = (struct httpcli_data*)hp->data;
    write(data->hdr.fd,"Hello World",256);
    close(data->hdr.fd);

    free(data);

    return 0;
}

int ev_init(void){
    int epfd;

    if((epfd = epoll_create(EV_MAX)) == -1){
	return -errno;
    }
    return epfd;
}
int ev_add(struct ev_header *data,int evts){
    struct epoll_event evt;

    evt.events = evts;
    evt.data.ptr = data;
    epoll_ctl(data->epfd,EPOLL_CTL_ADD,data->fd,&evt);

    return 0;
}
int ev_loop(int epfd){
    int evtc;
    struct epoll_event *evts;
    struct ev_header *hdr;

    evts = malloc(sizeof(*evts) * EV_MAX);
    while((evtc = epoll_wait(epfd,evts,EV_MAX,-1)) > 0){
	for(;evtc > 0;evtc--){
	    hdr = evts[evtc - 1].data.ptr;
	    hdr->callback(&evts[evtc - 1]);
	} 
    }

    if(evtc == -1){
	return -errno;
    }
    return 0;
}

int main(void){
    int i;
    int skfd;
    int on;
    struct sockaddr_in skin;

    int epfd;
    struct httpsrv_data *data;

    skfd = socket(AF_INET,SOCK_STREAM | SOCK_NONBLOCK,6);
    on = 1;
    setsockopt(skfd,SOL_SOCKET,SO_REUSEADDR,&on,sizeof(on));
    skin.sin_family = AF_INET;
    skin.sin_port = htons(4573);
    skin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    bind(skfd,(struct sockaddr*)&skin,sizeof(skin));
    listen(skfd,65536); 

    for(i = 1;i > 0;i--){
	if(i > 1 && fork() != 0){
	    continue;
	}

	epfd = ev_init();

	data = malloc(sizeof(*data));
	ev_init_header(&data->hdr,epfd,skfd,httpsrv_callback);
	ev_add(&data->hdr,EPOLLIN | EPOLLOUT | EPOLLET);
	
	ev_loop(epfd);
    }

    return 0;
}
