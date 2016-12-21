#include "mongoose.h"
#include "graph.h"
#include "GH.h"
#include<thread>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/PlatformThreadFactory.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/server/TThreadPoolServer.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>
#include <thrift/TToString.h>
#include <boost/make_shared.hpp>
#include <boost/thread.hpp>
#include <boost/chrono.hpp>
#include <thrift/transport/TBufferTransports.h>

#include <iostream>
#include <stdexcept>
#include <sstream>
#include <mutex>

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::concurrency;
using namespace apache::thrift::server;

using boost::shared_ptr;
using namespace GH;
using namespace std;

boost::shared_ptr<TTransport> graph_socket;
boost::shared_ptr<TTransport> graph_transport;
boost::shared_ptr<TProtocol> graph_protocol;
GHClient *graph_client;

int rpc_client_port = 9000;//for this partition port
int rpc_server_port = 9000;//for this partition port
static char * s_http_port = "8000";
static string rpc_next_ip;
static string ips[3];//for store partition ip
static string port[3];//for store partition port
mutex m;
// bool isLast = false;

static struct mg_serve_http_opts s_http_server_opts;


Graph graph;
//this is for rpc Request
class GHHandler : virtual public GHIf {
public:
  GHHandler() {
    // Your initialization goes here
  }

  int32_t add_node(const int32_t node_id) {
    m.lock();
    int ret = graph.add_node(node_id);
    printf("add_node\n");
    m.unlock();
    return ret;
  }

  int32_t add_edge(const int32_t node_a_id, const int32_t node_b_id) {
    m.lock();
    int ret = graph.add_edge(node_a_id,node_b_id);
    printf("add_edge\n");
    m.unlock();
    return ret;
  }

  int32_t remove_node(const int32_t node_id) {
    m.lock();
    int ret = graph.remove_node(node_id);
    printf("remove_node\n");
    m.unlock();
    return ret;
  }

  int32_t remove_edge(const int32_t node_a_id, const int32_t node_b_id) {
    m.lock();
    int ret = graph.remove_edge(node_a_id,node_b_id);
    printf("remove_edge\n");
    m.unlock();
    return ret;
  }

};


//below is for http request
/* this function is used to get node_id from JSON field*/
const char* get_node(const char *p, int & node_id){

  string node;
  while(p!= NULL && (*p > '9' || *p <'0')){p++;}
  while(p!= NULL && (*p <= '9' && *p >= '0')){node.append(1,*(p++));}
  node_id = strtoll(node.c_str(), NULL, 10);

  return p;
}

static void handle_add_node_call(struct mg_connection *nc, struct http_message *hm) {
  //cout<<"beforelockget in here"<<endl;
  m.lock();
  //cout<<"get in here"<<endl;
  const char *p = hm->body.p;
  int node_id; p = get_node(p,node_id);
  //cout << "handle add node" << endl;

  int ret = graph.add_node(node_id);

  /* if the node already exists */
  if(ret == 204){
    mg_printf(nc, "%s", "HTTP/1.1 204 OK\r\nTransfer-Encoding: chunked\r\n\r\n");
  }

  /* on success */
  else{
    mg_printf(nc, "%s", "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n");
    mg_printf_http_chunk(nc, "{ \"node_id\": %d }", node_id);
  }
  m.unlock();
  mg_send_http_chunk(nc, "", 0);
}

static void handle_add_edge_call(struct mg_connection *nc, struct http_message *hm) {
  m.lock();
  const char *p = hm->body.p;
  int node_a_id, node_b_id; p = get_node(p,node_a_id); p = get_node(p,node_b_id);
  int ret;

  //因为总是发给node_id小的那个server，所以让node_a_id为小的那个
  if(node_a_id > node_b_id) swap(node_a_id,node_b_id);

  bool aInGraph = false;
  graph.get_node(node_a_id, aInGraph);

  if(!aInGraph){
    cout << "node_a is not in graph" << endl;
    ret = 400;
  }

  else{
  //如果两个节点在一个partition里面，直接调用graph的函数，不需要rpc
    if(node_a_id % 3 == node_b_id % 3){
      bool bInGraph = false;
      graph.get_node(node_b_id, bInGraph);
      if(!bInGraph){
        cout << "node_b is not in graph" << endl;
        ret = 400;
      }
      else{
        ret = graph.add_edge(node_a_id,node_b_id);
        //cout << ret <<"firstline"<<endl;
        ret = graph.add_edge(node_b_id,node_a_id);
        //cout << ret <<"secondline"<<endl;
      }
    }
    else{
      int partition = node_b_id % 3;
      rpc_next_ip = ips[partition];

      graph_socket = boost::shared_ptr<TTransport>(new TSocket(rpc_next_ip.c_str(), stoi(port[partition])));
      graph_transport = boost::shared_ptr<TTransport>(new TBufferedTransport(graph_socket));
      graph_protocol = boost::shared_ptr<TProtocol>(new TBinaryProtocol(graph_transport));

      graph_client = new GHClient(graph_protocol);
      graph_transport->open();
      ret = graph_client->add_edge(node_b_id,node_a_id);
      graph_transport->close();

      if(ret != 200){
        cout << "RPC returns 400 or 204" << endl;
      }
      else{
        //调用本地的add_edge
        ret = graph.add_edge(node_a_id,node_b_id);
      }
    }
  }

  /* if either node doesn't exist, or if node_a_id is the same as node_b_id */
  if(ret == 400){
    mg_printf(nc, "%s", "HTTP/1.1 400 Bad Request\r\nTransfer-Encoding: chunked\r\n\r\n");
  }

  /* if the edge already exists */
  else if(ret == 204){
    mg_printf(nc, "%s", "HTTP/1.1 204 OK\r\nTransfer-Encoding: chunked\r\n\r\n");
  }

  /* on success */
  else{
    mg_printf(nc, "%s", "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n");
    mg_printf_http_chunk(nc, "{ \"node_a_id\": %d, ", node_a_id);
    mg_printf_http_chunk(nc, "\"node_b_id\": %d }", node_b_id);
  }
  m.unlock();
  mg_send_http_chunk(nc, "", 0);

}

static void handle_remove_node_call(struct mg_connection *nc, struct http_message *hm) {
  m.lock();
  const char *p = hm->body.p;
  int node_id; p = get_node(p,node_id);
  int ret = graph.remove_node(node_id);

  /* if the node does not exist */
  if(ret == 400){
    mg_printf(nc, "%s", "HTTP/1.1 400 Bad Request\r\nTransfer-Encoding: chunked\r\n\r\n");
  }

  /* on success */
  else{
    mg_printf(nc, "%s", "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n");
  }
  m.unlock();
  mg_send_http_chunk(nc, "", 0);
}

static void handle_remove_edge_call(struct mg_connection *nc, struct http_message *hm) {
  m.lock();
  const char *p = hm->body.p;
  int ret;
  int node_a_id, node_b_id; p = get_node(p,node_a_id); p = get_node(p,node_b_id);

  //因为总是发给node_id小的那个server，所以让node_a_id为小的那个
  if(node_a_id > node_b_id) swap(node_a_id,node_b_id);

  //如果两个节点在一个partition里面，直接调用graph的函数，不需要rpc
  if(node_a_id % 3 == node_b_id % 3){
    ret = graph.remove_edge(node_a_id,node_b_id);
    ret = graph.remove_edge(node_b_id,node_a_id);
  }

  else{
    int partition = node_b_id % 3;
    rpc_next_ip = ips[partition];

    graph_socket = boost::shared_ptr<TTransport>(new TSocket(rpc_next_ip.c_str(), stoi(port[partition])));
    graph_transport = boost::shared_ptr<TTransport>(new TBufferedTransport(graph_socket));
    graph_protocol = boost::shared_ptr<TProtocol>(new TBinaryProtocol(graph_transport));

    graph_client = new GHClient(graph_protocol);
    graph_transport->open();
    ret = graph_client->remove_edge(node_b_id,node_a_id);
    graph_transport->close();

    if(ret != 200){
      cout << "Bad request, RPC returns 400" <<endl;
    }
    //调用本地的add_edge
    else{
      ret = graph.remove_edge(node_a_id,node_b_id);
    }
  }

  /* if the edge does not exist */
  if(ret == 400){
    mg_printf(nc, "%s", "HTTP/1.1 400 Bad Request\r\nTransfer-Encoding: chunked\r\n\r\n");
  }

  /* on success */
  else{
    mg_printf(nc, "%s", "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n");
  }
  m.unlock();
  mg_send_http_chunk(nc, "", 0);
}

static void handle_get_node_call(struct mg_connection *nc, struct http_message *hm) {
  m.lock();
  const char *p = hm->body.p;
  int node_id; p = get_node(p,node_id);

  bool exists;
  graph.get_node(node_id,exists);

  /* on success*/
  mg_printf(nc, "%s", "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n");
  mg_printf_http_chunk(nc, "{ \"in_graph\": %d }", exists);
  m.unlock();
  mg_send_http_chunk(nc, "", 0); /* Send empty chunk, the end of response */
}

static void handle_get_edge_call(struct mg_connection *nc, struct http_message *hm) {
  m.lock();
  const char *p = hm->body.p;
  bool exists;
  int ret;
  int node_a_id, node_b_id; p = get_node(p,node_a_id); p = get_node(p,node_b_id);

  if(node_a_id > node_b_id) swap(node_a_id,node_b_id);

  if(node_a_id % 3 == node_b_id % 3){
    bool bInGraph = false;
    graph.get_node(node_b_id, bInGraph);
    if(!bInGraph){
      ret = 400;
    }
    else{
      ret = graph.get_edge(node_a_id,node_b_id,exists);
    }
  }
  else{

    int partition = node_b_id % 3;
    rpc_next_ip = ips[partition];

    graph_socket = boost::shared_ptr<TTransport>(new TSocket(rpc_next_ip.c_str(), stoi(port[partition])));
    graph_transport = boost::shared_ptr<TTransport>(new TBufferedTransport(graph_socket));
    graph_protocol = boost::shared_ptr<TProtocol>(new TBinaryProtocol(graph_transport));

    graph_client = new GHClient(graph_protocol);
    graph_transport->open();
    if(graph_client->add_node(node_b_id) == 200){
      ret = 400;
      graph_client->remove_node(node_b_id);
    }
    else{
      ret = graph.get_edge(node_a_id,node_b_id,exists);
    }
    graph_transport->close();
  }

  /* at least one of the vertices does not exist */
  if(ret == 400){
    mg_printf(nc, "%s", "HTTP/1.1 400 Bad Request\r\nTransfer-Encoding: chunked\r\n\r\n");
  }

  /* on success*/
  else{
    mg_printf(nc, "%s", "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n");
    mg_printf_http_chunk(nc, "{ \"in_graph\": %d }", exists);
  }
  m.unlock();
  mg_send_http_chunk(nc, "", 0);
}

static void handle_get_neighbors_call(struct mg_connection *nc, struct http_message *hm) {
  m.lock();
  const char *p = hm->body.p;
  int node_id; p = get_node(p,node_id);
  vector<int> neighbors;

  int ret = graph.get_neighbors(node_id,neighbors);

  /* if the node does not exist*/
  if(ret == 400){
    mg_printf(nc, "%s", "HTTP/1.1 400 Bad Request\r\nTransfer-Encoding: chunked\r\n\r\n");
  }

  /* on success*/
  else{
    mg_printf(nc, "%s", "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n");
    mg_printf_http_chunk(nc, "{ \"node_id\": %d,  \"neighbors\": [", node_id);
    for(int i = 0; i < neighbors.size(); i++){
      mg_printf_http_chunk(nc, "%d, ", neighbors[i]);
    }
    mg_printf_http_chunk(nc, "\t\t ] }");
  }
  m.unlock();
  mg_send_http_chunk(nc, "", 0); /* Send empty chunk, the end of response */
}

static void ev_handler(struct mg_connection *nc, int ev, void *ev_data) {
  struct http_message *hm = (struct http_message *) ev_data;

  switch (ev) {
    case MG_EV_HTTP_REQUEST:
    if (mg_vcmp(&hm->uri, "/api/v1/add_node") == 0) {

      handle_add_node_call(nc, hm);

    }
    else if (mg_vcmp(&hm->uri, "/api/v1/add_edge") == 0){
      handle_add_edge_call(nc, hm);
    }
    else if (mg_vcmp(&hm->uri, "/api/v1/remove_node") == 0){
      handle_remove_node_call(nc, hm);
    }
    else if (mg_vcmp(&hm->uri, "/api/v1/remove_edge") == 0){
      handle_remove_edge_call(nc, hm);
    }
    else if (mg_vcmp(&hm->uri, "/api/v1/get_node") == 0){
      handle_get_node_call(nc, hm);
    }
    else if (mg_vcmp(&hm->uri, "/api/v1/get_edge") == 0){
      handle_get_edge_call(nc, hm);
    }
    else if (mg_vcmp(&hm->uri, "/api/v1/get_neighbors") == 0){
      handle_get_neighbors_call(nc, hm);
    }
    // else if (mg_vcmp(&hm->uri, "/api/v1/shortest_path") == 0){
    //   handle_shortest_path_call(nc, hm);
    // }
    else{
      mg_send_http_chunk(nc, "", 0);
    }
    break;
    default:
    break;
  }
}

void rpc_listen(){
  boost::shared_ptr<GHHandler> handler(new GHHandler());
  boost::shared_ptr<TProcessor> processor(new GHProcessor(handler));
  boost::shared_ptr<TServerTransport> serverTransport(new TServerSocket(rpc_server_port));
  boost::shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
  boost::shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
  TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);
  cout << "rpc listend" << endl;
  server.serve();
}

int main(int argc, char *argv[]) {
  struct mg_mgr mgr;
  struct mg_connection *nc;
  struct mg_bind_opts bind_opts;
  const char *err_str;

  int partitionNumber; //my partitionNumber
  //int s_http_port;
  string argv5, argv6, argv7;
  if(argc == 8){
    partitionNumber = atoi(argv[3]) - 1;
    s_http_port = argv[1];//my http port

    argv5 = argv[5];
    argv6 = argv[6];
    argv7 = argv[7];

    ips[0] = argv5.substr(0, argv5.find(":"));
    port[0] = argv5.substr(0 + argv5.find(":") + 1, argv5.length() - ips[0].length());

    ips[1] = argv6.substr(0, argv6.find(":"));
    port[1] = argv6.substr(0 + argv6.find(":") + 1, argv6.length() - ips[1].length());

    ips[2] = argv7.substr(0, argv7.find(":"));
    port[2] = argv7.substr(0 + argv7.find(":") + 1, argv7.length() - ips[2].length());

    rpc_server_port = atoi(port[partitionNumber].c_str());//rpc server port for this partition
    rpc_client_port = atoi(port[partitionNumber].c_str());//rpc server port for this partition
  }
  else{
    cout << "wrong number of command line arguments" << endl;
  }

  //for rpc
  boost::thread nthread{rpc_listen};
  cout <<"RPC LISTEN ON PORT " << rpc_server_port<<endl;

  mg_mgr_init(&mgr, NULL);

  /* Set HTTP server options */
  memset(&bind_opts, 0, sizeof(bind_opts));
  bind_opts.error_string = &err_str;

  nc = mg_bind_opt(&mgr, s_http_port, ev_handler, bind_opts);
  if (nc == NULL) {
    fprintf(stderr, "Error starting server on port %s: %s\n", s_http_port,
    *bind_opts.error_string);
    exit(1);
  }

  mg_set_protocol_http_websocket(nc);
  s_http_server_opts.enable_directory_listing = "yes";

  printf("Starting server on port %s, serving %s\n", s_http_port,
  s_http_server_opts.document_root);

  for (;;) {mg_mgr_poll(&mgr, 1000);}
  mg_mgr_free(&mgr);

  return 0;
}
