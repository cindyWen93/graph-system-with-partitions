/* This is cpp file of class Graph*/

#include "graph.h"

Graph::Graph(){}

int Graph::add_node(int node_id) {

  /* if the node already exists */
  if(edges.find(node_id) != edges.end()){
    return 204;
  }

  /* on success */
  else{
    set<int> tmp;
    edges.insert(make_pair(node_id,tmp));
    return 200;
  }
}

int Graph::add_edge(int node_a_id, int node_b_id) {

  /* if either node doesn't exist, or if node_a_id is the same as node_b_id */
  if(node_a_id == node_b_id || edges.find(node_a_id) == edges.end()){
    return 400;
  }

  /* if the edge already exists */
  else if(edges[node_a_id].find(node_b_id) != edges[node_a_id].end()){
    return 204;
  }

  /* on success */
  else{
    edges[node_a_id].insert(node_b_id);
    return 200;
  }
}

int Graph::remove_node(int node_id) {

  /* if the node does not exist */
  if(edges.find(node_id) == edges.end()){
      return 400;
  }

  /* on success */
  else{
    edges.erase(node_id);
    return 200;
  }
}

int Graph::remove_edge(int node_a_id, int node_b_id) {

  /* if the edge does not exist */
  if(node_a_id == node_b_id || edges.find(node_a_id) == edges.end() || edges[node_a_id].find(node_b_id) == edges[node_a_id].end()){
    return 400;
  }

  /* on success */
  else{
    edges[node_a_id].erase(node_b_id);
    return 200;
  }
}

int Graph::get_node(int node_id, bool & flag) {
  if(edges.find(node_id) != edges.end()) flag = true;
  else flag = false;
  return 200;
}

int Graph::get_edge(int node_a_id, int node_b_id, bool & flag) {

  /* at least one of the vertices does not exist */
  if(node_a_id == node_b_id || edges.find(node_a_id) == edges.end()){
    return 400;
  }

  /* on success*/
  else{
    if(edges[node_a_id].find(node_b_id) != edges[node_a_id].end()){
      flag = true;
    }
    else flag = false;
  }
  return 200;
}

int Graph::get_neighbors(int node_id,vector<int> &neighbors) {

  /* if the node does not exist*/
  if(edges.find(node_id) == edges.end()){
    return 400;
  }

  /* on success*/
  else{
    for(auto s : edges[node_id]){
      neighbors.push_back(s);
    }
    return 200;
  }
}
