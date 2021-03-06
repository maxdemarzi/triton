/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "JSON.h"
#include "Nodes.h"

void Nodes::set_routes(routes &routes) {

  auto getNodes = new match_rule(&getNodesHandler);
  getNodes->add_str("/db/" + graph.GetName() + "/nodes");
  routes.add(getNodes, operation_type::GET);

  auto getNodesOfType = new match_rule(&getNodesOfTypeHandler);
  getNodesOfType->add_str("/db/" + graph.GetName() + "/nodes");
  getNodesOfType->add_param("type");
  routes.add(getNodesOfType, operation_type::GET);

  auto getNode = new match_rule(&getNodeHandler);
  getNode->add_str("/db/" + graph.GetName() + "/node");
  getNode->add_param("type");
  getNode->add_param("key");
  routes.add(getNode, operation_type::GET);

  auto getNodeById = new match_rule(&getNodeByIdHandler);
  getNodeById->add_str("/db/" + graph.GetName() + "/node");
  getNodeById->add_param("id");
  routes.add(getNodeById, operation_type::GET);

  auto postNode = new match_rule(&postNodeHandler);
  postNode->add_str("/db/" + graph.GetName() + "/node");
  postNode->add_param("type");
  postNode->add_param("key");
  routes.add(postNode, operation_type::POST);

  auto deleteNode = new match_rule(&deleteNodeHandler);
  deleteNode->add_str("/db/" + graph.GetName() + "/node");
  deleteNode->add_param("type");
  deleteNode->add_param("key");
  routes.add(deleteNode, operation_type::DELETE);

  auto deleteNodeById = new match_rule(&deleteNodeByIdHandler);
  deleteNodeById->add_str("/db/" + graph.GetName() + "/node");
  deleteNodeById->add_param("id");
  routes.add(deleteNodeById, operation_type::DELETE);
}

future<std::unique_ptr<reply>> Nodes::GetNodesHandler::handle(const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
  uint64_t limit = Server::validate_limit(req, rep);
  uint64_t offset = Server::validate_offset(req, rep);

  return parent.graph.shard.local().AllNodesPeered(offset, limit)
    .then([rep = std::move(rep), this](const std::vector<Node>& nodes) mutable {
           std::vector<node_json> json_array;
           json_array.reserve(nodes.size());
           for(Node n : nodes) {
             json_array.emplace_back(n, parent.graph);
           }
           rep->write_body("json", std::move(json::stream_object(json_array)));
           return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
    });
}

future<std::unique_ptr<reply>> Nodes::GetNodesOfTypeHandler::handle(const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
  bool valid_type = Server::validate_parameter(Server::TYPE, req, rep, "Invalid type");

  if(valid_type) {
    uint64_t limit = Server::validate_limit(req, rep);
    uint64_t offset = Server::validate_offset(req, rep);

    return parent.graph.shard.local().AllNodesPeered(req->param[Server::TYPE], offset, limit)
      .then([rep = std::move(rep), this](std::vector<Node> nodes) mutable {
             std::vector<node_json> json_array;
             json_array.reserve(nodes.size());
             if (!nodes.empty()) {
               std::string type = parent.graph.shard.local().NodeTypeGetType(nodes.front().getTypeId());
               for(Node n : nodes) {
                 json_array.emplace_back(n, type);
               }
               rep->write_body("json", std::move(json::stream_object(json_array)));
               return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
             }
             return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
      });
  }
  return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

future<std::unique_ptr<reply>> Nodes::GetNodeByIdHandler::handle(const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
  uint64_t id = Server::validate_id(req, rep);

  if (id > 0) {
    // Find Node by Id
    return parent.graph.shard.local().NodeGetPeered(id)
      .then([rep = std::move(rep), this] (Node node) mutable {
             std::string type = parent.graph.shard.local().NodeTypeGetType(node.getTypeId());
             rep->write_body("json", std::move(json::stream_object((node_json(node, parent.graph)))));
             return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
      });
  }

  return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

future<std::unique_ptr<reply>> Nodes::GetNodeHandler::handle(const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
  bool valid_type = Server::validate_parameter(Server::TYPE, req, rep, "Invalid type");
  bool valid_key = Server::validate_parameter(Server::KEY, req, rep, "Invalid key");

  if(valid_type && valid_key) {
    return parent.graph.shard.local().NodeGetPeered(req->param[Server::TYPE], req->param[Server::KEY])
      .then([rep = std::move(rep), this](Node node) mutable {
        std::string type = parent.graph.shard.local().NodeTypeGetType(node.getTypeId());
        rep->write_body("json", std::move(json::stream_object((node_json(node, parent.graph)))));
        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
      });
  }
  return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

future<std::unique_ptr<reply>> Nodes::PostNodeHandler::handle(const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
  bool valid_type = Server::validate_parameter(Server::TYPE, req, rep, "Invalid type");
  bool valid_key = Server::validate_parameter(Server::KEY, req, rep, "Invalid key");

  if(valid_type && valid_key) {
    // If there are no properties
    if (req->content.empty()) {
      return parent.graph.shard.local().NodeAddEmptyPeered(req->param[Server::TYPE], req->param[Server::KEY])
        .then([rep = std::move(rep), type = req->param[Server::TYPE], key = req->param[Server::KEY]](uint64_t id) mutable {
        if (id > 0) {
          rep->write_body("json", std::move(json::stream_object((node_json(id, type, key)))));
          rep->set_status(reply::status_type::created);
        } else {
          rep->write_body("json", std::move(json::stream_object("Invalid Request")));
          rep->set_status(reply::status_type::bad_request);
        }
        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
      });
    } else {
      return parent.graph.shard.local().NodeAddPeered(req->param[Server::TYPE], req->param[Server::KEY], req->content.c_str())
        .then([rep = std::move(rep), type = req->param[Server::TYPE], key = req->param[Server::KEY], this](uint64_t id) mutable {
        if (id > 0) {
          return parent.graph.shard.local().NodePropertiesGetPeered(id).then([rep = std::move(rep), type, key, id](std::map<std::string, std::any> properties) mutable {
            rep->write_body("json", std::move(json::stream_object((node_json(id, type, key, properties)))));
            rep->set_status(reply::status_type::created);
            return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
          });

        } else {
          rep->write_body("json", std::move(json::stream_object("Invalid Request")));
          rep->set_status(reply::status_type::bad_request);
        }
        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
      });
    }
  }

  return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

future<std::unique_ptr<reply>> Nodes::DeleteNodeHandler::handle(const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
  bool valid_type = Server::validate_parameter(Server::TYPE, req, rep, "Invalid type");
  bool valid_key = Server::validate_parameter(Server::KEY, req, rep, "Invalid key");

  if(valid_type && valid_key) {
    return parent.graph.shard.local().NodeRemovePeered(req->param[Server::TYPE], req->param[Server::KEY]).then([rep = std::move(rep)](bool success) mutable {
      if (success) {
        rep->set_status(reply::status_type::no_content);
      } else {
        rep->set_status(reply::status_type::not_modified);
      }
      return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
    });
  }
  return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

future<std::unique_ptr<reply>> Nodes::DeleteNodeByIdHandler::handle(const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
  uint64_t id = Server::validate_id(req, rep);

  if (id >0) {
    return parent.graph.shard.local().NodeRemovePeered(id).then([rep = std::move(rep)] (bool success) mutable {
           if(success) {
             rep->set_status(reply::status_type::no_content);
           } else {
             rep->set_status(reply::status_type::not_modified);
           }
           return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
    });
  }

  return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}