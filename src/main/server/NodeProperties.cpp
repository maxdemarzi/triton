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
#include "NodeProperties.h"
#include "Server.h"

void NodeProperties::set_routes(routes &routes) {
  auto getNodeProperty = new match_rule(&getNodePropertyHandler);
  getNodeProperty->add_str("/db/" + graph.GetName() + "/node");
  getNodeProperty->add_param("type");
  getNodeProperty->add_param("key");
  getNodeProperty->add_str("/property");
  getNodeProperty->add_param("property");
  routes.add(getNodeProperty, operation_type::GET);

  auto getNodePropertyById = new match_rule(&getNodePropertyByIdHandler);
  getNodePropertyById->add_str("/db/" + graph.GetName() + "/node");
  getNodePropertyById->add_param("id");
  getNodePropertyById->add_str("/property");
  getNodePropertyById->add_param("property");
  routes.add(getNodePropertyById, operation_type::GET);

  auto putNodeProperty = new match_rule(&putNodePropertyHandler);
  putNodeProperty->add_str("/db/" + graph.GetName() + "/node");
  putNodeProperty->add_param("type");
  putNodeProperty->add_param("key");
  putNodeProperty->add_str("/property");
  putNodeProperty->add_param("property");
  routes.add(putNodeProperty, operation_type::PUT);

  auto putNodePropertyById = new match_rule(&putNodePropertyByIdHandler);
  putNodePropertyById->add_str("/db/" + graph.GetName() + "/node");
  putNodePropertyById->add_param("id");
  putNodePropertyById->add_str("/property");
  putNodePropertyById->add_param("property");
  routes.add(putNodePropertyById, operation_type::PUT);

  auto deleteNodeProperty = new match_rule(&deleteNodePropertyHandler);
  deleteNodeProperty->add_str("/db/" + graph.GetName() + "/node");
  deleteNodeProperty->add_param("type");
  deleteNodeProperty->add_param("key");
  deleteNodeProperty->add_str("/property");
  deleteNodeProperty->add_param("property");
  routes.add(deleteNodeProperty, operation_type::DELETE);

  auto deleteNodePropertyById = new match_rule(&deleteNodePropertyByIdHandler);
  deleteNodePropertyById->add_str("/db/" + graph.GetName() + "/node");
  deleteNodePropertyById->add_param("id");
  deleteNodePropertyById->add_str("/property");
  deleteNodePropertyById->add_param("property");
  routes.add(deleteNodePropertyById, operation_type::DELETE);

  auto getNodeProperties = new match_rule(&getNodePropertiesHandler);
  getNodeProperties->add_str("/db/" + graph.GetName() + "/node");
  getNodeProperties->add_param("type");
  getNodeProperties->add_param("key");
  getNodeProperties->add_str("/properties");
  routes.add(getNodeProperties, operation_type::GET);

  auto getNodePropertiesById = new match_rule(&getNodePropertiesByIdHandler);
  getNodePropertiesById->add_str("/db/" + graph.GetName() + "/node");
  getNodePropertiesById->add_param("id");
  getNodePropertiesById->add_str("/properties");
  routes.add(getNodePropertiesById, operation_type::GET);

  auto postNodeProperties = new match_rule(&postNodePropertiesHandler);
  postNodeProperties->add_str("/db/" + graph.GetName() + "/node");
  postNodeProperties->add_param("type");
  postNodeProperties->add_param("key");
  postNodeProperties->add_str("/properties");
  routes.add(postNodeProperties, operation_type::POST);

  auto postNodePropertiesById = new match_rule(&postNodePropertiesByIdHandler);
  postNodePropertiesById->add_str("/db/" + graph.GetName() + "/node");
  postNodePropertiesById->add_param("id");
  postNodePropertiesById->add_str("/properties");
  routes.add(postNodePropertiesById, operation_type::POST);

  auto putNodeProperties = new match_rule(&putNodePropertiesHandler);
  putNodeProperties->add_str("/db/" + graph.GetName() + "/node");
  putNodeProperties->add_param("type");
  putNodeProperties->add_param("key");
  putNodeProperties->add_str("/properties");
  routes.add(putNodeProperties, operation_type::PUT);

  auto putNodePropertiesById = new match_rule(&putNodePropertiesByIdHandler);
  putNodePropertiesById->add_str("/db/" + graph.GetName() + "/node");
  putNodePropertiesById->add_param("id");
  putNodePropertiesById->add_str("/properties");
  routes.add(putNodePropertiesById, operation_type::PUT);

  auto deleteNodeProperties = new match_rule(&deleteNodePropertiesHandler);
  deleteNodeProperties->add_str("/db/" + graph.GetName() + "/node");
  deleteNodeProperties->add_param("type");
  deleteNodeProperties->add_param("key");
  deleteNodeProperties->add_str("/properties");
  routes.add(deleteNodeProperties, operation_type::DELETE);

  auto deleteNodePropertiesById = new match_rule(&deleteNodePropertiesByIdHandler);
  deleteNodePropertiesById->add_str("/db/" + graph.GetName() + "/node");
  deleteNodePropertiesById->add_param("id");
  deleteNodePropertiesById->add_str("/properties");
  routes.add(deleteNodePropertiesById, operation_type::DELETE);
}

future<std::unique_ptr<reply>> NodeProperties::GetNodePropertyHandler::handle(const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
  bool valid_type = Server::validate_parameter(Server::TYPE, req, rep, "Invalid type");
  bool valid_key = Server::validate_parameter(Server::KEY, req, rep, "Invalid key");
  bool valid_property = Server::validate_parameter(Server::PROPERTY, req, rep, "Invalid property");

  if(valid_type && valid_key && valid_property) {

    return parent.graph.shard.invoke_on(this_shard_id(), [req = std::move(req)](Shard &local_shard) {
           return local_shard.NodePropertyGetPeered(req->param[Server::TYPE], req->param[Server::KEY], req->param[Server::PROPERTY]);
    }).then([rep = std::move(rep)] (const std::any& property) mutable {
             Server::convert_property_to_json(rep, property);
             return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
      });
  }

  return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}


future<std::unique_ptr<reply>> NodeProperties::GetNodePropertyByIdHandler::handle(const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
  uint64_t id = Server::validate_id(req, rep);

  if (id > 0) {
    return parent.graph.shard.invoke_on(this_shard_id(), [id, req = std::move(req)] (Shard &local_shard) {
             return local_shard.NodePropertyGetPeered(id, req->param[Server::PROPERTY]);
      }).then([rep = std::move(rep)] (const std::any& property) mutable {
             Server::convert_property_to_json(rep, property);
             return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
      });
  }

  return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

future<std::unique_ptr<reply>> NodeProperties::PutNodePropertyHandler::handle(const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
  bool valid_type = Server::validate_parameter(Server::TYPE, req, rep, "Invalid type");
  bool valid_key = Server::validate_parameter(Server::KEY, req, rep, "Invalid key");
  bool valid_property = Server::validate_parameter(Server::PROPERTY, req, rep, "Invalid property");

  if(valid_type && valid_key && valid_property) {
    return parent.graph.shard.invoke_on(this_shard_id(), [req = std::move(req)] (Shard &local_shard) {
           return local_shard.NodePropertySetFromJsonPeered(req->param[Server::TYPE], req->param[Server::KEY], req->param[Server::PROPERTY], req->content.c_str());
    }).then([rep = std::move(rep)] (bool success) mutable {
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

future<std::unique_ptr<reply>> NodeProperties::PutNodePropertyByIdHandler::handle(const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
  uint64_t id = Server::validate_id(req, rep);
  bool valid_property = Server::validate_parameter(Server::PROPERTY, req, rep, "Invalid property");

  if (id > 0 && valid_property) {
    return parent.graph.shard.invoke_on(this_shard_id(), [id, req = std::move(req)] (Shard &local_shard) {
           return local_shard.NodePropertySetFromJson(id, req->param[Server::PROPERTY], req->content.c_str());
    }).then([rep = std::move(rep)] (bool success) mutable {
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

future<std::unique_ptr<reply>> NodeProperties::DeleteNodePropertyHandler::handle(const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
  bool valid_type = Server::validate_parameter(Server::TYPE, req, rep, "Invalid type");
  bool valid_key = Server::validate_parameter(Server::KEY, req, rep, "Invalid key");
  bool valid_property = Server::validate_parameter(Server::PROPERTY, req, rep, "Invalid property");

  if(valid_type && valid_key && valid_property) {
    return parent.graph.shard.invoke_on(this_shard_id(), [req = std::move(req)] (Shard &local_shard) {
           return local_shard.NodePropertyDelete(req->param[Server::TYPE], req->param[Server::KEY], req->param[Server::PROPERTY]);
    }).then([rep = std::move(rep)] (const std::any& property) mutable {
             Server::convert_property_to_json(rep, property);
             return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
      });
  }

  return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

future<std::unique_ptr<reply>> NodeProperties::DeleteNodePropertyByIdHandler::handle(const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
  uint64_t id = Server::validate_id(req, rep);
  bool valid_property = Server::validate_parameter(Server::PROPERTY, req, rep, "Invalid property");

  if (id > 0 && valid_property) {
    return parent.graph.shard.invoke_on(this_shard_id(), [id, req = std::move(req)] (Shard &local_shard) {
           return local_shard.NodePropertyDelete(id, req->param[Server::PROPERTY]);
    }).then([rep = std::move(rep)] (const std::any& property) mutable {
             Server::convert_property_to_json(rep, property);
             return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
      });
  }

  return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

future<std::unique_ptr<reply>> NodeProperties::GetNodePropertiesHandler::handle(const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
  bool valid_type = Server::validate_parameter(Server::TYPE, req, rep, "Invalid type");
  bool valid_key = Server::validate_parameter(Server::KEY, req, rep, "Invalid key");

  if(valid_type && valid_key) {
    return parent.graph.shard.invoke_on(this_shard_id(), [req = std::move(req)] (Shard &local_shard) {
           return local_shard.NodePropertiesGet(req->param[Server::TYPE], req->param[Server::KEY]);
    }).then([rep = std::move(rep)] (const std::map<std::string, std::any>& properties) mutable {
             json_properties_builder json;
             json.add_properties(properties);
             rep->write_body("json", sstring(json.as_json()));
             return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
      });
  }

  return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

future<std::unique_ptr<reply>> NodeProperties::GetNodePropertiesByIdHandler::handle(const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
  uint64_t id = Server::validate_id(req, rep);

  if (id > 0) {
    return parent.graph.shard.invoke_on(this_shard_id(), [id] (Shard &local_shard) {
           return local_shard.NodePropertiesGet(id);
    }).then([rep = std::move(rep)] (const std::map<std::string, std::any>& properties) mutable {
             json_properties_builder json;
             json.add_properties(properties);
             rep->write_body("json", sstring(json.as_json()));
             return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
      });
  }

  return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

future<std::unique_ptr<reply>> NodeProperties::PostNodePropertiesHandler::handle(const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
  bool valid_type = Server::validate_parameter(Server::TYPE, req, rep, "Invalid type");
  bool valid_key = Server::validate_parameter(Server::KEY, req, rep, "Invalid key");

  if(valid_type && valid_key) {
    return parent.graph.shard.invoke_on(this_shard_id(), [req = std::move(req)] (Shard &local_shard) {
           return local_shard.NodePropertiesResetFromJson(req->param[Server::TYPE], req->param[Server::KEY], req->content.c_str());
    }).then([rep = std::move(rep)] (bool success) mutable {
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

future<std::unique_ptr<reply>> NodeProperties::PostNodePropertiesByIdHandler::handle(const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
  uint64_t id = Server::validate_id(req, rep);

  if (id > 0) {
    return parent.graph.shard.invoke_on(this_shard_id(), [id, req = std::move(req)] (Shard &local_shard) {
           return local_shard.NodePropertiesResetFromJson(id, req->content.c_str());
    }).then([rep = std::move(rep)] (bool success) mutable {
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

future<std::unique_ptr<reply>> NodeProperties::PutNodePropertiesHandler::handle(const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
  bool valid_type = Server::validate_parameter(Server::TYPE, req, rep, "Invalid type");
  bool valid_key = Server::validate_parameter(Server::KEY, req, rep, "Invalid key");

  if(valid_type && valid_key) {
    return parent.graph.shard.invoke_on(this_shard_id(), [req = std::move(req)] (Shard &local_shard) {
           return local_shard.NodePropertiesSetFromJson(req->param[Server::TYPE], req->param[Server::KEY], req->content.c_str());
    }).then([rep = std::move(rep)] (bool success) mutable {
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

future<std::unique_ptr<reply>> NodeProperties::PutNodePropertiesByIdHandler::handle(const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
  uint64_t id = Server::validate_id(req, rep);

  if (id > 0) {
    return parent.graph.shard.invoke_on(this_shard_id(), [id, req = std::move(req)] (Shard &local_shard) {
           return local_shard.NodePropertiesSetFromJson(id, req->content.c_str());
    }).then([rep = std::move(rep)] (bool success) mutable {
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

future<std::unique_ptr<reply>> NodeProperties::DeleteNodePropertiesHandler::handle(const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
  bool valid_type = Server::validate_parameter(Server::TYPE, req, rep, "Invalid type");
  bool valid_key = Server::validate_parameter(Server::KEY, req, rep, "Invalid key");

  if(valid_type && valid_key) {
    return parent.graph.shard.invoke_on(this_shard_id(), [req = std::move(req)] (Shard &local_shard) {
           return local_shard.NodePropertiesDelete(req->param[Server::TYPE], req->param[Server::KEY]);
    }).then([rep = std::move(rep)] (bool success) mutable {
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

future<std::unique_ptr<reply>> NodeProperties::DeleteNodePropertiesByIdHandler::handle(const sstring &path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
  uint64_t id = Server::validate_id(req, rep);

  if (id > 0) {
    return parent.graph.shard.invoke_on(this_shard_id(), [id] (Shard &local_shard) {
           return local_shard.NodePropertiesDelete(id);
    }).then([rep = std::move(rep)] (bool success) mutable {
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
