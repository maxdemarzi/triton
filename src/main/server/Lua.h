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

#ifndef TRITON_LUA_H
#define TRITON_LUA_H

#include "Server.h"
#include <Graph.h>
#include <seastar/http/httpd.hh>

using namespace seastar;
using namespace httpd;
using namespace triton;

class Lua {

  class PostLuaHandler : public httpd::handler_base {
  public:
    explicit PostLuaHandler(Lua& lua) : parent(lua) {};

  private:
    Lua& parent;
    future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;
  };

private:
  Graph& graph;
  PostLuaHandler postLuaHandler;

public:
  explicit Lua(Graph &graph) : graph(graph), postLuaHandler(*this) {}
  void set_routes(routes& routes);
};


#endif//TRITON_LUA_H
