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

#ifndef TRITON_GRAPH_H
#define TRITON_GRAPH_H

#include "Shard.h"

namespace triton {

  class Graph {
  private:
    uint16_t cpus;
    std::string name;

  public:
    seastar::sharded<Shard> shard;
    explicit Graph(std::string name) :name(std::move(name)) {}

    std::string GetName();
    seastar::future<> start();
    seastar::future<> stop();
    void GetGreetingMessage(); // Change to Health Check
    void Clear();
    void Reserve(uint64_t reserved_nodes, uint64_t reserved_relationships);
  };
}// namespace triton

#endif//TRITON_GRAPH_H
