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

#include "Graph.h"
#include <iostream>

namespace triton {

  std::string Graph::GetName() {
    return name;
  }

  seastar::future<> Graph::start() {
    cpus = seastar::smp::count;
    // Will create a shard instance on each core
    return shard.start(cpus);
  }

  seastar::future<> Graph::stop() {
    return shard.stop();
  }

  void Graph::GetGreetingMessage() {
    seastar::future<> speak = shard.invoke_on_all([](Shard &local_shard) {
             return local_shard.speak();
      });
    static_cast<void>(seastar::when_all_succeed(std::move(speak))
                        .discard_result()
                        .handle_exception([](std::exception_ptr e) { std::cerr << "Exception in Graph::GetGreetingMessage\n"; }));
  }

  void Graph::Clear() {
    seastar::future<> clear = shard.invoke_on_all([](Shard &local_shard) {
           return local_shard.clear();
    });
    static_cast<void>(seastar::when_all_succeed(std::move(clear))
                        .discard_result()
                        .handle_exception([](std::exception_ptr e) { std::cerr << "Exception in Graph::Clear\n"; }));
  }
  
  void Graph::Reserve(uint64_t reserved_nodes, uint64_t reserved_relationships) {
    reserved_nodes = reserved_nodes / cpus;
    reserved_relationships = reserved_relationships / cpus;

    seastar::future<> reserve = shard.invoke_on_all([reserved_nodes, reserved_relationships](Shard &local_shard) {
           return local_shard.reserve(reserved_nodes, reserved_relationships);
    });

    static_cast<void>(seastar::when_all_succeed(std::move(reserve))
                        .discard_result()
                        .handle_exception([] (std::exception_ptr e) {
           std::cerr << "Exception in Graph::Reserve\n";
    }));
  }

}// namespace triton
