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

#define CATCH_CONFIG_ENABLE_BENCHMARKING
#include "../../lib/graph/Shard.h"
#include <catch2/catch.hpp>

SCENARIO( "Shard can reserve and clear", "[ids]" ) {
  GIVEN("An empty shard") {
    triton::Shard shard(4);
    shard.NodeTypeInsert("Node", 1);
    shard.NodeTypeInsert("User", 2);
    shard.NodeTypeInsert("Person", 3);
    WHEN("we reserve some nodes and relationships") {
      THEN("it should not throw") {
        REQUIRE_NOTHROW(shard.reserve(100, 100));
      }
    }

    WHEN("we reserve negative nodes and relationships") {
      THEN("it should not throw") {
        // because it expect an unsigned number, so it ignores this
        REQUIRE_NOTHROW(shard.reserve(-100, -100));
      }
    }

    WHEN("we add some nodes and relationships") {
      uint64_t node1id = shard.NodeAddEmpty("Node", 1, "one");
      uint64_t node3id = shard.NodeAddEmpty("User", 2, "two");
      uint64_t node2id = shard.NodeAddEmpty("Person", 3, "three");

      shard.RelationshipTypeInsert("LOVES", 1);
      shard.RelationshipTypeInsert("HATES", 2);
      shard.RelationshipAddEmptySameShard(1, node1id, node2id);
      shard.RelationshipAddEmptySameShard(1, node2id, node3id);
      shard.RelationshipAddEmptySameShard(2, node3id, node1id);

      THEN("clear the shard") {
        uint64_t likes_count = shard.RelationshipTypesGetCount("LOVES");
        uint64_t node_types_count = shard.NodeTypesGetCount();
        REQUIRE(likes_count == 2);
        REQUIRE(node_types_count == 3);
        shard.clear();
        likes_count = shard.RelationshipTypesGetCount("LOVES");
        node_types_count = shard.NodeTypesGetCount();
        REQUIRE(likes_count == 0);
        REQUIRE(node_types_count == 0);
      }
    }
  }
}