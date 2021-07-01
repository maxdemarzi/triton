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

SCENARIO( "Shard can handle All Nodes", "[node]" ) {

  GIVEN( "A shard with a bunch of nodes" ) {
    triton::Shard shard(4);
    shard.NodeTypeInsert("Node", 1);
    shard.NodeTypeInsert("User", 2);

    int64_t empty = shard.NodeAddEmpty("Node", 1,  "empty");
    int64_t existing = shard.NodeAdd("Node", 1,  "existing", R"({ "name":"max" })");

    int64_t three = shard.NodeAddEmpty("Node", 1,  "three");
    int64_t four = shard.NodeAddEmpty("Node", 1,  "four");
    int64_t five = shard.NodeAddEmpty("Node", 1,  "five");
    int64_t six = shard.NodeAddEmpty("Node", 1,  "six");

    REQUIRE( empty == 256 );
    REQUIRE( existing == 512 );
    REQUIRE( three == 768 );
    REQUIRE( four == 1024 );
    REQUIRE( five == 1280 );
    REQUIRE( six == 1536 );

    WHEN( "more nodes are added" ) {
      shard.NodeAddEmpty("User", 2, "one");
      shard.NodeAddEmpty("User", 2, "two");

      THEN( "the shard should get the right node counts" ) {
        auto counts = shard.AllNodeIdCounts();
        REQUIRE(counts.at(1) == 6);
        REQUIRE(counts.at(2) == 2);

        REQUIRE(shard.AllNodeIdCounts(1) == 6);
        REQUIRE(shard.AllNodeIdCounts(2) == 2);

        REQUIRE(shard.AllNodeIdCounts("Node") == 6);
        REQUIRE(shard.AllNodeIdCounts("User") == 2);

        REQUIRE(shard.AllNodeIdCounts(99) == 0);
        REQUIRE(shard.AllNodeIdCounts("Wrong") == 0);
      }
    }

    WHEN( "more nodes are added" ) {
      shard.NodeAddEmpty("User", 2, "one");
      shard.NodeAddEmpty("User", 2, "two");
      THEN( "the shard should get zero node ids if the wrong type is asked for" ) {
        Roaring64Map it = shard.AllNodeIdsMap("Wrong");
        int counter = 0;
        for(Roaring64MapSetBitForwardIterator i = it.begin() ; i != it.end() ; i++) {
          ++counter;
        }
        REQUIRE(counter == 0);
      }
    }

    WHEN( "more nodes are added" ) {
      shard.NodeAddEmpty("User", 2, "one");
      shard.NodeAddEmpty("User", 2, "two");
      THEN( "the shard should get zero node ids if the wrong type is asked for" ) {
        Roaring64Map it = shard.AllNodeIdsMap(99);
        int counter = 0;
        for(Roaring64MapSetBitForwardIterator i = it.begin() ; i != it.end() ; i++) {
          ++counter;
        }
        REQUIRE(counter == 0);
      }
    }

    WHEN( "more nodes are added" ) {
      shard.NodeAddEmpty("User", 2, "one");
      shard.NodeAddEmpty("User", 2, "two");
      THEN( "the shard should get zero nodes if the wrong type is asked for" ) {
        std::vector<triton::Node> it = shard.AllNodes("Wrong");
        REQUIRE(it.empty());
      }
    }

    WHEN( "more nodes are added" ) {
      shard.NodeAddEmpty("User", 2, "one");
      shard.NodeAddEmpty("User", 2, "two");

      THEN( "the shard should get all the node ids" ) {
        Roaring64Map it = shard.AllNodeIdsMap();
        int counter = 0;
        for(Roaring64MapSetBitForwardIterator i = it.begin() ; i != it.end() ; i++) {
          ++counter;
        }
        REQUIRE(counter == 8);
      }
    }

    WHEN( "more nodes are added" ) {
      int64_t user1 = shard.NodeAddEmpty("User", 2, "one");
      int64_t user2 = shard.NodeAddEmpty("User", 2, "two");

      THEN( "the shard should get all the nodes" ) {
        std::vector<triton::Node> it = shard.AllNodes();
        REQUIRE(it.size() == 8);
      }
    }

    WHEN( "more nodes of a different type are added" ) {
      shard.NodeAddEmpty("User", 2, "one");
      shard.NodeAddEmpty("User", 2, "two");

      THEN( "the shard should get all the node ids by type" ) {
        Roaring64Map it = shard.AllNodeIdsMap("User");
        int counter = 0;
        for(Roaring64MapSetBitForwardIterator i = it.begin() ; i != it.end() ; i++) {
          ++counter;
        }
        REQUIRE(counter == 2);

        it = shard.AllNodeIdsMap("Node");
        counter = 0;
        for(Roaring64MapSetBitForwardIterator i = it.begin() ; i != it.end() ; i++) {
          ++counter;
        }
        REQUIRE(counter == 6);
      }
    }

    WHEN( "more nodes of a different type are added" ) {
      shard.NodeAddEmpty("User", 2, "one");
      shard.NodeAddEmpty("User", 2, "two");

      THEN( "the shard should get all the node ids by type" ) {
        Roaring64Map it = shard.AllNodeIdsMap(2);
        int counter = 0;
        for(Roaring64MapSetBitForwardIterator i = it.begin() ; i != it.end() ; i++) {
          ++counter;
        }
        REQUIRE(counter == 2);

        it = shard.AllNodeIdsMap(1);
        counter = 0;
        for(Roaring64MapSetBitForwardIterator i = it.begin() ; i != it.end() ; i++) {
          ++counter;
        }
        REQUIRE(counter == 6);
      }
    }

    WHEN( "more nodes of a different type are added" ) {
      shard.NodeAddEmpty("User", 2, "one");
      shard.NodeAddEmpty("User", 2, "two");

      THEN( "the shard should get all the nodes by type" ) {
        std::vector<triton::Node> it = shard.AllNodes("User");
        REQUIRE(it.size() == 2);

        it = shard.AllNodes("Node");
        REQUIRE(it.size() == 6);

        std::vector<uint64_t> it2 = shard.AllNodeIds();
        REQUIRE(it2.size() == 8);

        it2 = shard.AllNodeIds("User");
        REQUIRE(it2.size() == 2);

        it2 = shard.AllNodeIds("User", 1, 2);
        REQUIRE(it2.size() == 1);

        it2 = shard.AllNodeIds(2, 1, 2);
        REQUIRE(it2.size() == 1);

        it2 = shard.AllNodeIds((uint64_t) 2,(uint64_t)3);
        REQUIRE(it2.size() == 3);
      }
    }
  }
}