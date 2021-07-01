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

SCENARIO( "Shard can handle Node Degrees", "[node]" ) {

  GIVEN( "A shard with an empty node and an existing node with properties" ) {
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

    shard.RelationshipTypeInsert("FRIENDS", 1);
    shard.RelationshipTypeInsert("ENEMIES", 2);
    
    WHEN( "and relationships are added" ) {
      shard.RelationshipAddEmptySameShard(1, four, five);
      shard.RelationshipAddEmptySameShard(2, five, four);

      THEN( "the shard should get the correct degree" ) {
        int64_t degree = shard.NodeGetDegree("Node","four");
        REQUIRE(2 == degree);
      }
    }

    WHEN( "and relationships are added" ) {
      shard.RelationshipAddEmptySameShard(1, four, five);
      shard.RelationshipAddEmptySameShard(2, five, four);

      THEN( "the shard should get the correct incoming degree" ) {
        int64_t degree = shard.NodeGetDegree("Node","four", IN);
        REQUIRE(1 == degree);
      }
    }

    WHEN( "and relationships are added" ) {
      shard.RelationshipAddEmptySameShard(1, four, five);
      shard.RelationshipAddEmptySameShard(2, five, four);

      THEN( "the shard should get the correct outgoing degree" ) {
        int64_t degree = shard.NodeGetDegree("Node","four", OUT);
        REQUIRE(1 == degree);
      }
    }

    WHEN( "and relationships are added" ) {
      shard.RelationshipAddEmptySameShard(1, four, five);
      shard.RelationshipAddEmptySameShard(2, five, four);

      THEN( "the shard should get the correct BOTH degree" ) {
        int64_t degree = shard.NodeGetDegree("Node","four", BOTH);
        REQUIRE(2 == degree);
      }
    }

    WHEN( "and relationships are added" ) {
      shard.RelationshipAddEmptySameShard(1, four, five);
      shard.RelationshipAddEmptySameShard(2, five, four);

      THEN( "the shard should get the correct IN degree of TYPE" ) {
        int64_t degree = shard.NodeGetDegree("Node","four", IN, "ENEMIES");
        REQUIRE(1 == degree);
      }
    }

    WHEN( "and relationships are added" ) {
      shard.RelationshipAddEmptySameShard(1, four, five);
      shard.RelationshipAddEmptySameShard(2, five, four);

      THEN( "the shard should get the correct OUT degree of TYPE" ) {
        int64_t degree = shard.NodeGetDegree("Node","four", OUT, "ENEMIES");
        REQUIRE(0 == degree);
      }
    }

    WHEN( "and relationships are added" ) {
      shard.RelationshipAddEmptySameShard(1, four, five);
      shard.RelationshipAddEmptySameShard(2, five, four);

      THEN( "the shard should get the correct BOTH degree of TYPE" ) {
        int64_t degree = shard.NodeGetDegree("Node","four", BOTH, "ENEMIES");
        REQUIRE(1 == degree);
      }
    }

    WHEN( "and relationships are added" ) {
      shard.RelationshipAddEmptySameShard(1, four, five);
      shard.RelationshipAddEmptySameShard(2, five, four);

      THEN( "the shard should get the correct IN degree of Multiple TYPE" ) {
        int64_t degree = shard.NodeGetDegree("Node","four", IN, std::vector<std::string>{"FRIENDS", "ENEMIES"});
        REQUIRE(1 == degree);
      }
    }

    WHEN( "and relationships are added" ) {
      shard.RelationshipAddEmptySameShard(1, four, five);
      shard.RelationshipAddEmptySameShard(2, five, four);

      THEN( "the shard should get the correct OUT degree of Multiple TYPE" ) {
        int64_t degree = shard.NodeGetDegree("Node","four", OUT, std::vector<std::string>{"FRIENDS", "ENEMIES"});
        REQUIRE(1 == degree);
      }
    }

    WHEN( "and relationships are added" ) {
      shard.RelationshipAddEmptySameShard(1, four, five);
      shard.RelationshipAddEmptySameShard(2, five, four);

      THEN( "the shard should get the correct BOTH degree of Multiple TYPE" ) {
        int64_t degree = shard.NodeGetDegree("Node","four", BOTH, std::vector<std::string>{"FRIENDS", "ENEMIES"});
        REQUIRE(2 == degree);
      }
    }

    WHEN( "an relationships are added" ) {
      shard.RelationshipAddEmptySameShard(1, four, five);
      shard.RelationshipAddEmptySameShard(2, five, four);

      THEN( "the shard should get the correct degree" ) {
        int64_t degree = shard.NodeGetDegree(four);
        REQUIRE(2 == degree);
      }
    }

    WHEN( "and relationships are added" ) {
      shard.RelationshipAddEmptySameShard(1, four, five);
      shard.RelationshipAddEmptySameShard(2, five, four);

      THEN( "the shard should get the correct incoming degree" ) {
        int64_t degree = shard.NodeGetDegree(four, IN);
        REQUIRE(1 == degree);
      }
    }

    WHEN( "and relationships are added" ) {
      shard.RelationshipAddEmptySameShard(1, four, five);
      shard.RelationshipAddEmptySameShard(2, five, four);

      THEN( "the shard should get the correct outgoing degree" ) {
        int64_t degree = shard.NodeGetDegree(four, OUT);
        REQUIRE(1 == degree);
      }
    }

    WHEN( "and relationships are added" ) {
      shard.RelationshipAddEmptySameShard(1, four, five);
      shard.RelationshipAddEmptySameShard(2, five, four);

      THEN( "the shard should get the correct BOTH degree" ) {
        int64_t degree = shard.NodeGetDegree(four, BOTH);
        REQUIRE(2 == degree);
      }
    }

    WHEN( "and relationships are added" ) {
      shard.RelationshipAddEmptySameShard(1, four, five);
      shard.RelationshipAddEmptySameShard(2, five, four);

      THEN( "the shard should get the correct IN degree of TYPE" ) {
        int64_t degree = shard.NodeGetDegree(four, IN, "ENEMIES");
        REQUIRE(1 == degree);
      }
    }

    WHEN( "and relationships are added" ) {
      shard.RelationshipAddEmptySameShard(1, four, five);
      shard.RelationshipAddEmptySameShard(2, five, four);

      THEN( "the shard should get the correct OUT degree of TYPE" ) {
        int64_t degree = shard.NodeGetDegree(four, OUT, "ENEMIES");
        REQUIRE(0 == degree);
      }
    }

    WHEN( "and relationships are added" ) {
      shard.RelationshipAddEmptySameShard(1, four, five);
      shard.RelationshipAddEmptySameShard(2, five, four);

      THEN( "the shard should get the correct BOTH degree of TYPE" ) {
        int64_t degree = shard.NodeGetDegree(four, BOTH, "ENEMIES");
        REQUIRE(1 == degree);
      }
    }

    WHEN( "and relationships are added" ) {
      shard.RelationshipAddEmptySameShard(1, four, five);
      shard.RelationshipAddEmptySameShard(2, five, four);

      THEN( "the shard should get the correct IN degree of Multiple TYPE" ) {
        int64_t degree = shard.NodeGetDegree(four, IN, std::vector<std::string>{"FRIENDS", "ENEMIES"});
        REQUIRE(1 == degree);
      }
    }

    WHEN( "and relationships are added" ) {
      shard.RelationshipAddEmptySameShard(1, four, five);
      shard.RelationshipAddEmptySameShard(2, five, four);

      THEN( "the shard should get the correct OUT degree of Multiple TYPE" ) {
        int64_t degree = shard.NodeGetDegree(four, OUT, std::vector<std::string>{"FRIENDS", "ENEMIES"});
        REQUIRE(1 == degree);
      }
    }

    WHEN( "and relationships are added" ) {
      shard.RelationshipAddEmptySameShard(1, four, five);
      shard.RelationshipAddEmptySameShard(2, five, four);

      THEN( "the shard should get the correct BOTH degree of Multiple TYPE" ) {
        int64_t degree = shard.NodeGetDegree(four, BOTH, std::vector<std::string>{"FRIENDS", "ENEMIES"});
        REQUIRE(2 == degree);
      }
    }
  }
}