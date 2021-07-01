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

SCENARIO( "Shard can handle internal and external id conversions", "[ids]" ) {
  GIVEN("An empty shard") {
    triton::Shard shard(4);

    WHEN("we convert external ids to internal ones") {
      THEN("we convert 256 to 1") {
        REQUIRE(shard.externalToInternal((uint64_t)256) == 1);
      }

      THEN("we convert 257 to 1") {
        REQUIRE(shard.externalToInternal((uint64_t)257) == 1);
      }

      THEN("we convert 258 to 1") {
        REQUIRE(shard.externalToInternal((uint64_t)258) == 1);
      }

      THEN("we convert 259 to 1") {
        REQUIRE(shard.externalToInternal((uint64_t)259) == 1);
      }

      THEN("we convert 513 to 2") {
        REQUIRE(shard.externalToInternal((uint64_t)513) == 2);
      }
    }

    WHEN("we convert internal ids to external ones") {
      THEN("we convert 1 to 256") {
        REQUIRE(shard.internalToExternal((uint64_t)1) == 256);
      }

      THEN("we convert 2 to 512") {
        REQUIRE(shard.internalToExternal((uint64_t)2) == 512);
      }

      THEN("we convert 3 to 768") {
        REQUIRE(shard.internalToExternal((uint64_t)3) == 768);
      }

      THEN("we convert 4 to 1024") {
        REQUIRE(shard.internalToExternal((uint64_t)4) == 1024);
      }

      THEN("we convert 5 to 1280") {
        REQUIRE(shard.internalToExternal((uint64_t)5) == 1280);
      }
    }
  }
}