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

SCENARIO( "Shard can handle Relationship Properties", "[relationship,properties]" ) {

  GIVEN("A shard with an empty node and an existing node with properties") {
    triton::Shard shard(4);
    shard.NodeTypeInsert("Node", 1);
    shard.NodeTypeInsert("User", 2);
    int64_t empty = shard.NodeAddEmpty("Node", 1, "empty");
    int64_t existing = shard.NodeAdd("Node", 1, "existing", R"({ "name":"max", "age":99, "weight":230.5 })");

    REQUIRE(empty == 256);
    REQUIRE(existing == 512);

    shard.RelationshipTypeInsert("KNOWS", 1);

    WHEN("a relationship with properties is added") {

      int64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                            R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

      THEN("the shard keeps it") {
        REQUIRE(added == 256);
        triton::Relationship added_relationship = shard.RelationshipGet(added);
        REQUIRE(added_relationship.getId() == added);
        REQUIRE(added_relationship.getTypeId() == 1);
        REQUIRE(added_relationship.getStartingNodeId() == empty);
        REQUIRE(added_relationship.getEndingNodeId() == existing);
        REQUIRE( "college" == shard.RelationshipPropertyGetString(added, "tag"));
        REQUIRE(!added_relationship.getProperties().empty());
      }
    }

    WHEN("a relationship with invalid properties is added") {

      int64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                            R"({ "active":true, "weight":1.0, "tag":"college", "invalid:3 })");

      THEN("the shard does not keep it") {
        REQUIRE(added == 0);
      }
    }

    WHEN("a string property is requested by id") {
      int64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                            R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

      THEN("the shard gets it") {
        REQUIRE(added == 256);
        triton::Relationship added_relationship = shard.RelationshipGet(added);
        REQUIRE(added_relationship.getId() == added);
        REQUIRE(added_relationship.getTypeId() == 1);
        REQUIRE(added_relationship.getStartingNodeId() == empty);
        REQUIRE(added_relationship.getEndingNodeId() == existing);
        REQUIRE( "college" == shard.RelationshipPropertyGetString(added, "tag"));
        REQUIRE(!added_relationship.getProperties().empty());
      }
    }

    WHEN("an integer property is requested by id") {
      int64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                            R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

      THEN("the shard gets it") {
        REQUIRE(added == 256);
        triton::Relationship added_relationship = shard.RelationshipGet(added);
        REQUIRE(added_relationship.getId() == added);
        REQUIRE(added_relationship.getTypeId() == 1);
        REQUIRE(added_relationship.getStartingNodeId() == empty);
        REQUIRE(added_relationship.getEndingNodeId() == existing);
        REQUIRE( 3 == shard.RelationshipPropertyGetInteger(added, "number"));
        REQUIRE(!added_relationship.getProperties().empty());
      }

    }

    WHEN("a double property is requested by id") {
      int64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                            R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

      THEN("the shard gets it") {
        REQUIRE(added == 256);
        triton::Relationship added_relationship = shard.RelationshipGet(added);
        REQUIRE(added_relationship.getId() == added);
        REQUIRE(added_relationship.getTypeId() == 1);
        REQUIRE(added_relationship.getStartingNodeId() == empty);
        REQUIRE(added_relationship.getEndingNodeId() == existing);
        REQUIRE( 1.0 == shard.RelationshipPropertyGetDouble(added, "weight"));
        REQUIRE(!added_relationship.getProperties().empty());
      }
    }

    WHEN("a boolean property is requested by id") {
      int64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                            R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

      THEN("the shard gets it") {
        REQUIRE(added == 256);
        triton::Relationship added_relationship = shard.RelationshipGet(added);
        REQUIRE(added_relationship.getId() == added);
        REQUIRE(added_relationship.getTypeId() == 1);
        REQUIRE(added_relationship.getStartingNodeId() == empty);
        REQUIRE(added_relationship.getEndingNodeId() == existing);
        REQUIRE(shard.RelationshipPropertyGetBoolean(added, "active"));
        REQUIRE(!added_relationship.getProperties().empty());
      }
    }

    WHEN("a string property is set by id") {
      int64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                            R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

      THEN("the shard sets it") {
        bool set = shard.RelationshipPropertySet(added, "name", std::string("alex"));
        REQUIRE(set);
        auto value = shard.RelationshipPropertyGetString(added, "name");
        REQUIRE("alex" == value);
      }
    }

    WHEN("a string literal property is set by id") {
      int64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                            R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

      THEN("the shard sets it") {
        bool set = shard.RelationshipPropertySet(added, "name", "alex");
        REQUIRE(set);
        auto value = shard.RelationshipPropertyGetString(added, "name");
        REQUIRE("alex" == value);
      }
    }

    WHEN("an integer property is set by id") {
      int64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                            R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

      THEN("the shard sets it") {
        bool set = shard.RelationshipPropertySet(added, "age", int64_t(55));
        REQUIRE(set);
        auto value = shard.RelationshipPropertyGetInteger(added, "age");
        REQUIRE(55 == value);
      }
    }

    WHEN("a double property is set by id") {
      int64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                            R"({ "active":true, "tag":"college", "number":3 })");

      THEN("the shard sets it") {
        bool set = shard.RelationshipPropertySet(added, "weight", 190.0);
        REQUIRE(set);
        auto value = shard.RelationshipPropertyGetDouble(added, "weight");
        REQUIRE(190.0 == value);
      }
    }

    WHEN("a boolean property is set by id") {
      int64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                            R"({ "active":true, "tag":"college", "number":3 })");

      THEN("the shard sets it") {
        bool set = shard.RelationshipPropertySet(added, "new", true);
        REQUIRE(set);
        auto value = shard.RelationshipPropertyGetBoolean(added, "new");
        REQUIRE(value);
      }
    }

    WHEN("a string property is set by id to an invalid id") {
      int64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                            R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

      THEN("the shard does not set it") {
        bool set = shard.RelationshipPropertySet(100 + added, "name", std::string("alex"));
        REQUIRE(set == false);
        auto value = shard.RelationshipPropertyGetString(100 + added, "name");
        REQUIRE(value.empty());
      }
    }

    WHEN("a string literal property is set by id to an invalid id") {
      int64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                            R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

      THEN("the shard does not set it") {
        bool set = shard.RelationshipPropertySet(100 + added, "name", "alex");
        REQUIRE(set == false);
        auto value = shard.RelationshipPropertyGetString(100 + added, "name");
        REQUIRE(value.empty());
      }
    }

    WHEN("a integer property is set by id to an invalid id") {
      int64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                            R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

      THEN("the shard does not set it") {
        bool set = shard.RelationshipPropertySet(100 + added, "age", int64_t(55));
        REQUIRE(set == false);
        auto value = shard.RelationshipPropertyGetInteger(100 + added, "age");
        REQUIRE(std::numeric_limits<int64_t>::min() == value);
      }
    }

    WHEN("a double property is set by id to an invalid id") {
      int64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                            R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

      THEN("the shard does not set it") {
        bool set = shard.RelationshipPropertySet(100 + added, "weight", 190.0);
        REQUIRE(set == false);
        auto value = shard.RelationshipPropertyGetDouble(100 + added, "weight");
        REQUIRE(std::numeric_limits<double>::min() == value);
      }
    }

    WHEN("a boolean property is set by id to an invalid id") {
      int64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                            R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

      THEN("the shard does not set it") {
        bool set = shard.RelationshipPropertySet(100 + added, "new", true);
        REQUIRE(set == false);
        auto value = shard.RelationshipPropertyGetBoolean(100 + added, "new");
        REQUIRE(!value);
      }
    }

    WHEN("a string property is set by id to a new property") {
      int64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                            R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

      THEN("the shard does set it") {
        bool set = shard.RelationshipPropertySet(added, "not_there", "alex");
        REQUIRE(set);
        auto value = shard.RelationshipPropertyGetString(added, "not_there");
        REQUIRE(value == "alex");
      }
    }

    WHEN("a integer property is set by id to an new property") {
      int64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                            R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

      THEN("the shard does set it") {
        bool set = shard.RelationshipPropertySet(added, "not_there", int64_t(55));
        REQUIRE(set);
        auto value = shard.RelationshipPropertyGetInteger(added, "not_there");
        REQUIRE(value == 55);
      }
    }

    WHEN("a double property is set by id to a new property") {
      int64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                            R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

      THEN("the shard does set it") {
        bool set = shard.RelationshipPropertySet(added, "not_there", 190.0);
        REQUIRE(set);
        auto value = shard.RelationshipPropertyGetDouble(added, "not_there");
        REQUIRE(value == 190.0);
      }
    }

    WHEN("a boolean property is set by id to a new property") {
      int64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                            R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

      THEN("the shard does set it") {
        bool set = shard.RelationshipPropertySet(added, "not_there", true);
        REQUIRE(set);
        auto value = shard.RelationshipPropertyGetBoolean(added, "not_there");
        REQUIRE(value);
      }
    }

    WHEN("a string property is deleted by id") {
      int64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                            R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

      THEN("the shard deletes it") {
        bool set = shard.RelationshipPropertyDelete(added, "tag");
        REQUIRE(set);
        auto value = shard.RelationshipPropertyGetString(added, "tag");
        REQUIRE(value.empty());
      }
    }

    WHEN("an integer property is deleted by id") {
      int64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                            R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

      THEN("the shard deletes it") {
        bool set = shard.RelationshipPropertyDelete(added, "number");
        REQUIRE(set);
        auto value = shard.RelationshipPropertyGetInteger(added, "number");
        REQUIRE(std::numeric_limits<int64_t>::min() == value);
      }
    }

    WHEN("a double property is deleted by id") {
      int64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                            R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

      THEN("the shard deletes it") {
        bool set = shard.RelationshipPropertyDelete(added, "weight");
        REQUIRE(set);
        auto value = shard.RelationshipPropertyGetDouble(added, "weight");
        REQUIRE(std::numeric_limits<double>::min() == value);
      }
    }

    WHEN("a boolean property is deleted by id") {
      int64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                            R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

      THEN("the shard deletes it") {
        bool set = shard.RelationshipPropertyDelete(added, "active");
        REQUIRE(set);
        auto value = shard.RelationshipPropertyGetBoolean(added, "active");
        REQUIRE(!value);
      }
    }

    WHEN("a non-existing property is deleted by id") {
      int64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                            R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

      THEN("the shard ignores it") {
        bool set = shard.RelationshipPropertyDelete(added, "not_there");
        REQUIRE(set == false);
        auto value = shard.RelationshipPropertyGetDouble(added, "not_there");
        REQUIRE(std::numeric_limits<double>::min() == value);
      }
    }

    WHEN("all properties are deleted by id") {
      int64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                            R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

      THEN("the shard deletes them") {
        bool set = shard.RelationshipPropertiesDelete(added+100);
        REQUIRE(!set);
        set = shard.RelationshipPropertiesDelete(added);
        REQUIRE(set);
        auto value = shard.RelationshipPropertyGetInteger(added, "number");
        REQUIRE(std::numeric_limits<int64_t>::min() == value);
      }
    }

    WHEN("all properties are set by id") {
      int64_t added = shard.RelationshipAddSameShard(1, "Node", "empty", "Node", "existing",
                                            R"({ "active":true, "weight":1.0, "tag":"college", "number":3 })");

      THEN("the shard sets them") {
        std::map<std::string, std::any> properties;
        properties.insert({"eyes", std::string("brown")});
        properties.insert({"height", 5.11});

        bool set = shard.RelationshipPropertiesSet(added, properties);
        REQUIRE(set);
        auto eyes = shard.RelationshipPropertyGetString(added, "eyes");
        REQUIRE("brown" == eyes);
        auto height = shard.RelationshipPropertyGetDouble(added, "height");
        REQUIRE(5.11 == height);
      }
    }
  }
}