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

SCENARIO( "Shard can handle Node Properties", "[node,properties]" ) {

  GIVEN("A shard with an empty node and an existing node with properties") {
    triton::Shard shard(4);
    shard.NodeTypeInsert("Node", 1);
    shard.NodeTypeInsert("User", 2);
    int64_t empty = shard.NodeAddEmpty("Node", 1, "empty");
    int64_t existing = shard.NodeAdd("Node", 1, "existing", R"({ "name":"max", "age":99, "weight":230.5, "bald":true, "nested":{ "inside":"yes" }, "vector":[1,2,3,4] })");

    REQUIRE( empty == 256 );
    REQUIRE( existing == 512 );

    WHEN("a property is requested by label/key") {
      THEN("the shard gets it") {
        auto value = shard.NodePropertyGet("Node", "existing", "name");
        REQUIRE(value.type() == typeid(std::string));
        REQUIRE("max" == std::any_cast<std::string>(value));
      }
    }

    WHEN("a string property is requested by label/key") {
      THEN("the shard gets it") {
        auto value = shard.NodePropertyGetString("Node", "existing", "name");
        REQUIRE("max" == value);
      }
    }

    WHEN("an integer property is requested by label/key") {
      THEN("the shard gets it") {
        auto value = shard.NodePropertyGetInteger("Node", "existing", "age");
        REQUIRE(99 == value);
      }
    }

    WHEN("a double property is requested by label/key") {
      THEN("the shard gets it") {
        auto value = shard.NodePropertyGetDouble("Node", "existing", "weight");
        REQUIRE(230.5 == value);
      }
    }

    WHEN("a boolean property is requested by label/key") {
      THEN("the shard gets it") {
        auto value = shard.NodePropertyGetBoolean("Node", "existing", "bald");
        REQUIRE(value);
      }
    }

    WHEN("an object property is requested by label/key") {
      THEN("the shard gets it") {
        auto value = shard.NodePropertyGetObject("Node", "existing", "nested");
        REQUIRE(!value.empty());
        REQUIRE(value.at("inside").has_value());
        REQUIRE("yes" == std::any_cast<std::string>(value.at("inside")));
      }
    }

    WHEN("a property is requested by id") {
      THEN("the shard gets it") {
        auto value = shard.NodePropertyGet(existing, "name");
        REQUIRE(value.type() == typeid(std::string));
        REQUIRE("max" == std::any_cast<std::string>(value));
      }
    }

    WHEN("a string property is requested by id") {
      THEN("the shard gets it") {
        auto value = shard.NodePropertyGetString(existing, "name");
        REQUIRE("max" == value);
      }
    }

    WHEN("an integer property is requested by id") {
      THEN("the shard gets it") {
        auto value = shard.NodePropertyGetInteger(existing, "age");
        REQUIRE(99 == value);
      }
    }

    WHEN("a double property is requested by id") {
      THEN("the shard gets it") {
        auto value = shard.NodePropertyGetDouble(existing, "weight");
        REQUIRE(230.5 == value);
      }
    }

    WHEN("a boolean property is requested by id") {
      THEN("the shard gets it") {
        auto value = shard.NodePropertyGetBoolean("Node", "existing", "bald");
        REQUIRE(value);
      }
    }

    WHEN("a string property is set by label/key to a previously empty node") {
      THEN("the shard sets it") {
        bool set = shard.NodePropertySet("Node", "empty", "name", "alex");
        REQUIRE(set);
        auto value = shard.NodePropertyGetString("Node", "empty", "name");
        REQUIRE("alex" == value);
      }
    }

    WHEN("a string property is set by label/key") {
      THEN("the shard sets it") {
        bool set = shard.NodePropertySet("Node", "existing", "name", std::string("alex"));
        REQUIRE(set);
        auto value = shard.NodePropertyGetString("Node", "existing", "name");
        REQUIRE("alex" == value);
      }
    }

    WHEN("a string literal property is set by label/key") {
      THEN("the shard sets it") {
        bool set = shard.NodePropertySet("Node", "existing", "name", "alex");
        REQUIRE(set);
        auto value = shard.NodePropertyGetString("Node", "existing", "name");
        REQUIRE("alex" == value);
      }
    }

    WHEN("an integer property is set by label/key") {
      THEN("the shard sets it") {
        bool set = shard.NodePropertySet("Node", "existing", "age", int64_t(55));
        REQUIRE(set);
        auto value = shard.NodePropertyGetInteger("Node", "existing", "age");
        REQUIRE(55 == value);
      }
    }

    WHEN("a double property is set by label/key") {
      THEN("the shard sets it") {
        bool set = shard.NodePropertySet("Node", "existing", "weight", 190.0);
        REQUIRE(set);
        auto value = shard.NodePropertyGetDouble("Node", "existing", "weight");
        REQUIRE(190.0 == value);
      }
    }

    WHEN("a boolean property is set by label/key") {
      THEN("the shard sets it") {
        bool set = shard.NodePropertySet("Node", "existing", "active", true);
        REQUIRE(set);
        auto value = shard.NodePropertyGetBoolean("Node", "existing", "active");
        REQUIRE(value);
      }
    }

    WHEN("an object property is set by label/key") {
      THEN("the shard sets it") {
        std::map<std::string, std::any> property {{"first_property", std::string("one")}, {"second_property", int64_t(9)}};
        bool set = shard.NodePropertySet("Node", "existing", "properties", property);
        REQUIRE(set);
        auto value = shard.NodePropertyGetObject("Node", "existing", "properties");
        REQUIRE(value.at("first_property").has_value());
        REQUIRE(value.at("second_property").has_value());
        REQUIRE(std::any_cast<std::string>(value.at("first_property")) == "one");
        REQUIRE(std::any_cast<std::int64_t>(value.at("second_property")) == 9);
      }
    }

    WHEN("an object property from JSON is set by label/key") {
      THEN("the shard sets it") {
        std::string property(R"({"first_property": "one", "second_property":9 })");
        bool set = shard.NodePropertySetFromJson("Node", "existing", "properties", property);
        REQUIRE(set);
        auto value = shard.NodePropertyGetObject("Node", "existing", "properties");
        REQUIRE(value.at("first_property").has_value());
        REQUIRE(value.at("second_property").has_value());
        REQUIRE(std::any_cast<std::string>(value.at("first_property")) == "one");
        REQUIRE(std::any_cast<std::int64_t>(value.at("second_property")) == 9);
      }
    }

    WHEN("a string property is set by id") {
      THEN("the shard sets it") {
        bool set = shard.NodePropertySet(existing, "name", std::string("alex"));
        REQUIRE(set);
        auto value = shard.NodePropertyGetString(existing, "name");
        REQUIRE("alex" == value);
      }
    }

    WHEN("a string literal property is set by id") {
      THEN("the shard sets it") {
        bool set = shard.NodePropertySet(existing, "name", "alex");
        REQUIRE(set);
        auto value = shard.NodePropertyGetString(existing, "name");
        REQUIRE("alex" == value);
      }
    }

    WHEN("an integer property is set by id") {
      THEN("the shard sets it") {
        bool set = shard.NodePropertySet(existing, "age", int64_t(55));
        REQUIRE(set);
        auto value = shard.NodePropertyGetInteger(existing, "age");
        REQUIRE(55 == value);
      }
    }

    WHEN("a double property is set by id") {
      THEN("the shard sets it") {
        bool set = shard.NodePropertySet(existing, "weight", 190.0);
        REQUIRE(set);
        auto value = shard.NodePropertyGetDouble(existing, "weight");
        REQUIRE(190.0 == value);
      }
    }

    WHEN("a boolean property is set by id") {
      THEN("the shard sets it") {
        bool set = shard.NodePropertySet(existing, "active", true);
        REQUIRE(set);
        auto value = shard.NodePropertyGetBoolean(existing, "active");
        REQUIRE(value);
      }
    }

    WHEN("an object property is set by id") {
      THEN("the shard sets it") {
        std::map<std::string, std::any> property {{"first_property", std::string("one")}, {"second_property", int64_t(9)}};
        bool set = shard.NodePropertySet(existing, "properties", property);
        REQUIRE(set);
        auto value = shard.NodePropertyGetObject(existing, "properties");
        REQUIRE(value.at("first_property").has_value());
        REQUIRE(value.at("second_property").has_value());
        REQUIRE(std::any_cast<std::string>(value.at("first_property")) == "one");
        REQUIRE(std::any_cast<std::int64_t>(value.at("second_property")) == 9);
      }
    }

    WHEN("an object property from JSON is set by id") {
      THEN("the shard sets it") {
        std::string property(R"({"first_property": "one", "second_property":9 })");
        bool set = shard.NodePropertySetFromJson(existing, "properties", property);
        REQUIRE(set);
        auto value = shard.NodePropertyGetObject(existing, "properties");
        REQUIRE(value.at("first_property").has_value());
        REQUIRE(value.at("second_property").has_value());
        REQUIRE(std::any_cast<std::string>(value.at("first_property")) == "one");
        REQUIRE(std::any_cast<std::int64_t>(value.at("second_property")) == 9);
      }
    }

    WHEN("a string property is set by label/key to an invalid label") {
      THEN("the shard does not set it") {
        bool set = shard.NodePropertySet("NotThere", "existing", "name", "alex");
        REQUIRE(set == false);
        auto value = shard.NodePropertyGetString("NotThere", "existing", "name");
        REQUIRE(value.empty());
      }
    }

    WHEN("an integer property is set by label/key to an invalid label") {
      THEN("the shard does not set it") {
        bool set = shard.NodePropertySet("NotThere", "existing", "age", int64_t(55));
        REQUIRE(set == false);
        auto value = shard.NodePropertyGetInteger("NotThere", "existing", "age");
        REQUIRE(std::numeric_limits<int64_t>::min() == value);
      }
    }

    WHEN("a double property is set by label/key to an invalid label") {
      THEN("the shard does not set it") {
        bool set = shard.NodePropertySet("NotThere", "existing", "weight", 190.0);
        REQUIRE(set == false);
        auto value = shard.NodePropertyGetDouble("NotThere", "existing", "weight");
        REQUIRE(std::numeric_limits<double>::min() == value);
      }
    }

    WHEN("a boolean property is set by label/key to an invalid label") {
      THEN("the shard does not set it") {
        bool set = shard.NodePropertySet("NotThere", "existing", "active", true);
        REQUIRE(set == false);
        auto value = shard.NodePropertyGetBoolean("NotThere", "existing", "active");
        REQUIRE(!value);
      }
    }

    WHEN("a string property is set by label/key to an invalid key") {
      THEN("the shard does not set it") {
        bool set = shard.NodePropertySet("Node", "not_existing", "name", "alex");
        REQUIRE(set == false);
        auto value = shard.NodePropertyGetString("Node", "not_existing", "name");
        REQUIRE(value.empty());
      }
    }

    WHEN("an integer property is set by label/key to an invalid key") {
      THEN("the shard does not set it") {
        bool set = shard.NodePropertySet("Node", "not_existing", "age", int64_t(55));
        REQUIRE(set == false);
        auto value = shard.NodePropertyGetInteger("Node", "not_existing", "age");
        REQUIRE(std::numeric_limits<int64_t>::min() == value);
      }
    }

    WHEN("a double property is set by label/key to an invalid key") {
      THEN("the shard does not set it") {
        bool set = shard.NodePropertySet("Node", "not_existing", "weight", 190.0);
        REQUIRE(set == false);
        auto value = shard.NodePropertyGetDouble("Node", "not_existing", "weight");
        REQUIRE(std::numeric_limits<double>::min() == value);
      }
    }

    WHEN("a boolean property is set by label/key to an invalid key") {
      THEN("the shard does not set it") {
        bool set = shard.NodePropertySet("Node", "not_existing", "active", true);
        REQUIRE(set == false);
        auto value = shard.NodePropertyGetBoolean("Node", "not_existing", "active");
        REQUIRE(!value);
      }
    }

    WHEN("a string property is set by id to an invalid id") {
      THEN("the shard does not set it") {
        bool set = shard.NodePropertySet(100 + existing, "name", "alex");
        REQUIRE(set == false);
        auto value = shard.NodePropertyGetString(100 + existing, "name");
        REQUIRE(value.empty());
      }
    }

    WHEN("a integer property is set by id to an invalid id") {
      THEN("the shard does not set it") {
        bool set = shard.NodePropertySet(100 + existing, "age", int64_t(55));
        REQUIRE(set == false);
        auto value = shard.NodePropertyGetInteger(100 + existing, "age");
        REQUIRE(std::numeric_limits<int64_t>::min() == value);
      }
    }

    WHEN("a double property is set by id to an invalid id") {
      THEN("the shard does not set it") {
        bool set = shard.NodePropertySet(100 + existing, "weight", 190.0);
        REQUIRE(set == false);
        auto value = shard.NodePropertyGetDouble(100 + existing, "weight");
        REQUIRE(std::numeric_limits<double>::min() == value);
      }
    }

    WHEN("a boolean property is set by id to an invalid id") {
      THEN("the shard does not set it") {
        bool set = shard.NodePropertySet(100 + existing, "active", true);
        REQUIRE(set == false);
        auto value = shard.NodePropertyGetBoolean(100 + existing, "active");
        REQUIRE(!value);
      }
    }

    WHEN("a string property is set by id to a new property") {
      THEN("the shard does set it") {
        bool set = shard.NodePropertySet(existing, "not_there", "alex");
        REQUIRE(set);
        auto value = shard.NodePropertyGetString(existing, "not_there");
        REQUIRE(value == "alex");
      }
    }

    WHEN("a integer property is set by id to an new property") {
      THEN("the shard does set it") {
        bool set = shard.NodePropertySet(existing, "not_there", int64_t(55));
        REQUIRE(set);
        auto value = shard.NodePropertyGetInteger(existing, "not_there");
        REQUIRE(value == 55);
      }
    }

    WHEN("a double property is set by id to a new property") {
      THEN("the shard does set it") {
        bool set = shard.NodePropertySet(existing, "not_there", 190.0);
        REQUIRE(set);
        auto value = shard.NodePropertyGetDouble(existing, "not_there");
        REQUIRE(value == 190.0);
      }
    }

    WHEN("a boolean property is set by id to a new property") {
      THEN("the shard does set it") {
        bool set = shard.NodePropertySet(existing, "not_there", true);
        REQUIRE(set);
        auto value = shard.NodePropertyGetBoolean(existing, "not_there");
        REQUIRE(value);
      }
    }

    WHEN("a string property is deleted by label/key") {
      THEN("the shard deletes it") {
        bool set = shard.NodePropertyDelete("Node", "existing", "name");
        REQUIRE(set);
        auto value = shard.NodePropertyGetString("Node", "existing", "name");
        REQUIRE(value.empty());
      }
    }

    WHEN("an integer property is deleted by label/key") {
      THEN("the shard deletes it") {
        bool set = shard.NodePropertyDelete("Node", "existing", "age");
        REQUIRE(set);
        auto value = shard.NodePropertyGetInteger("Node", "existing", "age");
        REQUIRE(std::numeric_limits<int64_t>::min() == value);
      }
    }

    WHEN("a double property is deleted by label/key") {
      THEN("the shard deletes it") {
        bool set = shard.NodePropertyDelete("Node", "existing", "weight");
        REQUIRE(set);
        auto value = shard.NodePropertyGetDouble("Node", "existing", "weight");
        REQUIRE(std::numeric_limits<double>::min() == value);
      }
    }

    WHEN("a string property is deleted by id") {
      THEN("the shard deletes it") {
        bool set = shard.NodePropertyDelete(existing, "name");
        REQUIRE(set);
        auto value = shard.NodePropertyGetString(existing, "name");
        REQUIRE(value.empty());
      }
    }

    WHEN("an integer property is deleted by id") {
      THEN("the shard deletes it") {
        bool set = shard.NodePropertyDelete(existing, "age");
        REQUIRE(set);
        auto value = shard.NodePropertyGetInteger(existing, "age");
        REQUIRE(std::numeric_limits<int64_t>::min() == value);
      }
    }

    WHEN("a double property is deleted by id") {
      THEN("the shard deletes it") {
        bool set = shard.NodePropertyDelete(existing, "weight");
        REQUIRE(set);
        auto value = shard.NodePropertyGetDouble(existing, "weight");
        REQUIRE(std::numeric_limits<double>::min() == value);
      }
    }

    WHEN("a non-existing property is deleted by label/key") {
      THEN("the shard deletes it") {
        bool set = shard.NodePropertyDelete("Node", "existing", "not_there");
        REQUIRE(set == false);
        auto value = shard.NodePropertyGetDouble("Node", "existing", "not_there");
        REQUIRE(std::numeric_limits<double>::min() == value);
      }
    }

    WHEN("a non-existing property is deleted by id") {
      THEN("the shard ignores it") {
        bool set = shard.NodePropertyDelete(existing, "not_there");
        REQUIRE(set == false);
        auto value = shard.NodePropertyGetDouble(existing, "not_there");
        REQUIRE(std::numeric_limits<double>::min() == value);
      }
    }

    WHEN("a double property is deleted by label/key to an invalid label") {
      THEN("the shard does not delete it") {
        bool set = shard.NodePropertyDelete("NotThere", "existing", "weight");
        REQUIRE(set == false);
        auto value = shard.NodePropertyGetDouble("NotThere", "existing", "weight");
        REQUIRE(std::numeric_limits<double>::min() == value);
      }
    }

    WHEN("a string property is deleted by label/key to an invalid key") {
      THEN("the shard does not delete it") {
        bool set = shard.NodePropertyDelete("Node", "not_existing", "name");
        REQUIRE(set == false);
        auto value = shard.NodePropertyGetString("Node", "not_existing", "name");
        REQUIRE(value.empty());
      }
    }

    WHEN("all properties are deleted by label/key") {
      THEN("the shard deletes them") {
        bool set = shard.NodePropertiesDelete("Node", "existing");
        REQUIRE(set);
        auto value = shard.NodePropertyGetInteger("Node", "existing", "age");
        REQUIRE(std::numeric_limits<int64_t>::min() == value);
      }
    }

    WHEN("all properties are deleted by id") {
      THEN("the shard deletes them") {
        bool set = shard.NodePropertiesDelete(existing);
        REQUIRE(set);
        auto value = shard.NodePropertyGetInteger(existing, "age");
        REQUIRE(std::numeric_limits<int64_t>::min() == value);
      }
    }

    WHEN("all properties are deleted by label/key to an invalid label") {
      THEN("the shard deletes them") {
        bool set = shard.NodePropertiesDelete("NotThere", "existing");
        REQUIRE(set == false);
        auto value = shard.NodePropertyGetInteger("NotThere", "existing", "age");
        REQUIRE(std::numeric_limits<int64_t>::min() == value);
      }
    }

    WHEN("all properties are deleted by label/key to an invalid key") {
      THEN("the shard ignores them") {
        bool set = shard.NodePropertiesDelete("Node", "not_existing");
        REQUIRE(set == false);
        auto value = shard.NodePropertyGetInteger("Node", "not_existing", "age");
        REQUIRE(std::numeric_limits<int64_t>::min() == value);
      }
    }

    WHEN("all properties are set by label/key") {
      THEN("the shard sets them") {
        std::map<std::string, std::any> properties;

        properties.insert({"eyes", std::string("brown")});
        properties.insert({"height", 5.11});
        bool set = shard.NodePropertiesSet("Node", "existing", properties);
        REQUIRE(set);
        auto eyes = shard.NodePropertyGetString("Node", "existing", "eyes");
        REQUIRE("brown" == eyes);
        auto height = shard.NodePropertyGetDouble("Node", "existing", "height");
        REQUIRE(5.11 == height);
      }
    }

    WHEN("all properties are set by id") {
      THEN("the shard sets them") {
        std::map<std::string, std::any> properties;
        properties.insert({"eyes", std::string("brown")});
        properties.insert({"height", 5.11});

        bool set = shard.NodePropertiesSet(existing, properties);
        REQUIRE(set);
        auto value = shard.NodePropertyGetString(existing, "name");
        auto eyes = shard.NodePropertyGetString("Node", "existing", "eyes");
        REQUIRE("brown" == eyes);
        auto height = shard.NodePropertyGetDouble("Node", "existing", "height");
        REQUIRE(5.11 == height);

      }
    }
  }
}