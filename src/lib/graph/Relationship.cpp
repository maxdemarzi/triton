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

#include "Relationship.h"
#include <ostream>
#include <algorithm>

namespace triton {
  static const std::any tombstone_any = std::any();

  Relationship::Relationship(uint64_t id,
                             uint64_t startingNodeId,
                             uint64_t endingNodeId,
                             uint16_t type,
                             const std::map<std::string, std::any>& property_map) : id(id),
                                                                           starting_node_id(startingNodeId),
                                                                           ending_node_id(endingNodeId),
                                                                           type(type) {
    for(const auto& property : property_map) {
      properties.emplace_back(property.first, property.second);
    }
  }

  Relationship::Relationship(uint64_t id,
                             uint64_t startingNodeId,
                             uint64_t endingNodeId,
                             uint16_t type) : id(id),
                                              starting_node_id(startingNodeId),
                                              ending_node_id(endingNodeId),
                                              type(type) {}

  uint64_t Relationship::getId() const {
    return id;
  }

  uint16_t Relationship::getTypeId() const {
    return type;
  }

  uint64_t Relationship::getStartingNodeId() const {
    return starting_node_id;
  }

  uint64_t Relationship::getEndingNodeId() const {
    return ending_node_id;
  }

  std::map<std::string, std::any> Relationship::getProperties() {
    std::map<std::string, std::any> property_map;
    for(auto prop : properties) {
      property_map.insert({prop.getKey(), prop.getValue()});
    }
    return property_map;
  }

  sol::table Relationship::getPropertiesLua(sol::this_state ts) {
    sol::state_view lua = ts;
    sol::table property_map = lua.create_table();
    for(auto prop : getProperties()) {
      const auto& value_type = prop.second.type();

      if(value_type == typeid(std::string)) {
        property_map[prop.first] = sol::make_object(lua, std::any_cast<std::string>(prop.second));
      }

      if(value_type == typeid(int64_t)) {
        property_map[prop.first] = sol::make_object(lua, std::any_cast<int64_t>(prop.second));
      }

      if(value_type == typeid(double)) {
        property_map[prop.first] = sol::make_object(lua, std::any_cast<double>(prop.second));
      }

      if(value_type == typeid(bool)) {
        property_map[prop.first] = sol::make_object(lua, std::any_cast<bool>(prop.second));
      }

      if(value_type == typeid(std::vector<std::string>)) {
        return sol::make_object(lua, sol::as_table(std::any_cast<std::vector<std::string>>(prop.second)));
      }

      if(value_type == typeid(std::vector<int64_t>)) {
        return sol::make_object(lua, sol::as_table(std::any_cast<std::vector<int64_t>>(prop.second)));
      }

      if(value_type == typeid(std::vector<double>)) {
        return sol::make_object(lua, sol::as_table(std::any_cast<std::vector<double>>(prop.second)));
      }

      if(value_type == typeid(std::vector<bool>)) {
        return sol::make_object(lua, sol::as_table(std::any_cast<std::vector<bool>>(prop.second)));
      }

    }
    return sol::as_table(property_map);
  }

  std::any Relationship::getProperty(const std::string& property) {
    auto result = std::find_if(std::begin(properties), std::end(properties), [property](const Property& prop) {
           return prop.getKey() == property;
    });

    if (result != std::end(properties)) {
      return result->getValue();
    }
    return tombstone_any;
  }

  void Relationship::setProperty(const std::string& property, const std::any& value) {
    deleteProperty(property);
    properties.emplace_back(property, value);
  }

  bool Relationship::deleteProperty(const std::string& property) {
    // return false if we didn't find it to erase, true otherwise.
    return properties.erase(std::remove_if(
      properties.begin(), properties.end(),
      [property](const Property& x) {
             return x.getKey() == property;
      }), properties.end()) != properties.end();
  }

  void Relationship::setProperties(const std::map<std::string, std::any> &new_properties) {
    properties.clear();
    for(const auto& prop : new_properties) {
      properties.emplace_back(Property(prop.first, prop.second));
    }
  }

  void Relationship::deleteProperties() {
    properties.clear();
  }

  std::ostream &operator<<(std::ostream &os, const Relationship &relationship) {
    os << "{ \"id\": " << relationship.id << ", \"type_id\": " << relationship.type << ", \"starting_node_id\": " << relationship.starting_node_id << ", \"ending_node_id\": " << relationship.ending_node_id << ", \"properties\": { ";
    bool initial = true;
    for (auto property : relationship.properties) {
      if (!initial) {
        os << ", ";
      }
      os << "\"" << property.getKey() << "\": ";

      if(property.getValue().type() == typeid(std::string)) {
        os << "\"" << std::any_cast<std::string>(property.getValue()) << "\"";
      }
      if(property.getValue().type() == typeid(int64_t)) {
        os << std::any_cast<int64_t>(property.getValue());
      }
      if(property.getValue().type() == typeid(double)) {
        os << std::any_cast<double>(property.getValue());
      }
      if(property.getValue().type() == typeid(bool)) {
        if (std::any_cast<bool>(property.getValue())) {
          os << "true" ;
        } else {
          os << "false";
        }
      }
      // todo: deal with nested objects and arrays

      initial = false;
    }
    os << " } }";

    return os;
  }

  Relationship::Relationship() = default;


} // namespace triton