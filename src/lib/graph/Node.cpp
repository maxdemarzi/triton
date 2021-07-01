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

#include "Node.h"

#include <utility>
#include <string>
#include <algorithm>

namespace triton {
  static const std::any tombstone_any = std::any();

  Node::Node() = default;

  Node::Node(uint64_t id, uint16_t type_id, std::string key) : id(id), type_id(type_id), key(std::move(key)) {}


  Node::Node(uint64_t id, uint16_t type_id, std::string key, const std::map<std::string, std::any>& property_map) : id(id),
                                                                                                        type_id(type_id),
                                                                                                        key(std::move(key)) {
    for(const auto& property : property_map) {
      properties.emplace_back(property.first, property.second);
    }
  }

  uint64_t Node::getId() const {
    return id;
  }

  uint16_t Node::getTypeId() const {
    return type_id;
  }

  std::string Node::getKey() const {
    return key;
  }

  std::map<std::string, std::any> Node::getProperties() {
    std::map<std::string, std::any> property_map;
    for(auto prop : properties) {
      property_map.insert({prop.getKey(), prop.getValue()});
    }
    return property_map;
  }

  sol::table Node::getPropertiesLua(sol::this_state ts) {
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

  void Node::setProperties(const std::map<std::string, std::any> &new_properties) {
    properties.clear();
    for(const auto& prop : new_properties) {
      properties.emplace_back(Property(prop.first, prop.second));
    }
  }

  void Node::deleteProperties() {
    properties.clear();
  }

  std::any Node::getProperty(const std::string& property) {
    auto result = std::find_if(std::begin(properties), std::end(properties), [property](const Property& prop) {
      return prop.getKey() == property;
    });

    if (result != std::end(properties)) {
      return result->getValue();
    }
    return tombstone_any;
  }

  void Node::setProperty(const std::string& property, const std::any& value) {
    deleteProperty(property);
    properties.emplace_back(property, value);
  }

  bool Node::deleteProperty(const std::string& property) {
  // return false if we didn't find it to erase, true otherwise.
    return properties.erase(std::remove_if(
      properties.begin(), properties.end(),
      [property](const Property& x) {
             return x.getKey() == property;
      }), properties.end()) != properties.end();

  }

  std::ostream& operator<<(std::ostream& os, const Node& node) {
    os << "{ \"id\": " << node.id << ", \"type_id\": " << node.type_id << ", \"key\": " << "\"" << node.key << "\"" << ", \"properties\": { ";
    bool initial = true;
    for (auto property : node.properties) {
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

      if(property.getValue().type() == typeid(std::vector<std::string>)) {
        os << '[';
        bool nested_initial = true;
        for (const auto& item : std::any_cast<std::vector<std::string>>(property.getValue())) {
          if (!nested_initial) {
            os << ", ";
          }
          os << item;
          nested_initial = false;
        }
        os << ']';
      }

      if(property.getValue().type() == typeid(std::vector<int64_t>)) {
        os << '[';
        bool nested_initial = true;
        for (const auto& item : std::any_cast<std::vector<int64_t>>(property.getValue())) {
          if (!nested_initial) {
            os << ", ";
          }
          os << item;
          nested_initial = false;
        }
        os << ']';
      }

      if(property.getValue().type() == typeid(std::vector<double>)) {
        os << '[';
        bool nested_initial = true;
        for (const auto& item : std::any_cast<std::vector<double>>(property.getValue())) {
          if (!nested_initial) {
            os << ", ";
          }
          os << item;
          nested_initial = false;
        }
        os << ']';
      }

      if(property.getValue().type() == typeid(std::vector<bool>)) {
        os << '[';
        bool nested_initial = true;
        for (const auto& item : std::any_cast<std::vector<bool>>(property.getValue())) {
          if (!nested_initial) {
            os << ", ";
          }
          os << item;
          nested_initial = false;
        }
        os << ']';
      }

      initial = false;
    }

    os << " } }";

    return os;
  }

} // namespace triton