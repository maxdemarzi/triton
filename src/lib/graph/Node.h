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

#ifndef TRITON_NODE_H
#define TRITON_NODE_H

#include <any>
#include <cstdint>
#include <iostream>
#include <map>
#include <string>
#include <vector>
#include "Property.h"
#include <sol.hpp>

namespace triton {

  class Node {
  public:
      Node();
      Node(uint64_t id, uint16_t type_id, std::string key);
      Node(uint64_t id, uint16_t type_id, std::string key, const std::map<std::string, std::any>& properties);

  private:
      uint64_t id{};
      uint16_t type_id{};
      std::string key;
      std::vector<Property> properties;

  public:
      [[nodiscard]] uint64_t getId() const;

      [[nodiscard]] uint16_t getTypeId() const;

      [[nodiscard]] std::string getKey() const;

      [[nodiscard]] std::map<std::string, std::any> getProperties();

      sol::table getPropertiesLua(sol::this_state ts);

      std::any getProperty(const std::string& property);

      void setProperty(const std::string& property, const std::any& value);

      bool deleteProperty(const std::string& property);

      void setProperties(const std::map<std::string, std::any> &new_properties);

      void deleteProperties();

      friend std::ostream& operator<<(std::ostream& os, const Node& node);
  };

} // namespace triton

#endif//TRITON_NODE_H
