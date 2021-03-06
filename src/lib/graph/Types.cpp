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

#include "Types.h"

namespace triton {
  Types::Types() : type_to_id(), id_to_type() {
    // start with empty blank type
    type_to_id.emplace("", 0);
    id_to_type.emplace(0, "");
    ids = std::unordered_map<uint16_t, Roaring64Map>();
    ids.emplace(0, Roaring64Map());
  }

  uint16_t Types::getTypeId(const std::string &token) {
    auto token_search = type_to_id.find(token);
    if (token_search != type_to_id.end()) {
      return token_search->second;
    }
    return 0;
  }

  uint16_t Types::insertOrGetTypeId(const std::string &token) {
    // Get
    auto token_search = type_to_id.find(token);
    if (token_search != type_to_id.end()) {
      return token_search->second;
    }
    // Insert
    uint16_t token_id = type_to_id.size();
    type_to_id.emplace(token, token_id);
    id_to_type.emplace(token_id, token);
    ids.emplace(token_id, Roaring64Map());
    return token_id;
  }

  std::string Types::getType(uint16_t type_id) {
    auto token_search = id_to_type.find(type_id);
    if (token_search != id_to_type.end()) {
      return token_search->second;
    }
    // If not found return empty
    return id_to_type.at(0);
  }

  bool Types::addId(uint16_t type_id, uint64_t id) {
    if (ValidTypeId(type_id)) {
      ids.at(type_id).add(id);
      return true;
    }
    // If not valid return false
    return false;
  }

  bool Types::removeId(uint16_t type_id, uint64_t id) {
    if (ValidTypeId(type_id)) {
      ids.at(type_id).remove(id);
      return true;
    }
    // If not valid return false
    return false;
  }

  bool Types::containsId(uint16_t type_id, uint64_t id) {
    if (ValidTypeId(type_id)) {
      return ids.at(type_id).contains(id);
    }
    // If not valid return false
    return false;
  }

  Roaring64Map Types::getIds() const {
    Roaring64Map allIds;
    for(const auto& entry : ids) {
      allIds.operator|=(entry.second);
    }
    return allIds;
  }

  Roaring64Map Types::getIds(uint16_t type_id) {
    if (ValidTypeId(type_id)) {
      return ids.at(type_id);
    }
    return ids.at(0);
  }

  bool Types::ValidTypeId(uint16_t type_id) const {
    // TypeId must be greater than zero
    return (type_id > 0 && type_id < id_to_type.size());
  }

  uint64_t Types::getCount(uint16_t type_id) {
    if (ValidTypeId(type_id)) {
      return ids.at(type_id).cardinality();
    }
    // If not valid return 0
    return 0;
  }

  uint16_t Types::getSize() const {
    return id_to_type.size() - 1;
  }

  std::set<std::string> Types::getTypes() {
    std::set<std::string> types;
    for (auto &it : id_to_type) {
      if (it.first > 0) {
        types.insert(it.second);
      }
    }

    return types;
  }

  std::set<uint16_t> Types::getTypeIds() {
    std::set<uint16_t> type_ids;
    for (auto &it : id_to_type) {
      if (it.first > 0) {
        type_ids.insert(it.first);
      }
    }

    return type_ids;
  }

  std::map<uint16_t,uint64_t> Types::getCounts() {
    std::map<uint16_t,uint64_t> counts;
    for (auto &it : id_to_type) {
      if (it.first > 0) {
        counts.insert({it.first, ids.at(it.first).cardinality()});
      }
    }

    return counts;
  }

  bool Types::addTypeId(const std::string& token, uint16_t token_id) {
    auto token_search = type_to_id.find(token);
    if (token_search != type_to_id.end()) {
      // Type already exists
      return false;
    } else {
      auto id_search = id_to_type.find(token_id);
      if (id_search != id_to_type.end()) {
        // Id already exists
        return false;
      }
      type_to_id.emplace(token, token_id);
      id_to_type.emplace(token_id, token);
      ids.emplace(token_id, Roaring64Map());
      return false;
    }
  }

}// namespace triton