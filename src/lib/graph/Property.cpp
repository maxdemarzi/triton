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

#include "Property.h"

#include <utility>

namespace triton {

  Property::Property() = default;
  Property::Property(const std::string& key, std::any value) : value(std::move(value)) {
    auto token_search = Property::token_to_id.find(key);
    if (token_search != Property::token_to_id.end()) {
      token_id = token_search->second;
    } else {
      token_id = std::hash<std::string>()(key);
      Property::token_to_id.emplace(key, token_id);
      Property::id_to_token.emplace(token_id, key);
    }
  }

  std::string Property::getKey() const {
    auto id_search = Property::id_to_token.find(token_id);
    if (id_search != Property::id_to_token.end()) {
      return Property::id_to_token.at(token_id);
    }
    return EMPTY;
  }

  std::any Property::getValue() {
    return value;
  }

} // namespace triton