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

#ifndef TRITON_PROPERTY_H
#define TRITON_PROPERTY_H

#include <any>
#include <cstdint>
#include <utility>
#include <string>
#include <tsl/sparse_map.h>

namespace triton {
  class Property {
  private:
    uint64_t token_id;
    std::any value;
    inline static tsl::sparse_map<std::string, uint64_t> token_to_id;
    inline static tsl::sparse_map<uint64_t, std::string> id_to_token;
    inline static std::string EMPTY = "";

  public:
    Property();
    Property(const std::string& key, std::any value);

    [[nodiscard]] std::string getKey() const;
    uint64_t getTokenId();
    std::any getValue();
  };

} // namespace triton
#endif//TRITON_PROPERTY_H
