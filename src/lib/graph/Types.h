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

#ifndef TRITON_TYPES_H
#define TRITON_TYPES_H

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>
#include <set>
#include <roaring/roaring64map.hh>

namespace triton {
  class Types {
  public:
    Types();

    uint16_t getTypeId(const std::string &);

    uint16_t insertOrGetTypeId(const std::string &);

    std::string getType(uint16_t);

    bool addId(uint16_t, uint64_t);

    bool removeId(uint16_t, uint64_t);

    bool containsId(uint16_t, uint64_t);

    Roaring64Map getIds() const;

    Roaring64Map getIds(uint16_t);

    bool ValidTypeId(uint16_t) const;

    uint64_t getCount(uint16_t);

    uint16_t getSize() const;

    std::set<std::string> getTypes();

    std::set<uint16_t> getTypeIds();

    std::map<uint16_t,uint64_t> getCounts();

    bool addTypeId(const std::string&, uint16_t);

  private:
    std::unordered_map<std::string, uint16_t> type_to_id;
    std::unordered_map<uint16_t, std::string> id_to_type;
    std::unordered_map<uint16_t, Roaring64Map> ids;
  };
} // namespace triton


#endif//TRITON_TYPES_H
