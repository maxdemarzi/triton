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

#include "Shard.h"
#include <iostream>
#include <simdjson/error.h>
#include <utilities/StringUtils.h>

#define SOL_ALL_SAFETIES_ON 1

namespace triton {

  static const unsigned int SHIFTED_BITS = 8U;
  static const unsigned int MASK = 0x00000000000000FFU;
  static const unsigned int SIXTY_FOUR = 64U;
  static const std::string EXCEPTION = "An exception has occurred: ";
  
  void Shard::speak() {
    std::stringstream ss;
    ss << "Hello from shard " << seastar::this_shard_id() << '\n';
    std::cout << ss.str();
  }

  seastar::future<> Shard::stop() {
    std::stringstream ss;
    ss << "Good bye from Shard " << seastar::this_shard_id() << '\n';
    std::cout << ss.str();
    return seastar::make_ready_future<>();
  }

  void Shard::clear() {
    node_keys.clear();
    nodes.clear();
    nodes.shrink_to_fit();
    relationships.clear();
    relationships.shrink_to_fit();
    outgoing_relationships.clear();
    outgoing_relationships.shrink_to_fit();
    incoming_relationships.clear();
    incoming_relationships.shrink_to_fit();
    deleted_nodes.clear();
    deleted_nodes.shrinkToFit();
    deleted_relationships.clear();
    deleted_relationships.shrinkToFit();
    node_types = Types();
    relationship_types = Types();

    // Reset Node zero
    nodes.emplace_back();
    relationships.emplace_back();
    outgoing_relationships.emplace_back();
    incoming_relationships.emplace_back();
  }

  void Shard::reserve(uint64_t reserved_nodes, uint64_t reserved_relationships) {
    if (reserved_nodes >= 0 && reserved_nodes < nodes.max_size() && reserved_relationships >= 0 && reserved_relationships < relationships.max_size()) {
      // Add the zero node and relationship
      ++reserved_nodes;
      ++reserved_relationships;
      nodes.reserve(reserved_nodes);
      relationships.reserve(reserved_relationships);
      outgoing_relationships.reserve(reserved_nodes);
      incoming_relationships.reserve(reserved_nodes);
    }
  }

  // Shard Ids =================================================================================================================================

  seastar::future<uint8_t> Shard::getShardId() {
    return seastar::make_ready_future<uint8_t>(shard_id);
  }

  seastar::future<std::vector<uint8_t>> Shard::getShardIds() {
    return container().map([](Shard &local_shard) {
           return local_shard.getShardId();
    });
  }

  sol::as_table_t<std::vector<std::uint8_t>> Shard::ShardIdsGet() {
    return sol::as_table(getShardIds().get0());
  }

  // Lua

  seastar::future<std::string> Shard::RunLua(const std::string &script) {

    return seastar::async([script, this] () {

       // Inject json encoding
       std::stringstream ss(script);
       std::string line;
       std::vector<std::string> lines;
       while(std::getline(ss,line,'\n')){
         lines.emplace_back(line);
       }

       std::string json_function2 = "local json = require('json')";
       lines.back() = "return json.encode({" + lines.back() + "})";

       std::string executable = json_function2 + join(lines, " ");
       sol::protected_function_result script_result;
        // We only have one Lua VM for each Core, so lock it during use.
       this->lua_lock.for_write().lock().get();
       try {
         script_result = state.script(executable, [] (lua_State *, sol::protected_function_result pfr) {
                return pfr;
         });
         this->lua_lock.for_write().unlock();
         if (script_result.valid()) {
           return script_result.get<std::string>();
         }

         sol::error err = script_result;
         std::string what = err.what();

         return EXCEPTION + what;
       } catch (...) {
         sol::error err = script_result;
         std::string what = err.what();
         // Unlock if we get an exception.
         this->lua_lock.for_write().unlock();
         return EXCEPTION + what;
       }
    });
  }

  // Ids =================================================================================================================================

  uint64_t Shard::externalToInternal(uint64_t id) {
    return (id >> SHIFTED_BITS);
  }

  uint64_t Shard::internalToExternal(uint64_t internal_id) const {
    return (internal_id << SHIFTED_BITS) + shard_id;
  }

  uint8_t Shard::CalculateShardId(uint64_t id) {
    if(id < MASK) {
      return 0;
    }
    return id & MASK;
  }

  uint8_t Shard::CalculateShardId(const std::string &type, const std::string &key) const {
    // We need to find where the node goes, so we use the type and key to create a 64 bit number
    uint64_t x64 = std::hash<std::string>()((type + '-' + key));

    // Then we bucket it into a shard depending on the number of cpus we have
    return (uint8_t)(((__uint128_t)x64 * (__uint128_t)cpus) >> SIXTY_FOUR);
  }

  // Relationship Types ===================================================================================================================
  uint16_t Shard::RelationshipTypesGetCount() {
    return relationship_types.getSize();
  }

  uint64_t Shard::RelationshipTypesGetCount(uint16_t type_id) {
    if (relationship_types.ValidTypeId(type_id)) {
      return relationship_types.getCount(type_id);
    }
    // Not a valid Relationship Type
    return 0;
  }

  uint64_t Shard::RelationshipTypesGetCount(const std::string &type) {
    uint16_t type_id = relationship_types.getTypeId(type);
    return RelationshipTypesGetCount(type_id);
  }

  std::set<std::string> Shard::RelationshipTypesGet() {
    return relationship_types.getTypes();
  }

  // Relationship Type ====================================================================================================================
  std::string Shard::RelationshipTypeGetType(uint16_t type_id) {
    return relationship_types.getType(type_id);
  }

  uint16_t Shard::RelationshipTypeGetTypeId(const std::string &type) {
    uint16_t type_id = relationship_types.getTypeId(type);
    if (relationship_types.ValidTypeId(type_id)) {
      return type_id;
    }
    // Not a valid Relationship Type
    return 0;
  }

  bool Shard::RelationshipTypeInsert(const std::string& type, uint16_t type_id) {
    return relationship_types.addTypeId(type, type_id);
  }

  // Node Types ===========================================================================================================================
  uint16_t Shard::NodeTypesGetCount() {
    return node_types.getSize();
  }

  uint64_t Shard::NodeTypesGetCount(uint16_t type_id) {
    return node_types.getCount(type_id);
  }

  uint64_t Shard::NodeTypesGetCount(const std::string &type) {
    uint16_t type_id = node_types.getTypeId(type);
    return NodeTypesGetCount(type_id);
  }

  std::set<std::string> Shard::NodeTypesGet() {
    return node_types.getTypes();
  }

  // Node Type ============================================================================================================================
  std::string Shard::NodeTypeGetType(uint16_t type_id) {
    return node_types.getType(type_id);
  }

  uint16_t Shard::NodeTypeGetTypeId(const std::string &type) {
    return node_types.getTypeId(type);
  }

  bool Shard::NodeTypeInsert(const std::string& type, uint16_t type_id) {
    tsl::sparse_map<std::string, uint64_t> empty;
    node_keys.emplace(type, empty);
    return node_types.addTypeId(type, type_id);
  }

  // Helpers ==============================================================================================================================
  bool Shard::NodeRemoveDeleteIncoming(uint64_t id, const std::map<uint16_t, std::vector<uint64_t>>&grouped_relationships) {
    for (const auto& rel_type_node_ids : grouped_relationships) {
      uint16_t rel_type_id = rel_type_node_ids.first;
      for (auto node_id : rel_type_node_ids.second) {
        uint64_t internal_id = externalToInternal(node_id);

        auto group = find_if(std::begin(incoming_relationships.at(internal_id)), std::end(incoming_relationships.at(internal_id)),
          [rel_type_id] (const Group& g) { return g.rel_type_id == rel_type_id; } );

        if (group != std::end(incoming_relationships.at(internal_id))) {
          group->ids.erase(std::remove_if(std::begin(group->ids), std::end(group->ids), [id](Ids entry) {
                 return entry.node_id == id;
          }), std::end(group->ids));
        }
      }
    }

    return true;
  }

  std::map<uint16_t, std::map<uint16_t, std::vector<uint64_t>>> Shard::NodeRemoveGetIncoming(uint64_t internal_id) {
    std::map<uint16_t, std::map<uint16_t, std::vector<uint64_t>>> relationships_to_delete;

    // Go through all the outgoing relationships and return the counterparts that I do not own
    for (auto &types : outgoing_relationships.at(internal_id)) {
      // Get the Relationship Type of the list
      uint16_t rel_type = types.rel_type_id;

      for (Ids ids : types.ids) {
        std::map<uint16_t, std::vector<uint64_t>> node_ids;
        for (int i = 0; i < cpus; i++) {
          node_ids.insert({ i, std::vector<uint64_t>() });
        }

        // Remove relationship from other node that I own
        uint16_t node_shard_id = CalculateShardId(ids.node_id);
        if ( node_shard_id != shard_id) {
          node_ids.at(node_shard_id).push_back(ids.node_id);
        }

        for (int i = 0; i < cpus; i++) {
          if(node_ids.at(i).empty()) {
            node_ids.erase(i);
          }
        }

        relationships_to_delete.insert({ rel_type, node_ids });
      }
    }

    return relationships_to_delete;
  }

  std::map<uint16_t, std::map<uint16_t, std::vector<uint64_t>>> Shard::NodeRemoveGetOutgoing(uint64_t internal_id) {
    std::map<uint16_t, std::map<uint16_t, std::vector<uint64_t>>> relationships_to_delete;

    // Go through all the incoming relationships and return the counterparts that I do not own
    for (auto &types : incoming_relationships.at(internal_id)) {
      // Get the Relationship Type of the list
      uint16_t rel_type = types.rel_type_id;

      for (Ids ids : types.ids) {
        std::map<uint16_t, std::vector<uint64_t>> node_ids;
        for (int i = 0; i < cpus; i++) {
          node_ids.insert({ i, std::vector<uint64_t>() });
        }

        // Remove relationship from other node that I own
        uint16_t node_shard_id = CalculateShardId(ids.node_id);
        if ( node_shard_id != shard_id) {
          node_ids.at(node_shard_id).push_back(ids.node_id);
        }

        for (int i = 0; i < cpus; i++) {
          if(node_ids.at(i).empty()) {
            node_ids.erase(i);
          }
        }

        relationships_to_delete.insert({ rel_type, node_ids });
      }
    }

    return relationships_to_delete;
  }

  bool Shard::NodeRemoveDeleteOutgoing(uint64_t id, const std::map<uint16_t, std::vector<uint64_t>> &grouped_relationships) {
    for (const auto& rel_type_node_ids : grouped_relationships) {
      uint16_t rel_type_id = rel_type_node_ids.first;
      for (auto node_id : rel_type_node_ids.second) {
        uint64_t internal_id = externalToInternal(node_id);

        auto group = find_if(std::begin(outgoing_relationships.at(internal_id)), std::end(outgoing_relationships.at(internal_id)),
                             [rel_type_id] (const Group& g) { return g.rel_type_id == rel_type_id; } );

        if (group != std::end(outgoing_relationships.at(internal_id))) {
          // Look in the relationship chain for any relationships of the node to be removed and delete them.
          group->ids.erase(std::remove_if(std::begin(group->ids), std::end(group->ids), [id, rel_type_id, this](Ids entry) {
            if (entry.node_id == id) {
              uint64_t internal_id = externalToInternal(entry.rel_id);
              deleted_relationships.add(internal_id);
              // Update the relationship type counts
              relationship_types.removeId(rel_type_id, entry.rel_id);
              // Clear the relationship
              relationships.at(internal_id) = Relationship();
              return true;
            }

            return false;
          }), std::end(group->ids));
        }

      }
    }

    return true;
  }

  // Nodes ================================================================================================================================
  uint64_t Shard::NodeAddEmpty(const std::string& type, uint16_t node_type, const std::string &key) {
    uint64_t internal_id = nodes.size();
    uint64_t external_id = 0;

    auto type_search = node_keys.find(type);
    // The Label will always exist
    if (type_search != std::end(node_keys)) {
      // Check if the key exists
      auto key_search = type_search->second.find(key);
      if (key_search == std::end(type_search->second)) {
        // If we have deleted nodes, fill in the space by adding the new node here
        if (deleted_nodes.isEmpty()) {
          external_id = internalToExternal(internal_id);
          // Set Metadata properties
          // Add the node to the end and prepare a place for its relationships
          nodes.emplace_back(external_id, node_type, key);
          outgoing_relationships.emplace_back();
          incoming_relationships.emplace_back();
          node_types.addId(node_type, external_id);
        } else {
          internal_id = deleted_nodes.minimum();
          external_id = internalToExternal(internal_id);
          // Set Metadata properties
          Node node(external_id, node_type, key);
          // Replace the deleted node and remove it from the list
          nodes.at(internal_id) = node;
          deleted_nodes.remove(internal_id);
          node_types.addId(node_type, external_id);
        }
        type_search->second.insert({ key, external_id });
      }
    }

    return external_id;
  }

  uint64_t Shard::NodeAdd(const std::string &type, uint16_t node_type, const std::string &key, const std::string &properties) {
    uint64_t internal_id = nodes.size();
    uint64_t external_id = 0;

    auto type_search = node_keys.find(type);
    // The Label will always exist
    if (type_search != std::end(node_keys)) {
      // Check if the key exists
      auto key_search = type_search->second.find(key);
      if (key_search == std::end(type_search->second)) {

        std::map<std::string, std::any> values;
        if (!properties.empty()) {
          // Get the properties
          simdjson::error_code error;

          dom::object object;
          error = Shard::parser.parse(properties).get(object);
          if (!error) {
            // Add the node properties
            convertProperties(values, object);
          } else {
            return 0;
          }
        }


        // If we have deleted nodes, fill in the space by adding the new node here
        if (deleted_nodes.isEmpty()) {
          external_id = internalToExternal(internal_id);
          // Set Metadata properties
          // Add the node to the end and prepare a place for its relationships
          nodes.emplace_back(external_id, node_type, key, values);
          outgoing_relationships.emplace_back();
          incoming_relationships.emplace_back();
          node_types.addId(node_type, external_id);
        } else {
          internal_id = deleted_nodes.minimum();
          external_id = internalToExternal(internal_id);
          // Set Metadata properties
          Node node(external_id, node_type, key, values);
          // Replace the deleted node and remove it from the list
          nodes.at(internal_id) = node;
          deleted_nodes.remove(internal_id);
          node_types.addId(node_type, external_id);
        }
        type_search->second.insert({ key, external_id });
      }
    }

    return external_id;
  }

  uint64_t Shard::NodeGetID(const std::string &type, const std::string &key) {
    // Check if the Type exists
    auto type_search = node_keys.find(type);
    if (type_search != std::end(node_keys)) {
      // Check if the key exists
      auto key_search = type_search->second.find(key);
      if (key_search != std::end(type_search->second)) {
        return key_search->second;
      }
    }
    // Invalid Type or Key
    return 0;
  }

  Node Shard::NodeGet(uint64_t id) {
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      return nodes.at(internal_id);
    }

    // Return the invalid zero node
    return nodes.at(0);
  }

  Node Shard::NodeGet(const std::string &type, const std::string &key) {
      return NodeGet(NodeGetID(type, key));
  }

  bool Shard::NodeRemove(const std::string &type, const std::string &key) {
    // Check if the type exists
    auto type_search = node_keys.find(type);
    if (type_search != std::end(node_keys)) {
      uint16_t node_type = node_types.getTypeId(type);
      // Check if the key exists
      auto key_search = type_search->second.find(key);
      uint64_t external_id = key_search->second;
      uint64_t internal_id = externalToInternal(external_id);
      // Leave Zero node alone
      if (internal_id > 0) {
        // remove the key
        type_search->second.erase(key);
        // empty the node
        nodes.at(internal_id) = Node();
        // add id to deleted nodes for reuse
        deleted_nodes.add(internal_id);
        // remove id from type
        node_types.removeId(node_type, external_id);

        // Go through all the outgoing relationships and delete them and their counterparts that I own
        for (auto &types : outgoing_relationships.at(internal_id)) {
          // Get the Relationship Type of the list
          uint16_t relType = types.rel_type_id;

          for (Ids ids : types.ids) {
            uint64_t internal_rel_id = externalToInternal(ids.rel_id);
            // Add the relationship to be recycled
            deleted_relationships.add(internal_rel_id);
            // Decrement the relationship type counts
            relationship_types.removeId(relType, ids.rel_id);

            // Clear the relationship properties
            Relationship emptyRelationship;
            relationships.at(internal_rel_id) = emptyRelationship;

            // Remove relationship from other node that I own
            if (CalculateShardId(ids.node_id) == shard_id) {
              uint64_t other_internal_id = externalToInternal(ids.node_id);

              auto group = find_if(std::begin(incoming_relationships.at(other_internal_id)), std::end(incoming_relationships.at(other_internal_id)),
                                   [relType] (const Group& g) { return g.rel_type_id == relType; } );

              if (group != std::end(incoming_relationships.at(other_internal_id))) {
                group->ids.erase(std::remove_if(std::begin(group->ids), std::end(group->ids), [ids](Ids entry) {
                       return entry.rel_id == ids.rel_id;
                }), std::end(group->ids));
              }

            }
          }
        }

        // Empty outgoing relationships
        std::vector<Group> emptyOut;
        outgoing_relationships.at(internal_id).swap(emptyOut);

        // Go through all the incoming relationships and delete them and their counterpart
        for (auto &types : incoming_relationships.at(internal_id)) {
          // Get the Relationship Type of the list
          uint16_t relType = types.rel_type_id;

          for (Ids ids : types.ids) {
            if (ids.node_id != external_id) {
              // Add the relationship to be recycled
              deleted_relationships.add(ids.rel_id);
              // Decrement the relationship type counts
              relationship_types.removeId(relType, ids.rel_id);

              // Clear the relationship properties
              Relationship emptyRelationship;
              uint64_t internal_rel_id = externalToInternal(ids.rel_id);
              relationships.at(internal_rel_id) = emptyRelationship;

              // Remove relationship from other node that I own
              if (CalculateShardId(ids.node_id) == shard_id) {
                uint64_t other_internal_id = externalToInternal(ids.node_id);

                auto group = find_if(std::begin(outgoing_relationships.at(other_internal_id)), std::end(outgoing_relationships.at(other_internal_id)),
                                     [relType] (const Group& g) { return g.rel_type_id == relType; } );

                if (group != std::end(outgoing_relationships.at(other_internal_id))) {
                  group->ids.erase(std::remove_if(std::begin(group->ids), std::end(group->ids), [ids](Ids entry) {
                         return entry.rel_id == ids.rel_id;
                  }), std::end(group->ids));
                }

              }
            }
          }
        }

        // Empty incoming relationships
        std::vector<Group> emptyIn;
        incoming_relationships.at(internal_id).swap(emptyIn);

        return true;
      }
    }
    return false;
  }

  bool Shard::NodeRemove(uint64_t id) {
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      Node node = nodes.at(internal_id);
      std::string type = node_types.getType(node.getTypeId());
      std::string key = node.getKey();
      return NodeRemove(type, key);
    }

    return false;
  }

  uint16_t Shard::NodeGetTypeId(uint64_t id) {
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      Node node = nodes.at(internal_id);
      return node.getTypeId();
    }
    // Invalid Node Id
    return 0;
  }

  std::string Shard::NodeGetType(uint64_t id) {
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      Node node = nodes.at(internal_id);
      uint16_t type_id = node.getTypeId();
      return node_types.getType(type_id);
    }
    // Invalid Node Id
    return node_types.getType(0);
  }

  std::string Shard::NodeGetKey(uint64_t id) {
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      Node node = nodes.at(internal_id);
      return node.getKey();
    }
    // Invalid Node Id
    return nodes.at(0).getKey();
  }

  // Node Properties

  std::any Shard::NodePropertyGet(const std::string &type, const std::string &key, const std::string &property) {
    uint64_t id = NodeGetID(type, key);
    return NodePropertyGet(id, property);
  }

  std::string Shard::NodePropertyGetString(const std::string &type, const std::string &key, const std::string &property) {
    uint64_t id = NodeGetID(type, key);
    return NodePropertyGetString(id, property);
  }

  int64_t Shard::NodePropertyGetInteger(const std::string &type, const std::string &key, const std::string &property) {
    uint64_t id = NodeGetID(type, key);
    return NodePropertyGetInteger(id, property);
  }

  double Shard::NodePropertyGetDouble(const std::string &type, const std::string &key, const std::string &property) {
    uint64_t id = NodeGetID(type, key);
    return NodePropertyGetDouble(id, property);
  }

  bool Shard::NodePropertyGetBoolean(const std::string &type, const std::string &key, const std::string &property) {
    uint64_t id = NodeGetID(type, key);
    return NodePropertyGetBoolean(id, property);
  }

  std::map<std::string, std::any> Shard::NodePropertyGetObject(const std::string &type, const std::string &key, const std::string &property) {
    uint64_t id = NodeGetID(type, key);
    return NodePropertyGetObject(id, property);
  }

  std::any Shard::NodePropertyGet(uint64_t id, const std::string &property) {
    // If the node is valid
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      // Look for the property
      return nodes.at(internal_id).getProperty(property);
    }
    // Invalid node id, property name or type
    return tombstone_any;
  }

  std::string Shard::NodePropertyGetString(uint64_t id, const std::string &property) {
    // If the node is valid
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      // Look for the property
      std::any value = nodes.at(internal_id).getProperty(property);
      if (value.type() == typeid(std::string) ) {
        return std::any_cast<std::string>(value);
      }
    }
    // Invalid node id, property name or type
    return tombstone_string;
  }

  int64_t Shard::NodePropertyGetInteger(uint64_t id, const std::string &property) {
    // If the node is valid
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      // Look for the property
      std::any value = nodes.at(internal_id).getProperty(property);
      if (value.type() == typeid(int64_t) ) {
        return std::any_cast<int64_t>(value);
      }
    }
    // Invalid node id, property name or type
    return tombstone_int;
  }

  double Shard::NodePropertyGetDouble(uint64_t id, const std::string &property) {
    // If the node is valid
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      // Look for the property
      std::any value = nodes.at(internal_id).getProperty(property);
      if (value.type() == typeid(double) ) {
        return std::any_cast<double>(value);
      }
    }
    // Invalid node id, property name or type
    return tombstone_double;
  }

  bool Shard::NodePropertyGetBoolean(uint64_t id, const std::string &property) {
    // If the node is valid
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      // Look for the property
      std::any value = nodes.at(internal_id).getProperty(property);
      if (value.type() == typeid(bool) ) {
        return std::any_cast<bool>(value);
      }
    }
    // Invalid node id, property name or type
    return tombstone_boolean;
  }

  std::map<std::string, std::any> Shard::NodePropertyGetObject(uint64_t id, const std::string &property) {
    // If the node is valid
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      // Look for the property
      std::any value = nodes.at(internal_id).getProperty(property);
      if (value.type() == typeid(std::map<std::string, std::any>) ) {
        return std::any_cast<std::map<std::string, std::any>>(value);
      }
    }
    // Invalid node id, property name or type
    return tombstone_object;
  }

  bool Shard::NodePropertySet(const std::string &type, const std::string &key, const std::string &property, std::string value) {
    uint64_t id = NodeGetID(type, key);
    return NodePropertySet(id, property, std::move(value));
  }

  bool Shard::NodePropertySet(const std::string &type, const std::string &key, const std::string &property, const char *value) {
    uint64_t id = NodeGetID(type, key);
    return NodePropertySet(id, property, std::string((value)));
  }

  bool Shard::NodePropertySet(const std::string &type, const std::string &key, const std::string &property, int64_t value) {
    uint64_t id = NodeGetID(type, key);
    return NodePropertySet(id, property, value);
  }

  bool Shard::NodePropertySet(const std::string &type, const std::string &key, const std::string &property, double value) {
    uint64_t id = NodeGetID(type, key);
    return NodePropertySet(id, property, value);
  }

  bool Shard::NodePropertySet(const std::string &type, const std::string &key, const std::string &property, bool value) {
    uint64_t id = NodeGetID(type, key);
    return NodePropertySet(id, property, value);
  }

  bool Shard::NodePropertySet(const std::string &type, const std::string &key, const std::string &property, std::map<std::string, std::any> value) {
    uint64_t id = NodeGetID(type, key);
    return NodePropertySet(id, property, std::move(value));
  }

  bool Shard::NodePropertySetFromJson(const std::string &type, const std::string &key, const std::string &property, const std::string &value) {
    uint64_t id = NodeGetID(type, key);
    return NodePropertySetFromJson(id, property, value);
  }

  bool Shard::NodePropertySet(uint64_t id, const std::string &property, std::string value) {
    // If the node is valid
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      nodes.at(internal_id).setProperty(property, value);
      return true;
    } else {
      return false;
    }
  }

  bool Shard::NodePropertiesDelete(const std::string &type, const std::string &key) {
    uint64_t id = NodeGetID(type, key);
    return NodePropertiesDelete(id);
  }

  bool Shard::NodePropertySet(uint64_t id, const std::string &property, const char *value) {
    // If the node is valid
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      nodes.at(internal_id).setProperty(property, std::string(value));
      return true;
    } else {
      return false;
    }
  }

  bool Shard::NodePropertySet(uint64_t id, const std::string &property, int64_t value) {
    // If the node is valid
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      nodes.at(internal_id).setProperty(property, value);
      return true;
    } else {
      return false;
    }
  }

  bool Shard::NodePropertySet(uint64_t id, const std::string &property, double value) {
    // If the node is valid
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      nodes.at(internal_id).setProperty(property, value);
      return true;
    } else {
      return false;
    }
  }

  bool Shard::NodePropertySet(uint64_t id, const std::string &property, bool value) {
    // If the node is valid
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      nodes.at(internal_id).setProperty(property, value);
      return true;
    } else {
      return false;
    }
  }

  bool Shard::NodePropertySet(uint64_t id, const std::string &property, std::map<std::string, std::any> value) {
    // If the node is valid
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      nodes.at(internal_id).setProperty(property, value);
      return true;
    } else {
      return false;
    }
  }

  bool Shard::NodePropertySetFromJson(uint64_t id, const std::string &property, const std::string &value) {
    // If the node is valid
    if (ValidNodeId(id)) {
      std::map<std::string, std::any> values;
      if (!value.empty()) {
        // Get the properties
        simdjson::error_code error;

        dom::object object;
        error = parser.parse(value).get(object);
        if (!error) {
          // Add the node properties
          convertProperties(values, object);
        } else {
          return false;
        }
      }
      uint64_t internal_id = externalToInternal(id);
      nodes.at(internal_id).setProperty(property, values);
      return true;
    } else {
      return false;
    }
  }

  bool Shard::NodePropertyDelete(const std::string &type, const std::string &key, const std::string &property) {
    uint64_t id = NodeGetID(type, key);
    return NodePropertyDelete(id, property);
  }

  bool Shard::NodePropertyDelete(uint64_t id, const std::string &property) {
    // If the node is valid
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      return nodes.at(internal_id).deleteProperty(property);
    } else {
      return false;
    }
  }

  std::map<std::string, std::any> Shard::NodePropertiesGet(const std::string &type, const std::string &key) {
    uint64_t id = NodeGetID(type, key);
    return NodePropertiesGet(id);
  }

  bool Shard::NodePropertiesSet(const std::string &type, const std::string &key, std::map<std::string, std::any> &value) {
    uint64_t id = NodeGetID(type, key);
    return NodePropertiesSet(id, value);
  }

  bool Shard::NodePropertiesSetFromJson(const std::string &type, const std::string &key, const std::string &value) {
    uint64_t id = NodeGetID(type, key);
    return NodePropertiesSetFromJson(id, value);
  }

  std::map<std::string, std::any> Shard::NodePropertiesGet(uint64_t id) {
    // If the node is valid
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      return nodes.at(internal_id).getProperties();
    } else {
      return tombstone_object;
    }
  }

  bool Shard::NodePropertiesSet(uint64_t id, std::map<std::string, std::any> &value) {
    // If the node is valid
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      std::map<std::string, std::any> values = nodes.at(internal_id).getProperties();
      value.merge(values);
      nodes.at(internal_id).setProperties(value);
      return true;
    } else {
      return false;
    }
  }

  bool Shard::NodePropertiesSetFromJson(uint64_t id, const std::string &value) {
    // If the node is valid
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      std::map<std::string, std::any> values = nodes.at(internal_id).getProperties();
      if (!value.empty()) {
        // Get the properties
        simdjson::error_code error;

        dom::object object;
        error = parser.parse(value).get(object);
        if (!error) {
          // Add the node properties
          convertProperties(values, object);
        } else {
          return false;
        }
      }

      nodes.at(internal_id).setProperties(values);
      return true;
    } else {
      return false;
    }
  }

  bool Shard::NodePropertiesReset(const std::string &type, const std::string &key, const std::map<std::string, std::any> &value) {
    uint64_t id = NodeGetID(type, key);
    return NodePropertiesReset(id, value);
  }

  bool Shard::NodePropertiesResetFromJson(const std::string &type, const std::string &key, const std::string &value) {
    uint64_t id = NodeGetID(type, key);
    return NodePropertiesResetFromJson(id, value);
  }

  bool Shard::NodePropertiesReset(uint64_t id, const std::map<std::string, std::any> &value) {
    // If the node is valid
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      nodes.at(internal_id).setProperties(value);
      return true;
    } else {
      return false;
    }
  }

  bool Shard::NodePropertiesResetFromJson(uint64_t id, const std::string &value) {
    // If the node is valid
    if (ValidNodeId(id)) {
      std::map<std::string, std::any> values;
      if (!value.empty()) {
        // Get the properties
        simdjson::error_code error;

        dom::object object;
        error = parser.parse(value).get(object);
        if (!error) {
          // Add the node properties
          convertProperties(values, object);
        } else {
          return false;
        }
      }
      uint64_t internal_id = externalToInternal(id);
      nodes.at(internal_id).setProperties(values);
      return true;
    } else {
      return false;
    }
  }

  bool Shard::NodePropertiesDelete(uint64_t id) {
    // If the node is valid
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      nodes.at(internal_id).deleteProperties();
      return true;
    } else {
      return false;
    }
  }

  // Relationships
  uint64_t Shard::RelationshipAddEmptySameShard(uint16_t rel_type, uint64_t id1, uint64_t id2) {
    uint64_t internal_id1 = externalToInternal(id1);
    uint64_t internal_id2 = externalToInternal(id2);
    uint64_t external_id = 0;

    if (ValidNodeId(id1) && ValidNodeId(id2)) {
      uint64_t internal_id = relationships.size();
      // If we have deleted relationships, fill in the space by reusing the new relationship
      if (!deleted_relationships.isEmpty()) {
        internal_id = deleted_relationships.minimum();
        external_id = internalToExternal(internal_id);
        Relationship relationship = Relationship(external_id, id1, id2, rel_type);
        relationships.at(internal_id) = relationship;
        deleted_relationships.remove(internal_id);
      } else {
        external_id = internalToExternal(internal_id);
        relationships.emplace_back(external_id, id1, id2, rel_type);
      }

      // Add the relationship to the outgoing node
      auto group = find_if(std::begin(outgoing_relationships.at(internal_id1)), std::end(outgoing_relationships.at(internal_id1)),
                           [rel_type] (const Group& g) { return g.rel_type_id == rel_type; } );
      // See if the relationship type is already there
      if (group != std::end(outgoing_relationships.at(internal_id1))) {
        group->ids.emplace_back(id2, external_id);
      } else {
        // otherwise create a new type with the ids
        outgoing_relationships.at(internal_id1).emplace_back(Group(rel_type, std::vector<triton::Ids>({Ids(id2, external_id)})));
      }

      // Add the relationship to the incoming node
      group = find_if(std::begin(incoming_relationships.at(internal_id2)), std::end(incoming_relationships.at(internal_id2)),
                      [rel_type] (const Group& g) { return g.rel_type_id == rel_type; } );
      // See if the relationship type is already there
      if (group != std::end(incoming_relationships.at(internal_id2))) {
        group->ids.emplace_back(id1, external_id);
      } else {
        // otherwise create a new type with the ids
        incoming_relationships.at(internal_id2).emplace_back(Group(rel_type, std::vector<triton::Ids>({Ids(id1, external_id)})));
      }

      // Add relationship id to Types
      relationship_types.addId(rel_type, external_id);

      return external_id;
    }
    // Invalid node ids
    return 0;
  }

  uint64_t Shard::RelationshipAddSameShard(uint16_t rel_type, uint64_t id1, uint64_t id2, const std::string& properties) {
    uint64_t internal_id1 = externalToInternal(id1);
    uint64_t internal_id2 = externalToInternal(id2);
    uint64_t external_id = 0;

    if (ValidNodeId(id1) && ValidNodeId(id2)) {
      uint64_t internal_id = relationships.size();

      std::map<std::string, std::any> values;
      if (!properties.empty()) {
        // Get the properties
        simdjson::error_code error;

        dom::object object;
        error = Shard::parser.parse(properties).get(object);
        if (!error) {
          // Add the node properties
          convertProperties(values, object);
        } else {
          return 0;
        }
      }

      // If we have deleted relationships, fill in the space by reusing the new relationship
      if (!deleted_relationships.isEmpty()) {
        internal_id = deleted_relationships.minimum();
        external_id = internalToExternal(internal_id);
        Relationship relationship = Relationship(external_id, id1, id2, rel_type, values);
        relationships.at(internal_id) = relationship;
        deleted_relationships.remove(internal_id);
      } else {
        external_id = internalToExternal(internal_id);
        relationships.emplace_back(external_id, id1, id2, rel_type, values);
      }

      // Add the relationship to the outgoing node
      auto group = find_if(std::begin(outgoing_relationships.at(internal_id1)), std::end(outgoing_relationships.at(internal_id1)),
                           [rel_type] (const Group& g) { return g.rel_type_id == rel_type; } );
      // See if the relationship type is already there
      if (group != std::end(outgoing_relationships.at(internal_id1))) {
        group->ids.emplace_back(id2, external_id);
      } else {
        // otherwise create a new type with the ids
        outgoing_relationships.at(internal_id1).emplace_back(Group(rel_type, std::vector<triton::Ids>({Ids(id2, external_id)})));
      }

      // Add the relationship to the incoming node
      group = find_if(std::begin(incoming_relationships.at(internal_id2)), std::end(incoming_relationships.at(internal_id2)),
                      [rel_type] (const Group& g) { return g.rel_type_id == rel_type; } );
      // See if the relationship type is already there
      if (group != std::end(incoming_relationships.at(internal_id2))) {
        group->ids.emplace_back(id1, external_id);
      } else {
        // otherwise create a new type with the ids
        incoming_relationships.at(internal_id2).emplace_back(Group(rel_type, std::vector<triton::Ids>({Ids(id1, external_id)})));
      }

      // Add relationship id to Types
      relationship_types.addId(rel_type, external_id);

      return external_id;
    }
    // Invalid node ids
    return 0;
  }

  uint64_t Shard::RelationshipAddEmptySameShard(uint16_t rel_type, const std::string &type1, const std::string &key1, const std::string &type2, const std::string &key2) {
    uint64_t id1 = NodeGetID(type1, key1);
    uint64_t id2 = NodeGetID(type2, key2);

    return RelationshipAddEmptySameShard(rel_type, id1, id2);
  }

  uint64_t Shard::RelationshipAddSameShard(uint16_t rel_type, const std::string &type1, const std::string &key1, const std::string &type2, const std::string &key2, const std::string& properties) {
    uint64_t id1 = NodeGetID(type1, key1);
    uint64_t id2 = NodeGetID(type2, key2);

    return RelationshipAddSameShard(rel_type, id1, id2, properties);
  }

  uint64_t Shard::RelationshipAddEmptyToOutgoing(uint16_t rel_type, uint64_t id1, uint64_t id2) {
    uint64_t internal_id = relationships.size();
    uint64_t external_id = 0;
    // If we have deleted relationships, fill in the space by reusing the new relationship
    if (!deleted_relationships.isEmpty()) {
      internal_id = deleted_relationships.minimum();
      external_id = internalToExternal(internal_id);
      Relationship relationship = Relationship(external_id, id1, id2, rel_type);
      relationships.at(internal_id) = relationship;
      deleted_relationships.remove(internal_id);
    } else {
      external_id = internalToExternal(internal_id);
      relationships.emplace_back(external_id, id1, id2, rel_type);
    }

    uint64_t internal_id1 = externalToInternal(id1);

    // Add the relationship to the outgoing node
    auto group = find_if(std::begin(outgoing_relationships.at(internal_id1)), std::end(outgoing_relationships.at(internal_id1)),
                         [rel_type] (const Group& g) { return g.rel_type_id == rel_type; } );
    // See if the relationship type is already there
    if (group != std::end(outgoing_relationships.at(internal_id1))) {
      group->ids.emplace_back(id2, external_id);
    } else {
      // otherwise create a new type with the ids
      outgoing_relationships.at(internal_id1).emplace_back(Group(rel_type, std::vector<triton::Ids>({Ids(id2, external_id)})));
    }

    // Add relationship id to Types
    relationship_types.addId(rel_type, external_id);

    return external_id;
  }

  uint64_t Shard::RelationshipAddToOutgoing(uint16_t rel_type, uint64_t id1, uint64_t id2, const std::string& properties) {
    uint64_t internal_id = relationships.size();
    uint64_t external_id = 0;

    std::map<std::string, std::any> values;
    if (!properties.empty()) {
      // Get the properties
      simdjson::error_code error;

      dom::object object;
      error = Shard::parser.parse(properties).get(object);
      if (!error) {
        // Add the node properties
        convertProperties(values, object);
      } else {
        return 0;
      }
    }

    // If we have deleted relationships, fill in the space by reusing the new relationship
    if (!deleted_relationships.isEmpty()) {
      internal_id = deleted_relationships.minimum();
      external_id = internalToExternal(internal_id);
      Relationship relationship = Relationship(external_id, id1, id2, rel_type, values);
      relationships.at(internal_id) = relationship;
      deleted_relationships.remove(internal_id);
    } else {
      external_id = internalToExternal(internal_id);
      relationships.emplace_back(external_id, id1, id2, rel_type, values);
    }

    uint64_t internal_id1 = externalToInternal(id1);

    // Add the relationship to the outgoing node
    auto group = find_if(std::begin(outgoing_relationships.at(internal_id1)), std::end(outgoing_relationships.at(internal_id1)),
                         [rel_type] (const Group& g) { return g.rel_type_id == rel_type; } );
    // See if the relationship type is already there
    if (group != std::end(outgoing_relationships.at(internal_id1))) {
      group->ids.emplace_back(id2, external_id);
    } else {
      // otherwise create a new type with the ids
      outgoing_relationships.at(internal_id1).emplace_back(Group(rel_type, std::vector<triton::Ids>({Ids(id2, external_id)})));
    }

    // Add relationship id to Types
    relationship_types.addId(rel_type, external_id);

    return external_id;
  }

  uint64_t Shard::RelationshipAddToIncoming(uint16_t rel_type, uint64_t rel_id, uint64_t id1, uint64_t id2) {
    uint64_t internal_id2 = externalToInternal(id2);
    // Add the relationship to the incoming node
    auto group = find_if(std::begin(incoming_relationships.at(internal_id2)), std::end(incoming_relationships.at(internal_id2)),
                    [rel_type] (const Group& g) { return g.rel_type_id == rel_type; } );
    // See if the relationship type is already there
    if (group != std::end(incoming_relationships.at(internal_id2))) {
      group->ids.emplace_back(id1, rel_id);
    } else {
      // otherwise create a new type with the ids
      incoming_relationships.at(internal_id2).emplace_back(Group(rel_type, std::vector<triton::Ids>({Ids(id1, rel_id)})));
    }

    return rel_id;
  }

  Relationship Shard::RelationshipGet(uint64_t rel_id) {
    if (ValidRelationshipId(rel_id)) {
      uint64_t internal_id = externalToInternal(rel_id);
      return relationships.at(internal_id);
    }       // Invalid Relationship
    return relationships.at(0);
  }

  bool Shard::ValidNodeId(uint64_t id) {
    // Node must be greater than zero,
    // less than maximum node id,
    // belong to this shard
    // and not deleted
    return id > 0 && externalToInternal(id) < nodes.size() && internalToExternal(externalToInternal(id)) == id;
  }

  bool Shard::ValidRelationshipId(uint64_t id) {
    // Relationship must be greater than zero,
    // less than maximum relationship id,
    // belong to this shard
    // and not deleted
    uint64_t internal_id = externalToInternal(id);
    return id > 0 && externalToInternal(id) < relationships.size() && internalToExternal(externalToInternal(id)) == id;
  }

  std::string Shard::RelationshipGetType(uint64_t id) {
    if (ValidRelationshipId(id)) {
      uint64_t internal_id = externalToInternal(id);
      return relationship_types.getType(relationships.at(internal_id).getTypeId());
    }
    // Invalid Relationship Id
    return relationship_types.getType(0);
  }

  uint16_t Shard::RelationshipGetTypeId(uint64_t id) {
    if (ValidRelationshipId(id)) {
      uint64_t internal_id = externalToInternal(id);
      return relationships.at(internal_id).getTypeId();
    }
    // Invalid Relationship Id
    return 0;
  }

  uint64_t Shard::RelationshipGetStartingNodeId(uint64_t id) {
    if (ValidRelationshipId(id)) {
      uint64_t internal_id = externalToInternal(id);
      return relationships.at(internal_id).getStartingNodeId();
    }
    // Invalid Relationship Id
    return 0;
  }

  uint64_t Shard::RelationshipGetEndingNodeId(uint64_t id) {
    if (ValidRelationshipId(id)) {
      uint64_t internal_id = externalToInternal(id);
      return relationships.at(internal_id).getEndingNodeId();
    }
    // Invalid Relationship Id
    return 0;
  }

  std::pair <uint16_t, uint64_t> Shard::RelationshipRemoveGetIncoming(uint64_t internal_id) {
    Relationship relationship = relationships.at(internal_id);
    uint64_t id1 = relationship.getStartingNodeId();
    uint64_t id2 = relationship.getEndingNodeId();
    uint64_t external_id = relationship.getId();
    uint16_t rel_type_id = relationship.getTypeId();

    // Update the relationship type counts
    relationship_types.removeId(relationship.getTypeId(), external_id);
    uint64_t internal_id1 = externalToInternal(id1);

    // Add to deleted relationships bitmap
    deleted_relationships.add(internal_id);

    // Remove relationship from Node 1
    auto group = find_if(std::begin(outgoing_relationships.at(internal_id1)), std::end(outgoing_relationships.at(internal_id1)),
                         [rel_type_id] (const Group& g) { return g.rel_type_id == rel_type_id; } );
    if (group != std::end(outgoing_relationships.at(internal_id1))) {
      auto rel_to_delete = find_if(std::begin(group->ids), std::end(group->ids), [external_id](Ids entry) {
             return entry.rel_id == external_id;
      });
      if (rel_to_delete != std::end(group->ids)) {
        group->ids.erase(rel_to_delete);
      }
    }

    // Clear the relationship
    relationships.at(internal_id) = Relationship();

    // Return the rel_type and other node Id
    return std::pair <uint16_t ,uint64_t> (rel_type_id, id2);
  }

  bool Shard::RelationshipRemoveIncoming(uint16_t rel_type_id, uint64_t external_id, uint64_t node_id) {
    // Remove relationship from Node 2
    uint64_t internal_id2 = externalToInternal(node_id);

    auto group = find_if(std::begin(incoming_relationships.at(internal_id2)), std::end(incoming_relationships.at(internal_id2)),
                         [rel_type_id] (const Group& g) { return g.rel_type_id == rel_type_id; } );

    auto rel_to_delete = find_if(std::begin(group->ids), std::end(group->ids), [external_id](Ids entry) {
           return entry.rel_id == external_id;
    });
    if (rel_to_delete != std::end(group->ids)) {
      group->ids.erase(rel_to_delete);
    }

    return true;
  }

  // Relationship Properties
  std::any Shard::RelationshipPropertyGet(uint64_t id, const std::string &property) {
    // If the relationship is valid
    if (ValidRelationshipId(id)) {
      uint64_t internal_id = externalToInternal(id);
      // Look for the property
      return relationships.at(internal_id).getProperty(property);
    }
    // Invalid relationship id, property name or type
    return tombstone_any;
  }

  std::string Shard::RelationshipPropertyGetString(uint64_t id, const std::string &property) {
    if (ValidRelationshipId(id)) {
      uint64_t internal_id = externalToInternal(id);
      // Look for the property
      std::any value = relationships.at(internal_id).getProperty(property);
      if (value.type() == typeid(std::string) ) {
        return std::any_cast<std::string>(value);
      }
    }

    // Invalid relationship id, property name, or type
    return tombstone_string;
  }

  int64_t Shard::RelationshipPropertyGetInteger(uint64_t id, const std::string &property) {
    if (ValidRelationshipId(id)) {
      uint64_t internal_id = externalToInternal(id);
      // Look for the property
      std::any value = relationships.at(internal_id).getProperty(property);
      if (value.type() == typeid(int64_t) ) {
        return std::any_cast<int64_t>(value);
      }
    }

    // Invalid relationship id, property name, or type
    return tombstone_int;
  }

  double Shard::RelationshipPropertyGetDouble(uint64_t id, const std::string &property) {
    if (ValidRelationshipId(id)) {
      uint64_t internal_id = externalToInternal(id);
      // Look for the property
      std::any value = relationships.at(internal_id).getProperty(property);
      if (value.type() == typeid(double) ) {
        return std::any_cast<double>(value);
      }
    }

    // Invalid relationship id, property name, or type
    return tombstone_double;
  }

  bool Shard::RelationshipPropertyGetBoolean(uint64_t id, const std::string &property) {
    if (ValidRelationshipId(id)) {
      uint64_t internal_id = externalToInternal(id);
      // Look for the property
      std::any value = relationships.at(internal_id).getProperty(property);
      if (value.type() == typeid(bool) ) {
        return std::any_cast<bool>(value);
      }
    }

    // Invalid relationship id, property name, or type
    return tombstone_boolean;
  }

  std::map<std::string, std::any> Shard::RelationshipPropertyGetObject(uint64_t id, const std::string &property) {
    if (ValidRelationshipId(id)) {
      uint64_t internal_id = externalToInternal(id);
      // Look for the property
      std::any value = relationships.at(internal_id).getProperty(property);
      if (value.type() == typeid(std::map<std::string, std::any>) ) {
        return std::any_cast<std::map<std::string, std::any>>(value);
      }
    }

    // Invalid relationship id, property name, or type
    return tombstone_object;
  }

  bool Shard::RelationshipPropertySet(uint64_t id, const std::string &property, std::string value) {
    // If the relationship is valid
    if (ValidRelationshipId(id)) {
      uint64_t internal_id = externalToInternal(id);
      relationships.at(internal_id).setProperty(property, value);
      return true;
    }
    // Invalid relationship id
    return false;
  }

  bool Shard::RelationshipPropertySet(uint64_t id, const std::string &property, const char *value) {
    // If the relationship is valid
    if (ValidRelationshipId(id)) {
      uint64_t internal_id = externalToInternal(id);
      relationships.at(internal_id).setProperty(property, std::string(value));
      return true;
    }
    // Invalid relationship id
    return false;
  }

  bool Shard::RelationshipPropertySet(uint64_t id, const std::string &property, int64_t value) {
    // If the relationship is valid
    if (ValidRelationshipId(id)) {
      uint64_t internal_id = externalToInternal(id);
      relationships.at(internal_id).setProperty(property, value);
      return true;
    }
    // Invalid relationship id
    return false;
  }

  bool Shard::RelationshipPropertySet(uint64_t id, const std::string &property, double value) {
    // If the relationship is valid
    if (ValidRelationshipId(id)) {
      uint64_t internal_id = externalToInternal(id);
      relationships.at(internal_id).setProperty(property, value);
      return true;
    }
    // Invalid relationship id
    return false;
  }

  bool Shard::RelationshipPropertySet(uint64_t id, const std::string &property, bool value) {
    // If the relationship is valid
    if (ValidRelationshipId(id)) {
      uint64_t internal_id = externalToInternal(id);
      relationships.at(internal_id).setProperty(property, value);
      return true;
    }
    // Invalid relationship id
    return false;
  }

  bool Shard::RelationshipPropertySet(uint64_t id, const std::string &property, std::map<std::string, std::any> value) {
    // If the relationship is valid
    if (ValidRelationshipId(id)) {
      uint64_t internal_id = externalToInternal(id);
      relationships.at(internal_id).setProperty(property, value);
      return true;
    }
    // Invalid relationship id
    return false;
  }

  bool Shard::RelationshipPropertySetFromJson(uint64_t id, const std::string &property, const std::string &value) {
    // If the relationship is valid
    if (ValidRelationshipId(id)) {
      std::map<std::string, std::any> values;
      if (!value.empty()) {
        // Get the properties
        simdjson::error_code error;

        dom::object object;
        error = parser.parse(value).get(object);
        if (!error) {
          // Add the relationship properties
          convertProperties(values, object);
        } else {
          return false;
        }
      }
      uint64_t internal_id = externalToInternal(id);
      relationships.at(internal_id).setProperty(property, values);
      return true;
    }
    // Invalid relationship id
    return false;
  }

  bool Shard::RelationshipPropertyDelete(uint64_t id, const std::string &property) {
    // If the relationship is valid
    if (ValidRelationshipId(id)) {
      uint64_t internal_id = externalToInternal(id);
      return relationships.at(internal_id).deleteProperty(property);
    }
    // Invalid relationship id
    return false;
  }

  std::map<std::string, std::any> Shard::RelationshipPropertiesGet(uint64_t id) {
    // If the relationship is valid
    if (ValidRelationshipId(id)) {
      uint64_t internal_id = externalToInternal(id);
      return relationships.at(internal_id).getProperties();
    } else {
      return tombstone_object;
    }
  }

  bool Shard::RelationshipPropertiesSet(uint64_t id, std::map<std::string, std::any> &value) {
    // If the relationship is valid
    if (ValidRelationshipId(id)) {
      uint64_t internal_id = externalToInternal(id);
      std::map<std::string, std::any> values = relationships.at(internal_id).getProperties();
      value.merge(values);
      relationships.at(internal_id).setProperties(value);
      return true;
    }
    // Invalid relationship id
    return false;
  }

  bool Shard::RelationshipPropertiesSetFromJson(uint64_t id, const std::string &value) {
    // If the relationship is valid
    if (ValidRelationshipId(id)) {
      uint64_t internal_id = externalToInternal(id);
      std::map<std::string, std::any> values = relationships.at(internal_id).getProperties();
      if (!value.empty()) {
        // Get the properties
        simdjson::error_code error;

        dom::object object;
        error = parser.parse(value).get(object);
        if (!error) {
          // Add the node properties
          convertProperties(values, object);
        } else {
          return false;
        }
      }

      nodes.at(internal_id).setProperties(values);
      return true;
    } else {
      return false;
    }
  }

  bool Shard::RelationshipPropertiesReset(uint64_t id, const std::map<std::string, std::any> &value) {
    // If the relationship is valid
    if (ValidRelationshipId(id)) {
      uint64_t internal_id = externalToInternal(id);
      relationships.at(internal_id).setProperties(value);
      return true;
    }
    // Invalid relationship id
    return false;
  }

  bool Shard::RelationshipPropertiesResetFromJson(uint64_t id, const std::string &value) {
    // If the relationship is valid
    if (ValidRelationshipId(id)) {
      uint64_t internal_id = externalToInternal(id);
      std::map<std::string, std::any> values;
      if (!value.empty()) {
        // Get the properties
        simdjson::error_code error;

        dom::object object;
        error = parser.parse(value).get(object);
        if (!error) {
          // Add the node properties
          convertProperties(values, object);
        } else {
          return false;
        }
      }

      nodes.at(internal_id).setProperties(values);
      return true;
    } else {
      return false;
    }
  }

  bool Shard::RelationshipPropertiesDelete(uint64_t id) {
    // If the relationship is valid
    if (ValidRelationshipId(id)) {
      uint64_t internal_id = externalToInternal(id);
      relationships.at(internal_id).deleteProperties();
      return true;
    }
    // Invalid relationship id
    return false;
  }

  // Node Degree
  uint64_t Shard::NodeGetDegree(const std::string &type, const std::string &key) {
    uint64_t id = NodeGetID(type, key);
    return NodeGetDegree(id);
  }

  uint64_t Shard::NodeGetDegree(const std::string &type, const std::string &key, Direction direction) {
    uint64_t id = NodeGetID(type, key);
    return NodeGetDegree(id, direction);
  }

  uint64_t Shard::NodeGetDegree(const std::string &type, const std::string &key, Direction direction, const std::string &rel_type) {
    uint64_t id = NodeGetID(type, key);
    return NodeGetDegree(id, direction, rel_type);
  }

  uint64_t Shard::NodeGetDegree(const std::string &type, const std::string &key, Direction direction, const std::vector<std::string> &rel_types) {
    uint64_t id = NodeGetID(type, key);
    return NodeGetDegree(id, direction, rel_types);
  }

  uint64_t Shard::NodeGetDegree(uint64_t id) {
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      return outgoing_relationships.at(internal_id).size() + incoming_relationships.at(internal_id).size();
    }
    return 0;
  }

  uint64_t Shard::NodeGetDegree(uint64_t id, Direction direction) {
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      uint64_t count = 0;
      // Use the two ifs to handle ALL for a direction
      if (direction != IN) {
        // For each type sum up the values
        for (const auto &[key, value] : outgoing_relationships.at(internal_id)) {
          count += value.size();
        }
      }
      if (direction != OUT) {
        // For each type sum up the values
        for (const auto &[key, value] : incoming_relationships.at(internal_id)) {
          count += value.size();
        }
      }
      return count;
    }

    return 0;
  }

  uint64_t Shard::NodeGetDegree(uint64_t id, Direction direction, const std::string &rel_type) {
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      uint16_t type_id = relationship_types.getTypeId(rel_type);
      if (type_id > 0) {
        uint64_t count = 0;
        // Use the two ifs to handle ALL for a direction
        if (direction != IN) {
          auto group = find_if(std::begin(outgoing_relationships.at(internal_id)), std::end(outgoing_relationships.at(internal_id)),
                               [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

          if (group != std::end(outgoing_relationships.at(internal_id))) {
            count += group->ids.size();
          }
        }
        if (direction != OUT) {
          auto group = find_if(std::begin(incoming_relationships.at(internal_id)), std::end(incoming_relationships.at(internal_id)),
                               [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

          if (group != std::end(incoming_relationships.at(internal_id))) {
            count += group->ids.size();
          }
        }
        return count;
      }
    }

    return 0;
  }

  uint64_t Shard::NodeGetDegree(uint64_t id, Direction direction, const std::vector<std::string> &rel_types) {
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      uint64_t count = 0;
      // Use the two ifs to handle ALL for a direction
      if (direction != IN) {
        // For each requested type sum up the values
        for (const auto &rel_type : rel_types) {
          uint16_t type_id = relationship_types.getTypeId(rel_type);
          if (type_id > 0) {
            auto group = find_if(std::begin(outgoing_relationships.at(internal_id)), std::end(outgoing_relationships.at(internal_id)),
                                 [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

            if (group != std::end(outgoing_relationships.at(internal_id))) {
              count += group->ids.size();
            }
          }
        }
      }
      if (direction != OUT) {
        // For each requested type sum up the values
        for (const auto &rel_type : rel_types) {
          uint16_t type_id = relationship_types.getTypeId(rel_type);
          if (type_id > 0) {
            auto group = find_if(std::begin(incoming_relationships.at(internal_id)), std::end(incoming_relationships.at(internal_id)),
                                 [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

            if (group != std::end(incoming_relationships.at(internal_id))) {
              count += group->ids.size();
            }
          }
        }
      }
      return count;
    }

    return 0;
  }

  // Traversing
  std::vector<Ids> Shard::NodeGetRelationshipsIDs(const std::string &type, const std::string &key) {
    uint64_t id = NodeGetID(type, key);
    return NodeGetRelationshipsIDs(id);
  }

  std::vector<Ids> Shard::NodeGetRelationshipsIDs(const std::string &type, const std::string &key, Direction direction) {
    uint64_t id = NodeGetID(type, key);
    return NodeGetRelationshipsIDs(id, direction);
  }

  std::vector<Ids> Shard::NodeGetRelationshipsIDs(const std::string &type, const std::string &key, Direction direction, const std::string &rel_type) {
    uint64_t id = NodeGetID(type, key);
    return NodeGetRelationshipsIDs(id, direction, rel_type);
  }

  std::vector<Ids> Shard::NodeGetRelationshipsIDs(const std::string &type, const std::string &key, Direction direction, uint16_t type_id) {
    uint64_t id = NodeGetID(type, key);
    return NodeGetRelationshipsIDs(id, direction, type_id);
  }

  std::vector<Ids> Shard::NodeGetRelationshipsIDs(const std::string &type, const std::string &key, Direction direction, const std::vector<std::string> &rel_types) {
    uint64_t id = NodeGetID(type, key);
    return NodeGetRelationshipsIDs(id, direction, rel_types);
  }

  std::vector<Ids> Shard::NodeGetRelationshipsIDs(uint64_t id) {
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      std::vector<Ids> ids;
      for (const auto &[type, list] : outgoing_relationships.at(internal_id)) {
        std::copy(std::begin(list), std::end(list), std::back_inserter(ids));
      }
      for (const auto &[type, list] : incoming_relationships.at(internal_id)) {
        std::copy(std::begin(list), std::end(list), std::back_inserter(ids));
      }
      return ids;
    }
    return std::vector<Ids>();
  }

  std::vector<Node> Shard::NodesGet(const std::vector<uint64_t>& node_ids) {
    std::vector<Node> sharded_nodes;

    for(uint64_t id : node_ids) {
      uint64_t internal_id = externalToInternal(id);
      sharded_nodes.push_back(nodes.at(internal_id));
    }

    return sharded_nodes;
  }

  std::vector<Relationship> Shard::RelationshipsGet(const std::vector<uint64_t>& rel_ids) {
    std::vector<Relationship> sharded_relationships;

    for(uint64_t id : rel_ids) {
      uint64_t internal_id = externalToInternal(id);
      sharded_relationships.push_back(relationships.at(internal_id));
    }

    return sharded_relationships;
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedRelationshipIDs(const std::string& type, const std::string& key) {
    uint64_t id = NodeGetID(type, key);

    return NodeGetShardedRelationshipIDs(id);
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedRelationshipIDs(const std::string& type, const std::string& key, const std::string& rel_type) {
    uint64_t id = NodeGetID(type, key);

    return NodeGetShardedRelationshipIDs(id, rel_type);
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedRelationshipIDs(const std::string& type, const std::string& key, uint16_t type_id) {
    uint64_t id = NodeGetID(type, key);

    return NodeGetShardedRelationshipIDs(id, type_id);
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedRelationshipIDs(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types) {
    uint64_t id = NodeGetID(type, key);

    return NodeGetShardedRelationshipIDs(id, rel_types);
  }


  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedRelationshipIDs(uint64_t id) {
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      std::map<uint16_t, std::vector<uint64_t>> sharded_relationships_ids;
      for (int i = 0; i < cpus; i++) {
        sharded_relationships_ids.insert({i, std::vector<uint64_t>() });
      }

      for (auto &types : outgoing_relationships.at(internal_id)) {
        for (Ids ids : types.ids) {
          sharded_relationships_ids.at(shard_id).emplace_back(ids.rel_id);
        }
      }

      for (auto &types : incoming_relationships.at(internal_id)) {
        for (Ids ids : types.ids) {
          uint16_t node_shard_id = CalculateShardId(ids.node_id);
          sharded_relationships_ids.at(node_shard_id).emplace_back(ids.rel_id);
        }
      }

      for (int i = 0; i < cpus; i++) {
        if (sharded_relationships_ids.at(i).empty()) {
          sharded_relationships_ids.erase(i);
        }
      }

      return sharded_relationships_ids;
    }
    return std::map<uint16_t , std::vector<uint64_t>>();
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedRelationshipIDs(uint64_t id, const std::string& rel_type) {
    if (ValidNodeId(id)) {
      uint16_t type_id = relationship_types.getTypeId(rel_type);
      uint64_t internal_id = externalToInternal(id);
      std::map<uint16_t, std::vector<uint64_t>> sharded_relationships_ids;
      for (int i = 0; i < cpus; i++) {
        sharded_relationships_ids.insert({i, std::vector<uint64_t>() });
      }

      auto group = find_if(std::begin(outgoing_relationships.at(internal_id)), std::end(outgoing_relationships.at(internal_id)),
                           [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

      if (group != std::end(outgoing_relationships.at(internal_id))) {
        for(Ids ids : group->ids) {
          sharded_relationships_ids.at(shard_id).emplace_back(ids.rel_id);
        }
      }

      group = find_if(std::begin(incoming_relationships.at(internal_id)), std::end(incoming_relationships.at(internal_id)),
                      [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

      if (group != std::end(incoming_relationships.at(internal_id))) {
        for(Ids ids : group->ids) {
          uint16_t node_shard_id = CalculateShardId(ids.node_id);
          sharded_relationships_ids.at(node_shard_id).emplace_back(ids.rel_id);
        }
      }

      for (int i = 0; i < cpus; i++) {
        if (sharded_relationships_ids.at(i).empty()) {
          sharded_relationships_ids.erase(i);
        }
      }

      return sharded_relationships_ids;
    }
    return std::map<uint16_t , std::vector<uint64_t>>();
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedRelationshipIDs(uint64_t id, uint16_t type_id) {
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      std::map<uint16_t, std::vector<uint64_t>> sharded_relationships_ids;
      for (int i = 0; i < cpus; i++) {
        sharded_relationships_ids.insert({i, std::vector<uint64_t>() });
      }

      auto group = find_if(std::begin(outgoing_relationships.at(internal_id)), std::end(outgoing_relationships.at(internal_id)),
                           [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

      if (group != std::end(outgoing_relationships.at(internal_id))) {
        for(Ids ids : group->ids) {
          sharded_relationships_ids.at(shard_id).emplace_back(ids.rel_id);
        }
      }

      group = find_if(std::begin(incoming_relationships.at(internal_id)), std::end(incoming_relationships.at(internal_id)),
                      [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

      if (group != std::end(incoming_relationships.at(internal_id))) {
        for(Ids ids : group->ids) {
          uint16_t node_shard_id = CalculateShardId(ids.node_id);
          sharded_relationships_ids.at(node_shard_id).emplace_back(ids.rel_id);
        }
      }

      for (int i = 0; i < cpus; i++) {
        if(sharded_relationships_ids.at(i).empty()) {
          sharded_relationships_ids.erase(i);
        }
      }

      return sharded_relationships_ids;
    }
    return std::map<uint16_t , std::vector<uint64_t>>();
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedRelationshipIDs(uint64_t id,const std::vector<std::string> &rel_types) {
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      std::map<uint16_t, std::vector<uint64_t>> sharded_relationships_ids;
      for (int i = 0; i < cpus; i++) {
        sharded_relationships_ids.insert({i, std::vector<uint64_t>() });
      }

      for (const auto &rel_type : rel_types) {
        uint16_t type_id = relationship_types.getTypeId(rel_type);
        if (type_id > 0) {

          auto group = find_if(std::begin(outgoing_relationships.at(internal_id)), std::end(outgoing_relationships.at(internal_id)),
                               [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

          if (group != std::end(outgoing_relationships.at(internal_id))) {
            for(Ids ids : group->ids) {
              sharded_relationships_ids.at(shard_id).emplace_back(ids.rel_id);
            }
          }

          group = find_if(std::begin(incoming_relationships.at(internal_id)), std::end(incoming_relationships.at(internal_id)),
                          [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

          if (group != std::end(incoming_relationships.at(internal_id))) {
            for(Ids ids : group->ids) {
              uint16_t node_shard_id = CalculateShardId(ids.node_id);
              sharded_relationships_ids.at(node_shard_id).emplace_back(ids.rel_id);
            }
          }

        }
      }

      for (int i = 0; i < cpus; i++) {
        if(sharded_relationships_ids.at(i).empty()) {
          sharded_relationships_ids.erase(i);
        }
      }

      return sharded_relationships_ids;
    }
    return std::map<uint16_t , std::vector<uint64_t>>();
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedNodeIDs(const std::string& type, const std::string& key) {
    uint64_t id = NodeGetID(type, key);

    return NodeGetShardedNodeIDs(id);
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedNodeIDs(const std::string& type, const std::string& key, const std::string& rel_type) {
    uint64_t id = NodeGetID(type, key);

    return NodeGetShardedNodeIDs(id, rel_type);
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedNodeIDs(const std::string& type, const std::string& key, uint16_t type_id) {
    uint64_t id = NodeGetID(type, key);

    return NodeGetShardedNodeIDs(id, type_id);
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedNodeIDs(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types) {
    uint64_t id = NodeGetID(type, key);

    return NodeGetShardedNodeIDs(id, rel_types);
  }


  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedNodeIDs(uint64_t id) {
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
      for (int i = 0; i < cpus; i++) {
        sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
      }

      for (auto &types : outgoing_relationships.at(internal_id)) {
        for (Ids ids : types.ids) {
          uint16_t node_shard_id = CalculateShardId(ids.node_id);
          sharded_nodes_ids.at(node_shard_id).push_back(ids.node_id);
        }
      }

      for (auto &types : incoming_relationships.at(internal_id)) {
        for (Ids ids : types.ids) {
          uint16_t node_shard_id = CalculateShardId(ids.node_id);
          sharded_nodes_ids.at(node_shard_id).push_back(ids.node_id);
        }
      }

      for (int i = 0; i < cpus; i++) {
        if (sharded_nodes_ids.at(i).empty()) {
          sharded_nodes_ids.erase(i);
        }
      }

      return sharded_nodes_ids;
    }
    return std::map<uint16_t , std::vector<uint64_t>>();
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedNodeIDs(uint64_t id, const std::string& rel_type) {
    if (ValidNodeId(id)) {
      uint16_t type_id = relationship_types.getTypeId(rel_type);
      uint64_t internal_id = externalToInternal(id);
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
      for (int i = 0; i < cpus; i++) {
        sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
      }

      auto group = find_if(std::begin(outgoing_relationships.at(internal_id)), std::end(outgoing_relationships.at(internal_id)),
                           [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

      if (group != std::end(outgoing_relationships.at(internal_id))) {
        for(Ids ids : group->ids) {
          sharded_nodes_ids.at(shard_id).emplace_back(ids.node_id);
        }
      }

      group = find_if(std::begin(incoming_relationships.at(internal_id)), std::end(incoming_relationships.at(internal_id)),
                      [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

      if (group != std::end(incoming_relationships.at(internal_id))) {
        for(Ids ids : group->ids) {
          uint16_t node_shard_id = CalculateShardId(ids.node_id);
          sharded_nodes_ids.at(node_shard_id).emplace_back(ids.node_id);
        }
      }

      for (int i = 0; i < cpus; i++) {
        if (sharded_nodes_ids.at(i).empty()) {
          sharded_nodes_ids.erase(i);
        }
      }

      return sharded_nodes_ids;
    }
    return std::map<uint16_t , std::vector<uint64_t>>();
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedNodeIDs(uint64_t id, uint16_t type_id) {
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
      for (int i = 0; i < cpus; i++) {
        sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
      }

      auto group = find_if(std::begin(outgoing_relationships.at(internal_id)), std::end(outgoing_relationships.at(internal_id)),
                           [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

      if (group != std::end(outgoing_relationships.at(internal_id))) {
        for(Ids ids : group->ids) {
          sharded_nodes_ids.at(shard_id).emplace_back(ids.node_id);
        }
      }

      group = find_if(std::begin(incoming_relationships.at(internal_id)), std::end(incoming_relationships.at(internal_id)),
                      [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

      if (group != std::end(incoming_relationships.at(internal_id))) {
        for(Ids ids : group->ids) {
          uint16_t node_shard_id = CalculateShardId(ids.node_id);
          sharded_nodes_ids.at(node_shard_id).emplace_back(ids.node_id);
        }
      }

      for (int i = 0; i < cpus; i++) {
        if(sharded_nodes_ids.at(i).empty()) {
          sharded_nodes_ids.erase(i);
        }
      }

      return sharded_nodes_ids;
    }
    return std::map<uint16_t , std::vector<uint64_t>>();
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedNodeIDs(uint64_t id,const std::vector<std::string> &rel_types) {
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
      for (int i = 0; i < cpus; i++) {
        sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
      }

      for (const auto &rel_type : rel_types) {
        uint16_t type_id = relationship_types.getTypeId(rel_type);
        if (type_id > 0) {
          auto group = find_if(std::begin(outgoing_relationships.at(internal_id)), std::end(outgoing_relationships.at(internal_id)),
                               [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

          if (group != std::end(outgoing_relationships.at(internal_id))) {
            for(Ids ids : group->ids) {
              sharded_nodes_ids.at(shard_id).emplace_back(ids.node_id);
            }
          }

          group = find_if(std::begin(incoming_relationships.at(internal_id)), std::end(incoming_relationships.at(internal_id)),
                          [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

          if (group != std::end(incoming_relationships.at(internal_id))) {
            for(Ids ids : group->ids) {
              uint16_t node_shard_id = CalculateShardId(ids.node_id);
              sharded_nodes_ids.at(node_shard_id).emplace_back(ids.node_id);
            }
          }
        }
      }

      for (int i = 0; i < cpus; i++) {
        if(sharded_nodes_ids.at(i).empty()) {
          sharded_nodes_ids.erase(i);
        }
      }

      return sharded_nodes_ids;
    }
    return std::map<uint16_t , std::vector<uint64_t>>();
  }

  std::vector<Relationship> Shard::NodeGetOutgoingRelationships(const std::string& type, const std::string& key) {
    uint64_t id = NodeGetID(type, key);
    return NodeGetOutgoingRelationships(id);
  }

  std::vector<Relationship> Shard::NodeGetOutgoingRelationships(const std::string& type, const std::string& key, const std::string& rel_type) {
    uint64_t id = NodeGetID(type, key);
    return NodeGetOutgoingRelationships(id, rel_type);
  }

  std::vector<Relationship> Shard::NodeGetOutgoingRelationships(const std::string& type, const std::string& key, uint16_t type_id) {
    uint64_t id = NodeGetID(type, key);
    return NodeGetOutgoingRelationships(id, type_id);
  }

  std::vector<Relationship> Shard::NodeGetOutgoingRelationships(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types) {
    uint64_t id = NodeGetID(type, key);
    return NodeGetOutgoingRelationships(id, rel_types);
  }

  std::vector<Relationship> Shard::NodeGetOutgoingRelationships(uint64_t id) {
    std::vector<Relationship> node_relationships;
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      for (auto &types : outgoing_relationships.at(internal_id)) {
        for (Ids ids : types.ids) {
          node_relationships.push_back(relationships.at(ids.rel_id));
        }
      }
    }
    return node_relationships;
  }

  std::vector<Relationship> Shard::NodeGetOutgoingRelationships(uint64_t id, const std::string& rel_type) {
    std::vector<Relationship> node_relationships;
    if (ValidNodeId(id)) {
      uint16_t type_id = relationship_types.getTypeId(rel_type);
      uint64_t internal_id = externalToInternal(id);

      auto group = find_if(std::begin(outgoing_relationships.at(internal_id)), std::end(outgoing_relationships.at(internal_id)),
                           [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

      if (group != std::end(outgoing_relationships.at(internal_id))) {
        for(Ids ids : group->ids) {
          node_relationships.emplace_back(relationships.at(ids.rel_id));
        }
      }

    }
    return node_relationships;
  }

  std::vector<Relationship> Shard::NodeGetOutgoingRelationships(uint64_t id, uint16_t type_id) {
    std::vector<Relationship> node_relationships;
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);

      auto group = find_if(std::begin(outgoing_relationships.at(internal_id)), std::end(outgoing_relationships.at(internal_id)),
                           [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

      if (group != std::end(outgoing_relationships.at(internal_id))) {
        for(Ids ids : group->ids) {
          node_relationships.emplace_back(relationships.at(ids.rel_id));
        }
      }

    }
    return node_relationships;
  }

  std::vector<Relationship> Shard::NodeGetOutgoingRelationships(uint64_t id, const std::vector<std::string> &rel_types) {
    std::vector<Relationship> node_relationships;
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      for (const auto &rel_type : rel_types) {
        uint16_t type_id = relationship_types.getTypeId(rel_type);
        if (type_id > 0) {
          auto group = find_if(std::begin(outgoing_relationships.at(internal_id)), std::end(outgoing_relationships.at(internal_id)),
                               [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

          if (group != std::end(outgoing_relationships.at(internal_id))) {
            for(Ids ids : group->ids) {
              node_relationships.emplace_back(relationships.at(ids.rel_id));
            }
          }
        }
      }
    }
    return node_relationships;
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingRelationshipIDs(const std::string& type, const std::string& key) {
    uint64_t id = NodeGetID(type, key);
    return NodeGetShardedIncomingRelationshipIDs(id);
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingRelationshipIDs(const std::string& type, const std::string& key, const std::string& rel_type) {
    uint64_t id = NodeGetID(type, key);
    return NodeGetShardedIncomingRelationshipIDs(id, rel_type);
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingRelationshipIDs(const std::string& type, const std::string& key, uint16_t type_id) {
    uint64_t id = NodeGetID(type, key);
    return NodeGetShardedIncomingRelationshipIDs(id, type_id);
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingRelationshipIDs(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types) {
    uint64_t id = NodeGetID(type, key);
    return NodeGetShardedIncomingRelationshipIDs(id, rel_types);
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingRelationshipIDs(uint64_t id) {
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      std::map<uint16_t, std::vector<uint64_t>> sharded_relationships_ids;
      for (int i = 0; i < cpus; i++) {
        sharded_relationships_ids.insert({i, std::vector<uint64_t>() });
      }

      for (auto &types : incoming_relationships.at(internal_id)) {
        for (Ids ids : types.ids) {
          uint16_t node_shard_id = CalculateShardId(ids.node_id);
          sharded_relationships_ids.at(node_shard_id).push_back(ids.rel_id);
        }
      }

      for (int i = 0; i < cpus; i++) {
        if(sharded_relationships_ids.at(i).empty()) {
          sharded_relationships_ids.erase(i);
        }
      }

      return sharded_relationships_ids;
    }
    return std::map<uint16_t , std::vector<uint64_t>>();
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingRelationshipIDs(uint64_t id, const std::string& rel_type) {
    if (ValidNodeId(id)) {
      uint16_t type_id = relationship_types.getTypeId(rel_type);
      uint64_t internal_id = externalToInternal(id);
      std::map<uint16_t, std::vector<uint64_t>> sharded_relationships_ids;
      for (int i = 0; i < cpus; i++) {
        sharded_relationships_ids.insert({i, std::vector<uint64_t>() });
      }

      auto group = find_if(std::begin(incoming_relationships.at(internal_id)), std::end(incoming_relationships.at(internal_id)),
                      [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

      if (group != std::end(incoming_relationships.at(internal_id))) {
        for(Ids ids : group->ids) {
          uint16_t node_shard_id = CalculateShardId(ids.node_id);
          sharded_relationships_ids.at(node_shard_id).emplace_back(ids.rel_id);
        }
      }

      for (int i = 0; i < cpus; i++) {
        if(sharded_relationships_ids.at(i).empty()) {
          sharded_relationships_ids.erase(i);
        }
      }

      return sharded_relationships_ids;
    }
    return std::map<uint16_t , std::vector<uint64_t>>();
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingRelationshipIDs(uint64_t id, uint16_t type_id) {
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      std::map<uint16_t, std::vector<uint64_t>> sharded_relationships_ids;
      for (int i = 0; i < cpus; i++) {
        sharded_relationships_ids.insert({i, std::vector<uint64_t>() });
      }

      auto group = find_if(std::begin(incoming_relationships.at(internal_id)), std::end(incoming_relationships.at(internal_id)),
                           [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

      if (group != std::end(incoming_relationships.at(internal_id))) {
        for(Ids ids : group->ids) {
          uint16_t node_shard_id = CalculateShardId(ids.node_id);
          sharded_relationships_ids.at(node_shard_id).emplace_back(ids.rel_id);
        }
      }

      for (int i = 0; i < cpus; i++) {
        if(sharded_relationships_ids.at(i).empty()) {
          sharded_relationships_ids.erase(i);
        }
      }

      return sharded_relationships_ids;
    }
    return std::map<uint16_t , std::vector<uint64_t>>();
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingRelationshipIDs(uint64_t id, const std::vector<std::string> &rel_types) {
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      std::map<uint16_t, std::vector<uint64_t>> sharded_relationships_ids;
      for (int i = 0; i < cpus; i++) {
        sharded_relationships_ids.insert({i, std::vector<uint64_t>() });
      }
      for (const auto &rel_type : rel_types) {
        uint16_t type_id = relationship_types.getTypeId(rel_type);
        if (type_id > 0) {
          auto group = find_if(std::begin(incoming_relationships.at(internal_id)), std::end(incoming_relationships.at(internal_id)),
                               [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

          if (group != std::end(incoming_relationships.at(internal_id))) {
            for(Ids ids : group->ids) {
              uint16_t node_shard_id = CalculateShardId(ids.node_id);
              sharded_relationships_ids.at(node_shard_id).emplace_back(ids.rel_id);
            }
          }
        }
      }

      for (int i = 0; i < cpus; i++) {
        if(sharded_relationships_ids.at(i).empty()) {
          sharded_relationships_ids.erase(i);
        }
      }

      return sharded_relationships_ids;
    }
    return std::map<uint16_t , std::vector<uint64_t>>();
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingNodeIDs(const std::string& type, const std::string& key) {
    uint64_t id = NodeGetID(type, key);
    return NodeGetShardedIncomingNodeIDs(id);
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingNodeIDs(const std::string& type, const std::string& key, const std::string& rel_type) {
    uint64_t id = NodeGetID(type, key);
    return NodeGetShardedIncomingNodeIDs(id, rel_type);
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingNodeIDs(const std::string& type, const std::string& key, uint16_t type_id) {
    uint64_t id = NodeGetID(type, key);
    return NodeGetShardedIncomingNodeIDs(id, type_id);
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingNodeIDs(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types) {
    uint64_t id = NodeGetID(type, key);
    return NodeGetShardedIncomingNodeIDs(id, rel_types);
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingNodeIDs(uint64_t id) {
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
      for (int i = 0; i < cpus; i++) {
        sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
      }

      for (auto &types : incoming_relationships.at(internal_id)) {
        for (Ids ids : types.ids) {
          uint16_t node_shard_id = CalculateShardId(ids.node_id);
          sharded_nodes_ids.at(node_shard_id).emplace_back(ids.node_id);
        }
      }

      for (int i = 0; i < cpus; i++) {
        if(sharded_nodes_ids.at(i).empty()) {
          sharded_nodes_ids.erase(i);
        }
      }

      return sharded_nodes_ids;
    }
    return std::map<uint16_t , std::vector<uint64_t>>();
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingNodeIDs(uint64_t id, const std::string& rel_type) {
    if (ValidNodeId(id)) {
      uint16_t type_id = relationship_types.getTypeId(rel_type);
      uint64_t internal_id = externalToInternal(id);
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
      for (int i = 0; i < cpus; i++) {
        sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
      }

      auto group = find_if(std::begin(incoming_relationships.at(internal_id)), std::end(incoming_relationships.at(internal_id)),
                      [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

      if (group != std::end(incoming_relationships.at(internal_id))) {
        for(Ids ids : group->ids) {
          uint16_t node_shard_id = CalculateShardId(ids.node_id);
          sharded_nodes_ids.at(node_shard_id).emplace_back(ids.node_id);
        }
      }

      for (int i = 0; i < cpus; i++) {
        if(sharded_nodes_ids.at(i).empty()) {
          sharded_nodes_ids.erase(i);
        }
      }

      return sharded_nodes_ids;
    }
    return std::map<uint16_t , std::vector<uint64_t>>();
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingNodeIDs(uint64_t id, uint16_t type_id) {
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
      for (int i = 0; i < cpus; i++) {
        sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
      }

      auto group = find_if(std::begin(incoming_relationships.at(internal_id)), std::end(incoming_relationships.at(internal_id)),
                           [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

      if (group != std::end(incoming_relationships.at(internal_id))) {
        for(Ids ids : group->ids) {
          uint16_t node_shard_id = CalculateShardId(ids.node_id);
          sharded_nodes_ids.at(node_shard_id).emplace_back(ids.node_id);
        }
      }

      for (int i = 0; i < cpus; i++) {
        if(sharded_nodes_ids.at(i).empty()) {
          sharded_nodes_ids.erase(i);
        }
      }

      return sharded_nodes_ids;
    }
    return std::map<uint16_t , std::vector<uint64_t>>();
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedIncomingNodeIDs(uint64_t id, const std::vector<std::string> &rel_types) {
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
      for (int i = 0; i < cpus; i++) {
        sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
      }
      for (const auto &rel_type : rel_types) {
        uint16_t type_id = relationship_types.getTypeId(rel_type);
        if (type_id > 0) {
          auto group = find_if(std::begin(incoming_relationships.at(internal_id)), std::end(incoming_relationships.at(internal_id)),
                               [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

          if (group != std::end(incoming_relationships.at(internal_id))) {
            for(Ids ids : group->ids) {
              uint16_t node_shard_id = CalculateShardId(ids.node_id);
              sharded_nodes_ids.at(node_shard_id).emplace_back(ids.node_id);
            }
          }
        }
      }

      for (int i = 0; i < cpus; i++) {
        if(sharded_nodes_ids.at(i).empty()) {
          sharded_nodes_ids.erase(i);
        }
      }

      return sharded_nodes_ids;
    }
    return std::map<uint16_t , std::vector<uint64_t>>();
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedOutgoingNodeIDs(const std::string& type, const std::string& key) {
    uint64_t id = NodeGetID(type, key);
    return NodeGetShardedOutgoingNodeIDs(id);
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedOutgoingNodeIDs(const std::string& type, const std::string& key, const std::string& rel_type) {
    uint64_t id = NodeGetID(type, key);
    return NodeGetShardedOutgoingNodeIDs(id, rel_type);
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedOutgoingNodeIDs(const std::string& type, const std::string& key, uint16_t type_id) {
    uint64_t id = NodeGetID(type, key);
    return NodeGetShardedOutgoingNodeIDs(id, type_id);
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedOutgoingNodeIDs(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types) {
    uint64_t id = NodeGetID(type, key);
    return NodeGetShardedOutgoingNodeIDs(id, rel_types);
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedOutgoingNodeIDs(uint64_t id) {
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
      for (int i = 0; i < cpus; i++) {
        sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
      }

      for (auto &types : outgoing_relationships.at(internal_id)) {
        for (Ids ids : types.ids) {
          uint16_t node_shard_id = CalculateShardId(ids.node_id);
          sharded_nodes_ids.at(node_shard_id).push_back(ids.node_id);
        }
      }

      for (int i = 0; i < cpus; i++) {
        if(sharded_nodes_ids.at(i).empty()) {
          sharded_nodes_ids.erase(i);
        }
      }

      return sharded_nodes_ids;
    }
    return std::map<uint16_t , std::vector<uint64_t>>();
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedOutgoingNodeIDs(uint64_t id, const std::string& rel_type) {
    if (ValidNodeId(id)) {
      uint16_t type_id = relationship_types.getTypeId(rel_type);
      uint64_t internal_id = externalToInternal(id);
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
      for (int i = 0; i < cpus; i++) {
        sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
      }

      auto group = find_if(std::begin(outgoing_relationships.at(internal_id)), std::end(outgoing_relationships.at(internal_id)),
                           [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

      if (group != std::end(outgoing_relationships.at(internal_id))) {
        for(Ids ids : group->ids) {
          sharded_nodes_ids.at(shard_id).emplace_back(ids.node_id);
        }
      }

      for (int i = 0; i < cpus; i++) {
        if(sharded_nodes_ids.at(i).empty()) {
          sharded_nodes_ids.erase(i);
        }
      }

      return sharded_nodes_ids;
    }
    return std::map<uint16_t , std::vector<uint64_t>>();
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedOutgoingNodeIDs(uint64_t id, uint16_t type_id) {
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
      for (int i = 0; i < cpus; i++) {
        sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
      }

      auto group = find_if(std::begin(outgoing_relationships.at(internal_id)), std::end(outgoing_relationships.at(internal_id)),
                           [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

      if (group != std::end(outgoing_relationships.at(internal_id))) {
        for(Ids ids : group->ids) {
          sharded_nodes_ids.at(shard_id).emplace_back(ids.node_id);
        }
      }

      for (int i = 0; i < cpus; i++) {
        if(sharded_nodes_ids.at(i).empty()) {
          sharded_nodes_ids.erase(i);
        }
      }

      return sharded_nodes_ids;
    }
    return std::map<uint16_t , std::vector<uint64_t>>();
  }

  std::map<uint16_t, std::vector<uint64_t>> Shard::NodeGetShardedOutgoingNodeIDs(uint64_t id, const std::vector<std::string> &rel_types) {
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      std::map<uint16_t, std::vector<uint64_t>> sharded_nodes_ids;
      for (int i = 0; i < cpus; i++) {
        sharded_nodes_ids.insert({i, std::vector<uint64_t>() });
      }
      for (const auto &rel_type : rel_types) {
        uint16_t type_id = relationship_types.getTypeId(rel_type);
        if (type_id > 0) {
          auto group = find_if(std::begin(outgoing_relationships.at(internal_id)), std::end(outgoing_relationships.at(internal_id)),
                               [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

          if (group != std::end(outgoing_relationships.at(internal_id))) {
            for(Ids ids : group->ids) {
              sharded_nodes_ids.at(shard_id).emplace_back(ids.node_id);
            }
          }
        }
      }

      for (int i = 0; i < cpus; i++) {
        if(sharded_nodes_ids.at(i).empty()) {
          sharded_nodes_ids.erase(i);
        }
      }

      return sharded_nodes_ids;
    }
    return std::map<uint16_t , std::vector<uint64_t>>();
  }

  std::vector<Ids> Shard::NodeGetRelationshipsIDs(uint64_t id, Direction direction) {
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      std::vector<Ids> ids;
      // Use the two ifs to handle ALL for a direction
      if (direction != IN) {
        for (const auto &[type, list] : outgoing_relationships.at(internal_id)) {
          std::copy(std::begin(list), std::end(list), std::back_inserter(ids));
        }
      }
      // Use the two ifs to handle ALL for a direction
      if (direction != OUT) {
        for (const auto &[type, list] : incoming_relationships.at(internal_id)) {
          std::copy(std::begin(list), std::end(list), std::back_inserter(ids));
        }
      }
      return ids;
    }
    return std::vector<Ids>();
  }

  std::vector<Ids> Shard::NodeGetRelationshipsIDs(uint64_t id, Direction direction, const std::string &rel_type) {
    if (ValidNodeId(id)) {
      uint16_t type_id = relationship_types.getTypeId(rel_type);
      uint64_t internal_id = externalToInternal(id);
      std::vector<Ids> ids;
      uint64_t size = 0;
      // Use the two ifs to handle ALL for a direction
      auto out_group = find_if(std::begin(outgoing_relationships.at(internal_id)), std::end(outgoing_relationships.at(internal_id)),
                           [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

      if (out_group != std::end(outgoing_relationships.at(internal_id))) {
        size += out_group->ids.size();
      }

      auto in_group = find_if(std::begin(incoming_relationships.at(internal_id)), std::end(incoming_relationships.at(internal_id)),
                           [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

      if (in_group != std::end(incoming_relationships.at(internal_id))) {
        size += in_group->ids.size();
      }

      ids.reserve(size);

      // Use the two ifs to handle ALL for a direction
      if (direction != IN) {
        std::copy(std::begin(out_group->ids), std::end(out_group->ids), std::back_inserter(ids));
      }
      if (direction != OUT) {
        std::copy(std::begin(in_group->ids), std::end(in_group->ids), std::back_inserter(ids));
      }
      return ids;
    }
    return std::vector<Ids>();
  }

  std::vector<Ids> Shard::NodeGetRelationshipsIDs(uint64_t id, Direction direction, uint16_t type_id) {
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      if (direction == IN) {

        auto in_group = find_if(std::begin(incoming_relationships.at(internal_id)), std::end(incoming_relationships.at(internal_id)),
                                [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

        if (in_group != std::end(incoming_relationships.at(internal_id))) {
          return in_group->ids;
        }
      }

      if (direction == OUT) {
        auto out_group = find_if(std::begin(outgoing_relationships.at(internal_id)), std::end(outgoing_relationships.at(internal_id)),
                                 [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

        if (out_group != std::end(outgoing_relationships.at(internal_id))) {
          return out_group->ids;
        }
      }

      std::vector<Ids> ids;
      uint64_t size = 0;
      auto out_group = find_if(std::begin(outgoing_relationships.at(internal_id)), std::end(outgoing_relationships.at(internal_id)),
                               [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

      if (out_group != std::end(outgoing_relationships.at(internal_id))) {
        size += out_group->ids.size();
      }

      auto in_group = find_if(std::begin(incoming_relationships.at(internal_id)), std::end(incoming_relationships.at(internal_id)),
                              [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

      if (in_group != std::end(incoming_relationships.at(internal_id))) {
        size += in_group->ids.size();
      }

      ids.reserve(size);

      std::copy(std::begin(out_group->ids), std::end(out_group->ids), std::back_inserter(ids));
      std::copy(std::begin(in_group->ids), std::end(in_group->ids), std::back_inserter(ids));

      return ids;
    }
    return std::vector<Ids>();
  }

  std::vector<Ids> Shard::NodeGetRelationshipsIDs(uint64_t id, Direction direction, const std::vector<std::string> &rel_types) {
    if (ValidNodeId(id)) {
      uint64_t internal_id = externalToInternal(id);
      std::vector<Ids> ids;
      // Use the two ifs to handle ALL for a direction
      if (direction != IN) {
        // For each requested type sum up the values
        for (const auto &rel_type : rel_types) {
          uint16_t type_id = relationship_types.getTypeId(rel_type);
          if (type_id > 0) {
            auto out_group = find_if(std::begin(outgoing_relationships.at(internal_id)), std::end(outgoing_relationships.at(internal_id)),
                                     [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

            if (out_group != std::end(outgoing_relationships.at(internal_id))) {
              std::copy(std::begin(out_group->ids), std::end(out_group->ids), std::back_inserter(ids));
            }
          }
        }
      }
      // Use the two ifs to handle ALL for a direction
      if (direction != OUT) {
        // For each requested type sum up the values
        for (const auto &rel_type : rel_types) {
          uint16_t type_id = relationship_types.getTypeId(rel_type);
          if (type_id > 0) {
            auto in_group = find_if(std::begin(incoming_relationships.at(internal_id)), std::end(incoming_relationships.at(internal_id)),
                                    [type_id] (const Group& g) { return g.rel_type_id == type_id; } );

            if (in_group != std::end(incoming_relationships.at(internal_id))) {
              std::copy(std::begin(in_group->ids), std::end(in_group->ids), std::back_inserter(ids));
            }
          }
        }
      }
      return ids;
    }
    return std::vector<Ids>();
  }

  void Shard::convertProperties(std::map<std::string, std::any> &values, const dom::object &object) const {
    for (auto[key, value] : object) {
      auto property = static_cast<std::string>(key);
      switch (value.type()) {
      case dom::element_type::INT64:
        values.insert({ property, int64_t(value) });
        break;
      case dom::element_type::UINT64:
        // Unsigned Integer Values are not allowed, convert to signed
        values.insert({ property, static_cast<std::make_signed_t<uint64_t>>(value) });
        break;
      case dom::element_type::DOUBLE:
        values.insert({ property, double(value) });
        break;
      case dom::element_type::STRING:
        values.insert({ property, std::string(value) });
        break;
      case dom::element_type::BOOL:
        values.insert({ property, bool(value) });
        break;
      case dom::element_type::NULL_VALUE:
        // Null Values are not allowed, just ignore them
        break;
      case dom::element_type::OBJECT: {
        std::map<std::string, std::any> nested;
        convertProperties(nested, value);
        values.insert({ property, nested });
        break;
      }
      case dom::element_type::ARRAY:
        // TODO: Finish this to Support Array properties
        auto array = dom::array(value);
        if (array.size() > 0) {
          dom::element first = array.at(0);
          std::vector<std::any> any_vector;
          std::vector<int64_t> int_vector;
          std::vector<double> double_vector;
          std::vector<std::string> string_vector;
          std::vector<bool> bool_vector;
          std::vector<std::map<std::string, std::any>> object_vector;
          switch (first.type()) {
          case dom::element_type::ARRAY:
            break;
          case dom::element_type::OBJECT:
            break;
          case dom::element_type::INT64:
            for (dom::element child : dom::array(value)) {
              int_vector.emplace_back(int64_t(child));
            }
            values.insert({ property, int_vector });
            break;
          case dom::element_type::UINT64:
            for (dom::element child : dom::array(value)) {
              int_vector.emplace_back(static_cast<std::make_signed_t<uint64_t>>(child));
            }
            values.insert({ property, int_vector });
            break;
          case dom::element_type::DOUBLE:
            for (dom::element child : dom::array(value)) {
              double_vector.emplace_back(double(child));
            }
            values.insert({ property, double_vector });
            break;
          case dom::element_type::STRING:
            for (dom::element child : dom::array(value)) {
              string_vector.emplace_back(child);
            }
            values.insert({ property, string_vector });
            break;
          case dom::element_type::BOOL:
            for (dom::element child : dom::array(value)) {
              bool_vector.emplace_back(bool(child));
            }
            values.insert({ property, bool_vector });
            break;
          case dom::element_type::NULL_VALUE:
            // Null Values are not allowed, just ignore them
            break;
          }
        }
        break;
      }
    }
  }

  // All Node Ids
  Roaring64Map Shard::AllNodeIdsMap() {
    return node_types.getIds();
  }

  Roaring64Map Shard::AllNodeIdsMap(const std::string &type) {
    uint16_t type_id = node_types.getTypeId(type);
    return AllNodeIdsMap(type_id);
  }

  Roaring64Map Shard::AllNodeIdsMap(uint16_t type_id) {
    return node_types.getIds(type_id);
  }

  std::vector<uint64_t> Shard::AllNodeIds(uint64_t skip, uint64_t limit) {
    std::vector<uint64_t> ids;
    Roaring64Map bitmap = node_types.getIds();
    int current = 1;
    for (unsigned long i : bitmap) {
      if (current > skip && current <= (skip + limit)) {
        ids.push_back((uint64_t)i);
      }
      current++;
    }
    return ids;
  }

  std::vector<uint64_t> Shard::AllNodeIds(uint16_t type_id, uint64_t skip, uint64_t limit) {
    std::vector<uint64_t> ids;
    Roaring64Map bitmap = node_types.getIds(type_id);
    int current = 1;
    for (unsigned long i : bitmap) {
      if (current > skip && current <= (skip + limit)) {
        ids.push_back((uint64_t)i);
      }
      current++;
    }
   return ids;
  }

  std::vector<uint64_t> Shard::AllNodeIds(const std::string& type, uint64_t skip, uint64_t limit) {
    std::vector<uint64_t> ids;
    uint16_t type_id = node_types.getTypeId(type);
    Roaring64Map bitmap = node_types.getIds(type_id);
    int current = 1;
    for (unsigned long i : bitmap) {
      if (current > skip && current <= (skip + limit)) {
        ids.push_back((uint64_t)i);
      }
      current++;
    }
    return ids;
  }

  std::vector<Node> Shard::AllNodes(uint64_t skip, uint64_t limit) {
    std::vector<Node> some_nodes;
    Roaring64Map bitmap = node_types.getIds();

    int current = 1;
    for (unsigned long i : bitmap) {
      if (current > skip && current <= (skip + limit)) {
        Node node = nodes.at(externalToInternal(i));
        some_nodes.push_back(node);
      }
      current++;
    }
    return some_nodes;
  }

  std::vector<Node> Shard::AllNodes(const std::string& type, uint64_t skip, uint64_t limit) {
    uint16_t type_id = node_types.getTypeId(type);
    return AllNodes(type_id, skip, limit);
  }

  std::vector<Node> Shard::AllNodes(uint16_t type_id, uint64_t skip, uint64_t limit) {
    std::vector<Node> some_nodes;
    Roaring64Map bitmap = node_types.getIds(type_id);
    int current = 1;
    for (unsigned long i : bitmap) {
      if (current > skip && current <= (skip + limit)) {
        Node node = nodes.at(externalToInternal(i));
        some_nodes.push_back(node);
      }
      current++;
    }
    return some_nodes;
  }

  Roaring64Map Shard::AllRelationshipIdsMap() {
    return relationship_types.getIds();
  }

  Roaring64Map Shard::AllRelationshipIdsMap(const std::string &rel_type) {
    uint16_t type_id = relationship_types.getTypeId(rel_type);
    return relationship_types.getIds(type_id);
  }

  Roaring64Map Shard::AllRelationshipIdsMap(uint16_t type_id) {
    return relationship_types.getIds(type_id);
  }

  std::vector<uint64_t> Shard::AllRelationshipIds(uint64_t skip, uint64_t limit) {
    std::vector<uint64_t> ids;
    Roaring64Map bitmap = relationship_types.getIds();
    int current = 1;
    for (unsigned long i : bitmap) {
      if (current > skip && current <= (skip + limit)) {
        ids.push_back({(uint64_t)i});
      }
      current++;
    }
    return ids;
  }

  std::vector<uint64_t> Shard::AllRelationshipIds(const std::string &rel_type, uint64_t skip, uint64_t limit) {
    std::vector<uint64_t> ids;
    uint16_t type_id = relationship_types.getTypeId(rel_type);
    Roaring64Map bitmap = relationship_types.getIds(type_id);
    int current = 1;
    for (unsigned long i : bitmap) {
      if (current > skip && current <= (skip + limit)) {
        ids.push_back({(uint64_t)i});
      }
      current++;
    }
    return ids;
  }

  std::vector<uint64_t> Shard::AllRelationshipIds(uint16_t type_id, uint64_t skip, uint64_t limit) {
    std::vector<uint64_t> ids;
    Roaring64Map bitmap = relationship_types.getIds(type_id);
    int current = 1;
    for (unsigned long i : bitmap) {
      if (current > skip && current <= (skip + limit)) {
        ids.push_back({(uint64_t)i});
      }
      current++;
    }
    return ids;
  }

  std::vector<Relationship> Shard::AllRelationships(uint64_t skip, uint64_t limit) {
    std::vector<Relationship> some_relationships;
    Roaring64Map bitmap = relationship_types.getIds();
    int current = 1;
    for (unsigned long i : bitmap) {
      if (current > skip && current <= (skip + limit)) {
        Relationship relationship = relationships.at(externalToInternal(i));
        some_relationships.push_back(relationship);
      }
      current++;
    }
    return some_relationships;
  }

  std::vector<Relationship> Shard::AllRelationships(const std::string& type, uint64_t skip, uint64_t limit) {
    uint16_t type_id = relationship_types.getTypeId(type);
    return AllRelationships(type_id, skip, limit);
  }

  std::vector<Relationship> Shard::AllRelationships(uint16_t type_id, uint64_t skip, uint64_t limit) {
    std::vector<Relationship> some_relationships;
    Roaring64Map bitmap = relationship_types.getIds(type_id);
    int current = 1;
    for (unsigned long i : bitmap) {
      if (current > skip && current <= (skip + limit)) {
        Relationship relationship = relationships.at(externalToInternal(i));
        some_relationships.push_back(relationship);
      }
      current++;
    }
    return some_relationships;
  }

  // Counts
  std::map<uint16_t, uint64_t> Shard::AllNodeIdCounts() {
    return node_types.getCounts();
  }

  uint64_t Shard::AllNodeIdCounts(const std::string &type) {
    uint16_t type_id = node_types.getTypeId(type);
    return AllNodeIdCounts(type_id);
  }

  uint64_t Shard::AllNodeIdCounts(uint16_t type_id) {
    return node_types.getCount(type_id);
  }

  std::map<uint16_t, uint64_t> Shard::AllRelationshipIdCounts() {
    return relationship_types.getCounts();
  }

  uint64_t Shard::AllRelationshipIdCounts(const std::string &type) {
    uint16_t type_id = relationship_types.getTypeId(type);
    return AllRelationshipIdCounts(type_id);
  }

  uint64_t Shard::AllRelationshipIdCounts(uint16_t type_id) {
    return relationship_types.getCount(type_id);
  }

  // *****************************************************************************************************************************
  //                                               Peered
  // *****************************************************************************************************************************

  // Relationship Types ===================================================================================================================
  uint16_t Shard::RelationshipTypesGetCountPeered() {
    return relationship_types.getSize();
  }

  seastar::future<uint64_t> Shard::RelationshipTypesGetCountPeered(uint16_t type_id) {
    seastar::future<std::vector<uint64_t>> v = container().map([type_id] (Shard &local) {
           return local.RelationshipTypesGetCount(type_id);
    });

    return v.then([] (std::vector<uint64_t> counts) {
           return accumulate(std::begin(counts), std::end(counts), uint64_t(0));
    });
  }

  seastar::future<uint64_t> Shard::RelationshipTypesGetCountPeered(const std::string &type) {
    seastar::future<std::vector<uint64_t>> v = container().map([type] (Shard &local) {
           return local.RelationshipTypesGetCount(type);
    });

    return v.then([] (std::vector<uint64_t> counts) {
           return accumulate(std::begin(counts), std::end(counts), uint64_t(0));
    });
  }

  std::set<std::string> Shard::RelationshipTypesGetPeered() {
    return relationship_types.getTypes();
  }

  // Relationship Type ====================================================================================================================
  std::string Shard::RelationshipTypeGetTypePeered(uint16_t type_id) {
    return relationship_types.getType(type_id);
  }

  uint16_t Shard::RelationshipTypeGetTypeIdPeered(const std::string &type) {
    return relationship_types.getTypeId(type);
  }

  seastar::future<uint16_t> Shard::RelationshipTypeInsertPeered(const std::string &rel_type) {
    // rel_type_id is global, so we need to calculate it here
    uint16_t rel_type_id = relationship_types.getTypeId(rel_type);
    if (rel_type_id == 0) {
      // rel_type_id is global so unfortunately we need to lock here
      this->rel_type_lock.for_write().lock().get();

      // The relationship type was not found and must therefore be new, add it to all shards.
      rel_type_id = relationship_types.insertOrGetTypeId(rel_type);
      this->rel_type_lock.for_write().unlock();
      return container().invoke_on_all([rel_type, rel_type_id](Shard &local_shard) {
                          local_shard.RelationshipTypeInsert(rel_type, rel_type_id);
                        })
        .then([rel_type_id] {
          return seastar::make_ready_future<uint16_t>(rel_type_id);
        });
    }

    return seastar::make_ready_future<uint16_t>(rel_type_id);
  }

  // Node Types ===========================================================================================================================
  uint16_t Shard::NodeTypesGetCountPeered() {
    return node_types.getSize();
  }

  seastar::future<uint64_t> Shard::NodeTypesGetCountPeered(uint16_t type_id) {
    seastar::future<std::vector<uint64_t>> v = container().map([type_id] (Shard &local) {
           return local.NodeTypesGetCount(type_id);
    });

    return v.then([] (std::vector<uint64_t> counts) {
           return accumulate(std::begin(counts), std::end(counts), uint64_t(0));
    });
  }

  seastar::future<uint64_t> Shard::NodeTypesGetCountPeered(const std::string &type) {
    seastar::future<std::vector<uint64_t>> v = container().map([type] (Shard &local) {
           return local.NodeTypesGetCount(type);
    });

    return v.then([] (std::vector<uint64_t> counts) {
           return accumulate(std::begin(counts), std::end(counts), uint64_t(0));
    });
  }

  std::set<std::string> Shard::NodeTypesGetPeered() {
    return node_types.getTypes();
  }

  // Node Type ===========================================================================================================================
  std::string Shard::NodeTypeGetTypePeered(uint16_t type_id) {
    return node_types.getType(type_id);
  }

  uint16_t Shard::NodeTypeGetTypeIdPeered(const std::string &type) {
    return node_types.getTypeId(type);
  }

  seastar::future<uint16_t> Shard::NodeTypeInsertPeered(const std::string &type) {
    uint16_t node_type_id = node_types.getTypeId(type);
    if (node_type_id == 0) {
      // node_type_id is global so unfortunately we need to lock here
      this->node_type_lock.for_write().lock().get();
      // The node type was not found and must therefore be new, add it to all shards.
      node_type_id = node_types.insertOrGetTypeId(type);
      return container().invoke_on_all([type, node_type_id](Shard &local_shard) {
                          local_shard.NodeTypeInsert(type, node_type_id);
                        })
        .then([node_type_id, this] {
          this->node_type_lock.for_write().unlock();
          return seastar::make_ready_future<uint16_t>(node_type_id);
        });
    }

    return seastar::make_ready_future<uint16_t>(node_type_id);
  }

  // Nodes ===============================================================================================================================

  seastar::future<uint64_t> Shard::NodeAddEmptyPeered(const std::string &type, const std::string &key) {
    uint16_t node_shard_id = CalculateShardId(type, key);
    uint16_t node_type_id = node_types.getTypeId(type);

    // The node type exists, so continue on
    if (node_type_id > 0) {
        return container().invoke_on(node_shard_id, [type, node_type_id, key](Shard &local_shard) {
            return local_shard.NodeAddEmpty(type, node_type_id, key);
        });
    }

    // The node type needs to be set by Shard 0 and propagated
    return container().invoke_on(0, [node_shard_id, type, key, this] (Shard &local_shard) {
      return local_shard.NodeTypeInsertPeered(type).then([node_shard_id, type, key, this] (uint16_t node_type_id) {
        return container().invoke_on(node_shard_id, [type, node_type_id, key](Shard &local_shard) {
          return local_shard.NodeAddEmpty(type, node_type_id, key);
        });
      });
    });

  }

  seastar::future<uint64_t> Shard::NodeAddPeered(const std::string &type, const std::string &key, const std::string &properties) {
    uint16_t node_shard_id = CalculateShardId(type, key);
    uint16_t node_type_id = node_types.getTypeId(type);

    // The node type exists, so continue on
    if (node_type_id > 0) {
      return container().invoke_on(node_shard_id, [type, node_type_id, key, properties](Shard &local_shard) {
        return local_shard.NodeAdd(type, node_type_id, key, properties);
      });
    }

    // The node type needs to be set by Shard 0 and propagated
    return container().invoke_on(0, [node_shard_id, type, key, properties, this](Shard &local_shard) {
      return local_shard.NodeTypeInsertPeered(type).then([node_shard_id, type, key, properties, this](uint16_t node_type_id) {
        return container().invoke_on(node_shard_id, [type, node_type_id, key, properties](Shard &local_shard) {
          return local_shard.NodeAdd(type, node_type_id, key, properties);
        });
      });
    });

  }

  seastar::future<uint64_t> Shard::NodeGetIDPeered(const std::string &type, const std::string &key) {
    // Check if the type even exists
    if (node_types.getTypeId(type) > 0) {
      uint16_t node_shard_id = CalculateShardId(type, key);

      if(node_shard_id == seastar::this_shard_id()) {
        return seastar::make_ready_future<uint64_t>(NodeGetID(type, key));
      }

      return container().invoke_on(node_shard_id, [type, key](Shard &local_shard) {
             return local_shard.NodeGetID(type, key);
      });
    }

    // Invalid Type or Key
    return seastar::make_ready_future<uint64_t>(0);
  }

  seastar::future<Node> Shard::NodeGetPeered(const std::string &type, const std::string &key) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<Node>(NodeGet(type, key));
    }

    return container().invoke_on(node_shard_id, [type, key](Shard &local_shard) {
           return local_shard.NodeGet(type, key);
    });
  }

  seastar::future<Node> Shard::NodeGetPeered(uint64_t id) {
    uint16_t node_shard_id = CalculateShardId(id);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<Node>(NodeGet(id));
    }

    return container().invoke_on(node_shard_id, [id](Shard &local_shard) {
           return local_shard.NodeGet(id);
    });
  }

  seastar::future<bool> Shard::NodeRemovePeered(const std::string &type, const std::string &key) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    if(node_shard_id == seastar::this_shard_id()) {
      uint64_t external_id = NodeGetID(type, key);
      return NodeRemovePeered(external_id);
    }

    return container().invoke_on(node_shard_id, [type, key] (Shard &local_shard) {
           return local_shard.NodeGetID(type, key);
    }).then([this] (uint64_t external_id) {
           return NodeRemovePeered(external_id);
    });
  }

  seastar::future<bool> Shard::NodeRemovePeered(uint64_t external_id) {
    uint16_t node_shard_id = CalculateShardId(external_id);

    return container().invoke_on(node_shard_id, [external_id](Shard &local_shard) {
           return local_shard.ValidNodeId(external_id);
    }).then([node_shard_id, external_id, this] (bool valid) {
           if(valid) {
             uint64_t internal_id = externalToInternal(external_id);

             seastar::future<std::vector<bool>> incoming = container().invoke_on(node_shard_id, [internal_id] (Shard &local_shard) {
                    return local_shard.NodeRemoveGetIncoming(internal_id);
             }).then([external_id, this] (auto sharded_grouped_rels) {
                    std::vector<seastar::future<bool>> futures;
                    for (auto const& [their_shard, grouped_rels] : sharded_grouped_rels ) {
                      auto future = container().invoke_on(their_shard, [external_id, grouped_rels = std::move(grouped_rels)] (Shard &local_shard) {
                             return local_shard.NodeRemoveDeleteIncoming(external_id, grouped_rels);
                      });
                      futures.push_back(std::move(future));
                    }

                    auto p = make_shared(std::move(futures));
                    return seastar::when_all_succeed(p->begin(), p->end());
             });

             seastar::future<std::vector<bool>> outgoing = container().invoke_on(shard_id, [internal_id] (Shard &local_shard) {
                    return local_shard.NodeRemoveGetOutgoing(internal_id);
             }).then([external_id, this] (auto sharded_grouped_rels) {
                    std::vector<seastar::future<bool>> futures;
                    for (auto const& [their_shard, grouped_rels] : sharded_grouped_rels ) {
                      auto future = container().invoke_on(their_shard, [external_id, grouped_rels = grouped_rels] (Shard &local_shard) {
                             return local_shard.NodeRemoveDeleteOutgoing(external_id, grouped_rels);
                      });
                      futures.push_back(std::move(future));
                    }

                    auto p = make_shared(std::move(futures));
                    return seastar::when_all_succeed(p->begin(), p->end());
             });

             return seastar::when_all(std::move(incoming), std::move(outgoing)).then([node_shard_id, external_id, this] (auto tup) {
                    if(std::get<0>(tup).failed() || std::get<1>(tup).failed()) {
                      return seastar::make_ready_future<bool>(false);
                    }
                    return container().invoke_on(node_shard_id, [external_id] (Shard &local_shard) {
                           return local_shard.NodeRemove(external_id);
                    });
             });
           }
           return seastar::make_ready_future<bool>(false);
    });
  }

  seastar::future<uint16_t>  Shard::NodeGetTypeIdPeered(uint64_t id) {
    uint16_t node_shard_id = CalculateShardId(id);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<uint16_t>(NodeGetTypeId(id));
    }

    return container().invoke_on(node_shard_id, [id](Shard &local_shard) {
           return local_shard.NodeGetTypeId(id);
    });
  }

  seastar::future<std::string> Shard::NodeGetTypePeered(uint64_t id) {
    uint16_t node_shard_id = CalculateShardId(id);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<std::string>(NodeGetType(id));
    }

    return container().invoke_on(node_shard_id, [id](Shard &local_shard) {
           return local_shard.NodeGetType(id);
    });
  }

  seastar::future<std::string> Shard::NodeGetKeyPeered(uint64_t id) {
    uint16_t node_shard_id = CalculateShardId(id);
    return container().invoke_on(node_shard_id, [id](Shard &local_shard) {
           return local_shard.NodeGetKey(id);
    });
  }

  // Node Properties ==========================================================================================================================
  seastar::future<std::any> Shard::NodePropertyGetPeered(const std::string &type, const std::string &key, const std::string &property) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<std::any>(NodePropertyGet(type, key, property));
    }

    return container().invoke_on(node_shard_id, [type, key, property](Shard &local_shard) {
           return local_shard.NodePropertyGet(type, key, property);
    });
  }

  seastar::future<std::string> Shard::NodePropertyGetStringPeered(const std::string &type, const std::string &key, const std::string &property) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<std::string>(NodePropertyGetString(type, key, property));
    }

    return container().invoke_on(node_shard_id, [type, key, property](Shard &local_shard) {
           return local_shard.NodePropertyGetString(type, key, property);
    });
  }

  seastar::future<int64_t> Shard::NodePropertyGetIntegerPeered(const std::string &type, const std::string &key, const std::string &property) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<int64_t>(NodePropertyGetInteger(type, key, property));
    }

    return container().invoke_on(node_shard_id, [type, key, property](Shard &local_shard) {
           return local_shard.NodePropertyGetInteger(type, key, property);
    });
  }

  seastar::future<double> Shard::NodePropertyGetDoublePeered(const std::string &type, const std::string &key, const std::string &property) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<double>(NodePropertyGetDouble(type, key, property));
    }

    return container().invoke_on(node_shard_id, [type, key, property](Shard &local_shard) {
           return local_shard.NodePropertyGetDouble(type, key, property);
    });
  }

  seastar::future<bool> Shard::NodePropertyGetBooleanPeered(const std::string &type, const std::string &key, const std::string &property) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(NodePropertyGetBoolean(type, key, property));
    }

    return container().invoke_on(node_shard_id, [type, key, property](Shard &local_shard) {
           return local_shard.NodePropertyGetBoolean(type, key, property);
    });
  }

  seastar::future<std::map<std::string, std::any>> Shard::NodePropertyGetObjectPeered(const std::string &type, const std::string &key, const std::string &property) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<std::map<std::string, std::any>>(NodePropertyGetObject(type, key, property));
    }

    return container().invoke_on(node_shard_id, [type, key, property](Shard &local_shard) {
           return local_shard.NodePropertyGetObject(type, key, property);
    });
  }

  seastar::future<std::any> Shard::NodePropertyGetPeered(uint64_t id, const std::string &property) {
    uint16_t node_shard_id = CalculateShardId(id);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<std::any>(NodePropertyGet(id, property));
    }

    return container().invoke_on(node_shard_id, [id, property](Shard &local_shard) {
           return local_shard.NodePropertyGet(id, property);
    });
  }

  seastar::future<std::string> Shard::NodePropertyGetStringPeered(uint64_t id, const std::string &property) {
    uint16_t node_shard_id = CalculateShardId(id);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<std::string>(NodePropertyGetString(id, property));
    }

    return container().invoke_on(node_shard_id, [id, property](Shard &local_shard) {
           return local_shard.NodePropertyGetString(id, property);
    });
  }

  seastar::future<int64_t> Shard::NodePropertyGetIntegerPeered(uint64_t id, const std::string &property) {
    uint16_t node_shard_id = CalculateShardId(id);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<int64_t>(NodePropertyGetInteger(id, property));
    }

    return container().invoke_on(node_shard_id, [id, property](Shard &local_shard) {
           return local_shard.NodePropertyGetInteger(id, property);
    });
  }

  seastar::future<double> Shard::NodePropertyGetDoublePeered(uint64_t id, const std::string &property) {
    uint16_t node_shard_id = CalculateShardId(id);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<double>(NodePropertyGetDouble(id, property));
    }

    return container().invoke_on(node_shard_id, [id, property](Shard &local_shard) {
           return local_shard.NodePropertyGetDouble(id, property);
    });
  }

  seastar::future<bool> Shard::NodePropertyGetBooleanPeered(uint64_t id, const std::string &property) {
    uint16_t node_shard_id = CalculateShardId(id);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(NodePropertyGetBoolean(id, property));
    }

    return container().invoke_on(node_shard_id, [id, property](Shard &local_shard) {
           return local_shard.NodePropertyGetBoolean(id, property);
    });
  }

  seastar::future<std::map<std::string, std::any>> Shard::NodePropertyGetObjectPeered(uint64_t id, const std::string &property) {
    uint16_t node_shard_id = CalculateShardId(id);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<std::map<std::string, std::any>>(NodePropertyGetObject(id, property));
    }

    return container().invoke_on(node_shard_id, [id, property](Shard &local_shard) {
           return local_shard.NodePropertyGetObject(id, property);
    });
  }

  seastar::future<bool> Shard::NodePropertySetPeered(const std::string &type, const std::string &key, const std::string &property, std::string value) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(NodePropertySet(type, key, property, value));
    }

    return container().invoke_on(node_shard_id, [type, key, property, value](Shard &local_shard) {
           return local_shard.NodePropertySet(type, key, property, value);
    });
  }

  seastar::future<bool> Shard::NodePropertySetPeered(const std::string &type, const std::string &key, const std::string &property, const char *value) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(NodePropertySet(type, key, property, value));
    }

    return container().invoke_on(node_shard_id, [type, key, property, value](Shard &local_shard) {
           return local_shard.NodePropertySet(type, key, property, value);
    });
  }

  seastar::future<bool> Shard::NodePropertySetPeered(const std::string &type, const std::string &key, const std::string &property, int64_t value) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(NodePropertySet(type, key, property, value));
    }

    return container().invoke_on(node_shard_id, [type, key, property, value](Shard &local_shard) {
           return local_shard.NodePropertySet(type, key, property, value);
    });
  }

  seastar::future<bool> Shard::NodePropertySetPeered(const std::string &type, const std::string &key, const std::string &property, double value) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(NodePropertySet(type, key, property, value));
    }

    return container().invoke_on(node_shard_id, [type, key, property, value](Shard &local_shard) {
           return local_shard.NodePropertySet(type, key, property, value);
    });;
  }

  seastar::future<bool> Shard::NodePropertySetPeered(const std::string &type, const std::string &key, const std::string &property, bool value) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(NodePropertySet(type, key, property, value));
    }

    return container().invoke_on(node_shard_id, [type, key, property, value](Shard &local_shard) {
           return local_shard.NodePropertySet(type, key, property, value);
    });
  }

  seastar::future<bool> Shard::NodePropertySetPeered(const std::string &type, const std::string &key, const std::string &property, std::map<std::string, std::any> value) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(NodePropertySet(type, key, property, value));
    }

    return container().invoke_on(node_shard_id, [type, key, property, value](Shard &local_shard) {
           return local_shard.NodePropertySet(type, key, property, value);
    });
  }

  seastar::future<bool> Shard::NodePropertySetFromJsonPeered(const std::string &type, const std::string &key, const std::string &property, const std::string &value) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(NodePropertySetFromJson(type, key, property, value));
    }

    return container().invoke_on(node_shard_id, [type, key, property, value](Shard &local_shard) {
           return local_shard.NodePropertySetFromJson(type, key, property, value);
    });
  }

  seastar::future<bool> Shard::NodePropertySetPeered(uint64_t id, const std::string &property, std::string value) {
    uint16_t node_shard_id = CalculateShardId(id);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(NodePropertySet(id, property, value));
    }

    return container().invoke_on(node_shard_id, [id, property, value](Shard &local_shard) {
           return local_shard.NodePropertySet(id, property, value);
    });
  }

  seastar::future<bool> Shard::NodePropertiesDeletePeered(const std::string &type, const std::string &key) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(NodePropertiesDelete(type, key));
    }

    return container().invoke_on(node_shard_id, [type, key](Shard &local_shard) {
           return local_shard.NodePropertiesDelete(type, key);
    });
  }

  seastar::future<bool> Shard::NodePropertySetPeered(uint64_t id, const std::string &property, const char *value) {
    uint16_t node_shard_id = CalculateShardId(id);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(NodePropertySet(id, property, value));
    }

    return container().invoke_on(node_shard_id, [id, property, value](Shard &local_shard) {
           return local_shard.NodePropertySet(id, property, value);
    });
  }

  seastar::future<bool> Shard::NodePropertySetPeered(uint64_t id, const std::string &property, int64_t value) {
    uint16_t node_shard_id = CalculateShardId(id);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(NodePropertySet(id, property, value));
    }

    return container().invoke_on(node_shard_id, [id, property, value](Shard &local_shard) {
           return local_shard.NodePropertySet(id, property, value);
    });
  }

  seastar::future<bool> Shard::NodePropertySetPeered(uint64_t id, const std::string &property, double value) {
    uint16_t node_shard_id = CalculateShardId(id);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(NodePropertySet(id, property, value));
    }

    return container().invoke_on(node_shard_id, [id, property, value](Shard &local_shard) {
           return local_shard.NodePropertySet(id, property, value);
    });
  }

  seastar::future<bool> Shard::NodePropertySetPeered(uint64_t id, const std::string &property, bool value) {
    uint16_t node_shard_id = CalculateShardId(id);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(NodePropertySet(id, property, value));
    }

    return container().invoke_on(node_shard_id, [id, property, value](Shard &local_shard) {
           return local_shard.NodePropertySet(id, property, value);
    });
  }

  seastar::future<bool> Shard::NodePropertySetPeered(uint64_t id, const std::string &property, std::map<std::string, std::any> value) {
    uint16_t node_shard_id = CalculateShardId(id);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(NodePropertySet(id, property, value));
    }

    return container().invoke_on(node_shard_id, [id, property, value](Shard &local_shard) {
           return local_shard.NodePropertySet(id, property, value);
    });
  }

  seastar::future<bool> Shard::NodePropertySetFromJsonPeered(uint64_t id, const std::string &property, const std::string &value) {
    uint16_t node_shard_id = CalculateShardId(id);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(NodePropertySetFromJson(id, property, value));
    }

    return container().invoke_on(node_shard_id, [id, property, value](Shard &local_shard) {
           return local_shard.NodePropertySetFromJson(id, property, value);
    });
  }

  seastar::future<bool> Shard::NodePropertyDeletePeered(const std::string &type, const std::string &key, const std::string &property) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(NodePropertyDelete(type, key, property));
    }

    return container().invoke_on(node_shard_id, [type, key, property](Shard &local_shard) {
           return local_shard.NodePropertyDelete(type, key, property);
    });
  }

  seastar::future<bool> Shard::NodePropertyDeletePeered(uint64_t id, const std::string &property) {
    uint16_t node_shard_id = CalculateShardId(id);


    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(NodePropertyDelete(id, property));
    }

    return container().invoke_on(node_shard_id, [id, property](Shard &local_shard) {
           return local_shard.NodePropertyDelete(id, property);
    });
  }

  seastar::future<std::map<std::string, std::any>> Shard::NodePropertiesGetPeered(const std::string& type, const std::string& key) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<std::map<std::string, std::any>>(NodePropertiesGet(type, key));
    }

    return container().invoke_on(node_shard_id, [type, key](Shard &local_shard) {
           return local_shard.NodePropertiesGet(type, key);
    });
  }

  seastar::future<bool> Shard::NodePropertiesSetPeered(const std::string &type, const std::string &key, std::map<std::string, std::any> &value) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(NodePropertiesSet(type, key, value));
    }

    return container().invoke_on(node_shard_id, [type, key, value](Shard &local_shard) mutable {
           return local_shard.NodePropertiesSet(type, key, value);
    });
  }

  seastar::future<std::map<std::string, std::any>> Shard::NodePropertiesGetPeered(uint64_t id) {
    uint16_t node_shard_id = CalculateShardId(id);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<std::map<std::string, std::any>>(NodePropertiesGet(id));
    }

    return container().invoke_on(node_shard_id, [id](Shard &local_shard) {
           return local_shard.NodePropertiesGet(id);
    });
  }

  seastar::future<bool> Shard::NodePropertiesSetPeered(uint64_t id, std::map<std::string, std::any> &value) {
    uint16_t node_shard_id = CalculateShardId(id);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(NodePropertiesSet(id, value));
    }

    return container().invoke_on(node_shard_id, [id, value](Shard &local_shard) mutable {
           return local_shard.NodePropertiesSet(id, value);
    });
  }

  seastar::future<bool> Shard::NodePropertiesSetFromJsonPeered(const std::string &type, const std::string &key, const std::string &value) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(NodePropertiesSetFromJson(type, key, value));
    }

    return container().invoke_on(node_shard_id, [type, key, value](Shard &local_shard) {
           return local_shard.NodePropertiesSetFromJson(type, key, value);
    });
  }

  seastar::future<bool> Shard::NodePropertiesResetPeered(uint64_t id, const std::map<std::string, std::any> &value) {
    uint16_t node_shard_id = CalculateShardId(id);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(NodePropertiesReset(id, value));
    }

    return container().invoke_on(node_shard_id, [id, value](Shard &local_shard) {
           return local_shard.NodePropertiesReset(id, value);
    });
  }

  seastar::future<bool> Shard::NodePropertiesResetFromJsonPeered(const std::string &type, const std::string &key, const std::string &value) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(NodePropertiesResetFromJson(type, key, value));
    }

    return container().invoke_on(node_shard_id, [type, key, value](Shard &local_shard) {
           return local_shard.NodePropertiesResetFromJson(type, key, value);
    });
  }

  seastar::future<bool> Shard::NodePropertiesResetFromJsonPeered(uint64_t id, const std::string &value) {
    uint16_t node_shard_id = CalculateShardId(id);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(NodePropertiesResetFromJson(id, value));
    }

    return container().invoke_on(node_shard_id, [id, value](Shard &local_shard) {
           return local_shard.NodePropertiesResetFromJson(id, value);
    });
  }

  seastar::future<bool> Shard::NodePropertiesSetFromJsonPeered(uint64_t id, const std::string &value) {
    uint16_t node_shard_id = CalculateShardId(id);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(NodePropertiesSetFromJson(id, value));
    }

    return container().invoke_on(node_shard_id, [id, value](Shard &local_shard) {
           return local_shard.NodePropertiesSetFromJson(id, value);
    });
  }

  seastar::future<bool> Shard::NodePropertiesDeletePeered(uint64_t id) {
    uint16_t node_shard_id = CalculateShardId(id);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(NodePropertiesDelete(id));
    }

    return container().invoke_on(node_shard_id, [id](Shard &local_shard) {
           return local_shard.NodePropertiesDelete(id);
    });
  }

  // Relationships ==========================================================================================================================
  seastar::future<uint64_t> Shard::RelationshipAddEmptyPeered(const std::string &rel_type, const std::string &type1, const std::string &key1, const std::string &type2, const std::string &key2) {
    uint16_t shard_id1 = CalculateShardId(type1, key1);
    uint16_t shard_id2 = CalculateShardId(type2, key2);
    uint16_t rel_type_id = relationship_types.getTypeId(rel_type);

    // The rel type exists, continue on
    if (rel_type_id > 0) {
      if(shard_id1 == shard_id2) {
        return container().invoke_on(shard_id1, [rel_type_id, type1, key1, type2, key2](Shard &local_shard) {
          return local_shard.RelationshipAddEmptySameShard(rel_type_id, type1, key1, type2, key2);
        });
      }

      // Get node id1, get node id 2 and call add Empty with ids
      seastar::future<uint64_t> getId1 = container().invoke_on(shard_id1, [type1, key1](Shard &local_shard) {
        return local_shard.NodeGetID(type1, key1);
      });

      seastar::future<uint64_t> getId2 = container().invoke_on(shard_id2, [type2, key2](Shard &local_shard) {
        return local_shard.NodeGetID(type2, key2);
      });

      std::vector<seastar::future<uint64_t>> futures;
      futures.push_back(std::move(getId1));
      futures.push_back(std::move(getId2));
      auto p = make_shared(std::move(futures));

      return seastar::when_all_succeed(p->begin(), p->end())
        .then([rel_type_id, this] (const std::vector<uint64_t> ids) {
           if(ids.at(0) > 0 && ids.at(1) > 0) {
             return RelationshipAddEmptyPeered(rel_type_id, ids.at(0), ids.at(1));
           }
           // Invalid node ids
           return seastar::make_ready_future<uint64_t>(uint64_t (0));
        });
    }

    // The relationship type needs to be set by Shard 0 and propagated
    return container().invoke_on(0, [shard_id1, shard_id2, rel_type, type1, key1, type2, key2, this] (Shard &local_shard) {
           return local_shard.RelationshipTypeInsertPeered(rel_type)
        .then([shard_id1, shard_id2, rel_type, type1, key1, type2, key2, this] (uint16_t rel_type_id) {
           if(shard_id1 == shard_id2) {
             return container().invoke_on(shard_id1, [rel_type_id, type1, key1, type2, key2](Shard &local_shard) {
                    return local_shard.RelationshipAddEmptySameShard(rel_type_id, type1, key1, type2, key2);
             });
           }

           // Get node id1, get node id 2 and call add Empty with ids
           seastar::future<uint64_t> getId1 = container().invoke_on(shard_id1, [type1, key1](Shard &local_shard) {
                  return local_shard.NodeGetID(type1, key1);
           });

           seastar::future<uint64_t> getId2 = container().invoke_on(shard_id2, [type2, key2](Shard &local_shard) {
                  return local_shard.NodeGetID(type2, key2);
           });

           std::vector<seastar::future<uint64_t>> futures;
           futures.push_back(std::move(getId1));
           futures.push_back(std::move(getId2));
           auto p = make_shared(std::move(futures));

           return seastar::when_all_succeed(p->begin(), p->end())
             .then([rel_type_id, this] (const std::vector<uint64_t>& ids) {
                    if(ids.at(0) > 0 && ids.at(1) > 0) {
                      return RelationshipAddEmptyPeered(rel_type_id, ids.at(0), ids.at(1));
                    }
                    // Invalid node ids
                    return seastar::make_ready_future<uint64_t>(uint64_t (0));
             });

          });
      });

  }

  seastar::future<uint64_t> Shard::RelationshipAddPeered(const std::string &rel_type, const std::string &type1, const std::string &key1,
                                                         const std::string &type2, const std::string &key2, const std::string& properties) {
    uint16_t shard_id1 = CalculateShardId(type1, key1);
    uint16_t shard_id2 = CalculateShardId(type2, key2);
    uint16_t rel_type_id = relationship_types.getTypeId(rel_type);

    // The rel type exists, continue on
    if (rel_type_id > 0) {
      if(shard_id1 == shard_id2) {
        return container().invoke_on(shard_id1, [rel_type_id, type1, key1, type2, key2, properties](Shard &local_shard) {
               return local_shard.RelationshipAddSameShard(rel_type_id, type1, key1, type2, key2, properties);
        });
      }

      // Get node id1, get node id 2 and call add Empty with ids
      seastar::future<uint64_t> getId1 = container().invoke_on(shard_id1, [type1, key1](Shard &local_shard) {
             return local_shard.NodeGetID(type1, key1);
      });

      seastar::future<uint64_t> getId2 = container().invoke_on(shard_id2, [type2, key2](Shard &local_shard) {
             return local_shard.NodeGetID(type2, key2);
      });

      std::vector<seastar::future<uint64_t>> futures;
      futures.push_back(std::move(getId1));
      futures.push_back(std::move(getId2));
      auto p = make_shared(std::move(futures));

      return seastar::when_all_succeed(p->begin(), p->end())
        .then([rel_type_id, properties, this] (const std::vector<uint64_t> ids) {
               if(ids.at(0) > 0 && ids.at(1) > 0) {
                 return RelationshipAddPeered(rel_type_id, ids.at(0), ids.at(1), properties);
               }
               // Invalid node ids
               return seastar::make_ready_future<uint64_t>(uint64_t (0));
        });
    }

    // The relationship type needs to be set by Shard 0 and propagated
    return container().invoke_on(0, [shard_id1, shard_id2, rel_type, type1, key1, type2, key2, properties, this] (Shard &local_shard) {
         return local_shard.RelationshipTypeInsertPeered(rel_type)
           .then([shard_id1, shard_id2, rel_type, type1, key1, type2, key2, properties, this] (uint16_t rel_type_id) {
              if(shard_id1 == shard_id2) {
                return container().invoke_on(shard_id1, [rel_type_id, type1, key1, type2, key2, properties](Shard &local_shard) {
                       return local_shard.RelationshipAddSameShard(rel_type_id, type1, key1, type2, key2, properties);
                });
              }

              // Get node id1, get node id 2 and call add Empty with ids
              seastar::future<uint64_t> getId1 = container().invoke_on(shard_id1, [type1, key1](Shard &local_shard) {
                return local_shard.NodeGetID(type1, key1);
              });

              seastar::future<uint64_t> getId2 = container().invoke_on(shard_id2, [type2, key2](Shard &local_shard) {
                return local_shard.NodeGetID(type2, key2);
              });

              std::vector<seastar::future<uint64_t>> futures;
              futures.push_back(std::move(getId1));
              futures.push_back(std::move(getId2));
              auto p = make_shared(std::move(futures));

              return seastar::when_all_succeed(p->begin(), p->end())
                .then([rel_type_id, properties, this] (const std::vector<uint64_t>& ids) {
                   if(ids.at(0) > 0 && ids.at(1) > 0) {
                     return RelationshipAddPeered(rel_type_id, ids.at(0), ids.at(1), properties);
                   }
                   // Invalid node ids
                   return seastar::make_ready_future<uint64_t>(uint64_t (0));
                });

           });
    });
  }

  seastar::future<uint64_t> Shard::RelationshipAddEmptyPeered(const std::string &rel_type, uint64_t id1, uint64_t id2) {
    uint16_t shard_id1 = CalculateShardId(id1);
    uint16_t shard_id2 = CalculateShardId(id2);
    uint16_t rel_type_id = relationship_types.getTypeId(rel_type);
    // The rel type exists, continue on
    if (rel_type_id > 0) {
      if (shard_id1 == shard_id2) {
        return container().invoke_on(shard_id1, [rel_type_id, id1, id2](Shard &local_shard) {
          return local_shard.RelationshipAddEmptySameShard(rel_type_id, id1, id2);
        });
      }

      return RelationshipAddEmptyPeered(rel_type_id, id1, id2);
    }
    // The relationship type needs to be set by Shard 0 and propagated
    return container().invoke_on(0, [shard_id1, shard_id2, rel_type, id1, id2, this](Shard &local_shard) {
      return local_shard.RelationshipTypeInsertPeered(rel_type)
        .then([shard_id1, shard_id2, rel_type, id1, id2, this](uint16_t rel_type_id) {
          if (shard_id1 == shard_id2) {
            return container().invoke_on(shard_id1, [rel_type_id, id1, id2](Shard &local_shard) {
              return local_shard.RelationshipAddEmptySameShard(rel_type_id, id1, id2);
            });
          }

          // Get node id1, get node id 2 and call add Empty with ids
          seastar::future<bool> validateId1 = container().invoke_on(shard_id1, [id1](Shard &local_shard) {
            return local_shard.ValidNodeId(id1);
          });

          seastar::future<bool> validateId2 = container().invoke_on(shard_id2, [id2](Shard &local_shard) {
            return local_shard.ValidNodeId(id2);
          });

          std::vector<seastar::future<bool>> futures;
          futures.push_back(std::move(validateId1));
          futures.push_back(std::move(validateId2));
          auto p = make_shared(std::move(futures));

          // if they are valid, call outgoing on shard 1, get the rel_id and use it to call incoming on shard 2
          return seastar::when_all_succeed(p->begin(), p->end())
            .then([rel_type_id, shard_id1, shard_id2, id1, id2, this](const std::vector<bool>& valid) {
              if (valid.at(0) && valid.at(1)) {
                return container().invoke_on(shard_id1, [rel_type_id, shard_id2, id1, id2, this](Shard &local_shard) {
                  return seastar::make_ready_future<uint64_t>(local_shard.RelationshipAddEmptyToOutgoing(rel_type_id, id1, id2))
                    .then([rel_type_id, shard_id2, id1, id2, this](uint64_t rel_id) {
                      return container().invoke_on(shard_id2, [rel_type_id, id1, id2, rel_id](Shard &local_shard) {
                        return local_shard.RelationshipAddToIncoming(rel_type_id, rel_id, id1, id2);
                      });
                    });
                });
              }
              // Invalid node ids
              return seastar::make_ready_future<uint64_t>(uint64_t(0));
            });
        });
    });
  }

  seastar::future<uint64_t> Shard::RelationshipAddPeered(const std::string &rel_type, uint64_t id1, uint64_t id2, const std::string& properties) {
    uint16_t shard_id1 = CalculateShardId(id1);
    uint16_t shard_id2 = CalculateShardId(id2);

    uint16_t rel_type_id = relationship_types.getTypeId(rel_type);
    // The rel type exists, continue on
    if (rel_type_id > 0) {
      if (shard_id1 == shard_id2) {
        return container().invoke_on(shard_id1, [rel_type_id, id1, id2, properties](Shard &local_shard) {
               return local_shard.RelationshipAddSameShard(rel_type_id, id1, id2, properties);
        });
      }

      return RelationshipAddPeered(rel_type_id, id1, id2, properties);
    }

    // The relationship type needs to be set by Shard 0 and propagated
    return container().invoke_on(0, [shard_id1, shard_id2, rel_type, id1, id2, properties, this](Shard &local_shard) {
           return local_shard.RelationshipTypeInsertPeered(rel_type)
             .then([shard_id1, shard_id2, rel_type, id1, id2, properties, this](uint16_t rel_type_id) {
                    if (shard_id1 == shard_id2) {
                      return container().invoke_on(shard_id1, [rel_type_id, id1, id2, properties](Shard &local_shard) {
                             return local_shard.RelationshipAddSameShard(rel_type_id, id1, id2, properties);
                      });
                    }

                    // Get node id1, get node id 2 and call add Empty with ids
                    seastar::future<bool> validateId1 = container().invoke_on(shard_id1, [id1](Shard &local_shard) {
                           return local_shard.ValidNodeId(id1);
                    });

                    seastar::future<bool> validateId2 = container().invoke_on(shard_id2, [id2](Shard &local_shard) {
                           return local_shard.ValidNodeId(id2);
                    });

                    std::vector<seastar::future<bool>> futures;
                    futures.push_back(std::move(validateId1));
                    futures.push_back(std::move(validateId2));
                    auto p = make_shared(std::move(futures));

                    // if they are valid, call outgoing on shard 1, get the rel_id and use it to call incoming on shard 2
                    return seastar::when_all_succeed(p->begin(), p->end())
                      .then([rel_type_id, shard_id1, shard_id2, id1, id2, properties, this](const std::vector<bool>& valid) {
                         if (valid.at(0) && valid.at(1)) {
                           return container().invoke_on(shard_id1, [rel_type_id, shard_id2, id1, id2, properties, this](Shard &local_shard) {
                              return seastar::make_ready_future<uint64_t>(local_shard.RelationshipAddToOutgoing(rel_type_id, id1, id2, properties))
                                .then([rel_type_id, shard_id2, id1, id2, this](uint64_t rel_id) {
                                   return container().invoke_on(shard_id2, [rel_type_id, id1, id2, rel_id](Shard &local_shard) {
                                      return local_shard.RelationshipAddToIncoming(rel_type_id, rel_id, id1, id2);
                                   });
                                });
                           });
                         }
                         // Invalid node ids
                         return seastar::make_ready_future<uint64_t>(uint64_t(0));
                      });
             });
    });
  }

  seastar::future<uint64_t> Shard::RelationshipAddEmptyPeered(uint16_t rel_type_id, uint64_t id1, uint64_t id2) {
    uint16_t shard_id1 = CalculateShardId(id1);
    uint16_t shard_id2 = CalculateShardId(id2);

    if (relationship_types.ValidTypeId(rel_type_id)) {
    // Get node id1, get node id 2 and call add Empty with ids
    seastar::future<bool> validateId1 = container().invoke_on(shard_id1, [id1] (Shard &local_shard) {
           return local_shard.ValidNodeId(id1);
    });

    seastar::future<bool> validateId2 = container().invoke_on(shard_id2, [id2] (Shard &local_shard) {
           return local_shard.ValidNodeId(id2);
    });

    std::vector<seastar::future<bool>> futures;
    futures.push_back(std::move(validateId1));
    futures.push_back(std::move(validateId2));
    auto p = make_shared(std::move(futures));

    // if they are valid, call outgoing on shard 1, get the rel_id and use it to call incoming on shard 2
    return seastar::when_all_succeed(p->begin(), p->end())
      .then([rel_type_id, shard_id1, shard_id2, id1, id2, this] (const std::vector<bool>& valid) {
             if(valid.at(0) && valid.at(1)) {
               return container().invoke_on(shard_id1, [rel_type_id, shard_id2, id1, id2, this] (Shard &local_shard) {
                      return seastar::make_ready_future<uint64_t>(local_shard.RelationshipAddEmptyToOutgoing(rel_type_id, id1, id2))
                        .then([rel_type_id, shard_id2, id1, id2, this] (uint64_t rel_id) {
                               return container().invoke_on(shard_id2, [rel_type_id, id1, id2, rel_id] (Shard &local_shard) {
                                      return local_shard.RelationshipAddToIncoming(rel_type_id, rel_id, id1, id2);
                               });
                        });
               });
             }
             // Invalid node ids
             return seastar::make_ready_future<uint64_t>(uint64_t (0));
      });
    }

    // Invalid rel type id
    return seastar::make_ready_future<uint64_t>(uint64_t (0));
  }

  seastar::future<uint64_t> Shard::RelationshipAddPeered(uint16_t rel_type_id, uint64_t id1, uint64_t id2, const std::string& properties) {
    uint16_t shard_id1 = CalculateShardId(id1);
    uint16_t shard_id2 = CalculateShardId(id2);
    if (relationship_types.ValidTypeId(rel_type_id)) {
      // Get node id1, get node id 2 and call add with ids
      seastar::future<bool> validateId1 = container().invoke_on(shard_id1, [id1](Shard &local_shard) {
        return local_shard.ValidNodeId(id1);
      });

      seastar::future<bool> validateId2 = container().invoke_on(shard_id2, [id2](Shard &local_shard) {
        return local_shard.ValidNodeId(id2);
      });

      std::vector<seastar::future<bool>> futures;
      futures.push_back(std::move(validateId1));
      futures.push_back(std::move(validateId2));
      auto p = make_shared(std::move(futures));

      // if they are valid, call outgoing on shard 1, get the rel_id and use it to call incoming on shard 2
      return seastar::when_all_succeed(p->begin(), p->end())
        .then([rel_type_id, shard_id1, shard_id2, properties, id1, id2, this](const std::vector<bool> valid) {
          if (valid.at(0) && valid.at(1)) {
            return container().invoke_on(shard_id1, [rel_type_id, shard_id2, id1, id2, properties, this](Shard &local_shard) {
              return seastar::make_ready_future<uint64_t>(local_shard.RelationshipAddToOutgoing(rel_type_id, id1, id2, properties))
                .then([rel_type_id, shard_id2, id1, id2, this](uint64_t rel_id) {
                  return container().invoke_on(shard_id2, [rel_type_id, id1, id2, rel_id](Shard &local_shard) {
                    return local_shard.RelationshipAddToIncoming(rel_type_id, rel_id, id1, id2);
                  });
                });
            });
          }
          // Invalid node ids
          return seastar::make_ready_future<uint64_t>(uint64_t(0));
        });
    }

    // Invalid rel type id
    return seastar::make_ready_future<uint64_t>(uint64_t(0));
  }

  seastar::future<Relationship> Shard::RelationshipGetPeered(uint64_t id) {
    uint16_t rel_shard_id = CalculateShardId(id);

    if(rel_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<Relationship>(RelationshipGet(id));
    }

    return container().invoke_on(rel_shard_id, [id] (Shard &local_shard) {
           return local_shard.RelationshipGet(id);
    });
  }

  seastar::future<bool> Shard::RelationshipRemovePeered(uint64_t external_id) {
    uint16_t rel_shard_id = CalculateShardId(external_id);

    return container().invoke_on(rel_shard_id, [external_id] (Shard &local_shard) {
           return local_shard.ValidRelationshipId(external_id);
    }).then([rel_shard_id, external_id, this] (bool valid) {
           if(valid) {
             uint64_t internal_id = externalToInternal(external_id);
             return container().invoke_on(rel_shard_id, [internal_id] (Shard &local_shard) {
                    return local_shard.RelationshipRemoveGetIncoming(internal_id);
             }).then([external_id, this] (std::pair <uint16_t, uint64_t> rel_type_incoming_node_id) {

                    uint16_t shard_id2 = CalculateShardId(rel_type_incoming_node_id.second);
                    return container().invoke_on(shard_id2, [rel_type_incoming_node_id, external_id] (Shard &local_shard) {
                           return local_shard.RelationshipRemoveIncoming(rel_type_incoming_node_id.first, external_id, rel_type_incoming_node_id.second);
                    });
             });
           }
           return seastar::make_ready_future<bool>(false);
    });
  }

  seastar::future<std::string> Shard::RelationshipGetTypePeered(uint64_t id) {
    uint16_t rel_shard_id = CalculateShardId(id);

    if(rel_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<std::string>(RelationshipGetType(id));
    }

    return container().invoke_on(rel_shard_id, [id] (Shard &local_shard) {
           return local_shard.RelationshipGetType(id);
    });
  }

  seastar::future<uint16_t> Shard::RelationshipGetTypeIdPeered(uint64_t id) {
    uint16_t rel_shard_id = CalculateShardId(id);

    if(rel_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<uint16_t>(RelationshipGetTypeId(id));
    }

    return container().invoke_on(rel_shard_id, [id] (Shard &local_shard) {
           return local_shard.RelationshipGetTypeId(id);
    });
  }

  seastar::future<uint64_t> Shard::RelationshipGetStartingNodeIdPeered(uint64_t id) {
    uint16_t rel_shard_id = CalculateShardId(id);

    if(rel_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<uint64_t>(RelationshipGetStartingNodeId(id));
    }

    return container().invoke_on(rel_shard_id, [id] (Shard &local_shard) {
           return local_shard.RelationshipGetStartingNodeId(id);
    });
  }

  seastar::future<uint64_t> Shard::RelationshipGetEndingNodeIdPeered(uint64_t id) {
    uint16_t rel_shard_id = CalculateShardId(id);

    if(rel_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<uint64_t>(RelationshipGetEndingNodeId(id));
    }

    return container().invoke_on(rel_shard_id, [id] (Shard &local_shard) {
           return local_shard.RelationshipGetEndingNodeId(id);
    });
  }

  // Relationship Properties
  seastar::future<std::any> Shard::RelationshipPropertyGetPeered(uint64_t id, const std::string &property) {
    uint16_t rel_shard_id = CalculateShardId(id);

    if(rel_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<std::any>(RelationshipPropertyGet(id, property));
    }

    return container().invoke_on(rel_shard_id, [id, property](Shard &local_shard) {
           return local_shard.RelationshipPropertyGet(id, property);
    });
  }

  seastar::future<std::string> Shard::RelationshipPropertyGetStringPeered(uint64_t id, const std::string &property) {
    uint16_t rel_shard_id = CalculateShardId(id);

    if(rel_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<std::string>(RelationshipPropertyGetString(id, property));
    }

    return container().invoke_on(rel_shard_id, [id, property](Shard &local_shard) {
           return local_shard.RelationshipPropertyGetString(id, property);
    });
  }

  seastar::future<int64_t> Shard::RelationshipPropertyGetIntegerPeered(uint64_t id, const std::string &property) {
    uint16_t rel_shard_id = CalculateShardId(id);

    if(rel_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<int64_t>(RelationshipPropertyGetInteger(id, property));
    }

    return container().invoke_on(rel_shard_id, [id, property](Shard &local_shard) {
           return local_shard.RelationshipPropertyGetInteger(id, property);
    });
  }

  seastar::future<double> Shard::RelationshipPropertyGetDoublePeered(uint64_t id, const std::string &property) {
    uint16_t rel_shard_id = CalculateShardId(id);

    if(rel_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<double>(RelationshipPropertyGetDouble(id, property));
    }

    return container().invoke_on(rel_shard_id, [id, property](Shard &local_shard) {
           return local_shard.RelationshipPropertyGetDouble(id, property);
    });
  }

  seastar::future<bool> Shard::RelationshipPropertyGetBooleanPeered(uint64_t id, const std::string &property) {
    uint16_t rel_shard_id = CalculateShardId(id);

    if(rel_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(RelationshipPropertyGetBoolean(id, property));
    }

    return container().invoke_on(rel_shard_id, [id, property](Shard &local_shard) {
           return local_shard.RelationshipPropertyGetBoolean(id, property);
    });
  }

  seastar::future<std::map<std::string, std::any>> Shard::RelationshipPropertyGetObjectPeered(uint64_t id, const std::string &property) {
    uint16_t rel_shard_id = CalculateShardId(id);

    if(rel_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<std::map<std::string, std::any>>(RelationshipPropertyGetObject(id, property));
    }

    return container().invoke_on(rel_shard_id, [id, property](Shard &local_shard) {
           return local_shard.RelationshipPropertyGetObject(id, property);
    });
  }

  seastar::future<bool> Shard::RelationshipPropertySetPeered(uint64_t id, const std::string &property, std::string value) {
    uint16_t rel_shard_id = CalculateShardId(id);

    if(rel_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(RelationshipPropertySet(id, property, value));
    }

    return container().invoke_on(rel_shard_id, [id, property, value](Shard &local_shard) {
           return local_shard.RelationshipPropertySet(id, property, value);
    });
  }

  seastar::future<bool> Shard::RelationshipPropertySetPeered(uint64_t id, const std::string &property, const char *value) {
    uint16_t rel_shard_id = CalculateShardId(id);

    if(rel_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(RelationshipPropertySet(id, property, value));
    }

    return container().invoke_on(rel_shard_id, [id, property, value](Shard &local_shard) {
           return local_shard.RelationshipPropertySet(id, property, value);
    });
  }

  seastar::future<bool> Shard::RelationshipPropertySetPeered(uint64_t id, const std::string &property, int64_t value) {
    uint16_t rel_shard_id = CalculateShardId(id);

    if(rel_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(RelationshipPropertySet(id, property, value));
    }

    return container().invoke_on(rel_shard_id, [id, property, value](Shard &local_shard) {
           return local_shard.RelationshipPropertySet(id, property, value);
    });
  }

  seastar::future<bool> Shard::RelationshipPropertySetPeered(uint64_t id, const std::string &property, double value) {
    uint16_t rel_shard_id = CalculateShardId(id);

    if(rel_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(RelationshipPropertySet(id, property, value));
    }

    return container().invoke_on(rel_shard_id, [id, property, value](Shard &local_shard) {
           return local_shard.RelationshipPropertySet(id, property, value);
    });
  }

  seastar::future<bool> Shard::RelationshipPropertySetPeered(uint64_t id, const std::string &property, bool value) {
    uint16_t rel_shard_id = CalculateShardId(id);

    if(rel_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(RelationshipPropertySet(id, property, value));
    }

    return container().invoke_on(rel_shard_id, [id, property, value](Shard &local_shard) {
           return local_shard.RelationshipPropertySet(id, property, value);
    });
  }

  seastar::future<bool> Shard::RelationshipPropertySetPeered(uint64_t id, const std::string &property, std::map<std::string, std::any> value) {
    uint16_t rel_shard_id = CalculateShardId(id);

    if(rel_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(RelationshipPropertySet(id, property, value));
    }

    return container().invoke_on(rel_shard_id, [id, property, value](Shard &local_shard) {
           return local_shard.RelationshipPropertySet(id, property, value);
    });
  }

  seastar::future<bool> Shard::RelationshipPropertySetFromJsonPeered(uint64_t id, const std::string &property, const std::string &value) {
    uint16_t rel_shard_id = CalculateShardId(id);

    if(rel_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(RelationshipPropertySetFromJson(id, property, value));
    }

    return container().invoke_on(rel_shard_id, [id, property, value](Shard &local_shard) {
           return local_shard.RelationshipPropertySetFromJson(id, property, value);
    });
  }

  seastar::future<bool> Shard::RelationshipPropertyDeletePeered(uint64_t id, const std::string &property) {
    uint16_t rel_shard_id = CalculateShardId(id);

    if(rel_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(RelationshipPropertyDelete(id, property));
    }

    return container().invoke_on(rel_shard_id, [id, property](Shard &local_shard) {
           return local_shard.RelationshipPropertyDelete(id, property);
    });
  }

  seastar::future<std::map<std::string, std::any>> Shard::RelationshipPropertiesGetPeered(uint64_t id) {
    uint16_t rel_shard_id = CalculateShardId(id);

    if(rel_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<std::map<std::string, std::any>>(RelationshipPropertiesGet(id));
    }

    return container().invoke_on(rel_shard_id, [id](Shard &local_shard) {
           return local_shard.RelationshipPropertiesGet(id);
    });
  }

  seastar::future<bool> Shard::RelationshipPropertiesSetPeered(uint64_t id, std::map<std::string, std::any> &value) {
    uint16_t rel_shard_id = CalculateShardId(id);

    if(rel_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(RelationshipPropertiesSet(id, value));
    }

    return container().invoke_on(rel_shard_id, [id, value](Shard &local_shard) mutable {
           return local_shard.RelationshipPropertiesSet(id, value);
    });
  }

  seastar::future<bool> Shard::RelationshipPropertiesResetPeered(uint64_t id, const std::map<std::string, std::any> &value) {
    uint16_t rel_shard_id = CalculateShardId(id);

    if(rel_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(RelationshipPropertiesReset(id, value));
    }

    return container().invoke_on(rel_shard_id, [id, value](Shard &local_shard) {
           return local_shard.RelationshipPropertiesReset(id, value);
    });
  }

  seastar::future<bool> Shard::RelationshipPropertiesSetFromJsonPeered(uint64_t id, const std::string &value) {
    uint16_t rel_shard_id = CalculateShardId(id);

    if(rel_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(RelationshipPropertiesSetFromJson(id, value));
    }

    return container().invoke_on(rel_shard_id, [id, value](Shard &local_shard) mutable {
           return local_shard.RelationshipPropertiesSetFromJson(id, value);
    });
  }

  seastar::future<bool> Shard::RelationshipPropertiesResetFromJsonPeered(uint64_t id, const std::string &value) {
    uint16_t rel_shard_id = CalculateShardId(id);

    if(rel_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(RelationshipPropertiesResetFromJson(id, value));
    }

    return container().invoke_on(rel_shard_id, [id, value](Shard &local_shard) {
           return local_shard.RelationshipPropertiesResetFromJson(id, value);
    });
  }

  seastar::future<bool> Shard::RelationshipPropertiesDeletePeered(uint64_t id) {
    uint16_t rel_shard_id = CalculateShardId(id);

    if(rel_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<bool>(RelationshipPropertiesDelete(id));
    }

    return container().invoke_on(rel_shard_id, [id](Shard &local_shard) {
           return local_shard.RelationshipPropertiesDelete(id);
    });
  }

  // Node Degree
  seastar::future<uint64_t> Shard::NodeGetDegreePeered(const std::string &type, const std::string &key) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<uint64_t>(NodeGetDegree(type, key));
    }

    return container().invoke_on(node_shard_id, [type, key](Shard &local_shard) {
           return local_shard.NodeGetDegree(type, key);
    });
  }

  seastar::future<uint64_t> Shard::NodeGetDegreePeered(const std::string &type, const std::string &key, Direction direction) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<uint64_t>(NodeGetDegree(type, key, direction));
    }

    return container().invoke_on(node_shard_id, [type, key, direction](Shard &local_shard) {
           return local_shard.NodeGetDegree(type, key, direction);
    });
  }

  seastar::future<uint64_t> Shard::NodeGetDegreePeered(const std::string &type, const std::string &key, Direction direction, const std::string &rel_type) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<uint64_t>(NodeGetDegree(type, key, direction, rel_type));
    }

    return container().invoke_on(node_shard_id, [type, key, direction, rel_type](Shard &local_shard) {
           return local_shard.NodeGetDegree(type, key, direction, rel_type);
    });
  }

  seastar::future<uint64_t> Shard::NodeGetDegreePeered(const std::string &type, const std::string &key, const std::string &rel_type) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<uint64_t>(NodeGetDegree(type, key, BOTH, rel_type));
    }

    return container().invoke_on(node_shard_id, [type, key, rel_type](Shard &local_shard) {
           return local_shard.NodeGetDegree(type, key, BOTH, rel_type);
    });
  }

  seastar::future<uint64_t> Shard::NodeGetDegreePeered(const std::string &type, const std::string &key, Direction direction, const std::vector<std::string> &rel_types) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<uint64_t>(NodeGetDegree(type, key, direction, rel_types));
    }

    return container().invoke_on(node_shard_id, [type, key, direction, rel_types](Shard &local_shard) {
           return local_shard.NodeGetDegree(type, key, direction, rel_types);
    });
  }

  seastar::future<uint64_t> Shard::NodeGetDegreePeered(const std::string &type, const std::string &key, const std::vector<std::string> &rel_types) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<uint64_t>(NodeGetDegree(type, key, BOTH, rel_types));
    }

    return container().invoke_on(node_shard_id, [type, key, rel_types](Shard &local_shard) {
           return local_shard.NodeGetDegree(type, key, BOTH, rel_types);
    });
  }

  seastar::future<uint64_t> Shard::NodeGetDegreePeered(uint64_t external_id) {
    uint16_t node_shard_id = CalculateShardId(external_id);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<uint64_t>(NodeGetDegree(external_id));
    }

    return container().invoke_on(node_shard_id, [external_id](Shard &local_shard) {
           return local_shard.NodeGetDegree(external_id);
    });
  }

  seastar::future<uint64_t> Shard::NodeGetDegreePeered(uint64_t external_id, Direction direction) {
    uint16_t node_shard_id = CalculateShardId(external_id);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<uint64_t>(NodeGetDegree(external_id, direction));
    }

    return container().invoke_on(node_shard_id, [external_id, direction](Shard &local_shard) {
           return local_shard.NodeGetDegree(external_id, direction);
    });
  }

  seastar::future<uint64_t> Shard::NodeGetDegreePeered(uint64_t external_id, Direction direction, const std::string &rel_type) {
    uint16_t node_shard_id = CalculateShardId(external_id);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<uint64_t>(NodeGetDegree(external_id, direction, rel_type));
    }

    return container().invoke_on(node_shard_id, [external_id, direction, rel_type](Shard &local_shard) {
           return local_shard.NodeGetDegree(external_id, direction, rel_type);
    });
  }

  seastar::future<uint64_t> Shard::NodeGetDegreePeered(uint64_t external_id, const std::string &rel_type) {
    uint16_t node_shard_id = CalculateShardId(external_id);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<uint64_t>(NodeGetDegree(external_id, BOTH, rel_type));
    }

    return container().invoke_on(node_shard_id, [external_id, rel_type](Shard &local_shard) {
           return local_shard.NodeGetDegree(external_id, BOTH, rel_type);
    });
  }

  seastar::future<uint64_t> Shard::NodeGetDegreePeered(uint64_t external_id, Direction direction, const std::vector<std::string> &rel_types) {
    uint16_t node_shard_id = CalculateShardId(external_id);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<uint64_t>(NodeGetDegree(external_id, direction, rel_types));
    }

    return container().invoke_on(node_shard_id, [external_id, direction, rel_types](Shard &local_shard) {
           return local_shard.NodeGetDegree(external_id, direction, rel_types);
    });
  }

  seastar::future<uint64_t> Shard::NodeGetDegreePeered(uint64_t external_id, const std::vector<std::string> &rel_types) {
    uint16_t node_shard_id = CalculateShardId(external_id);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<uint64_t>(NodeGetDegree(external_id, BOTH, rel_types));
    }

    return container().invoke_on(node_shard_id, [external_id, rel_types](Shard &local_shard) {
           return local_shard.NodeGetDegree(external_id, BOTH, rel_types);
    });
  }


  // Traversing
  seastar::future<std::vector<Ids>> Shard::NodeGetRelationshipsIDsPeered(const std::string &type, const std::string &key) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<std::vector<Ids>>(NodeGetRelationshipsIDs(type, key));
    }

    return container().invoke_on(node_shard_id, [type, key](Shard &local_shard) {
           return local_shard.NodeGetRelationshipsIDs(type, key);
    });
  }

  seastar::future<std::vector<Ids>> Shard::NodeGetRelationshipsIDsPeered(const std::string &type, const std::string &key, Direction direction) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<std::vector<Ids>>(NodeGetRelationshipsIDs(type, key, direction));
    }

    return container().invoke_on(node_shard_id, [type, key, direction](Shard &local_shard) {
           return local_shard.NodeGetRelationshipsIDs(type, key, direction);
    });
  }

  seastar::future<std::vector<Ids>> Shard::NodeGetRelationshipsIDsPeered(const std::string &type, const std::string &key, Direction direction, const std::string &rel_type) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<std::vector<Ids>>(NodeGetRelationshipsIDs(type, key, direction, rel_type));
    }

    return container().invoke_on(node_shard_id, [type, key, direction, rel_type](Shard &local_shard) {
           return local_shard.NodeGetRelationshipsIDs(type, key, direction, rel_type);
    });
  }

  seastar::future<std::vector<Ids>> Shard::NodeGetRelationshipsIDsPeered(const std::string &type, const std::string &key, Direction direction, uint16_t type_id) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<std::vector<Ids>>(NodeGetRelationshipsIDs(type, key, direction, type_id));
    }

    return container().invoke_on(node_shard_id, [type, key, direction, type_id](Shard &local_shard) {
           return local_shard.NodeGetRelationshipsIDs(type, key, direction, type_id);
    });
  }

  seastar::future<std::vector<Ids>> Shard::NodeGetRelationshipsIDsPeered(const std::string &type, const std::string &key, Direction direction, const std::vector<std::string> &rel_types) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<std::vector<Ids>>(NodeGetRelationshipsIDs(type, key, direction, rel_types));
    }

    return container().invoke_on(node_shard_id, [type, key, direction, rel_types](Shard &local_shard) {
           return local_shard.NodeGetRelationshipsIDs(type, key, direction, rel_types);
    });
  }

  seastar::future<std::vector<Ids>> Shard::NodeGetRelationshipsIDsPeered(const std::string &type, const std::string &key, const std::string &rel_type) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<std::vector<Ids>>(NodeGetRelationshipsIDs(type, key, BOTH, rel_type));
    }

    return container().invoke_on(node_shard_id, [type, key, rel_type](Shard &local_shard) {
           return local_shard.NodeGetRelationshipsIDs(type, key, BOTH, rel_type);
    });
  }

  seastar::future<std::vector<Ids>> Shard::NodeGetRelationshipsIDsPeered(const std::string &type, const std::string &key, uint16_t type_id) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<std::vector<Ids>>(NodeGetRelationshipsIDs(type, key, BOTH, type_id));
    }

    return container().invoke_on(node_shard_id, [type, key, type_id](Shard &local_shard) {
           return local_shard.NodeGetRelationshipsIDs(type, key, BOTH, type_id);
    });
  }

  seastar::future<std::vector<Ids>> Shard::NodeGetRelationshipsIDsPeered(const std::string &type, const std::string &key, const std::vector<std::string> &rel_types) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<std::vector<Ids>>(NodeGetRelationshipsIDs(type, key, BOTH, rel_types));
    }

    return container().invoke_on(node_shard_id, [type, key, rel_types](Shard &local_shard) {
           return local_shard.NodeGetRelationshipsIDs(type, key, BOTH, rel_types);
    });
  }

  seastar::future<std::vector<Ids>> Shard::NodeGetRelationshipsIDsPeered(uint64_t external_id) {
    uint16_t node_shard_id = CalculateShardId(external_id);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<std::vector<Ids>>(NodeGetRelationshipsIDs(external_id));
    }

    return container().invoke_on(node_shard_id, [external_id](Shard &local_shard) {
           return local_shard.NodeGetRelationshipsIDs(external_id);
    });
  }

  seastar::future<std::vector<Ids>> Shard::NodeGetRelationshipsIDsPeered(uint64_t external_id, Direction direction) {
    uint16_t node_shard_id = CalculateShardId(external_id);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<std::vector<Ids>>(NodeGetRelationshipsIDs(external_id, direction));
    }

    return container().invoke_on(node_shard_id, [external_id, direction](Shard &local_shard) {
           return local_shard.NodeGetRelationshipsIDs(external_id, direction);
    });
  }

  seastar::future<std::vector<Ids>> Shard::NodeGetRelationshipsIDsPeered(uint64_t external_id, Direction direction, const std::string &rel_type) {
    uint16_t node_shard_id = CalculateShardId(external_id);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<std::vector<Ids>>(NodeGetRelationshipsIDs(external_id, direction, rel_type));
    }

    return container().invoke_on(node_shard_id, [external_id, direction, rel_type](Shard &local_shard) {
           return local_shard.NodeGetRelationshipsIDs(external_id, direction, rel_type);
    });
  }

  seastar::future<std::vector<Ids>> Shard::NodeGetRelationshipsIDsPeered(uint64_t external_id, Direction direction, uint16_t type_id) {
    uint16_t node_shard_id = CalculateShardId(external_id);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<std::vector<Ids>>(NodeGetRelationshipsIDs(external_id, direction, type_id));
    }

    return container().invoke_on(node_shard_id, [external_id, direction, type_id](Shard &local_shard) {
           return local_shard.NodeGetRelationshipsIDs(external_id, direction, type_id);
    });
  }

  seastar::future<std::vector<Ids>> Shard::NodeGetRelationshipsIDsPeered(uint64_t external_id, Direction direction, const std::vector<std::string> &rel_types) {
    uint16_t node_shard_id = CalculateShardId(external_id);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<std::vector<Ids>>(NodeGetRelationshipsIDs(external_id, direction, rel_types));
    }

    return container().invoke_on(node_shard_id, [external_id, direction, rel_types](Shard &local_shard) {
           return local_shard.NodeGetRelationshipsIDs(external_id, direction, rel_types);
    });
  }

  seastar::future<std::vector<Ids>> Shard::NodeGetRelationshipsIDsPeered(uint64_t external_id, const std::string &rel_type) {
    uint16_t node_shard_id = CalculateShardId(external_id);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<std::vector<Ids>>(NodeGetRelationshipsIDs(external_id, BOTH, rel_type));
    }

    return container().invoke_on(node_shard_id, [external_id, rel_type](Shard &local_shard) {
           return local_shard.NodeGetRelationshipsIDs(external_id, BOTH, rel_type);
    });
  }

  seastar::future<std::vector<Ids>> Shard::NodeGetRelationshipsIDsPeered(uint64_t external_id, uint16_t type_id) {
    uint16_t node_shard_id = CalculateShardId(external_id);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<std::vector<Ids>>(NodeGetRelationshipsIDs(external_id, BOTH, type_id));
    }

    return container().invoke_on(node_shard_id, [external_id, type_id](Shard &local_shard) {
           return local_shard.NodeGetRelationshipsIDs(external_id, BOTH, type_id);
    });
  }

  seastar::future<std::vector<Ids>> Shard::NodeGetRelationshipsIDsPeered(uint64_t external_id, const std::vector<std::string> &rel_types) {
    uint16_t node_shard_id = CalculateShardId(external_id);

    if(node_shard_id == seastar::this_shard_id()) {
      return seastar::make_ready_future<std::vector<Ids>>(NodeGetRelationshipsIDs(external_id, BOTH, rel_types));
    }

    return container().invoke_on(node_shard_id, [external_id, rel_types](Shard &local_shard) {
           return local_shard.NodeGetRelationshipsIDs(external_id, BOTH, rel_types);
    });
  }

  seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(const std::string& type, const std::string& key) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    return container().invoke_on(node_shard_id, [type, key](Shard &local_shard) {
             return local_shard.NodeGetShardedRelationshipIDs(type, key); })
      .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_relationships_ids) {
             std::vector<seastar::future<std::vector<Relationship>>> futures;
             for (auto const& [their_shard, grouped_rel_ids] : sharded_relationships_ids ) {
               auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids] (Shard &local_shard) {
                      return local_shard.RelationshipsGet(grouped_rel_ids);
               });
               futures.push_back(std::move(future));
             }

             auto p = make_shared(std::move(futures));
             return seastar::when_all_succeed(p->begin(), p->end()).then([] (const std::vector<std::vector<Relationship>>& results) {
                    std::vector<Relationship> combined;

                    for(const std::vector<Relationship>& sharded : results) {
                      combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                    }
                    return combined;
             });
      });
  }

  seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(const std::string& type, const std::string& key, const std::string& rel_type) {
    uint16_t node_shard_id = CalculateShardId(type, key);
    uint16_t rel_type_id = relationship_types.getTypeId(rel_type);

    if (rel_type_id > 0) {
      return container().invoke_on(node_shard_id, [type, key, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedRelationshipIDs(type, key, rel_type_id); })
        .then([this](const std::map<uint16_t, std::vector<uint64_t>> &sharded_relationships_ids) {
               std::vector<seastar::future<std::vector<Relationship>>> futures;
               for (auto const &[their_shard, grouped_rel_ids] : sharded_relationships_ids) {
                 auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids](Shard &local_shard) {
                        return local_shard.RelationshipsGet(grouped_rel_ids);
                 });
                 futures.push_back(std::move(future));
               }

               auto p = make_shared(std::move(futures));
               return seastar::when_all_succeed(p->begin(), p->end()).then([](const std::vector<std::vector<Relationship>> &results) {
                      std::vector<Relationship> combined;

                      for (const std::vector<Relationship> &sharded : results) {
                        combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                      }
                      return combined;
               });
        });
    }

    return seastar::make_ready_future<std::vector<Relationship>>();
  }

  seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(const std::string& type, const std::string& key, uint16_t rel_type_id) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    if (rel_type_id > 0) {
      return container().invoke_on(node_shard_id, [type, key, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedRelationshipIDs(type, key, rel_type_id); })
        .then([this](const std::map<uint16_t, std::vector<uint64_t>> &sharded_relationships_ids) {
               std::vector<seastar::future<std::vector<Relationship>>> futures;
               for (auto const &[their_shard, grouped_rel_ids] : sharded_relationships_ids) {
                 auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids](Shard &local_shard) {
                        return local_shard.RelationshipsGet(grouped_rel_ids);
                 });
                 futures.push_back(std::move(future));
               }

               auto p = make_shared(std::move(futures));
               return seastar::when_all_succeed(p->begin(), p->end()).then([](const std::vector<std::vector<Relationship>> &results) {
                      std::vector<Relationship> combined;

                      for (const std::vector<Relationship> &sharded : results) {
                        combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                      }
                      return combined;
               });
        });
    }

    return seastar::make_ready_future<std::vector<Relationship>>();
  }

  seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    return container().invoke_on(node_shard_id, [type, key, rel_types](Shard &local_shard) { return local_shard.NodeGetShardedRelationshipIDs(type, key, rel_types); })
      .then([this](const std::map<uint16_t, std::vector<uint64_t>> &sharded_relationships_ids) {
             std::vector<seastar::future<std::vector<Relationship>>> futures;
             for (auto const &[their_shard, grouped_rel_ids] : sharded_relationships_ids) {
               auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids](Shard &local_shard) {
                      return local_shard.RelationshipsGet(grouped_rel_ids);
               });
               futures.push_back(std::move(future));
             }

             auto p = make_shared(std::move(futures));
             return seastar::when_all_succeed(p->begin(), p->end()).then([](const std::vector<std::vector<Relationship>> &results) {
                    std::vector<Relationship> combined;

                    for (const std::vector<Relationship> &sharded : results) {
                      combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                    }
                    return combined;
             });
      });
  }

  seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(uint64_t external_id) {
    uint16_t node_shard_id = CalculateShardId(external_id);

    return container().invoke_on(node_shard_id, [external_id](Shard &local_shard) {
             return local_shard.NodeGetShardedRelationshipIDs(external_id); })
      .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_relationships_ids) {
             std::vector<seastar::future<std::vector<Relationship>>> futures;
             for (auto const& [their_shard, grouped_rel_ids] : sharded_relationships_ids ) {
               auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids] (Shard &local_shard) {
                      return local_shard.RelationshipsGet(grouped_rel_ids);
               });
               futures.push_back(std::move(future));
             }

             auto p = make_shared(std::move(futures));
             return seastar::when_all_succeed(p->begin(), p->end()).then([] (const std::vector<std::vector<Relationship>>& results) {
                    std::vector<Relationship> combined;

                    for(const std::vector<Relationship>& sharded : results) {
                      combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                    }
                    return combined;
             });
      });
  }

  seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(uint64_t external_id, const std::string& rel_type) {
    uint16_t node_shard_id = CalculateShardId(external_id);
    uint16_t rel_type_id = relationship_types.getTypeId(rel_type);

    if (rel_type_id > 0) {
      return container().invoke_on(node_shard_id, [external_id, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedRelationshipIDs(external_id, rel_type_id); })
        .then([this](const std::map<uint16_t, std::vector<uint64_t>> &sharded_relationships_ids) {
               std::vector<seastar::future<std::vector<Relationship>>> futures;
               for (auto const &[their_shard, grouped_rel_ids] : sharded_relationships_ids) {
                 auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids](Shard &local_shard) {
                        return local_shard.RelationshipsGet(grouped_rel_ids);
                 });
                 futures.push_back(std::move(future));
               }

               auto p = make_shared(std::move(futures));
               return seastar::when_all_succeed(p->begin(), p->end()).then([](const std::vector<std::vector<Relationship>> &results) {
                      std::vector<Relationship> combined;

                      for (const std::vector<Relationship> &sharded : results) {
                        combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                      }
                      return combined;
               });
        });
    }

    return seastar::make_ready_future<std::vector<Relationship>>();
  }

  seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(uint64_t external_id,  uint16_t rel_type_id) {
    uint16_t node_shard_id = CalculateShardId(external_id);

    if (rel_type_id > 0) {
      return container().invoke_on(node_shard_id, [external_id, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedRelationshipIDs(external_id, rel_type_id); })
        .then([this](const std::map<uint16_t, std::vector<uint64_t>> &sharded_relationships_ids) {
               std::vector<seastar::future<std::vector<Relationship>>> futures;
               for (auto const &[their_shard, grouped_rel_ids] : sharded_relationships_ids) {
                 auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids](Shard &local_shard) {
                        return local_shard.RelationshipsGet(grouped_rel_ids);
                 });
                 futures.push_back(std::move(future));
               }

               auto p = make_shared(std::move(futures));
               return seastar::when_all_succeed(p->begin(), p->end()).then([](const std::vector<std::vector<Relationship>> &results) {
                      std::vector<Relationship> combined;

                      for (const std::vector<Relationship> &sharded : results) {
                        combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                      }
                      return combined;
               });
        });
    }

    return seastar::make_ready_future<std::vector<Relationship>>();
  }

  seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(uint64_t external_id, const std::vector<std::string> &rel_types) {
    uint16_t node_shard_id = CalculateShardId(external_id);

    return container().invoke_on(node_shard_id, [external_id, rel_types](Shard &local_shard) { return local_shard.NodeGetShardedRelationshipIDs(external_id, rel_types); })
      .then([this](const std::map<uint16_t, std::vector<uint64_t>> &sharded_relationships_ids) {
             std::vector<seastar::future<std::vector<Relationship>>> futures;
             for (auto const &[their_shard, grouped_rel_ids] : sharded_relationships_ids) {
               auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids](Shard &local_shard) {
                      return local_shard.RelationshipsGet(grouped_rel_ids);
               });
               futures.push_back(std::move(future));
             }

             auto p = make_shared(std::move(futures));
             return seastar::when_all_succeed(p->begin(), p->end()).then([](const std::vector<std::vector<Relationship>> &results) {
                    std::vector<Relationship> combined;

                    for (const std::vector<Relationship> &sharded : results) {
                      combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                    }
                    return combined;
             });
      });
  }

  seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(const std::string& type, const std::string& key, Direction direction) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    switch(direction) {
    case OUT: {
      return container().invoke_on(node_shard_id, [type, key](Shard &local_shard) {
             return local_shard.NodeGetOutgoingRelationships(type, key); });
    }
    case IN: {
      return container().invoke_on(node_shard_id, [type, key](Shard &local_shard) {
               return local_shard.NodeGetShardedIncomingRelationshipIDs(type, key); })
        .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_relationships_ids) {
               std::vector<seastar::future<std::vector<Relationship>>> futures;
               for (auto const& [their_shard, grouped_rel_ids] : sharded_relationships_ids ) {
                 auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids] (Shard &local_shard) {
                        return local_shard.RelationshipsGet(grouped_rel_ids);
                 });
                 futures.push_back(std::move(future));
               }

               auto p = make_shared(std::move(futures));
               return seastar::when_all_succeed(p->begin(), p->end()).then([] (const std::vector<std::vector<Relationship>>& results) {
                      std::vector<Relationship> combined;

                      for(const std::vector<Relationship>& sharded : results) {
                        combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                      }
                      return combined;
               });
        });
    }
    default: return NodeGetRelationshipsPeered(type, key);
    }
  }

  seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type) {
    uint16_t node_shard_id = CalculateShardId(type, key);
    uint16_t rel_type_id = relationship_types.getTypeId(rel_type);
    if (rel_type_id != 0) {
      switch (direction) {
      case OUT: {
        return container().invoke_on(node_shard_id, [type, key, rel_type_id](Shard &local_shard) { return local_shard.NodeGetOutgoingRelationships(type, key, rel_type_id); });
      }
      case IN: {
        return container().invoke_on(node_shard_id, [type, key, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedIncomingRelationshipIDs(type, key, rel_type_id); })
          .then([this](const std::map<uint16_t, std::vector<uint64_t>>& sharded_relationships_ids) {
                 std::vector<seastar::future<std::vector<Relationship>>> futures;
                 for (auto const &[their_shard, grouped_rel_ids] : sharded_relationships_ids) {
                   auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids](Shard &local_shard) {
                          return local_shard.RelationshipsGet(grouped_rel_ids);
                   });
                   futures.push_back(std::move(future));
                 }

                 auto p = make_shared(std::move(futures));
                 return seastar::when_all_succeed(p->begin(), p->end()).then([](const std::vector<std::vector<Relationship>>& results) {
                        std::vector<Relationship> combined;

                        for (const std::vector<Relationship>& sharded : results) {
                          combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                        }
                        return combined;
                 });
          });
      }
      default:
        return NodeGetRelationshipsPeered(type, key, rel_type_id);
      }
    }

    return seastar::make_ready_future<std::vector<Relationship>>();
  }

  seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(const std::string& type, const std::string& key, Direction direction, uint16_t rel_type_id) {
    uint16_t node_shard_id = CalculateShardId(type, key);
    if (rel_type_id != 0) {
      switch (direction) {
      case OUT: {
        return container().invoke_on(node_shard_id, [type, key, rel_type_id](Shard &local_shard) { return local_shard.NodeGetOutgoingRelationships(type, key, rel_type_id); });
      }
      case IN: {
        return container().invoke_on(node_shard_id, [type, key, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedIncomingRelationshipIDs(type, key, rel_type_id); })
          .then([this](const std::map<uint16_t, std::vector<uint64_t>>& sharded_relationships_ids) {
                 std::vector<seastar::future<std::vector<Relationship>>> futures;
                 for (auto const &[their_shard, grouped_rel_ids] : sharded_relationships_ids) {
                   auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids](Shard &local_shard) {
                          return local_shard.RelationshipsGet(grouped_rel_ids);
                   });
                   futures.push_back(std::move(future));
                 }

                 auto p = make_shared(std::move(futures));
                 return seastar::when_all_succeed(p->begin(), p->end()).then([](const std::vector<std::vector<Relationship>>& results) {
                        std::vector<Relationship> combined;

                        for (const std::vector<Relationship>& sharded : results) {
                          combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                        }
                        return combined;
                 });
          });
      }
      default:
        return NodeGetRelationshipsPeered(type, key, rel_type_id);
      }
    }

    return seastar::make_ready_future<std::vector<Relationship>>();
  }

  seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(const std::string& type, const std::string& key, Direction direction, const std::vector<std::string> &rel_types) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    switch(direction) {
    case OUT: {
      return container().invoke_on(node_shard_id, [type, key, rel_types](Shard &local_shard) {
             return local_shard.NodeGetOutgoingRelationships(type, key, rel_types); });
    }
    case IN: {
      return container().invoke_on(node_shard_id, [type, key, rel_types](Shard &local_shard) {
               return local_shard.NodeGetShardedIncomingRelationshipIDs(type, key, rel_types); })
        .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_relationships_ids) {
               std::vector<seastar::future<std::vector<Relationship>>> futures;
               for (auto const& [their_shard, grouped_rel_ids] : sharded_relationships_ids ) {
                 auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids] (Shard &local_shard) {
                        return local_shard.RelationshipsGet(grouped_rel_ids);
                 });
                 futures.push_back(std::move(future));
               }

               auto p = make_shared(std::move(futures));
               return seastar::when_all_succeed(p->begin(), p->end()).then([] (const std::vector<std::vector<Relationship>>& results) {
                      std::vector<Relationship> combined;

                      for(const std::vector<Relationship>& sharded : results) {
                        combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                      }
                      return combined;
               });
        });
    }
    default: return NodeGetRelationshipsPeered(type, key, rel_types);
    }
  }


  seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(uint64_t external_id, Direction direction) {
    uint16_t node_shard_id = CalculateShardId(external_id);

    switch(direction) {
    case OUT: {
      return container().invoke_on(node_shard_id, [external_id](Shard &local_shard) {
             return local_shard.NodeGetOutgoingRelationships(external_id); });
    }
    case IN: {
      return container().invoke_on(node_shard_id, [external_id](Shard &local_shard) {
               return local_shard.NodeGetShardedIncomingRelationshipIDs(external_id); })
        .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_relationships_ids) {
               std::vector<seastar::future<std::vector<Relationship>>> futures;
               for (auto const& [their_shard, grouped_rel_ids] : sharded_relationships_ids ) {
                 auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids] (Shard &local_shard) {
                        return local_shard.RelationshipsGet(grouped_rel_ids);
                 });
                 futures.push_back(std::move(future));
               }

               auto p = make_shared(std::move(futures));
               return seastar::when_all_succeed(p->begin(), p->end()).then([] (const std::vector<std::vector<Relationship>>& results) {
                      std::vector<Relationship> combined;

                      for(const std::vector<Relationship>& sharded : results) {
                        combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                      }
                      return combined;
               });
        });
    }
    default: return NodeGetRelationshipsPeered(external_id);
    }
  }

  seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(uint64_t external_id, Direction direction, const std::string& rel_type) {
    uint16_t node_shard_id = CalculateShardId(external_id);
    uint16_t rel_type_id = relationship_types.getTypeId(rel_type);
    if (rel_type_id != 0) {
      switch (direction) {
      case OUT: {
        return container().invoke_on(node_shard_id, [external_id, rel_type_id](Shard &local_shard) { return local_shard.NodeGetOutgoingRelationships(external_id, rel_type_id); });
      }
      case IN: {
        return container().invoke_on(node_shard_id, [external_id, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedIncomingRelationshipIDs(external_id, rel_type_id); })
          .then([this](const std::map<uint16_t, std::vector<uint64_t>>& sharded_relationships_ids) {
                 std::vector<seastar::future<std::vector<Relationship>>> futures;
                 for (auto const &[their_shard, grouped_rel_ids] : sharded_relationships_ids) {
                   auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids](Shard &local_shard) {
                          return local_shard.RelationshipsGet(grouped_rel_ids);
                   });
                   futures.push_back(std::move(future));
                 }

                 auto p = make_shared(std::move(futures));
                 return seastar::when_all_succeed(p->begin(), p->end()).then([](const std::vector<std::vector<Relationship>>& results) {
                        std::vector<Relationship> combined;

                        for (const std::vector<Relationship>& sharded : results) {
                          combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                        }
                        return combined;
                 });
          });
      }
      default:
        return NodeGetRelationshipsPeered(external_id, rel_type_id);
      }
    }

    return seastar::make_ready_future<std::vector<Relationship>>();
  }

  seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(uint64_t external_id, Direction direction, uint16_t rel_type_id) {
    uint16_t node_shard_id = CalculateShardId(external_id);
    if (rel_type_id != 0) {
      switch (direction) {
      case OUT: {
        return container().invoke_on(node_shard_id, [external_id, rel_type_id](Shard &local_shard) { return local_shard.NodeGetOutgoingRelationships(external_id, rel_type_id); });
      }
      case IN: {
        return container().invoke_on(node_shard_id, [external_id, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedIncomingRelationshipIDs(external_id, rel_type_id); })
          .then([this](const std::map<uint16_t, std::vector<uint64_t>>& sharded_relationships_ids) {
                 std::vector<seastar::future<std::vector<Relationship>>> futures;
                 for (auto const &[their_shard, grouped_rel_ids] : sharded_relationships_ids) {
                   auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids](Shard &local_shard) {
                          return local_shard.RelationshipsGet(grouped_rel_ids);
                   });
                   futures.push_back(std::move(future));
                 }

                 auto p = make_shared(std::move(futures));
                 return seastar::when_all_succeed(p->begin(), p->end()).then([](const std::vector<std::vector<Relationship>>& results) {
                        std::vector<Relationship> combined;

                        for (const std::vector<Relationship>& sharded : results) {
                          combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                        }
                        return combined;
                 });
          });
      }
      default:
        return NodeGetRelationshipsPeered(external_id, rel_type_id);
      }
    }

    return seastar::make_ready_future<std::vector<Relationship>>();
  }

  seastar::future<std::vector<Relationship>> Shard::NodeGetRelationshipsPeered(uint64_t external_id, Direction direction, const std::vector<std::string> &rel_types) {
    uint16_t node_shard_id = CalculateShardId(external_id);

    switch(direction) {
    case OUT: {
      return container().invoke_on(node_shard_id, [external_id, rel_types](Shard &local_shard) {
             return local_shard.NodeGetOutgoingRelationships(external_id, rel_types); });
    }
    case IN: {
      return container().invoke_on(node_shard_id, [external_id, rel_types](Shard &local_shard) {
               return local_shard.NodeGetShardedIncomingRelationshipIDs(external_id, rel_types); })
        .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_relationships_ids) {
               std::vector<seastar::future<std::vector<Relationship>>> futures;
               for (auto const& [their_shard, grouped_rel_ids] : sharded_relationships_ids ) {
                 auto future = container().invoke_on(their_shard, [grouped_rel_ids = grouped_rel_ids] (Shard &local_shard) {
                        return local_shard.RelationshipsGet(grouped_rel_ids);
                 });
                 futures.push_back(std::move(future));
               }

               auto p = make_shared(std::move(futures));
               return seastar::when_all_succeed(p->begin(), p->end()).then([] (const std::vector<std::vector<Relationship>>& results) {
                      std::vector<Relationship> combined;

                      for(const std::vector<Relationship>& sharded : results) {
                        combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                      }
                      return combined;
               });
        });
    }
    default: return NodeGetRelationshipsPeered(external_id, rel_types);
    }
  }


  seastar::future<std::vector<Node>> Shard::NodeGetNeighborsPeered(const std::string& type, const std::string& key) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    return container().invoke_on(node_shard_id, [type, key](Shard &local_shard) {
             return local_shard.NodeGetShardedNodeIDs(type, key); })
      .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_nodes_ids) {
             std::vector<seastar::future<std::vector<Node>>> futures;
             for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
               auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
                      return local_shard.NodesGet(grouped_node_ids);
               });
               futures.push_back(std::move(future));
             }

             auto p = make_shared(std::move(futures));
             return seastar::when_all_succeed(p->begin(), p->end()).then([] (const std::vector<std::vector<Node>>& results) {
                    std::vector<Node> combined;

                    for(const std::vector<Node>& sharded : results) {
                      combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                    }
                    return combined;
             });
      });
  }

  seastar::future<std::vector<Node>> Shard::NodeGetNeighborsPeered(const std::string& type, const std::string& key, const std::string& rel_type) {
    uint16_t node_shard_id = CalculateShardId(type, key);
    uint16_t rel_type_id = relationship_types.getTypeId(rel_type);
    if (rel_type_id > 0) {
      return container().invoke_on(node_shard_id, [type, key, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedNodeIDs(type, key, rel_type_id); })
        .then([this](const std::map<uint16_t, std::vector<uint64_t>> &sharded_nodes_ids) {
               std::vector<seastar::future<std::vector<Node>>> futures;
               for (auto const &[their_shard, grouped_node_ids] : sharded_nodes_ids) {
                 auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids](Shard &local_shard) {
                        return local_shard.NodesGet(grouped_node_ids);
                 });
                 futures.push_back(std::move(future));
               }

               auto p = make_shared(std::move(futures));
               return seastar::when_all_succeed(p->begin(), p->end()).then([](const std::vector<std::vector<Node>> &results) {
                      std::vector<Node> combined;

                      for (const std::vector<Node> &sharded : results) {
                        combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                      }
                      return combined;
               });
        });
    }

    return seastar::make_ready_future<std::vector<Node>>();
  }

  seastar::future<std::vector<Node>> Shard::NodeGetNeighborsPeered(const std::string& type, const std::string& key, uint16_t rel_type_id) {
    uint16_t node_shard_id = CalculateShardId(type, key);
    if (rel_type_id > 0) {
      return container().invoke_on(node_shard_id, [type, key, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedNodeIDs(type, key, rel_type_id); })
        .then([this](const std::map<uint16_t, std::vector<uint64_t>> &sharded_nodes_ids) {
               std::vector<seastar::future<std::vector<Node>>> futures;
               for (auto const &[their_shard, grouped_node_ids] : sharded_nodes_ids) {
                 auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids](Shard &local_shard) {
                        return local_shard.NodesGet(grouped_node_ids);
                 });
                 futures.push_back(std::move(future));
               }

               auto p = make_shared(std::move(futures));
               return seastar::when_all_succeed(p->begin(), p->end()).then([](const std::vector<std::vector<Node>> &results) {
                      std::vector<Node> combined;

                      for (const std::vector<Node> &sharded : results) {
                        combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                      }
                      return combined;
               });
        });
    }

    return seastar::make_ready_future<std::vector<Node>>();
  }

  seastar::future<std::vector<Node>> Shard::NodeGetNeighborsPeered(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types) {
    uint16_t node_shard_id = CalculateShardId(type, key);
    return container().invoke_on(node_shard_id, [type, key, rel_types](Shard &local_shard) { return local_shard.NodeGetShardedNodeIDs(type, key, rel_types); })
      .then([this](const std::map<uint16_t, std::vector<uint64_t>> &sharded_nodes_ids) {
             std::vector<seastar::future<std::vector<Node>>> futures;
             for (auto const &[their_shard, grouped_node_ids] : sharded_nodes_ids) {
               auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids](Shard &local_shard) {
                      return local_shard.NodesGet(grouped_node_ids);
               });
               futures.push_back(std::move(future));
             }

             auto p = make_shared(std::move(futures));
             return seastar::when_all_succeed(p->begin(), p->end()).then([](const std::vector<std::vector<Node>> &results) {
                    std::vector<Node> combined;

                    for (const std::vector<Node> &sharded : results) {
                      combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                    }
                    return combined;
             });
      });
  }

  seastar::future<std::vector<Node>> Shard::NodeGetNeighborsPeered(uint64_t external_id) {
    uint16_t node_shard_id = CalculateShardId(external_id);

    return container().invoke_on(node_shard_id, [external_id](Shard &local_shard) {
             return local_shard.NodeGetShardedNodeIDs(external_id); })
      .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_nodes_ids) {
             std::vector<seastar::future<std::vector<Node>>> futures;
             for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
               auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
                      return local_shard.NodesGet(grouped_node_ids);
               });
               futures.push_back(std::move(future));
             }

             auto p = make_shared(std::move(futures));
             return seastar::when_all_succeed(p->begin(), p->end()).then([] (const std::vector<std::vector<Node>>& results) {
                    std::vector<Node> combined;

                    for(const std::vector<Node>& sharded : results) {
                      combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                    }
                    return combined;
             });
      });
  }

  seastar::future<std::vector<Node>> Shard::NodeGetNeighborsPeered(uint64_t external_id, const std::string& rel_type) {
    uint16_t node_shard_id = CalculateShardId(external_id);
    uint16_t rel_type_id = relationship_types.getTypeId(rel_type);
    if (rel_type_id > 0) {
      return container().invoke_on(node_shard_id, [external_id, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedNodeIDs(external_id, rel_type_id); })
        .then([this](const std::map<uint16_t, std::vector<uint64_t>> &sharded_nodes_ids) {
               std::vector<seastar::future<std::vector<Node>>> futures;
               for (auto const &[their_shard, grouped_node_ids] : sharded_nodes_ids) {
                 auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids](Shard &local_shard) {
                        return local_shard.NodesGet(grouped_node_ids);
                 });
                 futures.push_back(std::move(future));
               }

               auto p = make_shared(std::move(futures));
               return seastar::when_all_succeed(p->begin(), p->end()).then([](const std::vector<std::vector<Node>> &results) {
                      std::vector<Node> combined;

                      for (const std::vector<Node> &sharded : results) {
                        combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                      }
                      return combined;
               });
        });
    }

    return seastar::make_ready_future<std::vector<Node>>();
  }

  seastar::future<std::vector<Node>> Shard::NodeGetNeighborsPeered(uint64_t external_id,  uint16_t rel_type_id) {
    uint16_t node_shard_id = CalculateShardId(external_id);
    if (rel_type_id > 0) {
      return container().invoke_on(node_shard_id, [external_id, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedNodeIDs(external_id, rel_type_id); })
        .then([this](const std::map<uint16_t, std::vector<uint64_t>> &sharded_nodes_ids) {
               std::vector<seastar::future<std::vector<Node>>> futures;
               for (auto const &[their_shard, grouped_node_ids] : sharded_nodes_ids) {
                 auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids](Shard &local_shard) {
                        return local_shard.NodesGet(grouped_node_ids);
                 });
                 futures.push_back(std::move(future));
               }

               auto p = make_shared(std::move(futures));
               return seastar::when_all_succeed(p->begin(), p->end()).then([](const std::vector<std::vector<Node>> &results) {
                      std::vector<Node> combined;

                      for (const std::vector<Node> &sharded : results) {
                        combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                      }
                      return combined;
               });
        });
    }

    return seastar::make_ready_future<std::vector<Node>>();
  }

  seastar::future<std::vector<Node>> Shard::NodeGetNeighborsPeered(uint64_t external_id, const std::vector<std::string> &rel_types) {
    uint16_t node_shard_id = CalculateShardId(external_id);
    return container().invoke_on(node_shard_id, [external_id, rel_types](Shard &local_shard) { return local_shard.NodeGetShardedNodeIDs(external_id, rel_types); })
      .then([this](const std::map<uint16_t, std::vector<uint64_t>> &sharded_nodes_ids) {
             std::vector<seastar::future<std::vector<Node>>> futures;
             for (auto const &[their_shard, grouped_node_ids] : sharded_nodes_ids) {
               auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids](Shard &local_shard) {
                      return local_shard.NodesGet(grouped_node_ids);
               });
               futures.push_back(std::move(future));
             }

             auto p = make_shared(std::move(futures));
             return seastar::when_all_succeed(p->begin(), p->end()).then([](const std::vector<std::vector<Node>> &results) {
                    std::vector<Node> combined;

                    for (const std::vector<Node> &sharded : results) {
                      combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                    }
                    return combined;
             });
      });
  }


  seastar::future<std::vector<Node>> Shard::NodeGetNeighborsPeered(const std::string& type, const std::string& key, Direction direction) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    switch(direction) {
    case OUT: {
      return container().invoke_on(node_shard_id, [type, key](Shard &local_shard) {
               return local_shard.NodeGetShardedOutgoingNodeIDs(type, key); })
        .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_nodes_ids) {
               std::vector<seastar::future<std::vector<Node>>> futures;
               for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
                 auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
                        return local_shard.NodesGet(grouped_node_ids);
                 });
                 futures.push_back(std::move(future));
               }

               auto p = make_shared(std::move(futures));
               return seastar::when_all_succeed(p->begin(), p->end()).then([] (const std::vector<std::vector<Node>>& results) {
                      std::vector<Node> combined;

                      for(const std::vector<Node>& sharded : results) {
                        combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                      }
                      return combined;
               });
        });
    }
    case IN: {
      return container().invoke_on(node_shard_id, [type, key](Shard &local_shard) {
               return local_shard.NodeGetShardedIncomingNodeIDs(type, key); })
        .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_nodes_ids) {
               std::vector<seastar::future<std::vector<Node>>> futures;
               for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
                 auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
                        return local_shard.NodesGet(grouped_node_ids);
                 });
                 futures.push_back(std::move(future));
               }

               auto p = make_shared(std::move(futures));
               return seastar::when_all_succeed(p->begin(), p->end()).then([] (const std::vector<std::vector<Node>>& results) {
                      std::vector<Node> combined;

                      for(const std::vector<Node>& sharded : results) {
                        combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                      }
                      return combined;
               });
        });
    }
    default: return NodeGetNeighborsPeered(type, key);
    }
  }

  seastar::future<std::vector<Node>> Shard::NodeGetNeighborsPeered(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type) {
    uint16_t node_shard_id = CalculateShardId(type, key);
    uint16_t rel_type_id = relationship_types.getTypeId(rel_type);
    if (rel_type_id != 0) {
      switch (direction) {
      case OUT: {
        return container().invoke_on(node_shard_id, [type, key, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedOutgoingNodeIDs(type, key, rel_type_id); })
          .then([this](const std::map<uint16_t, std::vector<uint64_t>>& sharded_nodes_ids) {
                 std::vector<seastar::future<std::vector<Node>>> futures;
                 for (auto const &[their_shard, grouped_node_ids] : sharded_nodes_ids) {
                   auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids](Shard &local_shard) {
                          return local_shard.NodesGet(grouped_node_ids);
                   });
                   futures.push_back(std::move(future));
                 }

                 auto p = make_shared(std::move(futures));
                 return seastar::when_all_succeed(p->begin(), p->end()).then([](const std::vector<std::vector<Node>>& results) {
                        std::vector<Node> combined;

                        for (const std::vector<Node>& sharded : results) {
                          combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                        }
                        return combined;
                 });
          });
      }
      case IN: {
        return container().invoke_on(node_shard_id, [type, key, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedIncomingNodeIDs(type, key, rel_type_id); })
          .then([this](const std::map<uint16_t, std::vector<uint64_t>>& sharded_nodes_ids) {
                 std::vector<seastar::future<std::vector<Node>>> futures;
                 for (auto const &[their_shard, grouped_node_ids] : sharded_nodes_ids) {
                   auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids](Shard &local_shard) {
                          return local_shard.NodesGet(grouped_node_ids);
                   });
                   futures.push_back(std::move(future));
                 }

                 auto p = make_shared(std::move(futures));
                 return seastar::when_all_succeed(p->begin(), p->end()).then([](const std::vector<std::vector<Node>>& results) {
                        std::vector<Node> combined;

                        for (const std::vector<Node>& sharded : results) {
                          combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                        }
                        return combined;
                 });
          });
      }
      default:
        return NodeGetNeighborsPeered(type, key, rel_type_id);
      }
    }

    return seastar::make_ready_future<std::vector<Node>>();
  }

  seastar::future<std::vector<Node>> Shard::NodeGetNeighborsPeered(const std::string& type, const std::string& key, Direction direction, uint16_t rel_type_id) {
    uint16_t node_shard_id = CalculateShardId(type, key);
    if (rel_type_id != 0) {
      switch (direction) {
      case OUT: {
        return container().invoke_on(node_shard_id, [type, key, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedOutgoingNodeIDs(type, key, rel_type_id); })
          .then([this](const std::map<uint16_t, std::vector<uint64_t>>& sharded_nodes_ids) {
                 std::vector<seastar::future<std::vector<Node>>> futures;
                 for (auto const &[their_shard, grouped_node_ids] : sharded_nodes_ids) {
                   auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids](Shard &local_shard) {
                          return local_shard.NodesGet(grouped_node_ids);
                   });
                   futures.push_back(std::move(future));
                 }

                 auto p = make_shared(std::move(futures));
                 return seastar::when_all_succeed(p->begin(), p->end()).then([](const std::vector<std::vector<Node>>& results) {
                        std::vector<Node> combined;

                        for (const std::vector<Node>& sharded : results) {
                          combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                        }
                        return combined;
                 });
          });
      }
      case IN: {
        return container().invoke_on(node_shard_id, [type, key, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedIncomingNodeIDs(type, key, rel_type_id); })
          .then([this](const std::map<uint16_t, std::vector<uint64_t>>& sharded_nodes_ids) {
                 std::vector<seastar::future<std::vector<Node>>> futures;
                 for (auto const &[their_shard, grouped_node_ids] : sharded_nodes_ids) {
                   auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids](Shard &local_shard) {
                          return local_shard.NodesGet(grouped_node_ids);
                   });
                   futures.push_back(std::move(future));
                 }

                 auto p = make_shared(std::move(futures));
                 return seastar::when_all_succeed(p->begin(), p->end()).then([](const std::vector<std::vector<Node>>& results) {
                        std::vector<Node> combined;

                        for (const std::vector<Node>& sharded : results) {
                          combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                        }
                        return combined;
                 });
          });
      }
      default:
        return NodeGetNeighborsPeered(type, key, rel_type_id);
      }
    }

    return seastar::make_ready_future<std::vector<Node>>();
  }

  seastar::future<std::vector<Node>> Shard::NodeGetNeighborsPeered(const std::string& type, const std::string& key, Direction direction, const std::vector<std::string> &rel_types) {
    uint16_t node_shard_id = CalculateShardId(type, key);

    switch(direction) {
    case OUT: {
      return container().invoke_on(node_shard_id, [type, key, rel_types](Shard &local_shard) {
               return local_shard.NodeGetShardedOutgoingNodeIDs(type, key, rel_types); })
        .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_nodes_ids) {
               std::vector<seastar::future<std::vector<Node>>> futures;
               for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
                 auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
                        return local_shard.NodesGet(grouped_node_ids);
                 });
                 futures.push_back(std::move(future));
               }

               auto p = make_shared(std::move(futures));
               return seastar::when_all_succeed(p->begin(), p->end()).then([] (const std::vector<std::vector<Node>>& results) {
                      std::vector<Node> combined;

                      for(const std::vector<Node>& sharded : results) {
                        combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                      }
                      return combined;
               });
        });
    }
    case IN: {
      return container().invoke_on(node_shard_id, [type, key, rel_types](Shard &local_shard) {
               return local_shard.NodeGetShardedIncomingNodeIDs(type, key, rel_types); })
        .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_nodes_ids) {
               std::vector<seastar::future<std::vector<Node>>> futures;
               for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
                 auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
                        return local_shard.NodesGet(grouped_node_ids);
                 });
                 futures.push_back(std::move(future));
               }

               auto p = make_shared(std::move(futures));
               return seastar::when_all_succeed(p->begin(), p->end()).then([] (const std::vector<std::vector<Node>>& results) {
                      std::vector<Node> combined;

                      for(const std::vector<Node>& sharded : results) {
                        combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                      }
                      return combined;
               });
        });
    }
    default: return NodeGetNeighborsPeered(type, key, rel_types);
    }
  }


  seastar::future<std::vector<Node>> Shard::NodeGetNeighborsPeered(uint64_t external_id, Direction direction) {
    uint16_t node_shard_id = CalculateShardId(external_id);

    switch(direction) {
    case OUT: {
      return container().invoke_on(node_shard_id, [external_id](Shard &local_shard) {
               return local_shard.NodeGetShardedOutgoingNodeIDs(external_id); })
        .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_nodes_ids) {
               std::vector<seastar::future<std::vector<Node>>> futures;
               for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
                 auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
                        return local_shard.NodesGet(grouped_node_ids);
                 });
                 futures.push_back(std::move(future));
               }

               auto p = make_shared(std::move(futures));
               return seastar::when_all_succeed(p->begin(), p->end()).then([] (const std::vector<std::vector<Node>>& results) {
                      std::vector<Node> combined;

                      for(const std::vector<Node>& sharded : results) {
                        combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                      }
                      return combined;
               });
        });
    }
    case IN: {
      return container().invoke_on(node_shard_id, [external_id](Shard &local_shard) {
               return local_shard.NodeGetShardedIncomingNodeIDs(external_id); })
        .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_nodes_ids) {
               std::vector<seastar::future<std::vector<Node>>> futures;
               for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
                 auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
                        return local_shard.NodesGet(grouped_node_ids);
                 });
                 futures.push_back(std::move(future));
               }

               auto p = make_shared(std::move(futures));
               return seastar::when_all_succeed(p->begin(), p->end()).then([] (const std::vector<std::vector<Node>>& results) {
                      std::vector<Node> combined;

                      for(const std::vector<Node>& sharded : results) {
                        combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                      }
                      return combined;
               });
        });
    }
    default: return NodeGetNeighborsPeered(external_id);
    }
  }

  seastar::future<std::vector<Node>> Shard::NodeGetNeighborsPeered(uint64_t external_id, Direction direction, const std::string& rel_type) {
    uint16_t node_shard_id = CalculateShardId(external_id);
    uint16_t rel_type_id = relationship_types.getTypeId(rel_type);
    if (rel_type_id != 0) {
      switch (direction) {
      case OUT: {
        return container().invoke_on(node_shard_id, [external_id, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedOutgoingNodeIDs(external_id, rel_type_id); })
          .then([this](const std::map<uint16_t, std::vector<uint64_t>>& sharded_nodes_ids) {
                 std::vector<seastar::future<std::vector<Node>>> futures;
                 for (auto const &[their_shard, grouped_node_ids] : sharded_nodes_ids) {
                   auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids](Shard &local_shard) {
                          return local_shard.NodesGet(grouped_node_ids);
                   });
                   futures.push_back(std::move(future));
                 }

                 auto p = make_shared(std::move(futures));
                 return seastar::when_all_succeed(p->begin(), p->end()).then([](const std::vector<std::vector<Node>>& results) {
                        std::vector<Node> combined;

                        for (const std::vector<Node>& sharded : results) {
                          combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                        }
                        return combined;
                 });
          });
      }
      case IN: {
        return container().invoke_on(node_shard_id, [external_id, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedIncomingNodeIDs(external_id, rel_type_id); })
          .then([this](const std::map<uint16_t, std::vector<uint64_t>>& sharded_nodes_ids) {
                 std::vector<seastar::future<std::vector<Node>>> futures;
                 for (auto const &[their_shard, grouped_node_ids] : sharded_nodes_ids) {
                   auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids](Shard &local_shard) {
                          return local_shard.NodesGet(grouped_node_ids);
                   });
                   futures.push_back(std::move(future));
                 }

                 auto p = make_shared(std::move(futures));
                 return seastar::when_all_succeed(p->begin(), p->end()).then([](const std::vector<std::vector<Node>>& results) {
                        std::vector<Node> combined;

                        for (const std::vector<Node>& sharded : results) {
                          combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                        }
                        return combined;
                 });
          });
      }
      default:
        return NodeGetNeighborsPeered(external_id, rel_type_id);
      }
    }

    return seastar::make_ready_future<std::vector<Node>>();
  }

  seastar::future<std::vector<Node>> Shard::NodeGetNeighborsPeered(uint64_t external_id, Direction direction, uint16_t rel_type_id) {
    uint16_t node_shard_id = CalculateShardId(external_id);
    if (rel_type_id != 0) {
      switch (direction) {
      case OUT: {
        return container().invoke_on(node_shard_id, [external_id, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedOutgoingNodeIDs(external_id, rel_type_id); })
          .then([this](const std::map<uint16_t, std::vector<uint64_t>>& sharded_nodes_ids) {
                 std::vector<seastar::future<std::vector<Node>>> futures;
                 for (auto const &[their_shard, grouped_node_ids] : sharded_nodes_ids) {
                   auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids](Shard &local_shard) {
                          return local_shard.NodesGet(grouped_node_ids);
                   });
                   futures.push_back(std::move(future));
                 }

                 auto p = make_shared(std::move(futures));
                 return seastar::when_all_succeed(p->begin(), p->end()).then([](const std::vector<std::vector<Node>>& results) {
                        std::vector<Node> combined;

                        for (const std::vector<Node>& sharded : results) {
                          combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                        }
                        return combined;
                 });
          });
      }
      case IN: {
        return container().invoke_on(node_shard_id, [external_id, rel_type_id](Shard &local_shard) { return local_shard.NodeGetShardedIncomingNodeIDs(external_id, rel_type_id); })
          .then([this](const std::map<uint16_t, std::vector<uint64_t>>& sharded_nodes_ids) {
                 std::vector<seastar::future<std::vector<Node>>> futures;
                 for (auto const &[their_shard, grouped_node_ids] : sharded_nodes_ids) {
                   auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids](Shard &local_shard) {
                          return local_shard.NodesGet(grouped_node_ids);
                   });
                   futures.push_back(std::move(future));
                 }

                 auto p = make_shared(std::move(futures));
                 return seastar::when_all_succeed(p->begin(), p->end()).then([](const std::vector<std::vector<Node>>& results) {
                        std::vector<Node> combined;

                        for (const std::vector<Node>& sharded : results) {
                          combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                        }
                        return combined;
                 });
          });
      }
      default:
        return NodeGetNeighborsPeered(external_id, rel_type_id);
      }
    }

    return seastar::make_ready_future<std::vector<Node>>();
  }

  seastar::future<std::vector<Node>> Shard::NodeGetNeighborsPeered(uint64_t external_id, Direction direction, const std::vector<std::string> &rel_types) {
    uint16_t node_shard_id = CalculateShardId(external_id);

    switch(direction) {
    case OUT: {
      return container().invoke_on(node_shard_id, [external_id, rel_types](Shard &local_shard) {
               return local_shard.NodeGetShardedOutgoingNodeIDs(external_id, rel_types); })
        .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_nodes_ids) {
               std::vector<seastar::future<std::vector<Node>>> futures;
               for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
                 auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
                        return local_shard.NodesGet(grouped_node_ids);
                 });
                 futures.push_back(std::move(future));
               }

               auto p = make_shared(std::move(futures));
               return seastar::when_all_succeed(p->begin(), p->end()).then([] (const std::vector<std::vector<Node>>& results) {
                      std::vector<Node> combined;

                      for(const std::vector<Node>& sharded : results) {
                        combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                      }
                      return combined;
               });
        });
    }
    case IN: {
      return container().invoke_on(node_shard_id, [external_id, rel_types](Shard &local_shard) {
               return local_shard.NodeGetShardedIncomingNodeIDs(external_id, rel_types); })
        .then([this] (const std::map<uint16_t, std::vector<uint64_t>>& sharded_nodes_ids) {
               std::vector<seastar::future<std::vector<Node>>> futures;
               for (auto const& [their_shard, grouped_node_ids] : sharded_nodes_ids ) {
                 auto future = container().invoke_on(their_shard, [grouped_node_ids = grouped_node_ids] (Shard &local_shard) {
                        return local_shard.NodesGet(grouped_node_ids);
                 });
                 futures.push_back(std::move(future));
               }

               auto p = make_shared(std::move(futures));
               return seastar::when_all_succeed(p->begin(), p->end()).then([] (const std::vector<std::vector<Node>>& results) {
                      std::vector<Node> combined;

                      for(const std::vector<Node>& sharded : results) {
                        combined.insert(std::end(combined), std::begin(sharded), std::end(sharded));
                      }
                      return combined;
               });
        });
    }
    default: return NodeGetNeighborsPeered(external_id, rel_types);
    }
  }

  // All
  seastar::future<std::vector<uint64_t>> Shard::AllNodeIdsPeered(uint64_t skip, uint64_t limit) {
    uint64_t max = skip + limit;

    // Get the {Node Type Id, Count} map for each core
    std::vector<seastar::future<std::map<uint16_t, uint64_t>>> futures;
    for (int i=0; i<cpus; i++) {
      auto future = container().invoke_on(i, [] (Shard &local_shard) mutable {
             return local_shard.AllNodeIdCounts();
      });
      futures.push_back(std::move(future));
    }

    auto p = make_shared(std::move(futures));
    return seastar::when_all_succeed(p->begin(), p->end()).then([skip, max, limit, this] (const std::vector<std::map<uint16_t, uint64_t>>& results) {
           uint64_t current = 0;
           uint64_t next = 0;
           int current_shard_id = 0;
           std::vector<uint64_t> ids;
           std::map<uint16_t, std::map<uint16_t, std::pair<uint64_t , uint64_t>>> requests;
           for (const auto& map : results) {
             std::map<uint16_t, std::pair<uint64_t , uint64_t>> threaded_requests;
             for (auto entry : map) {
               next = current + entry.second;
               if (next > skip) {
                 std::pair<uint64_t, uint64_t> pair = std::make_pair(skip - current, limit);
                 threaded_requests.insert({ entry.first, pair });
                 if (next <= max) {
                   break; // We have everything we need
                 }
               }
               current = next;
             }
             requests.insert({current_shard_id++, threaded_requests});
           }

           std::vector<seastar::future<std::vector<uint64_t>>> futures;

           for (const auto& request : requests) {
             for (auto entry : request.second) {
               auto future = container().invoke_on(request.first, [entry] (Shard &local_shard) mutable {
                      return local_shard.AllNodeIds(entry.first, entry.second.first, entry.second.second);
               });
               futures.push_back(std::move(future));
             }
           }

           auto p2 = make_shared(std::move(futures));
           return seastar::when_all_succeed(p2->begin(), p2->end()).then([limit] (const std::vector<std::vector<uint64_t>>& results) {
                  std::vector<uint64_t> ids;
                  ids.reserve(limit);
                  for (auto result : results) {
                    ids.insert(std::end(ids), std::begin(result), std::end(result));
                  }
                  return ids;
           });
    });
  }

  seastar::future<std::vector<uint64_t>> Shard::AllNodeIdsPeered(const std::string &type, uint64_t skip, uint64_t limit) {
    uint16_t node_type_id = node_types.getTypeId(type);
    uint64_t max = skip + limit;

    // Get the {Node Type Id, Count} map for each core
    std::vector<seastar::future<uint64_t>> futures;
    for (int i=0; i<cpus; i++) {
      auto future = container().invoke_on(i, [node_type_id] (Shard &local_shard) mutable {
             return local_shard.AllNodeIdCounts(node_type_id);
      });
      futures.push_back(std::move(future));
    }

    auto p = make_shared(std::move(futures));
    return seastar::when_all_succeed(p->begin(), p->end()).then([node_type_id, skip, max, limit, this] (const std::vector<uint64_t>& results) {
           uint64_t current = 0;
           uint64_t next = 0;
           int current_shard_id = 0;
           std::vector<uint64_t> ids;
           std::map<uint16_t, std::pair<uint64_t , uint64_t>> requests;
           for (const auto& count : results) {
             next = current + count;
             if (next > skip) {
               std::pair<uint64_t, uint64_t> pair = std::make_pair(skip - current, limit);
               requests.insert({ current_shard_id++, pair });
               if (next <= max) {
                 break; // We have everything we need
               }
             }
             current = next;
           }

           std::vector<seastar::future<std::vector<uint64_t>>> futures;

           for (const auto& request : requests) {
             auto future = container().invoke_on(request.first, [node_type_id, request] (Shard &local_shard) mutable {
                    return local_shard.AllNodeIds(node_type_id, request.second.first, request.second.second);
             });
             futures.push_back(std::move(future));

           }

           auto p2 = make_shared(std::move(futures));
           return seastar::when_all_succeed(p2->begin(), p2->end()).then([limit] (const std::vector<std::vector<uint64_t>>& results) {
                  std::vector<uint64_t> ids;
                  ids.reserve(limit);
                  for (auto result : results) {
                    ids.insert(std::end(ids), std::begin(result), std::end(result));
                  }
                  return ids;
           });
    });
  }

  seastar::future<std::vector<Node>> Shard::AllNodesPeered(uint64_t skip, uint64_t limit) {
    uint64_t max = skip + limit;

    // Get the {Node Type Id, Count} map for each core
    std::vector<seastar::future<std::map<uint16_t, uint64_t>>> futures;
    for (int i=0; i<cpus; i++) {
      auto future = container().invoke_on(i, [] (Shard &local_shard) mutable {
             return local_shard.AllNodeIdCounts();
      });
      futures.push_back(std::move(future));
    }

    auto p = make_shared(std::move(futures));
    return seastar::when_all_succeed(p->begin(), p->end()).then([skip, max, limit, this] (const std::vector<std::map<uint16_t, uint64_t>>& results) {
           uint64_t current = 0;
           uint64_t next = 0;
           int current_shard_id = 0;
           std::vector<uint64_t> ids;
           std::map<uint16_t, std::map<uint16_t, std::pair<uint64_t , uint64_t>>> requests;
           for (const auto& map : results) {
             std::map<uint16_t, std::pair<uint64_t , uint64_t>> threaded_requests;
             for (auto entry : map) {
               next = current + entry.second;
               if (next > skip) {
                 std::pair<uint64_t, uint64_t> pair = std::make_pair(skip - current, limit);
                 threaded_requests.insert({ entry.first, pair });
                 if (next <= max) {
                   break; // We have everything we need
                 }
               }
               current = next;
             }
             requests.insert({current_shard_id++, threaded_requests});
           }

           std::vector<seastar::future<std::vector<Node>>> futures;

           for (const auto& request : requests) {
             for (auto entry : request.second) {
               auto future = container().invoke_on(request.first, [entry] (Shard &local_shard) mutable {
                      return local_shard.AllNodes(entry.first, entry.second.first, entry.second.second);
               });
               futures.push_back(std::move(future));
             }
           }

           auto p2 = make_shared(std::move(futures));
           return seastar::when_all_succeed(p2->begin(), p2->end()).then([limit] (const std::vector<std::vector<Node>>& results) {
                  std::vector<Node> requested_nodes;
                  requested_nodes.reserve(limit);
                  for (auto result : results) {
                    requested_nodes.insert(std::end(requested_nodes), std::begin(result), std::end(result));
                  }
                  return requested_nodes;
           });
    });
  }

  seastar::future<std::vector<Node>> Shard::AllNodesPeered(const std::string &type, uint64_t skip, uint64_t limit) {
    uint16_t node_type_id = node_types.getTypeId(type);
    uint64_t max = skip + limit;

    // Get the {Node Type Id, Count} map for each core
    std::vector<seastar::future<uint64_t>> futures;
    for (int i=0; i<cpus; i++) {
      auto future = container().invoke_on(i, [node_type_id] (Shard &local_shard) mutable {
             return local_shard.AllNodeIdCounts(node_type_id);
      });
      futures.push_back(std::move(future));
    }

    auto p = make_shared(std::move(futures));
    return seastar::when_all_succeed(p->begin(), p->end()).then([node_type_id, skip, max, limit, this] (const std::vector<uint64_t>& results) {
           uint64_t current = 0;
           uint64_t next = 0;
           int current_shard_id = 0;
           std::vector<uint64_t> ids;
           std::map<uint16_t, std::pair<uint64_t , uint64_t>> requests;
           for (const auto& count : results) {
             next = current + count;
             if (next > skip) {
               std::pair<uint64_t, uint64_t> pair = std::make_pair(skip - current, limit);
               requests.insert({ current_shard_id++, pair });
               if (next <= max) {
                 break; // We have everything we need
               }
             }
             current = next;
           }

           std::vector<seastar::future<std::vector<Node>>> futures;

           for (const auto& request : requests) {
             auto future = container().invoke_on(request.first, [node_type_id, request] (Shard &local_shard) mutable {
                    return local_shard.AllNodes(node_type_id, request.second.first, request.second.second);
             });
             futures.push_back(std::move(future));

           }

           auto p2 = make_shared(std::move(futures));
           return seastar::when_all_succeed(p2->begin(), p2->end()).then([limit] (const std::vector<std::vector<Node>>& results) {
                  std::vector<Node> requested_nodes;
                  requested_nodes.reserve(limit);
                  for (auto result : results) {
                    requested_nodes.insert(std::end(requested_nodes), std::begin(result), std::end(result));
                  }
                  return requested_nodes;
           });
    });
  }

  seastar::future<std::vector<uint64_t>> Shard::AllRelationshipIdsPeered(uint64_t skip, uint64_t limit) {
    uint64_t max = skip + limit;

    // Get the {Relationship Type Id, Count} map for each core
    std::vector<seastar::future<std::map<uint16_t, uint64_t>>> futures;
    for (int i=0; i<cpus; i++) {
      auto future = container().invoke_on(i, [] (Shard &local_shard) mutable {
             return local_shard.AllRelationshipIdCounts();
      });
      futures.push_back(std::move(future));
    }

    auto p = make_shared(std::move(futures));
    return seastar::when_all_succeed(p->begin(), p->end()).then([skip, max, limit, this] (const std::vector<std::map<uint16_t, uint64_t>>& results) {
           uint64_t current = 0;
           uint64_t next = 0;
           int current_shard_id = 0;
           std::vector<uint64_t> ids;
           std::map<uint16_t, std::map<uint16_t, std::pair<uint64_t , uint64_t>>> requests;
           for (const auto& map : results) {
             std::map<uint16_t, std::pair<uint64_t , uint64_t>> threaded_requests;
             for (auto entry : map) {
               next = current + entry.second;
               if (next > skip) {
                 std::pair<uint64_t, uint64_t> pair = std::make_pair(skip - current, limit);
                 threaded_requests.insert({ entry.first, pair });
                 if (next <= max) {
                   break; // We have everything we need
                 }
               }
               current = next;
             }
             requests.insert({current_shard_id++, threaded_requests});
           }

           std::vector<seastar::future<std::vector<uint64_t>>> futures;

           for (const auto& request : requests) {
             for (auto entry : request.second) {
               auto future = container().invoke_on(request.first, [entry] (Shard &local_shard) mutable {
                      return local_shard.AllRelationshipIds(entry.first, entry.second.first, entry.second.second);
               });
               futures.push_back(std::move(future));
             }
           }

           auto p2 = make_shared(std::move(futures));
           return seastar::when_all_succeed(p2->begin(), p2->end()).then([limit] (const std::vector<std::vector<uint64_t>>& results) {
                  std::vector<uint64_t> ids;
                  ids.reserve(limit);
                  for (auto result : results) {
                    ids.insert(std::end(ids), std::begin(result), std::end(result));
                  }
                  return ids;
           });
    });
  }

  seastar::future<std::vector<uint64_t>> Shard::AllRelationshipIdsPeered(const std::string &rel_type, uint64_t skip, uint64_t limit) {
    uint16_t relationship_type_id = relationship_types.getTypeId(rel_type);
    uint64_t max = skip + limit;

    // Get the {Relationship Type Id, Count} map for each core
    std::vector<seastar::future<uint64_t>> futures;
    for (int i=0; i<cpus; i++) {
      auto future = container().invoke_on(i, [relationship_type_id] (Shard &local_shard) mutable {
             return local_shard.AllRelationshipIdCounts(relationship_type_id);
      });
      futures.push_back(std::move(future));
    }

    auto p = make_shared(std::move(futures));
    return seastar::when_all_succeed(p->begin(), p->end()).then([relationship_type_id, skip, max, limit, this] (const std::vector<uint64_t>& results) {
           uint64_t current = 0;
           uint64_t next = 0;
           int current_shard_id = 0;
           std::vector<uint64_t> ids;
           std::map<uint16_t, std::pair<uint64_t , uint64_t>> requests;
           for (const auto& count : results) {
             next = current + count;
             if (next > skip) {
               std::pair<uint64_t, uint64_t> pair = std::make_pair(skip - current, limit);
               requests.insert({ current_shard_id++, pair });
               if (next <= max) {
                 break; // We have everything we need
               }
             }
             current = next;
           }

           std::vector<seastar::future<std::vector<uint64_t>>> futures;

           for (const auto& request : requests) {
             auto future = container().invoke_on(request.first, [relationship_type_id, request] (Shard &local_shard) mutable {
                    return local_shard.AllRelationshipIds(relationship_type_id, request.second.first, request.second.second);
             });
             futures.push_back(std::move(future));

           }

           auto p2 = make_shared(std::move(futures));
           return seastar::when_all_succeed(p2->begin(), p2->end()).then([limit] (const std::vector<std::vector<uint64_t>>& results) {
                  std::vector<uint64_t> ids;
                  ids.reserve(limit);
                  for (auto result : results) {
                    ids.insert(std::end(ids), std::begin(result), std::end(result));
                  }
                  return ids;
           });
    });
  }

  seastar::future<std::vector<Relationship>> Shard::AllRelationshipsPeered(uint64_t skip, uint64_t limit) {
    uint64_t max = skip + limit;

    // Get the {Relationship Type Id, Count} map for each core
    std::vector<seastar::future<std::map<uint16_t, uint64_t>>> futures;
    for (int i=0; i<cpus; i++) {
      auto future = container().invoke_on(i, [] (Shard &local_shard) mutable {
             return local_shard.AllRelationshipIdCounts();
      });
      futures.push_back(std::move(future));
    }

    auto p = make_shared(std::move(futures));
    return seastar::when_all_succeed(p->begin(), p->end()).then([skip, max, limit, this] (const std::vector<std::map<uint16_t, uint64_t>>& results) {
           uint64_t current = 0;
           uint64_t next = 0;
           int current_shard_id = 0;
           std::vector<uint64_t> ids;
           std::map<uint16_t, std::map<uint16_t, std::pair<uint64_t , uint64_t>>> requests;
           for (const auto& map : results) {
             std::map<uint16_t, std::pair<uint64_t , uint64_t>> threaded_requests;
             for (auto entry : map) {
               next = current + entry.second;
               if (next > skip) {
                 std::pair<uint64_t, uint64_t> pair = std::make_pair(skip - current, limit);
                 threaded_requests.insert({ entry.first, pair });
                 if (next <= max) {
                   break; // We have everything we need
                 }
               }
               current = next;
             }
             requests.insert({current_shard_id++, threaded_requests});
           }

           std::vector<seastar::future<std::vector<Relationship>>> futures;

           for (const auto& request : requests) {
             for (auto entry : request.second) {
               auto future = container().invoke_on(request.first, [entry] (Shard &local_shard) mutable {
                      return local_shard.AllRelationships(entry.first, entry.second.first, entry.second.second);
               });
               futures.push_back(std::move(future));
             }
           }

           auto p2 = make_shared(std::move(futures));
           return seastar::when_all_succeed(p2->begin(), p2->end()).then([limit] (const std::vector<std::vector<Relationship>>& results) {
                  std::vector<Relationship> requested_relationships;
                  requested_relationships.reserve(limit);
                  for (auto result : results) {
                    requested_relationships.insert(std::end(requested_relationships), std::begin(result), std::end(result));
                  }
                  return requested_relationships;
           });
    });
  }

  seastar::future<std::vector<Relationship>> Shard::AllRelationshipsPeered(const std::string &rel_type, uint64_t skip, uint64_t limit) {
    uint16_t relationship_type_id = node_types.getTypeId(rel_type);
    uint64_t max = skip + limit;

    // Get the {Relationship Type Id, Count} map for each core
    std::vector<seastar::future<uint64_t>> futures;
    for (int i=0; i<cpus; i++) {
      auto future = container().invoke_on(i, [relationship_type_id] (Shard &local_shard) mutable {
             return local_shard.AllRelationshipIdCounts(relationship_type_id);
      });
      futures.push_back(std::move(future));
    }

    auto p = make_shared(std::move(futures));
    return seastar::when_all_succeed(p->begin(), p->end()).then([relationship_type_id, skip, max, limit, this] (const std::vector<uint64_t>& results) {
           uint64_t current = 0;
           uint64_t next = 0;
           int current_shard_id = 0;
           std::vector<uint64_t> ids;
           std::map<uint16_t, std::pair<uint64_t , uint64_t>> requests;
           for (const auto& count : results) {
             next = current + count;
             if (next > skip) {
               std::pair<uint64_t, uint64_t> pair = std::make_pair(skip - current, limit);
               requests.insert({ current_shard_id++, pair });
               if (next <= max) {
                 break; // We have everything we need
               }
             }
             current = next;
           }

           std::vector<seastar::future<std::vector<Relationship>>> futures;

           for (const auto& request : requests) {
             auto future = container().invoke_on(request.first, [relationship_type_id, request] (Shard &local_shard) mutable {
                    return local_shard.AllRelationships(relationship_type_id, request.second.first, request.second.second);
             });
             futures.push_back(std::move(future));

           }

           auto p2 = make_shared(std::move(futures));
           return seastar::when_all_succeed(p2->begin(), p2->end()).then([limit] (const std::vector<std::vector<Relationship>>& results) {
                  std::vector<Relationship> requested_relationships;
                  requested_relationships.reserve(limit);
                  for (auto result : results) {
                    requested_relationships.insert(std::end(requested_relationships), std::begin(result), std::end(result));
                  }
                  return requested_relationships;
           });
    });
  }


  // *****************************************************************************************************************************
  //                                               Via Lua
  // *****************************************************************************************************************************


  // Relationship Types
  uint16_t Shard::RelationshipTypesGetCountViaLua() {
    return RelationshipTypesGetCount();
  }

  uint64_t Shard::RelationshipTypesGetCountByTypeViaLua(const std::string& type){
    return RelationshipTypesGetCountPeered(type).get0();
  }

  uint64_t Shard::RelationshipTypesGetCountByIdViaLua(uint16_t type_id) {
    return RelationshipTypesGetCountPeered(type_id).get0();
  }

  sol::as_table_t<std::set<std::string>> Shard::RelationshipTypesGetViaLua() {
    return sol::as_table(RelationshipTypesGetPeered());
  }

  // Relationship Type
  std::string Shard::RelationshipTypeGetTypeViaLua(uint16_t type_id) {
    return RelationshipTypeGetTypePeered(type_id);
  }

  uint16_t Shard::RelationshipTypeGetTypeIdViaLua(const std::string& type) {
    return RelationshipTypeGetTypeIdPeered(type);
  }

  uint16_t Shard::RelationshipTypeInsertViaLua(const std::string& type) {
    return RelationshipTypeInsertPeered(type).get0();
  }

  // Node Types
  uint16_t Shard::NodeTypesGetCountViaLua() {
    return NodeTypesGetCountPeered();
  }

  uint64_t Shard::NodeTypesGetCountByTypeViaLua(const std::string& type) {
    return NodeTypesGetCountPeered(type).get0();
  }

  uint64_t Shard::NodeTypesGetCountByIdViaLua(uint16_t type_id) {
    return NodeTypesGetCountPeered(type_id).get0();
  }

  sol::as_table_t<std::set<std::string>> Shard::NodeTypesGetViaLua() {
    return sol::as_table(NodeTypesGetPeered());
  }

  // Node Type
  std::string Shard::NodeTypeGetTypeViaLua(uint16_t type_id) {
    return NodeTypeGetTypePeered(type_id);
  }

  uint16_t Shard::NodeTypeGetTypeIdViaLua(const std::string& type) {
    return NodeTypeGetTypeIdPeered(type);
  }

  uint16_t Shard::NodeTypeInsertViaLua(const std::string& type) {
    return NodeTypeInsertPeered(type).get0();
  }

  //Nodes
  uint64_t Shard::NodeAddEmptyViaLua(const std::string& type, const std::string& key) {
    return NodeAddEmptyPeered(type, key).get0();
  }

  uint64_t Shard::NodeAddViaLua(const std::string& type, const std::string& key, const std::string& properties) {
    return NodeAddPeered(type, key, properties).get0();
  }

  uint64_t Shard::NodeGetIdViaLua(const std::string& type, const std::string& key) {
    return NodeGetIDPeered(type, key).get0();
  }

  Node Shard::NodeGetViaLua(const std::string& type, const std::string& key) {
    return NodeGetPeered(type, key).get0();
  }

  Node Shard::NodeGetByIdViaLua(uint64_t id) {
    return NodeGetPeered(id).get0();
  }

  bool Shard::NodeRemoveViaLua(const std::string& type, const std::string& key) {
    return NodeRemovePeered(type, key).get0();
  }

  bool Shard::NodeRemoveByIdViaLua(uint64_t id) {
    return NodeRemovePeered(id).get0();
  }

  uint16_t Shard::NodeGetTypeIdViaLua(uint64_t id) {
    return NodeGetTypeIdPeered(id).get0();
  }

  std::string Shard::NodeGetTypeViaLua(uint64_t id) {
    return NodeGetTypePeered(id).get0();
  }

  std::string Shard::NodeGetKeyViaLua(uint64_t id) {
    return NodeGetKeyPeered(id).get0();
  }

  // Shard::Node Properties
  sol::object Shard::NodePropertyGetViaLua(const std::string& type, const std::string& key, const std::string& property) {
    std::any value = NodePropertyGetPeered(type, key, property).get0();
    const auto& value_type = value.type();

    if(value_type == typeid(std::string)) {
      return sol::make_object(state.lua_state(), std::any_cast<std::string>(value));
    }

    if(value_type == typeid(int64_t)) {
      return sol::make_object(state.lua_state(), std::any_cast<int64_t>(value));
    }

    if(value_type == typeid(double)) {
      return sol::make_object(state.lua_state(), std::any_cast<double>(value));
    }

    if(value_type == typeid(bool)) {
      return sol::make_object(state.lua_state(), std::any_cast<bool>(value));
    }

    // todo: handle array and object types
    if(value_type == typeid(std::vector<std::string>)) {
      return sol::make_object(state.lua_state(), sol::as_table(std::any_cast<std::vector<std::string>>(value)));
    }

    if(value_type == typeid(std::vector<int64_t>)) {
      return sol::make_object(state.lua_state(), sol::as_table(std::any_cast<std::vector<int64_t>>(value)));
    }

    if(value_type == typeid(std::vector<double>)) {
      return sol::make_object(state.lua_state(), sol::as_table(std::any_cast<std::vector<double>>(value)));
    }

    if(value_type == typeid(std::vector<bool>)) {
      return sol::make_object(state.lua_state(), sol::as_table(std::any_cast<std::vector<bool>>(value)));
    }

    if(value_type == typeid(std::map<std::string, std::string>)) {
      return sol::make_object(state.lua_state(), sol::as_table(std::any_cast<std::map<std::string, std::string>>(value)));
    }

    if(value_type == typeid(std::map<std::string, int64_t>)) {
      return sol::make_object(state.lua_state(), sol::as_table(std::any_cast<std::map<std::string, int64_t>>(value)));
    }

    if(value_type == typeid(std::map<std::string, double>)) {
      return sol::make_object(state.lua_state(), sol::as_table(std::any_cast<std::map<std::string, double>>(value)));
    }

    if(value_type == typeid(std::map<std::string, bool>)) {
      return sol::make_object(state.lua_state(), sol::as_table(std::any_cast<std::map<std::string, bool>>(value)));
    }

    return sol::make_object(state.lua_state(), sol::lua_nil);
  }

  sol::object Shard::NodePropertyGetByIdViaLua(uint64_t id, const std::string& property) {
    std::any value = NodePropertyGetPeered(id, property).get0();
    const auto& value_type = value.type();

    if(value_type == typeid(std::string)) {
      return sol::make_object(state.lua_state(), std::any_cast<std::string>(value));
    }

    if(value_type == typeid(int64_t)) {
      return sol::make_object(state.lua_state(), std::any_cast<int64_t>(value));
    }

    if(value_type == typeid(double)) {
      return sol::make_object(state.lua_state(), std::any_cast<double>(value));
    }

    if(value_type == typeid(bool)) {
      return sol::make_object(state.lua_state(), std::any_cast<bool>(value));
    }

    // todo: handle array and object types
    if(value_type == typeid(std::vector<std::string>)) {
      return sol::make_object(state.lua_state(), sol::as_table(std::any_cast<std::vector<std::string>>(value)));
    }

    if(value_type == typeid(std::vector<int64_t>)) {
      return sol::make_object(state.lua_state(), sol::as_table(std::any_cast<std::vector<int64_t>>(value)));
    }

    if(value_type == typeid(std::vector<double>)) {
      return sol::make_object(state.lua_state(), sol::as_table(std::any_cast<std::vector<double>>(value)));
    }

    if(value_type == typeid(std::vector<bool>)) {
      return sol::make_object(state.lua_state(), sol::as_table(std::any_cast<std::vector<bool>>(value)));
    }

    if(value_type == typeid(std::map<std::string, std::string>)) {
      return sol::make_object(state.lua_state(), sol::as_table(std::any_cast<std::map<std::string, std::string>>(value)));
    }

    if(value_type == typeid(std::map<std::string, int64_t>)) {
      return sol::make_object(state.lua_state(), sol::as_table(std::any_cast<std::map<std::string, int64_t>>(value)));
    }

    if(value_type == typeid(std::map<std::string, double>)) {
      return sol::make_object(state.lua_state(), sol::as_table(std::any_cast<std::map<std::string, double>>(value)));
    }

    if(value_type == typeid(std::map<std::string, bool>)) {
      return sol::make_object(state.lua_state(), sol::as_table(std::any_cast<std::map<std::string, bool>>(value)));
    }

    return sol::make_object(state.lua_state(), sol::lua_nil);
  }

  bool Shard::NodePropertySetViaLua(const std::string& type, const std::string& key, const std::string& property, const sol::object& value) {
    if (value == sol::lua_nil) {
      return false;
    }

    if (value.is<std::string>()) {
      return NodePropertySetPeered(type, key, property, value.as<std::string>()).get0();
    }
    if (value.is<int64_t>()) {
      return NodePropertySetPeered(type, key, property, value.as<int64_t>()).get0();
    }
    if (value.is<double>()) {
      return NodePropertySetPeered(type, key, property, value.as<double>()).get0();
    }
    if (value.is<bool>()) {
      return NodePropertySetPeered(type, key, property, value.as<bool>()).get0();
    }

    return false;
  }

  bool Shard::NodePropertySetByIdViaLua(uint64_t id, const std::string& property, const sol::object& value) {
    if (value == sol::lua_nil) {
      return false;
    }

    if (value.is<std::string>()) {
      return NodePropertySetPeered(id, property, value.as<std::string>()).get0();
    }
    if (value.is<int64_t>()) {
      return NodePropertySetPeered(id, property, value.as<int64_t>()).get0();
    }
    if (value.is<double>()) {
      return NodePropertySetPeered(id, property, value.as<double>()).get0();
    }
    if (value.is<bool>()) {
      return NodePropertySetPeered(id, property, value.as<bool>()).get0();
    }

    return false;
  }

  bool Shard::NodePropertiesSetFromJsonViaLua(const std::string& type, const std::string& key, const std::string& value) {
    return NodePropertiesSetFromJsonPeered(type, key, value).get0();
  }

  bool Shard::NodePropertiesSetFromJsonByIdViaLua(uint64_t id, const std::string& value) {
    return NodePropertiesSetFromJsonPeered(id, value).get0();
  }

  bool Shard::NodePropertiesResetFromJsonViaLua(const std::string& type, const std::string& key, const std::string& value) {
    return NodePropertiesResetFromJsonPeered(type, key, value).get0();
  }

  bool Shard::NodePropertiesResetFromJsonByIdViaLua(uint64_t id, const std::string& value) {
    return NodePropertiesResetFromJsonPeered(id, value).get0();
  }

  bool Shard::NodePropertyDeleteViaLua(const std::string& type, const std::string& key, const std::string& property) {
    return NodePropertyDeletePeered(type, key, property).get0();
  }

  bool Shard::NodePropertyDeleteByIdViaLua(uint64_t id, const std::string& property) {
    return NodePropertyDeletePeered(id, property).get0();
  }

  bool Shard::NodePropertiesDeleteViaLua(const std::string& type, const std::string& key) {
    return NodePropertiesDeletePeered(type, key).get0();
  }

  bool Shard::NodePropertiesDeleteByIdViaLua(uint64_t id) {
    return NodePropertiesDeletePeered(id).get0();
  }

  // Shard::Relationships
  uint64_t Shard::RelationshipAddEmptyViaLua(const std::string& rel_type, const std::string& type1, const std::string& key1,
                                             const std::string& type2, const std::string& key2) {
    return RelationshipAddEmptyPeered(rel_type, type1, key1, type2, key2).get0();
  }

  uint64_t Shard::RelationshipAddEmptyByTypeIdByIdsViaLua(uint16_t rel_type_id, uint64_t id1, uint64_t id2) {
    return RelationshipAddEmptyPeered(rel_type_id, id1, id2).get0();
  }

  uint64_t Shard::RelationshipAddEmptyByIdsViaLua(const std::string& rel_type, uint64_t id1, uint64_t id2) {
    return RelationshipAddEmptyPeered(rel_type, id1, id2).get0();
  }

  uint64_t Shard::RelationshipAddViaLua(const std::string& rel_type, const std::string& type1, const std::string& key1,
                                        const std::string& type2, const std::string& key2, const std::string& properties) {
    return RelationshipAddPeered(rel_type, type1, key1, type2, key2, properties).get0();
  }

  uint64_t Shard::RelationshipAddByTypeIdByIdsViaLua(uint16_t rel_type_id, uint64_t id1, uint64_t id2, const std::string& properties) {
    return RelationshipAddPeered(rel_type_id, id1, id2, properties).get0();
  }

  uint64_t Shard::RelationshipAddByIdsViaLua(const std::string& rel_type, uint64_t id1, uint64_t id2, const std::string& properties) {
    return RelationshipAddPeered(rel_type, id1, id2, properties).get0();
  }

  Relationship Shard::RelationshipGetViaLua(uint64_t id) {
    return RelationshipGetPeered(id).get0();
  }

  bool Shard::RelationshipRemoveViaLua(uint64_t id) {
    return RelationshipRemovePeered(id).get0();
  }

  std::string Shard::RelationshipGetTypeViaLua(uint64_t id) {
    return RelationshipGetTypePeered(id).get0();
  }

  uint16_t Shard::RelationshipGetTypeIdViaLua(uint64_t id) {
    return RelationshipGetTypeIdPeered(id).get0();
  }

  uint64_t Shard::RelationshipGetStartingNodeIdViaLua(uint64_t id) {
    return RelationshipGetStartingNodeIdPeered(id).get0();
  }

  uint64_t Shard::RelationshipGetEndingNodeIdViaLua(uint64_t id) {
    return RelationshipGetEndingNodeIdPeered(id).get0();
  }

  // Shard::Relationship Properties
  sol::object Shard::RelationshipPropertyGetViaLua(uint64_t id, const std::string& property) {
    std::any value = RelationshipPropertyGetPeered(id, property).get0();
    const auto& value_type = value.type();

    if(value_type == typeid(std::string)) {
      return sol::make_object(state.lua_state(), std::any_cast<std::string>(value));
    }

    if(value_type == typeid(int64_t)) {
      return sol::make_object(state.lua_state(), std::any_cast<int64_t>(value));
    }

    if(value_type == typeid(double)) {
      return sol::make_object(state.lua_state(), std::any_cast<double>(value));
    }

    if(value_type == typeid(bool)) {
      return sol::make_object(state.lua_state(), std::any_cast<bool>(value));
    }

    // todo: handle array and object types
    if(value_type == typeid(std::vector<std::string>)) {
      return sol::make_object(state.lua_state(), sol::as_table(std::any_cast<std::vector<std::string>>(value)));
    }

    if(value_type == typeid(std::vector<int64_t>)) {
      return sol::make_object(state.lua_state(), sol::as_table(std::any_cast<std::vector<int64_t>>(value)));
    }

    if(value_type == typeid(std::vector<double>)) {
      return sol::make_object(state.lua_state(), sol::as_table(std::any_cast<std::vector<double>>(value)));
    }

    if(value_type == typeid(std::vector<bool>)) {
      return sol::make_object(state.lua_state(), sol::as_table(std::any_cast<std::vector<bool>>(value)));
    }

    if(value_type == typeid(std::map<std::string, std::string>)) {
      return sol::make_object(state.lua_state(), sol::as_table(std::any_cast<std::map<std::string, std::string>>(value)));
    }

    if(value_type == typeid(std::map<std::string, int64_t>)) {
      return sol::make_object(state.lua_state(), sol::as_table(std::any_cast<std::map<std::string, int64_t>>(value)));
    }

    if(value_type == typeid(std::map<std::string, double>)) {
      return sol::make_object(state.lua_state(), sol::as_table(std::any_cast<std::map<std::string, double>>(value)));
    }

    if(value_type == typeid(std::map<std::string, bool>)) {
      return sol::make_object(state.lua_state(), sol::as_table(std::any_cast<std::map<std::string, bool>>(value)));
    }

    return sol::make_object(state.lua_state(), sol::lua_nil);
  }

  bool Shard::RelationshipPropertySetViaLua(uint64_t id, const std::string& property, const sol::object& value) {
    if (value == sol::lua_nil) {
      return false;
    }

    if (value.is<std::string>()) {
      return RelationshipPropertySetPeered(id, property, value.as<std::string>()).get0();
    }
    if (value.is<int64_t>()) {
      return RelationshipPropertySetPeered(id, property, value.as<int64_t>()).get0();
    }
    if (value.is<double>()) {
      return RelationshipPropertySetPeered(id, property, value.as<double>()).get0();
    }
    if (value.is<bool>()) {
      return RelationshipPropertySetPeered(id, property, value.as<bool>()).get0();
    }

    return false;
  }

  bool Shard::RelationshipPropertySetFromJsonViaLua(uint64_t id, const std::string& property, const std::string& value) {
    return RelationshipPropertySetFromJsonPeered(id, property, value).get0();
  }

  bool Shard::RelationshipPropertyDeleteViaLua(uint64_t id, const std::string& property) {
    return RelationshipPropertyDeletePeered(id, property).get0();
  }

  bool Shard::RelationshipPropertiesSetFromJsonViaLua(uint64_t id, const std::string &value) {
    return RelationshipPropertiesSetFromJsonPeered(id, value).get0();
  }

  bool Shard::RelationshipPropertiesResetFromJsonViaLua(uint64_t id, const std::string &value) {
    return RelationshipPropertiesResetFromJsonPeered(id, value).get0();
  }

  bool Shard::RelationshipPropertiesDeleteViaLua(uint64_t id) {
    return RelationshipPropertiesDeletePeered(id).get0();
  }

  // Shard::Node Degree
  uint64_t Shard::NodeGetDegreeViaLua(const std::string& type, const std::string& key) {
    return NodeGetDegreePeered(type, key).get0();
  }

  uint64_t Shard::NodeGetDegreeForDirectionViaLua(const std::string& type, const std::string& key, Direction direction) {
    return NodeGetDegreePeered(type, key, direction).get0();
  }

  uint64_t Shard::NodeGetDegreeForDirectionForTypeViaLua(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type) {
    return NodeGetDegreePeered(type, key, direction, rel_type).get0();
  }

  uint64_t Shard::NodeGetDegreeForTypeViaLua(const std::string& type, const std::string& key, const std::string& rel_type) {
    return NodeGetDegreePeered(type, key, rel_type).get0();
  }

  uint64_t Shard::NodeGetDegreeForDirectionForTypesViaLua(const std::string& type, const std::string& key, Direction direction,
                                                          const std::vector<std::string>& rel_types) {
    return NodeGetDegreePeered(type, key, direction, rel_types).get0();
  }

  uint64_t Shard::NodeGetDegreeForTypesViaLua(const std::string& type, const std::string& key,
                                              const std::vector<std::string>& rel_types) {
    return NodeGetDegreePeered(type, key, rel_types).get0();
  }

  uint64_t Shard::NodeGetDegreeByIdViaLua(uint64_t id) {
    return NodeGetDegreePeered(id).get0();
  }

  uint64_t Shard::NodeGetDegreeByIdForDirectionViaLua(uint64_t id, Direction direction) {
    return NodeGetDegreePeered(id, direction).get0();
  }

  uint64_t Shard::NodeGetDegreeByIdForDirectionForTypeViaLua(uint64_t id, Direction direction, const std::string& rel_type) {
    return NodeGetDegreePeered(id, direction, rel_type).get0();
  }

  uint64_t Shard::NodeGetDegreeByIdForTypeViaLua(uint64_t id, const std::string& rel_type) {
    return NodeGetDegreePeered(id, rel_type).get0();
  }

  uint64_t Shard::NodeGetDegreeByIdForDirectionForTypesViaLua(uint64_t id, Direction direction, const std::vector<std::string> &rel_types) {
    return NodeGetDegreePeered(id, direction, rel_types).get0();
  }

  uint64_t Shard::NodeGetDegreeByIdForTypesViaLua(uint64_t id, const std::vector<std::string> &rel_types) {
    return NodeGetDegreePeered(id, rel_types).get0();
  }

  // Traversing
  sol::as_table_t<std::vector<Ids>> Shard::NodeGetRelationshipsIdsViaLua(const std::string& type, const std::string& key) {
    return sol::as_table(NodeGetRelationshipsIDsPeered(type, key).get0());
  }

  sol::as_table_t<std::vector<Ids>> Shard::NodeGetRelationshipsIdsForDirectionViaLua(const std::string& type, const std::string& key, Direction direction) {
    return sol::as_table(NodeGetRelationshipsIDsPeered(type, key, direction).get0());
  }

  sol::as_table_t<std::vector<Ids>> Shard::NodeGetRelationshipsIdsForDirectionForTypeViaLua(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type) {
    return sol::as_table(NodeGetRelationshipsIDsPeered(type, key, direction, rel_type).get0());
  }

  sol::as_table_t<std::vector<Ids>> Shard::NodeGetRelationshipsIdsForDirectionForTypeIdViaLua(const std::string& type, const std::string& key, Direction direction, uint16_t type_id) {
    return sol::as_table(NodeGetRelationshipsIDsPeered(type, key, direction, type_id).get0());
  }

  sol::as_table_t<std::vector<Ids>> Shard::NodeGetRelationshipsIdsForDirectionForTypesViaLua(const std::string& type, const std::string& key, Direction direction, const std::vector<std::string> &rel_types) {
    return sol::as_table(NodeGetRelationshipsIDsPeered(type, key, direction, rel_types).get0());
  }

  sol::as_table_t<std::vector<Ids>> Shard::NodeGetRelationshipsIdsForTypeViaLua(const std::string& type, const std::string& key, const std::string& rel_type) {
    return sol::as_table(NodeGetRelationshipsIDsPeered(type, key, rel_type).get0());
  }

  sol::as_table_t<std::vector<Ids>> Shard::NodeGetRelationshipsIdsForTypeIdViaLua(const std::string& type, const std::string& key, uint16_t type_id) {
    return sol::as_table(NodeGetRelationshipsIDsPeered(type, key, type_id).get0());
  }

  sol::as_table_t<std::vector<Ids>> Shard::NodeGetRelationshipsIdsForTypesViaLua(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types) {
    return sol::as_table(NodeGetRelationshipsIDsPeered(type, key, rel_types).get0());
  }

  sol::as_table_t<std::vector<Ids>> Shard::NodeGetRelationshipsIdsByIdViaLua(uint64_t id) {
    return sol::as_table(NodeGetRelationshipsIDsPeered(id).get0());
  }

  sol::as_table_t<std::vector<Ids>> Shard::NodeGetRelationshipsIdsByIdForDirectionViaLua(uint64_t id, Direction direction) {
    return sol::as_table(NodeGetRelationshipsIDsPeered(id, direction).get0());
  }

  sol::as_table_t<std::vector<Ids>> Shard::NodeGetRelationshipsIdsByIdForDirectionForTypeViaLua(uint64_t id, Direction direction, const std::string& rel_type) {
    return sol::as_table(NodeGetRelationshipsIDsPeered(id, direction, rel_type).get0());
  }

  sol::as_table_t<std::vector<Ids>> Shard::NodeGetRelationshipsIdsByIdForDirectionForTypeIdViaLua(uint64_t id, Direction direction, uint16_t type_id) {
    return sol::as_table(NodeGetRelationshipsIDsPeered(id, direction, type_id).get0());
  }

  sol::as_table_t<std::vector<Ids>> Shard::NodeGetRelationshipsIdsByIdForDirectionForTypesViaLua(uint64_t id, Direction direction, const std::vector<std::string> &rel_types) {
    return sol::as_table(NodeGetRelationshipsIDsPeered(id, direction, rel_types).get0());
  }

  sol::as_table_t<std::vector<Ids>> Shard::NodeGetRelationshipsIdsByIdForTypeViaLua(uint64_t id, const std::string& rel_type) {
    return sol::as_table(NodeGetRelationshipsIDsPeered(id, rel_type).get0());
  }

  sol::as_table_t<std::vector<Ids>> Shard::NodeGetRelationshipsIdsByIdForTypeIdViaLua(uint64_t id, uint16_t type_id) {
    return sol::as_table(NodeGetRelationshipsIDsPeered(id, type_id).get0());
  }

  sol::as_table_t<std::vector<Ids>> Shard::NodeGetRelationshipsIdsByIdForTypesViaLua(uint64_t id, const std::vector<std::string> &rel_types) {
    return sol::as_table(NodeGetRelationshipsIDsPeered(id, rel_types).get0());
  }


  sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsViaLua(const std::string& type, const std::string& key) {
    return sol::as_table(NodeGetRelationshipsPeered(type, key).get0());
  }

  sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsForTypeViaLua(const std::string& type, const std::string& key, const std::string& rel_type) {
    return sol::as_table(NodeGetRelationshipsPeered(type, key, rel_type).get0());
  }

  sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsForTypeIdViaLua(const std::string& type, const std::string& key, uint16_t type_id) {
    return sol::as_table(NodeGetRelationshipsPeered(type, key, type_id).get0());
  }

  sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsForTypesViaLua(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types) {
    return sol::as_table(NodeGetRelationshipsPeered(type, key, rel_types).get0());
  }

  sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsByIdViaLua(uint64_t id) {
    return sol::as_table(NodeGetRelationshipsPeered(id).get0());
  }

  sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsByIdForTypeViaLua(uint64_t id, const std::string& rel_type) {
    return sol::as_table(NodeGetRelationshipsPeered(id, rel_type).get0());
  }

  sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsByIdForTypeIdViaLua(uint64_t id, uint16_t type_id) {
    return sol::as_table(NodeGetRelationshipsPeered(id, type_id).get0());
  }

  sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsByIdForTypesViaLua(uint64_t id, const std::vector<std::string> &rel_types) {
    return sol::as_table(NodeGetRelationshipsPeered(id, rel_types).get0());
  }

  sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsForDirectionViaLua(const std::string& type, const std::string& key, Direction direction) {
    return sol::as_table(NodeGetRelationshipsPeered(type, key, direction).get0());
  }

  sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsForDirectionForTypeViaLua(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type) {
    return sol::as_table(NodeGetRelationshipsPeered(type, key, direction, rel_type).get0());
  }

  sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsForDirectionForTypeIdViaLua(const std::string& type, const std::string& key, Direction direction, uint16_t type_id) {
    return sol::as_table(NodeGetRelationshipsPeered(type, key, direction, type_id).get0());
  }

  sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsForDirectionForTypesViaLua(const std::string& type, const std::string& key, Direction direction, const std::vector<std::string> &rel_types) {
    return sol::as_table(NodeGetRelationshipsPeered(type, key, direction, rel_types).get0());
  }

  sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsByIdForDirectionViaLua(uint64_t id, Direction direction) {
    return sol::as_table(NodeGetRelationshipsPeered(id, direction).get0());
  }

  sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsByIdForDirectionForTypeViaLua(uint64_t id, Direction direction, const std::string& rel_type) {
    return sol::as_table(NodeGetRelationshipsPeered(id, direction, rel_type).get0());
  }

  sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsByIdForDirectionForTypeIdViaLua(uint64_t id, Direction direction, uint16_t type_id) {
    return sol::as_table(NodeGetRelationshipsPeered(id, direction, type_id).get0());
  }

  sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsByIdForDirectionForTypesViaLua(uint64_t id, Direction direction, const std::vector<std::string> &rel_types) {
    return sol::as_table(NodeGetRelationshipsPeered(id, direction, rel_types).get0());
  }


  sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsViaLua(const std::string& type, const std::string& key) {
    return sol::as_table(NodeGetNeighborsPeered(type, key).get0());
  }

  sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsForTypeViaLua(const std::string& type, const std::string& key, const std::string& rel_type) {
    return sol::as_table(NodeGetNeighborsPeered(type, key, rel_type).get0());
  }

  sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsForTypeIdViaLua(const std::string& type, const std::string& key, uint16_t type_id) {
    return sol::as_table(NodeGetNeighborsPeered(type, key, type_id).get0());
  }

  sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsForTypesViaLua(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types) {
    return sol::as_table(NodeGetNeighborsPeered(type, key, rel_types).get0());
  }

  sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsByIdViaLua(uint64_t id) {
    return sol::as_table(NodeGetNeighborsPeered(id).get0());
  }

  sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsByIdForTypeViaLua(uint64_t id, const std::string& rel_type) {
    return sol::as_table(NodeGetNeighborsPeered(id, rel_type).get0());
  }

  sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsByIdForTypeIdViaLua(uint64_t id, uint16_t type_id) {
    return sol::as_table(NodeGetNeighborsPeered(id, type_id).get0());
  }

  sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsByIdForTypesViaLua(uint64_t id, const std::vector<std::string> &rel_types) {
    return sol::as_table(NodeGetNeighborsPeered(id, rel_types).get0());
  }

  sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsForDirectionViaLua(const std::string& type, const std::string& key, Direction direction) {
    return sol::as_table(NodeGetNeighborsPeered(type, key, direction).get0());
  }

  sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsForDirectionForTypeViaLua(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type) {
    return sol::as_table(NodeGetNeighborsPeered(type, key, direction, rel_type).get0());
  }

  sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsForDirectionForTypeIdViaLua(const std::string& type, const std::string& key, Direction direction, uint16_t type_id) {
    return sol::as_table(NodeGetNeighborsPeered(type, key, direction, type_id).get0());
  }

  sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsForDirectionForTypesViaLua(const std::string& type, const std::string& key, Direction direction, const std::vector<std::string> &rel_types) {
    return sol::as_table(NodeGetNeighborsPeered(type, key, direction, rel_types).get0());
  }


  sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsByIdForDirectionViaLua(uint64_t id, Direction direction) {
    return sol::as_table(NodeGetNeighborsPeered(id, direction).get0());
  }

  sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsByIdForDirectionForTypeViaLua(uint64_t id, Direction direction, const std::string& rel_type) {
    return sol::as_table(NodeGetNeighborsPeered(id, direction, rel_type).get0());
  }

  sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsByIdForDirectionForTypeIdViaLua(uint64_t id, Direction direction, uint16_t type_id) {
    return sol::as_table(NodeGetNeighborsPeered(id, direction, type_id).get0());
  }

  sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsByIdForDirectionForTypesViaLua(uint64_t id, Direction direction, const std::vector<std::string> &rel_types) {
    return sol::as_table(NodeGetNeighborsPeered(id, direction, rel_types).get0());
  }

  // All
  sol::as_table_t<std::vector<uint64_t>> Shard::AllNodeIdsViaLua(uint64_t skip, uint64_t limit) {
    return sol::as_table(AllNodeIdsPeered(skip, limit).get0());
  }

  sol::as_table_t<std::vector<uint64_t>> Shard::AllNodeIdsForTypeViaLua(const std::string& type, uint64_t skip, uint64_t limit) {
    return sol::as_table(AllNodeIdsPeered(type, skip, limit).get0());
  }

  sol::as_table_t<std::vector<uint64_t>> Shard::AllRelationshipIdsViaLua(uint64_t skip, uint64_t limit) {
    return sol::as_table(AllRelationshipIdsPeered(skip, limit).get0());
  }

  sol::as_table_t<std::vector<uint64_t>> Shard::AllRelationshipIdsForTypeViaLua(const std::string& rel_type, uint64_t skip, uint64_t limit) {
    return sol::as_table(AllRelationshipIdsPeered(rel_type, skip, limit).get0());
  }

  sol::as_table_t<std::vector<Node>> Shard::AllNodesViaLua(uint64_t skip, uint64_t limit) {
    return sol::as_table(AllNodesPeered(skip, limit).get0());
  }

  sol::as_table_t<std::vector<Node>> Shard::AllNodesForTypeViaLua(const std::string& type, uint64_t skip, uint64_t limit) {
    return sol::as_table(AllNodesPeered(type, skip, limit).get0());
  }

  sol::as_table_t<std::vector<Relationship>> Shard::AllRelationshipsViaLua(uint64_t skip, uint64_t limit) {
    return sol::as_table(AllRelationshipsPeered(skip, limit).get0());
  }

  sol::as_table_t<std::vector<Relationship>> Shard::AllRelationshipsForTypeViaLua(const std::string& rel_type, uint64_t skip, uint64_t limit) {
    return sol::as_table(AllRelationshipsPeered(skip, limit).get0());
  }

} // namespace triton