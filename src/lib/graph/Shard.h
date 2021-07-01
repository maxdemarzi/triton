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

#ifndef TRITON_SHARD_H
#define TRITON_SHARD_H

#define SOL_ALL_SAFETIES_ON 1

#include "Direction.h"
#include "Ids.h"
#include "Node.h"
#include "Relationship.h"
#include "Types.h"
#include "Group.h"
#include <roaring/roaring64map.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/thread.hh>
#include <simdjson.h>
#include <simdjson/dom/object.h>
#include <tsl/sparse_map.h>
#include <sol.hpp>
#include <utilities/csvmonkey.hpp>
#include <seastar/core/seastar.hh>

using namespace simdjson;

namespace triton {
  class Shard : public seastar::peering_sharded_service<Shard> {

  private:
    uint8_t cpus;
    uint8_t shard_id;
    dom::parser parser;
    sol::state state;
    seastar::sstring command_log_file_name;

    seastar::rwlock rel_type_lock;
    seastar::rwlock node_type_lock;
    seastar::rwlock lua_lock;

    std::map<std::string, tsl::sparse_map<std::string, uint64_t>> node_keys;// "Index" to get node id by type:key
    std::vector<triton::Node> nodes;// Store of the properties of Nodes
    std::vector<triton::Relationship> relationships;// Store of the properties of Relationships
    std::vector<std::vector<Group>> outgoing_relationships;// Outgoing relationships of each node
    std::vector<std::vector<Group>> incoming_relationships;// Incoming relationships of each node
    Roaring64Map deleted_nodes;// Keep track of deleted nodes in order to reuse them
    Roaring64Map deleted_relationships;// Keep track of deleted relationships in order to reuse them
    triton::Types node_types;// Store string and id of node types
    triton::Types relationship_types;// Store string and id of relationship types

    // Tombstone Property values
    const std::any tombstone_any = std::any();
    const std::string tombstone_string = std::string("");
    const int64_t tombstone_int = std::numeric_limits<int64_t>::min();
    const double tombstone_double = std::numeric_limits<double>::min();
    const bool tombstone_boolean = false;
    const std::map<std::string, std::any> tombstone_object = std::map<std::string, std::any>();

    inline static const uint64_t SKIP = 0;
    inline static const uint64_t LIMIT = 100;

  public:
    explicit Shard(uint8_t cpus) : cpus(cpus), shard_id(seastar::this_shard_id()) {
      command_log_file_name = "command_" + std::to_string(shard_id) + ".log";

      // Always start with node and relationship Zero and use unsigned integers for ids except 0.
      nodes.emplace_back();
      Relationship relationship = {0,0,0,0,std::map<std::string, std::any>()};
      relationships.push_back(relationship);
      outgoing_relationships.emplace_back();
      incoming_relationships.emplace_back();

      state.open_libraries(sol::lib::base, sol::lib::package, sol::lib::math, sol::lib::string, sol::lib::table);
      state.require_file("json", "./src/lua/json.lua");

      // todo: Create a sanitized environment to sandbox the user, and put these user types there.

      state.new_usertype<Node>("Node",
        // 3 constructors
                                                               sol::constructors<
                                                                 Node(),
                                                                 Node(uint64_t, uint16_t, std::string),
                                                                 Node(uint64_t, uint16_t, std::string, std::map<std::string, std::any>)>(),
                                                               "getId", &Node::getId,
                                                               "getTypeId", &Node::getTypeId,
                                                               "getKey", &Node::getKey,
                                                               "getProperties", &Node::getPropertiesLua,
                                                               "setProperty", &Node::setProperty,
                                                               "deleteProperty", &Node::deleteProperty,
                                                               "setProperties", &Node::setProperties,
                                                               "deleteProperties", &Node::deleteProperties);


      state.new_usertype<Relationship>("Relationship",
        // 3 constructors
                                                                                       sol::constructors<
                                                                                         Relationship(),
                                                                                         Relationship(uint64_t, uint64_t, uint64_t, uint16_t),
                                                                                         Relationship(uint64_t, uint64_t, uint64_t, uint16_t, std::map<std::string, std::any>)>(),
                                                                                       "getId", &Relationship::getId,
                                                                                       "getTypeId", &Relationship::getTypeId,
                                                                                       "getStartingNodeId", &Relationship::getStartingNodeId,
                                                                                       "getEndingNodeId", &Relationship::getEndingNodeId,
                                                                                       "getProperties", &Relationship::getPropertiesLua,
                                                                                       "setProperty", &Relationship::setProperty,
                                                                                       "deleteProperty", &Relationship::deleteProperty,
                                                                                       "setProperties", &Relationship::setProperties,
                                                                                       "deleteProperties", &Relationship::deleteProperties);

      state.new_usertype<Ids>("Ids",
                                                            sol::constructors<Ids(uint64_t, uint64_t)>(),
                                                            "node_id", &Ids::node_id,
                                                            "rel_id", &Ids::rel_id);

      state.set_function("ShardIdsGet", &Shard::ShardIdsGet, this);
      // Lua does not like overloading, Sol warns about performance problems if we overload, so overloaded methods have been renamed.

      // Relationship Types
      state.set_function("RelationshipTypesGetCount", &Shard::RelationshipTypesGetCountViaLua, this);
      state.set_function("RelationshipTypesGetCountByType", &Shard::RelationshipTypesGetCountByTypeViaLua, this);
      state.set_function("RelationshipTypesGetCountById", &Shard::RelationshipTypesGetCountByIdViaLua, this);
      state.set_function("RelationshipTypesGet", &Shard::RelationshipTypesGetViaLua, this);

      // Relationship Type
      state.set_function("RelationshipTypeGetType", &Shard::RelationshipTypeGetTypeViaLua, this);
      state.set_function("RelationshipTypeGetTypeId", &Shard::RelationshipTypeGetTypeIdViaLua, this);
      state.set_function("RelationshipTypeInsert", &Shard::RelationshipTypeInsertViaLua, this);

      // Node Types
      state.set_function("NodeTypesGetCount", &Shard::NodeTypesGetCountViaLua, this);
      state.set_function("NodeTypesGetCountByType", &Shard::NodeTypesGetCountByTypeViaLua, this);
      state.set_function("NodeTypesGetCountById", &Shard::NodeTypesGetCountByIdViaLua, this);
      state.set_function("NodeTypesGet", &Shard::NodeTypesGetViaLua, this);

      // Node Type
      state.set_function("NodeTypeGetType", &Shard::NodeTypeGetTypeViaLua, this);
      state.set_function("NodeTypeGetTypeId", &Shard::NodeTypeGetTypeIdViaLua, this);
      state.set_function("NodeTypeInsert", &Shard::NodeTypeInsertViaLua, this);

      //Nodes
      state.set_function("NodeAddEmpty", &Shard::NodeAddEmptyViaLua, this);
      state.set_function("NodeAdd", &Shard::NodeAddViaLua, this);
      state.set_function("NodeGetId", &Shard::NodeGetIdViaLua, this);
      state.set_function("NodeGet", &Shard::NodeGetViaLua, this);
      state.set_function("NodeGetById", &Shard::NodeGetByIdViaLua, this);
      state.set_function("NodeRemove", &Shard::NodeRemoveViaLua, this);
      state.set_function("NodeRemoveById", &Shard::NodeRemoveByIdViaLua, this);
      state.set_function("NodeGetTypeId", &Shard::NodeGetTypeIdViaLua, this);
      state.set_function("NodeGetType", &Shard::NodeGetTypeViaLua, this);
      state.set_function("NodeGetKey", &Shard::NodeGetKeyViaLua, this);

      // Node Properties
      state.set_function("NodePropertyGet", &Shard::NodePropertyGetViaLua, this);
      state.set_function("NodePropertyGetById", &Shard::NodePropertyGetByIdViaLua, this);
      state.set_function("NodePropertySet", &Shard::NodePropertySetViaLua, this);
      state.set_function("NodePropertySetById", &Shard::NodePropertySetByIdViaLua, this);
      state.set_function("NodePropertiesSetFromJson", &Shard::NodePropertiesSetFromJsonViaLua, this);
      state.set_function("NodePropertiesSetFromJsonById", &Shard::NodePropertiesSetFromJsonByIdViaLua, this);
      state.set_function("NodePropertiesResetFromJson", &Shard::NodePropertiesResetFromJsonViaLua, this);
      state.set_function("NodePropertiesResetFromJsonById", &Shard::NodePropertiesResetFromJsonByIdViaLua, this);
      state.set_function("NodePropertyDelete", &Shard::NodePropertyDeleteViaLua, this);
      state.set_function("NodePropertyDeleteById", &Shard::NodePropertyDeleteByIdViaLua, this);
      state.set_function("NodePropertiesDelete", &Shard::NodePropertiesDeleteViaLua, this);
      state.set_function("NodePropertiesDeleteById", &Shard::NodePropertiesDeleteByIdViaLua, this);

      // Relationships
      state.set_function("RelationshipAddEmpty", &Shard::RelationshipAddEmptyViaLua, this);
      state.set_function("RelationshipAddEmptyByTypeIdByIds", &Shard::RelationshipAddEmptyByTypeIdByIdsViaLua, this);
      state.set_function("RelationshipAddEmptyByIds", &Shard::RelationshipAddEmptyByIdsViaLua, this);
      state.set_function("RelationshipAdd", &Shard::RelationshipAddViaLua, this);
      state.set_function("RelationshipAddByTypeIdByIds", &Shard::RelationshipAddByTypeIdByIdsViaLua, this);
      state.set_function("RelationshipAddByIds", &Shard::RelationshipAddByIdsViaLua, this);
      state.set_function("RelationshipGet", &Shard::RelationshipGetViaLua, this);
      state.set_function("RelationshipRemove", &Shard::RelationshipRemoveViaLua, this);
      state.set_function("RelationshipGetType", &Shard::RelationshipGetTypeViaLua, this);
      state.set_function("RelationshipGetTypeId", &Shard::RelationshipGetTypeIdViaLua, this);
      state.set_function("RelationshipGetStartingNodeId", &Shard::RelationshipGetStartingNodeIdViaLua, this);
      state.set_function("RelationshipGetEndingNodeId", &Shard::RelationshipGetEndingNodeIdViaLua, this);

      // Relationship Properties
      state.set_function("RelationshipPropertyGet", &Shard::RelationshipPropertyGetViaLua, this);
      state.set_function("RelationshipPropertySet", &Shard::RelationshipPropertySetViaLua, this);
      state.set_function("RelationshipPropertySetFromJson", &Shard::RelationshipPropertySetFromJsonViaLua, this);
      state.set_function("RelationshipPropertyDelete", &Shard::RelationshipPropertyDeleteViaLua, this);
      state.set_function("RelationshipPropertiesSetFromJson", &Shard::RelationshipPropertiesSetFromJsonViaLua, this);
      state.set_function("RelationshipPropertiesResetFromJson", &Shard::RelationshipPropertiesResetFromJsonViaLua, this);
      state.set_function("RelationshipPropertiesDelete", &Shard::RelationshipPropertiesDeleteViaLua, this);

      // Node Degree
      state.set_function("NodeGetDegree", &Shard::NodeGetDegreeViaLua, this);
      state.set_function("NodeGetDegreeForDirection", &Shard::NodeGetDegreeForDirectionViaLua, this);
      state.set_function("NodeGetDegreeForDirectionForType", &Shard::NodeGetDegreeForDirectionForTypeViaLua, this);
      state.set_function("NodeGetDegreeForType", &Shard::NodeGetDegreeForTypeViaLua, this);
      state.set_function("NodeGetDegreeForDirectionForTypes", &Shard::NodeGetDegreeForDirectionForTypesViaLua, this);
      state.set_function("NodeGetDegreeForTypes", &Shard::NodeGetDegreeForTypesViaLua, this);
      state.set_function("NodeGetDegreeById", &Shard::NodeGetDegreeByIdViaLua, this);
      state.set_function("NodeGetDegreeByIdForDirection", &Shard::NodeGetDegreeByIdForDirectionViaLua, this);
      state.set_function("NodeGetDegreeByIdForDirectionForType", &Shard::NodeGetDegreeByIdForDirectionForTypeViaLua, this);
      state.set_function("NodeGetDegreeByIdForType", &Shard::NodeGetDegreeByIdForTypeViaLua, this);
      state.set_function("NodeGetDegreeByIdForDirectionForTypes", &Shard::NodeGetDegreeByIdForDirectionForTypesViaLua, this);
      state.set_function("NodeGetDegreeByIdForTypes", &Shard::NodeGetDegreeByIdForTypesViaLua, this);

      // Traversing
      state.set_function("NodeGetRelationshipsIds", &Shard::NodeGetRelationshipsIdsViaLua, this);
      state.set_function("NodeGetRelationshipsIdsForDirection", &Shard::NodeGetRelationshipsIdsForDirectionViaLua, this);
      state.set_function("NodeGetRelationshipsIdsForDirectionForType", &Shard::NodeGetRelationshipsIdsForDirectionForTypeViaLua, this);
      state.set_function("NodeGetRelationshipsIdsForDirectionForTypeId", &Shard::NodeGetRelationshipsIdsForDirectionForTypeIdViaLua, this);
      state.set_function("NodeGetRelationshipsIdsForDirectionForTypes", &Shard::NodeGetRelationshipsIdsForDirectionForTypesViaLua, this);
      state.set_function("NodeGetRelationshipsIdsForType", &Shard::NodeGetRelationshipsIdsForTypeViaLua, this);
      state.set_function("NodeGetRelationshipsIdsForTypeId", &Shard::NodeGetRelationshipsIdsForTypeIdViaLua, this);
      state.set_function("NodeGetRelationshipsIdsForTypes", &Shard::NodeGetRelationshipsIdsForTypesViaLua, this);
      state.set_function("NodeGetRelationshipsIdsById", &Shard::NodeGetRelationshipsIdsByIdViaLua, this);
      state.set_function("NodeGetRelationshipsIdsByIdForDirection", &Shard::NodeGetRelationshipsIdsByIdForDirectionViaLua, this);
      state.set_function("NodeGetRelationshipsIdsByIdForDirectionForType", &Shard::NodeGetRelationshipsIdsByIdForDirectionForTypeViaLua, this);
      state.set_function("NodeGetRelationshipsIdsByIdForDirectionForTypeId", &Shard::NodeGetRelationshipsIdsByIdForDirectionForTypeIdViaLua, this);
      state.set_function("NodeGetRelationshipsIdsByIdForDirectionForTypes", &Shard::NodeGetRelationshipsIdsByIdForDirectionForTypesViaLua, this);
      state.set_function("NodeGetRelationshipsIdsByIdForType", &Shard::NodeGetRelationshipsIdsByIdForTypeViaLua, this);
      state.set_function("NodeGetRelationshipsIdsByIdForTypeId", &Shard::NodeGetRelationshipsIdsByIdForTypeIdViaLua, this);
      state.set_function("NodeGetRelationshipsIdsByIdForTypes", &Shard::NodeGetRelationshipsIdsByIdForTypesViaLua, this);

      state.set_function("NodeGetRelationships", &Shard::NodeGetRelationshipsViaLua, this);
      state.set_function("NodeGetRelationshipsForType", &Shard::NodeGetRelationshipsForTypeViaLua, this);
      state.set_function("NodeGetRelationshipsForTypeId", &Shard::NodeGetRelationshipsForTypeIdViaLua, this);
      state.set_function("NodeGetRelationshipsForTypes", &Shard::NodeGetRelationshipsForTypesViaLua, this);
      state.set_function("NodeGetRelationshipsById", &Shard::NodeGetRelationshipsByIdViaLua, this);
      state.set_function("NodeGetRelationshipsByIdForType", &Shard::NodeGetRelationshipsByIdForTypeViaLua, this);
      state.set_function("NodeGetRelationshipsByIdForTypeId", &Shard::NodeGetRelationshipsByIdForTypeIdViaLua, this);
      state.set_function("NodeGetRelationshipsByIdForTypes", &Shard::NodeGetRelationshipsByIdForTypesViaLua, this);
      state.set_function("NodeGetRelationshipsForDirection", &Shard::NodeGetRelationshipsForDirectionViaLua, this);
      state.set_function("NodeGetRelationshipsForDirectionForType", &Shard::NodeGetRelationshipsForDirectionForTypeViaLua, this);
      state.set_function("NodeGetRelationshipsForDirectionForTypeId", &Shard::NodeGetRelationshipsForDirectionForTypeIdViaLua, this);
      state.set_function("NodeGetRelationshipsForDirectionForTypes", &Shard::NodeGetRelationshipsForDirectionForTypesViaLua, this);
      state.set_function("NodeGetRelationshipsByIdForDirection", &Shard::NodeGetRelationshipsByIdForDirectionViaLua, this);
      state.set_function("NodeGetRelationshipsByIdForDirectionForType", &Shard::NodeGetRelationshipsByIdForDirectionForTypeViaLua, this);
      state.set_function("NodeGetRelationshipsByIdForDirectionForTypeId", &Shard::NodeGetRelationshipsByIdForDirectionForTypeIdViaLua, this);
      state.set_function("NodeGetRelationshipsByIdForDirectionForTypes", &Shard::NodeGetRelationshipsByIdForDirectionForTypesViaLua, this);

      state.set_function("NodeGetNeighbors", &Shard::NodeGetNeighborsViaLua, this);
      state.set_function("NodeGetNeighborsForType", &Shard::NodeGetNeighborsForTypeViaLua, this);
      state.set_function("NodeGetNeighborsForTypeId", &Shard::NodeGetNeighborsForTypeIdViaLua, this);
      state.set_function("NodeGetNeighborsForTypes", &Shard::NodeGetNeighborsForTypesViaLua, this);
      state.set_function("NodeGetNeighborsById", &Shard::NodeGetNeighborsByIdViaLua, this);
      state.set_function("NodeGetNeighborsByIdForType", &Shard::NodeGetNeighborsByIdForTypeViaLua, this);
      state.set_function("NodeGetNeighborsByIdForTypeId", &Shard::NodeGetNeighborsByIdForTypeIdViaLua, this);
      state.set_function("NodeGetNeighborsByIdForTypes", &Shard::NodeGetNeighborsByIdForTypesViaLua, this);
      state.set_function("NodeGetNeighborsForDirection", &Shard::NodeGetNeighborsForDirectionViaLua, this);
      state.set_function("NodeGetNeighborsForDirectionForType", &Shard::NodeGetNeighborsForDirectionForTypeViaLua, this);
      state.set_function("NodeGetNeighborsForDirectionForTypeId", &Shard::NodeGetNeighborsForDirectionForTypeIdViaLua, this);
      state.set_function("NodeGetNeighborsForDirectionForTypes", &Shard::NodeGetNeighborsForDirectionForTypesViaLua, this);
      state.set_function("NodeGetNeighborsByIdForDirection", &Shard::NodeGetNeighborsByIdForDirectionViaLua, this);
      state.set_function("NodeGetNeighborsByIdForDirectionForType", &Shard::NodeGetNeighborsByIdForDirectionForTypeViaLua, this);
      state.set_function("NodeGetNeighborsByIdForDirectionForTypeId", &Shard::NodeGetNeighborsByIdForDirectionForTypeIdViaLua, this);
      state.set_function("NodeGetNeighborsByIdForDirectionForTypes", &Shard::NodeGetNeighborsByIdForDirectionForTypesViaLua, this);

      state.set_function("AllNodeIds", &Shard::AllNodeIdsViaLua, this);
      state.set_function("AllNodeIdsForType", &Shard::AllNodeIdsForTypeViaLua, this);
      state.set_function("AllRelationshipIds", &Shard::AllRelationshipIdsViaLua, this);
      state.set_function("AllRelationshipIdsForType", &Shard::AllRelationshipIdsForTypeViaLua, this);
      state.set_function("AllNodes", &Shard::AllNodesViaLua, this);
      state.set_function("AllNodesForType", &Shard::AllNodesForTypeViaLua, this);
      state.set_function("AllRelationships", &Shard::AllRelationshipsViaLua, this);
      state.set_function("AllRelationshipsForType", &Shard::AllRelationshipsForTypeViaLua, this);
    }

    static void speak();
    static seastar::future<> stop();
    void clear();
    void reserve(uint64_t reserved_nodes, uint64_t reserved_relationships);

    seastar::future<uint8_t> getShardId();
    seastar::future<std::vector<uint8_t>> getShardIds();


    sol::as_table_t<std::vector<std::uint8_t>> ShardIdsGet();

    // Lua
    seastar::future<std::string> RunLua(const std::string &script);

    // Ids
    uint64_t internalToExternal(uint64_t internal_id) const;
    static uint64_t externalToInternal(uint64_t id);
    static uint8_t CalculateShardId(uint64_t id);
    uint8_t CalculateShardId(const std::string &type, const std::string &key) const;

    // Relationship Types
    uint16_t RelationshipTypesGetCount();
    uint64_t RelationshipTypesGetCount(uint16_t type_id);
    uint64_t RelationshipTypesGetCount(const std::string& type);
    std::set<std::string> RelationshipTypesGet();

    // Relationship Type
    std::string RelationshipTypeGetType(uint16_t type_id);
    uint16_t RelationshipTypeGetTypeId(const std::string& type);
    bool RelationshipTypeInsert(const std::string& type, uint16_t type_id);

    // Node Types
    uint16_t NodeTypesGetCount();
    uint64_t NodeTypesGetCount(uint16_t type_id);
    uint64_t NodeTypesGetCount(const std::string& type);
    std::set<std::string> NodeTypesGet();

    // Node Type
    std::string NodeTypeGetType(uint16_t type_id);
    uint16_t NodeTypeGetTypeId(const std::string& type);
    bool NodeTypeInsert(const std::string& type, uint16_t type_id);

    // Helpers
    std::map<uint16_t, std::map<uint16_t, std::vector<uint64_t>>> NodeRemoveGetIncoming(uint64_t internal_id);
    bool NodeRemoveDeleteIncoming(uint64_t id, const std::map<uint16_t, std::vector<uint64_t>>&grouped_relationships);
    std::map<uint16_t, std::map<uint16_t, std::vector<uint64_t>>> NodeRemoveGetOutgoing(uint64_t internal_id);
    bool NodeRemoveDeleteOutgoing(uint64_t id, const std::map<uint16_t, std::vector<uint64_t>>&grouped_relationships);
    std::pair <uint16_t ,uint64_t> RelationshipRemoveGetIncoming(uint64_t internal_id);
    bool RelationshipRemoveIncoming(uint16_t rel_type_id, uint64_t external_id, uint64_t node_id);

    // Nodes
    uint64_t NodeAddEmpty(const std::string& type, uint16_t type_id, const std::string& key);
    uint64_t NodeAdd(const std::string& type, uint16_t type_id, const std::string& key, const std::string& properties);
    uint64_t NodeGetID(const std::string& type, const std::string& key);
    Node NodeGet(uint64_t id);
    Node NodeGet(const std::string& type, const std::string& key);
    bool NodeRemove(uint64_t id);
    bool NodeRemove(const std::string& type, const std::string& key);
    uint16_t NodeGetTypeId(uint64_t id);
    std::string NodeGetType(uint64_t id);
    std::string NodeGetKey(uint64_t id);

    // Node Properties
    std::any NodePropertyGet(const std::string& type, const std::string& key, const std::string& property);
    std::string NodePropertyGetString(const std::string& type, const std::string& key, const std::string& property);
    int64_t NodePropertyGetInteger(const std::string& type, const std::string& key, const std::string& property);
    double NodePropertyGetDouble(const std::string& type, const std::string& key, const std::string& property);
    bool NodePropertyGetBoolean(const std::string& type, const std::string& key, const std::string &property);
    std::map<std::string, std::any> NodePropertyGetObject(const std::string& type, const std::string& key, const std::string& property);

    std::any NodePropertyGet(uint64_t id, const std::string& property);
    std::string NodePropertyGetString(uint64_t id, const std::string& property);
    int64_t NodePropertyGetInteger(uint64_t id, const std::string& property);
    double NodePropertyGetDouble(uint64_t id, const std::string& property);
    bool NodePropertyGetBoolean(uint64_t id, const std::string &property);
    std::map<std::string, std::any> NodePropertyGetObject(uint64_t id, const std::string& property);
    bool NodePropertySet(const std::string& type, const std::string& key, const std::string& property, std::string value);

    // Deal with string literal being confused for a boolean
    bool NodePropertySet(const std::string& type, const std::string& key, const std::string& property, const char* value);
    bool NodePropertySet(const std::string& type, const std::string& key, const std::string& property, int64_t value);
    bool NodePropertySet(const std::string& type, const std::string& key, const std::string& property, double value);
    bool NodePropertySet(const std::string& type, const std::string& key, const std::string& property, bool value);
    bool NodePropertySet(const std::string& type, const std::string& key, const std::string& property, std::map<std::string, std::any> value);
    bool NodePropertySetFromJson(const std::string& type, const std::string& key, const std::string& property, const std::string& value);
    bool NodePropertySet(uint64_t id, const std::string& property, std::string value);

    std::map<std::string, std::any> NodePropertiesGet(const std::string& type, const std::string& key);
    bool NodePropertiesSet(const std::string& type, const std::string& key, std::map<std::string, std::any> &value);
    bool NodePropertiesSetFromJson(const std::string& type, const std::string& key, const std::string& value);
    bool NodePropertiesReset(const std::string& type, const std::string& key, const std::map<std::string, std::any> &value);
    bool NodePropertiesResetFromJson(const std::string& type, const std::string& key, const std::string& value);
    bool NodePropertiesDelete(const std::string& type, const std::string& key);

    // Deal with string literal being confused for a boolean
    bool NodePropertySet(uint64_t id, const std::string& property, const char* value);
    bool NodePropertySet(uint64_t id, const std::string& property, int64_t value);
    bool NodePropertySet(uint64_t id, const std::string& property, double value);
    bool NodePropertySet(uint64_t id, const std::string& property, bool value);
    bool NodePropertySet(uint64_t id, const std::string& property, std::map<std::string, std::any> value);
    bool NodePropertySetFromJson(uint64_t id, const std::string& property, const std::string& value);
    bool NodePropertyDelete(const std::string& type, const std::string& key, const std::string& property);
    bool NodePropertyDelete(uint64_t id, const std::string& property);

    std::map<std::string, std::any> NodePropertiesGet(uint64_t id);
    bool NodePropertiesSet(uint64_t id, std::map<std::string, std::any> &value);
    bool NodePropertiesSetFromJson(uint64_t id, const std::string& value);
    bool NodePropertiesReset(uint64_t id, const std::map<std::string, std::any> &value);
    bool NodePropertiesResetFromJson(uint64_t id, const std::string& value);
    bool NodePropertiesDelete(uint64_t id);

    // Relationships
    uint64_t RelationshipAddEmptySameShard(uint16_t rel_type, uint64_t id1, uint64_t id2);
    uint64_t RelationshipAddEmptySameShard(uint16_t rel_type, const std::string& type1, const std::string& key1,
                                                            const std::string& type2, const std::string& key2);
    uint64_t RelationshipAddEmptyToOutgoing(uint16_t rel_type, uint64_t id1, uint64_t id2);
    uint64_t RelationshipAddToIncoming(uint16_t rel_type, uint64_t rel_id, uint64_t id1, uint64_t id2);

    uint64_t RelationshipAddSameShard(uint16_t rel_type, uint64_t id1, uint64_t id2, const std::string& properties);
    uint64_t RelationshipAddSameShard(uint16_t rel_type, const std::string& type1, const std::string& key1,
                                           const std::string& type2, const std::string& key2, const std::string& properties);
    uint64_t RelationshipAddToOutgoing(uint16_t rel_type, uint64_t id1, uint64_t id2, const std::string& properties);

    Relationship RelationshipGet(uint64_t rel_id);
    std::string RelationshipGetType(uint64_t id);
    uint16_t RelationshipGetTypeId(uint64_t id);
    uint64_t RelationshipGetStartingNodeId(uint64_t id);
    uint64_t RelationshipGetEndingNodeId(uint64_t id);

    // Relationship Properties
    std::any RelationshipPropertyGet(uint64_t id, const std::string& property);
    std::string RelationshipPropertyGetString(uint64_t id, const std::string& property);
    int64_t RelationshipPropertyGetInteger(uint64_t id, const std::string& property);
    double RelationshipPropertyGetDouble(uint64_t id, const std::string& property);
    bool RelationshipPropertyGetBoolean(uint64_t id, const std::string &property);
    std::map<std::string, std::any> RelationshipPropertyGetObject(uint64_t id, const std::string& property);
    bool RelationshipPropertySet(uint64_t id, const std::string& property, std::string value);

    // Deal with string literal being confused for a boolean
    bool RelationshipPropertySet(uint64_t id, const std::string& property, const char* value);
    bool RelationshipPropertySet(uint64_t id, const std::string& property, int64_t value);
    bool RelationshipPropertySet(uint64_t id, const std::string& property, double value);
    bool RelationshipPropertySet(uint64_t id, const std::string& property, bool value);
    bool RelationshipPropertySet(uint64_t id, const std::string& property, std::map<std::string, std::any> value);
    bool RelationshipPropertySetFromJson(uint64_t id, const std::string& property, const std::string& value);
    bool RelationshipPropertyDelete(uint64_t id, const std::string& property);

    std::map<std::string, std::any> RelationshipPropertiesGet(uint64_t id);
    bool RelationshipPropertiesSet(uint64_t id, std::map<std::string, std::any> &value);
    bool RelationshipPropertiesSetFromJson(uint64_t id, const std::string &value);
    bool RelationshipPropertiesReset(uint64_t id, const std::map<std::string, std::any> &value);
    bool RelationshipPropertiesResetFromJson(uint64_t id, const std::string &value);
    bool RelationshipPropertiesDelete(uint64_t id);

    // Node Degree
    uint64_t NodeGetDegree(const std::string& type, const std::string& key);
    uint64_t NodeGetDegree(const std::string& type, const std::string& key, Direction direction);
    uint64_t NodeGetDegree(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type);
    uint64_t NodeGetDegree(const std::string& type, const std::string& key, Direction direction,
                           const std::vector<std::string>& rel_types);
    uint64_t NodeGetDegree(uint64_t id);
    uint64_t NodeGetDegree(uint64_t id, Direction direction);
    uint64_t NodeGetDegree(uint64_t id, Direction direction, const std::string& rel_type);
    uint64_t NodeGetDegree(uint64_t id, Direction direction, const std::vector<std::string> &rel_types);

    // Traversing
    std::vector<Ids> NodeGetRelationshipsIDs(const std::string& type, const std::string& key);
    std::vector<Ids> NodeGetRelationshipsIDs(const std::string& type, const std::string& key, Direction direction);
    std::vector<Ids> NodeGetRelationshipsIDs(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type);
    std::vector<Ids> NodeGetRelationshipsIDs(const std::string& type, const std::string& key, Direction direction, uint16_t type_id);
    std::vector<Ids> NodeGetRelationshipsIDs(const std::string& type, const std::string& key, Direction direction, const std::vector<std::string> &rel_types);
    std::vector<Ids> NodeGetRelationshipsIDs(uint64_t id);
    std::vector<Ids> NodeGetRelationshipsIDs(uint64_t id, Direction direction);
    std::vector<Ids> NodeGetRelationshipsIDs(uint64_t id, Direction direction, const std::string& rel_type);
    std::vector<Ids> NodeGetRelationshipsIDs(uint64_t id, Direction direction, uint16_t type_id);
    std::vector<Ids> NodeGetRelationshipsIDs(uint64_t id, Direction direction, const std::vector<std::string> &rel_types);

    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedRelationshipIDs(const std::string& type, const std::string& key);
    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedRelationshipIDs(const std::string& type, const std::string& key, const std::string& rel_type);
    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedRelationshipIDs(const std::string& type, const std::string& key, uint16_t type_id);
    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedRelationshipIDs(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types);

    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedRelationshipIDs(uint64_t id);
    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedRelationshipIDs(uint64_t id, const std::string& rel_type);
    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedRelationshipIDs(uint64_t id, uint16_t type_id);
    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedRelationshipIDs(uint64_t id, const std::vector<std::string> &rel_types);

    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedNodeIDs(const std::string& type, const std::string& key);
    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedNodeIDs(const std::string& type, const std::string& key, const std::string& rel_type);
    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedNodeIDs(const std::string& type, const std::string& key, uint16_t type_id);
    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedNodeIDs(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types);

    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedNodeIDs(uint64_t id);
    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedNodeIDs(uint64_t id, const std::string& rel_type);
    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedNodeIDs(uint64_t id, uint16_t type_id);
    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedNodeIDs(uint64_t id, const std::vector<std::string> &rel_types);

    std::vector<Relationship> NodeGetOutgoingRelationships(const std::string& type, const std::string& key);
    std::vector<Relationship> NodeGetOutgoingRelationships(const std::string& type, const std::string& key, const std::string& rel_type);
    std::vector<Relationship> NodeGetOutgoingRelationships(const std::string& type, const std::string& key, uint16_t type_id);
    std::vector<Relationship> NodeGetOutgoingRelationships(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types);

    std::vector<Relationship> NodeGetOutgoingRelationships(uint64_t id);
    std::vector<Relationship> NodeGetOutgoingRelationships(uint64_t id, const std::string& rel_type);
    std::vector<Relationship> NodeGetOutgoingRelationships(uint64_t id, uint16_t type_id);
    std::vector<Relationship> NodeGetOutgoingRelationships(uint64_t id, const std::vector<std::string> &rel_types);

    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedIncomingRelationshipIDs(const std::string& type, const std::string& key);
    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedIncomingRelationshipIDs(const std::string& type, const std::string& key, const std::string& rel_type);
    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedIncomingRelationshipIDs(const std::string& type, const std::string& key, uint16_t type_id);
    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedIncomingRelationshipIDs(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types);

    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedIncomingRelationshipIDs(uint64_t id);
    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedIncomingRelationshipIDs(uint64_t id, const std::string& rel_type);
    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedIncomingRelationshipIDs(uint64_t id, uint16_t type_id);
    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedIncomingRelationshipIDs(uint64_t id, const std::vector<std::string> &rel_types);

    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedIncomingNodeIDs(const std::string& type, const std::string& key);
    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedIncomingNodeIDs(const std::string& type, const std::string& key, const std::string& rel_type);
    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedIncomingNodeIDs(const std::string& type, const std::string& key, uint16_t type_id);
    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedIncomingNodeIDs(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types);

    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedIncomingNodeIDs(uint64_t id);
    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedIncomingNodeIDs(uint64_t id, const std::string& rel_type);
    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedIncomingNodeIDs(uint64_t id, uint16_t type_id);
    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedIncomingNodeIDs(uint64_t id, const std::vector<std::string> &rel_types);

    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedOutgoingNodeIDs(const std::string& type, const std::string& key);
    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedOutgoingNodeIDs(const std::string& type, const std::string& key, const std::string& rel_type);
    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedOutgoingNodeIDs(const std::string& type, const std::string& key, uint16_t type_id);
    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedOutgoingNodeIDs(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types);

    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedOutgoingNodeIDs(uint64_t id);
    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedOutgoingNodeIDs(uint64_t id, const std::string& rel_type);
    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedOutgoingNodeIDs(uint64_t id, uint16_t type_id);
    std::map<uint16_t, std::vector<uint64_t>> NodeGetShardedOutgoingNodeIDs(uint64_t id, const std::vector<std::string> &rel_types);

    std::vector<Node> NodesGet(const std::vector<uint64_t>&);
    std::vector<Relationship> RelationshipsGet(const std::vector<uint64_t>&);

    // All

    std::map<uint16_t, uint64_t> AllNodeIdCounts();
    uint64_t AllNodeIdCounts(const std::string& type);
    uint64_t AllNodeIdCounts(uint16_t type_id);

    std::map<uint16_t, uint64_t> AllRelationshipIdCounts();
    uint64_t AllRelationshipIdCounts(const std::string& type);
    uint64_t AllRelationshipIdCounts(uint16_t type_id);

    Roaring64Map AllNodeIdsMap();
    Roaring64Map AllNodeIdsMap(const std::string& type);
    Roaring64Map AllNodeIdsMap(uint16_t type_id);

    std::vector<uint64_t> AllNodeIds(uint64_t skip = SKIP, uint64_t limit = LIMIT);
    std::vector<uint64_t> AllNodeIds(const std::string& type, uint64_t skip = SKIP, uint64_t limit = LIMIT);
    std::vector<uint64_t> AllNodeIds(uint16_t type_id, uint64_t skip = SKIP, uint64_t limit = LIMIT);

    std::vector<Node> AllNodes(uint64_t skip = SKIP, uint64_t limit = LIMIT);
    std::vector<Node> AllNodes(const std::string& type, uint64_t skip = SKIP, uint64_t limit = LIMIT);
    std::vector<Node> AllNodes(uint16_t type_id, uint64_t skip = SKIP, uint64_t limit = LIMIT);

    Roaring64Map AllRelationshipIdsMap();
    Roaring64Map AllRelationshipIdsMap(const std::string& rel_type);
    Roaring64Map AllRelationshipIdsMap(uint16_t type_id);

    std::vector<uint64_t> AllRelationshipIds(uint64_t skip = SKIP, uint64_t limit = LIMIT);
    std::vector<uint64_t> AllRelationshipIds(const std::string& rel_type, uint64_t skip = SKIP, uint64_t limit = LIMIT);
    std::vector<uint64_t> AllRelationshipIds(uint16_t type_id, uint64_t skip = SKIP, uint64_t limit = LIMIT);

    std::vector<Relationship> AllRelationships(uint64_t skip = SKIP, uint64_t limit = LIMIT);
    std::vector<Relationship> AllRelationships(const std::string& type, uint64_t skip = SKIP, uint64_t limit = LIMIT);
    std::vector<Relationship> AllRelationships(uint16_t type_id, uint64_t skip = SKIP, uint64_t limit = LIMIT);

    // Validations
    bool ValidNodeId(uint64_t id);
    bool ValidRelationshipId(uint64_t id);

    // Property Helper
    void convertProperties(std::map<std::string, std::any> &values, const dom::object &object) const;



    // *****************************************************************************************************************************
    //                                               Peered
    // *****************************************************************************************************************************

    // Relationship Types
    uint16_t RelationshipTypesGetCountPeered();
    seastar::future<uint64_t> RelationshipTypesGetCountPeered(uint16_t type_id);
    seastar::future<uint64_t> RelationshipTypesGetCountPeered(const std::string& type);
    std::set<std::string> RelationshipTypesGetPeered();

    // Relationship Type
    std::string RelationshipTypeGetTypePeered(uint16_t type_id);
    uint16_t RelationshipTypeGetTypeIdPeered(const std::string& type);
    seastar::future<uint16_t> RelationshipTypeInsertPeered(const std::string& type);

    // Node Types
    uint16_t NodeTypesGetCountPeered();
    seastar::future<uint64_t> NodeTypesGetCountPeered(uint16_t type_id);
    seastar::future<uint64_t> NodeTypesGetCountPeered(const std::string& type);
    std::set<std::string> NodeTypesGetPeered();

    // Node Type
    std::string NodeTypeGetTypePeered(uint16_t type_id);
    uint16_t NodeTypeGetTypeIdPeered(const std::string& type);
    seastar::future<uint16_t> NodeTypeInsertPeered(const std::string& type);

    // Nodes
    seastar::future<uint64_t> NodeAddEmptyPeered(const std::string& type, const std::string& key);
    seastar::future<uint64_t> NodeAddPeered(const std::string& type, const std::string& key, const std::string& properties);
    seastar::future<uint64_t> NodeGetIDPeered(const std::string& type, const std::string& key);

    seastar::future<Node> NodeGetPeered(const std::string& type, const std::string& key);
    seastar::future<Node> NodeGetPeered(uint64_t id);
    seastar::future<bool> NodeRemovePeered(const std::string& type, const std::string& key);
    seastar::future<bool> NodeRemovePeered(uint64_t id);
    seastar::future<uint16_t> NodeGetTypeIdPeered(uint64_t id);
    seastar::future<std::string> NodeGetTypePeered(uint64_t id);
    seastar::future<std::string> NodeGetKeyPeered(uint64_t id);

    // Node Properties
    seastar::future<std::any> NodePropertyGetPeered(const std::string& type, const std::string& key, const std::string& property);
    seastar::future<std::string> NodePropertyGetStringPeered(const std::string& type, const std::string& key, const std::string& property);
    seastar::future<int64_t> NodePropertyGetIntegerPeered(const std::string& type, const std::string& key, const std::string& property);
    seastar::future<double> NodePropertyGetDoublePeered(const std::string& type, const std::string& key, const std::string& property);
    seastar::future<bool> NodePropertyGetBooleanPeered(const std::string& type, const std::string& key, const std::string &property);
    seastar::future<std::map<std::string, std::any>> NodePropertyGetObjectPeered(const std::string& type, const std::string& key, const std::string& property);
    seastar::future<std::any> NodePropertyGetPeered(uint64_t id, const std::string& property);
    seastar::future<std::string> NodePropertyGetStringPeered(uint64_t id, const std::string& property);
    seastar::future<int64_t> NodePropertyGetIntegerPeered(uint64_t id, const std::string& property);
    seastar::future<double> NodePropertyGetDoublePeered(uint64_t id, const std::string& property);
    seastar::future<bool> NodePropertyGetBooleanPeered(uint64_t id, const std::string &property);
    seastar::future<std::map<std::string, std::any>> NodePropertyGetObjectPeered(uint64_t id, const std::string& property);
    seastar::future<bool> NodePropertySetPeered(const std::string& type, const std::string& key, const std::string& property, std::string value);
    // Deal with string literal being confused for a boolean
    seastar::future<bool> NodePropertySetPeered(const std::string& type, const std::string& key, const std::string& property, const char* value);
    seastar::future<bool> NodePropertySetPeered(const std::string& type, const std::string& key, const std::string& property, int64_t value);
    seastar::future<bool> NodePropertySetPeered(const std::string& type, const std::string& key, const std::string& property, double value);
    seastar::future<bool> NodePropertySetPeered(const std::string& type, const std::string& key, const std::string& property, bool value);
    seastar::future<bool> NodePropertySetPeered(const std::string& type, const std::string& key, const std::string& property, std::map<std::string, std::any> value);
    seastar::future<bool> NodePropertySetFromJsonPeered(const std::string& type, const std::string& key, const std::string& property, const std::string& value);
    seastar::future<bool> NodePropertySetPeered(uint64_t id, const std::string& property, std::string value);

    seastar::future<std::map<std::string, std::any>> NodePropertiesGetPeered(const std::string& type, const std::string& key);
    seastar::future<bool> NodePropertiesSetPeered(const std::string& type, const std::string& key, std::map<std::string, std::any> &value);
    seastar::future<bool> NodePropertiesSetFromJsonPeered(const std::string& type, const std::string& key, const std::string& value);
    seastar::future<bool> NodePropertiesResetPeered(const std::string& type, const std::string& key, const std::map<std::string, std::any> &value);
    seastar::future<bool> NodePropertiesResetFromJsonPeered(const std::string& type, const std::string& key, const std::string& value);
    seastar::future<bool> NodePropertiesDeletePeered(const std::string& type, const std::string& key);

    // Deal with string literal being confused for a boolean
    seastar::future<bool> NodePropertySetPeered(uint64_t id, const std::string& property, const char* value);
    seastar::future<bool> NodePropertySetPeered(uint64_t id, const std::string& property, int64_t value);
    seastar::future<bool> NodePropertySetPeered(uint64_t id, const std::string& property, double value);
    seastar::future<bool> NodePropertySetPeered(uint64_t id, const std::string& property, bool value);
    seastar::future<bool> NodePropertySetPeered(uint64_t id, const std::string& property, std::map<std::string, std::any> value);
    seastar::future<bool> NodePropertySetFromJsonPeered(uint64_t id, const std::string& property, const std::string& value);
    seastar::future<bool> NodePropertyDeletePeered(const std::string& type, const std::string& key, const std::string& property);
    seastar::future<bool> NodePropertyDeletePeered(uint64_t id, const std::string& property);

    seastar::future<std::map<std::string, std::any>> NodePropertiesGetPeered(uint64_t id);
    seastar::future<bool> NodePropertiesSetPeered(uint64_t id, std::map<std::string, std::any> &value);
    seastar::future<bool> NodePropertiesSetFromJsonPeered(uint64_t id, const std::string& value);
    seastar::future<bool> NodePropertiesResetPeered(uint64_t id, const std::map<std::string, std::any> &value);
    seastar::future<bool> NodePropertiesResetFromJsonPeered(uint64_t id, const std::string& value);
    seastar::future<bool> NodePropertiesDeletePeered(uint64_t id);

    // Relationships
    seastar::future<uint64_t> RelationshipAddEmptyPeered(const std::string& rel_type, const std::string& type1, const std::string& key1,
                                                         const std::string& type2, const std::string& key2);
    seastar::future<uint64_t> RelationshipAddEmptyPeered(uint16_t rel_type_id, uint64_t id1, uint64_t id2);
    seastar::future<uint64_t> RelationshipAddEmptyPeered(const std::string& rel_type, uint64_t id1, uint64_t id2);
    seastar::future<uint64_t> RelationshipAddPeered(const std::string& rel_type, const std::string& type1, const std::string& key1,
                                                    const std::string& type2, const std::string& key2, const std::string& properties);
    seastar::future<uint64_t> RelationshipAddPeered(uint16_t rel_type_id, uint64_t id1, uint64_t id2, const std::string& properties);
    seastar::future<uint64_t> RelationshipAddPeered(const std::string& rel_type, uint64_t id1, uint64_t id2, const std::string& properties);
    seastar::future<Relationship> RelationshipGetPeered(uint64_t id);
    seastar::future<bool> RelationshipRemovePeered(uint64_t id);
    seastar::future<std::string> RelationshipGetTypePeered(uint64_t id);
    seastar::future<uint16_t> RelationshipGetTypeIdPeered(uint64_t id);
    seastar::future<uint64_t> RelationshipGetStartingNodeIdPeered(uint64_t id);
    seastar::future<uint64_t> RelationshipGetEndingNodeIdPeered(uint64_t id);

    // Relationship Properties
    seastar::future<std::any> RelationshipPropertyGetPeered(uint64_t id, const std::string& property);
    seastar::future<std::string> RelationshipPropertyGetStringPeered(uint64_t id, const std::string& property);
    seastar::future<int64_t> RelationshipPropertyGetIntegerPeered(uint64_t id, const std::string& property);
    seastar::future<double> RelationshipPropertyGetDoublePeered(uint64_t id, const std::string& property);
    seastar::future<bool> RelationshipPropertyGetBooleanPeered(uint64_t id, const std::string &property);
    seastar::future<std::map<std::string, std::any>> RelationshipPropertyGetObjectPeered(uint64_t id, const std::string& property);
    seastar::future<bool> RelationshipPropertySetPeered(uint64_t id, const std::string& property, std::string value);

    // Deal with string literal being confused for a boolean
    seastar::future<bool> RelationshipPropertySetPeered(uint64_t id, const std::string& property, const char* value);
    seastar::future<bool> RelationshipPropertySetPeered(uint64_t id, const std::string& property, int64_t value);
    seastar::future<bool> RelationshipPropertySetPeered(uint64_t id, const std::string& property, double value);
    seastar::future<bool> RelationshipPropertySetPeered(uint64_t id, const std::string& property, bool value);
    seastar::future<bool> RelationshipPropertySetPeered(uint64_t id, const std::string& property, std::map<std::string, std::any> value);
    seastar::future<bool> RelationshipPropertySetFromJsonPeered(uint64_t id, const std::string& property, const std::string& value);
    seastar::future<bool> RelationshipPropertyDeletePeered(uint64_t id, const std::string& property);
    seastar::future<std::map<std::string, std::any>> RelationshipPropertiesGetPeered(uint64_t id);
    seastar::future<bool> RelationshipPropertiesSetPeered(uint64_t id, std::map<std::string, std::any> &value);
    seastar::future<bool> RelationshipPropertiesSetFromJsonPeered(uint64_t id, const std::string &value);
    seastar::future<bool> RelationshipPropertiesResetPeered(uint64_t id, const std::map<std::string, std::any> &value);
    seastar::future<bool> RelationshipPropertiesResetFromJsonPeered(uint64_t id, const std::string &value);
    seastar::future<bool> RelationshipPropertiesDeletePeered(uint64_t id);

    // Node Degree
    seastar::future<uint64_t> NodeGetDegreePeered(const std::string& type, const std::string& key);
    seastar::future<uint64_t> NodeGetDegreePeered(const std::string& type, const std::string& key, Direction direction);
    seastar::future<uint64_t> NodeGetDegreePeered(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type);
    seastar::future<uint64_t> NodeGetDegreePeered(const std::string& type, const std::string& key, const std::string& rel_type);
    seastar::future<uint64_t> NodeGetDegreePeered(const std::string& type, const std::string& key, Direction direction,
                                                  const std::vector<std::string>& rel_types);
    seastar::future<uint64_t> NodeGetDegreePeered(const std::string& type, const std::string& key,
                                                  const std::vector<std::string>& rel_types);
    seastar::future<uint64_t> NodeGetDegreePeered(uint64_t id);
    seastar::future<uint64_t> NodeGetDegreePeered(uint64_t id, Direction direction);
    seastar::future<uint64_t> NodeGetDegreePeered(uint64_t id, Direction direction, const std::string& rel_type);
    seastar::future<uint64_t> NodeGetDegreePeered(uint64_t id, const std::string& rel_type);
    seastar::future<uint64_t> NodeGetDegreePeered(uint64_t id, Direction direction, const std::vector<std::string> &rel_types);
    seastar::future<uint64_t> NodeGetDegreePeered(uint64_t id, const std::vector<std::string> &rel_types);

    // Traversing
    seastar::future<std::vector<Ids>> NodeGetRelationshipsIDsPeered(const std::string& type, const std::string& key);
    seastar::future<std::vector<Ids>> NodeGetRelationshipsIDsPeered(const std::string& type, const std::string& key, Direction direction);
    seastar::future<std::vector<Ids>> NodeGetRelationshipsIDsPeered(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type);
    seastar::future<std::vector<Ids>> NodeGetRelationshipsIDsPeered(const std::string& type, const std::string& key, Direction direction, uint16_t type_id);
    seastar::future<std::vector<Ids>> NodeGetRelationshipsIDsPeered(const std::string& type, const std::string& key, Direction direction, const std::vector<std::string> &rel_types);

    seastar::future<std::vector<Ids>> NodeGetRelationshipsIDsPeered(const std::string& type, const std::string& key, const std::string& rel_type);
    seastar::future<std::vector<Ids>> NodeGetRelationshipsIDsPeered(const std::string& type, const std::string& key, uint16_t type_id);
    seastar::future<std::vector<Ids>> NodeGetRelationshipsIDsPeered(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types);

    seastar::future<std::vector<Ids>> NodeGetRelationshipsIDsPeered(uint64_t id);
    seastar::future<std::vector<Ids>> NodeGetRelationshipsIDsPeered(uint64_t id, Direction direction);
    seastar::future<std::vector<Ids>> NodeGetRelationshipsIDsPeered(uint64_t id, Direction direction, const std::string& rel_type);
    seastar::future<std::vector<Ids>> NodeGetRelationshipsIDsPeered(uint64_t id, Direction direction, uint16_t type_id);
    seastar::future<std::vector<Ids>> NodeGetRelationshipsIDsPeered(uint64_t id, Direction direction, const std::vector<std::string> &rel_types);

    seastar::future<std::vector<Ids>> NodeGetRelationshipsIDsPeered(uint64_t id, const std::string& rel_type);
    seastar::future<std::vector<Ids>> NodeGetRelationshipsIDsPeered(uint64_t id, uint16_t type_id);
    seastar::future<std::vector<Ids>> NodeGetRelationshipsIDsPeered(uint64_t id, const std::vector<std::string> &rel_types);

    seastar::future<std::vector<Relationship>> NodeGetRelationshipsPeered(const std::string& type, const std::string& key);
    seastar::future<std::vector<Relationship>> NodeGetRelationshipsPeered(const std::string& type, const std::string& key, const std::string& rel_type);
    seastar::future<std::vector<Relationship>> NodeGetRelationshipsPeered(const std::string& type, const std::string& key, uint16_t type_id);
    seastar::future<std::vector<Relationship>> NodeGetRelationshipsPeered(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types);

    seastar::future<std::vector<Relationship>> NodeGetRelationshipsPeered(uint64_t id);
    seastar::future<std::vector<Relationship>> NodeGetRelationshipsPeered(uint64_t id, const std::string& rel_type);
    seastar::future<std::vector<Relationship>> NodeGetRelationshipsPeered(uint64_t id, uint16_t type_id);
    seastar::future<std::vector<Relationship>> NodeGetRelationshipsPeered(uint64_t id, const std::vector<std::string> &rel_types);

    seastar::future<std::vector<Relationship>> NodeGetRelationshipsPeered(const std::string& type, const std::string& key, Direction direction);
    seastar::future<std::vector<Relationship>> NodeGetRelationshipsPeered(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type);
    seastar::future<std::vector<Relationship>> NodeGetRelationshipsPeered(const std::string& type, const std::string& key, Direction direction, uint16_t type_id);
    seastar::future<std::vector<Relationship>> NodeGetRelationshipsPeered(const std::string& type, const std::string& key, Direction direction, const std::vector<std::string> &rel_types);

    seastar::future<std::vector<Relationship>> NodeGetRelationshipsPeered(uint64_t id, Direction direction);
    seastar::future<std::vector<Relationship>> NodeGetRelationshipsPeered(uint64_t id, Direction direction, const std::string& rel_type);
    seastar::future<std::vector<Relationship>> NodeGetRelationshipsPeered(uint64_t id, Direction direction, uint16_t type_id);
    seastar::future<std::vector<Relationship>> NodeGetRelationshipsPeered(uint64_t id, Direction direction, const std::vector<std::string> &rel_types);

    seastar::future<std::vector<Node>> NodeGetNeighborsPeered(const std::string& type, const std::string& key);
    seastar::future<std::vector<Node>> NodeGetNeighborsPeered(const std::string& type, const std::string& key, const std::string& rel_type);
    seastar::future<std::vector<Node>> NodeGetNeighborsPeered(const std::string& type, const std::string& key, uint16_t type_id);
    seastar::future<std::vector<Node>> NodeGetNeighborsPeered(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types);

    seastar::future<std::vector<Node>> NodeGetNeighborsPeered(uint64_t id);
    seastar::future<std::vector<Node>> NodeGetNeighborsPeered(uint64_t id, const std::string& rel_type);
    seastar::future<std::vector<Node>> NodeGetNeighborsPeered(uint64_t id, uint16_t type_id);
    seastar::future<std::vector<Node>> NodeGetNeighborsPeered(uint64_t id, const std::vector<std::string> &rel_types);

    seastar::future<std::vector<Node>> NodeGetNeighborsPeered(const std::string& type, const std::string& key, Direction direction);
    seastar::future<std::vector<Node>> NodeGetNeighborsPeered(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type);
    seastar::future<std::vector<Node>> NodeGetNeighborsPeered(const std::string& type, const std::string& key, Direction direction, uint16_t type_id);
    seastar::future<std::vector<Node>> NodeGetNeighborsPeered(const std::string& type, const std::string& key, Direction direction, const std::vector<std::string> &rel_types);

    seastar::future<std::vector<Node>> NodeGetNeighborsPeered(uint64_t id, Direction direction);
    seastar::future<std::vector<Node>> NodeGetNeighborsPeered(uint64_t id, Direction direction, const std::string& rel_type);
    seastar::future<std::vector<Node>> NodeGetNeighborsPeered(uint64_t id, Direction direction, uint16_t type_id);
    seastar::future<std::vector<Node>> NodeGetNeighborsPeered(uint64_t id, Direction direction, const std::vector<std::string> &rel_types);

    // All
    seastar::future<std::vector<uint64_t>> AllNodeIdsPeered(uint64_t skip = 0, uint64_t limit = 100);
    seastar::future<std::vector<uint64_t>> AllNodeIdsPeered(const std::string& type, uint64_t skip = 0, uint64_t limit = 100);
    seastar::future<std::vector<uint64_t>> AllRelationshipIdsPeered(uint64_t skip = 0, uint64_t limit = 100);
    seastar::future<std::vector<uint64_t>> AllRelationshipIdsPeered(const std::string& rel_type, uint64_t skip = 0, uint64_t limit = 100);

    seastar::future<std::vector<Node>> AllNodesPeered(uint64_t skip = 0, uint64_t limit = 100);
    seastar::future<std::vector<Node>> AllNodesPeered(const std::string& type, uint64_t skip = 0, uint64_t limit = 100);
    seastar::future<std::vector<Relationship>> AllRelationshipsPeered(uint64_t skip = 0, uint64_t limit = 100);
    seastar::future<std::vector<Relationship>> AllRelationshipsPeered(const std::string& rel_type, uint64_t skip = 0, uint64_t limit = 100);


    // *****************************************************************************************************************************
    //                                                              Via Lua
    // *****************************************************************************************************************************

    // Relationship Types
    uint16_t RelationshipTypesGetCountViaLua();
    uint64_t RelationshipTypesGetCountByTypeViaLua(const std::string& type);
    uint64_t RelationshipTypesGetCountByIdViaLua(uint16_t type_id);
    sol::as_table_t<std::set<std::string>> RelationshipTypesGetViaLua();

    // Relationship Type
    std::string RelationshipTypeGetTypeViaLua(uint16_t type_id);
    uint16_t RelationshipTypeGetTypeIdViaLua(const std::string& type);
    uint16_t RelationshipTypeInsertViaLua(const std::string& type);

    // Node Types
    uint16_t NodeTypesGetCountViaLua();
    uint64_t NodeTypesGetCountByTypeViaLua(const std::string& type);
    uint64_t NodeTypesGetCountByIdViaLua(uint16_t type_id);
    sol::as_table_t<std::set<std::string>> NodeTypesGetViaLua();

    // Node Type
    std::string NodeTypeGetTypeViaLua(uint16_t type_id);
    uint16_t NodeTypeGetTypeIdViaLua(const std::string& type);
    uint16_t NodeTypeInsertViaLua(const std::string& type);

    //Nodes
    uint64_t NodeAddEmptyViaLua(const std::string& type, const std::string& key);
    uint64_t NodeAddViaLua(const std::string& type, const std::string& key, const std::string& properties);
    uint64_t NodeGetIdViaLua(const std::string& type, const std::string& key);
    Node NodeGetViaLua(const std::string& type, const std::string& key);
    Node NodeGetByIdViaLua(uint64_t id);
    bool NodeRemoveViaLua(const std::string& type, const std::string& key);
    bool NodeRemoveByIdViaLua(uint64_t id);
    uint16_t NodeGetTypeIdViaLua(uint64_t id);
    std::string NodeGetTypeViaLua(uint64_t id);
    std::string NodeGetKeyViaLua(uint64_t id);

    // Node Properties
    sol::object NodePropertyGetViaLua(const std::string& type, const std::string& key, const std::string& property);
    sol::object NodePropertyGetByIdViaLua(uint64_t id, const std::string& property);
    bool NodePropertySetViaLua(const std::string& type, const std::string& key, const std::string& property, const sol::object& value);
    bool NodePropertySetByIdViaLua(uint64_t id, const std::string& property, const sol::object& value);
    bool NodePropertiesSetFromJsonViaLua(const std::string& type, const std::string& key, const std::string& value);
    bool NodePropertiesSetFromJsonByIdViaLua(uint64_t id, const std::string& value);
    bool NodePropertiesResetFromJsonViaLua(const std::string& type, const std::string& key, const std::string& value);
    bool NodePropertiesResetFromJsonByIdViaLua(uint64_t id, const std::string& value);
    bool NodePropertyDeleteViaLua(const std::string& type, const std::string& key, const std::string& property);
    bool NodePropertyDeleteByIdViaLua(uint64_t id, const std::string& property);
    bool NodePropertiesDeleteViaLua(const std::string& type, const std::string& key);
    bool NodePropertiesDeleteByIdViaLua(uint64_t id);

    // Relationships
    uint64_t RelationshipAddEmptyViaLua(const std::string& rel_type, const std::string& type1, const std::string& key1,
                                        const std::string& type2, const std::string& key2);
    uint64_t RelationshipAddEmptyByTypeIdByIdsViaLua(uint16_t rel_type_id, uint64_t id1, uint64_t id2);
    uint64_t RelationshipAddEmptyByIdsViaLua(const std::string& rel_type, uint64_t id1, uint64_t id2);
    uint64_t RelationshipAddViaLua(const std::string& rel_type, const std::string& type1, const std::string& key1,
                                   const std::string& type2, const std::string& key2, const std::string& properties);
    uint64_t RelationshipAddByTypeIdByIdsViaLua(uint16_t rel_type_id, uint64_t id1, uint64_t id2, const std::string& properties);
    uint64_t RelationshipAddByIdsViaLua(const std::string& rel_type, uint64_t id1, uint64_t id2, const std::string& properties);
    Relationship RelationshipGetViaLua(uint64_t id);
    bool RelationshipRemoveViaLua(uint64_t id);
    std::string RelationshipGetTypeViaLua(uint64_t id);
    uint16_t RelationshipGetTypeIdViaLua(uint64_t id);
    uint64_t RelationshipGetStartingNodeIdViaLua(uint64_t id);
    uint64_t RelationshipGetEndingNodeIdViaLua(uint64_t id);

    // Relationship Properties
    sol::object RelationshipPropertyGetViaLua(uint64_t id, const std::string& property);
    bool RelationshipPropertySetViaLua(uint64_t id, const std::string& property, const sol::object& value);
    bool RelationshipPropertySetFromJsonViaLua(uint64_t id, const std::string& property, const std::string& value);
    bool RelationshipPropertyDeleteViaLua(uint64_t id, const std::string& property);
    bool RelationshipPropertiesSetFromJsonViaLua(uint64_t id, const std::string &value);
    bool RelationshipPropertiesResetFromJsonViaLua(uint64_t id, const std::string &value);
    bool RelationshipPropertiesDeleteViaLua(uint64_t id);

    // Node Degree
    uint64_t NodeGetDegreeViaLua(const std::string& type, const std::string& key);
    uint64_t NodeGetDegreeForDirectionViaLua(const std::string& type, const std::string& key, Direction direction);
    uint64_t NodeGetDegreeForDirectionForTypeViaLua(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type);
    uint64_t NodeGetDegreeForTypeViaLua(const std::string& type, const std::string& key, const std::string& rel_type);
    uint64_t NodeGetDegreeForDirectionForTypesViaLua(const std::string& type, const std::string& key, Direction direction,
                                                     const std::vector<std::string>& rel_types);
    uint64_t NodeGetDegreeForTypesViaLua(const std::string& type, const std::string& key,
                                         const std::vector<std::string>& rel_types);
    uint64_t NodeGetDegreeByIdViaLua(uint64_t id);
    uint64_t NodeGetDegreeByIdForDirectionViaLua(uint64_t id, Direction direction);
    uint64_t NodeGetDegreeByIdForDirectionForTypeViaLua(uint64_t id, Direction direction, const std::string& rel_type);
    uint64_t NodeGetDegreeByIdForTypeViaLua(uint64_t id, const std::string& rel_type);
    uint64_t NodeGetDegreeByIdForDirectionForTypesViaLua(uint64_t id, Direction direction, const std::vector<std::string> &rel_types);
    uint64_t NodeGetDegreeByIdForTypesViaLua(uint64_t id, const std::vector<std::string> &rel_types);

    // Traversing
    sol::as_table_t<std::vector<Ids>> NodeGetRelationshipsIdsViaLua(const std::string& type, const std::string& key);
    sol::as_table_t<std::vector<Ids>> NodeGetRelationshipsIdsForDirectionViaLua(const std::string& type, const std::string& key, Direction direction);
    sol::as_table_t<std::vector<Ids>> NodeGetRelationshipsIdsForDirectionForTypeViaLua(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type);
    sol::as_table_t<std::vector<Ids>> NodeGetRelationshipsIdsForDirectionForTypeIdViaLua(const std::string& type, const std::string& key, Direction direction, uint16_t type_id);
    sol::as_table_t<std::vector<Ids>> NodeGetRelationshipsIdsForDirectionForTypesViaLua(const std::string& type, const std::string& key, Direction direction, const std::vector<std::string> &rel_types);

    sol::as_table_t<std::vector<Ids>> NodeGetRelationshipsIdsForTypeViaLua(const std::string& type, const std::string& key, const std::string& rel_type);
    sol::as_table_t<std::vector<Ids>> NodeGetRelationshipsIdsForTypeIdViaLua(const std::string& type, const std::string& key, uint16_t type_id);
    sol::as_table_t<std::vector<Ids>> NodeGetRelationshipsIdsForTypesViaLua(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types);

    sol::as_table_t<std::vector<Ids>> NodeGetRelationshipsIdsByIdViaLua(uint64_t id);
    sol::as_table_t<std::vector<Ids>> NodeGetRelationshipsIdsByIdForDirectionViaLua(uint64_t id, Direction direction);
    sol::as_table_t<std::vector<Ids>> NodeGetRelationshipsIdsByIdForDirectionForTypeViaLua(uint64_t id, Direction direction, const std::string& rel_type);
    sol::as_table_t<std::vector<Ids>> NodeGetRelationshipsIdsByIdForDirectionForTypeIdViaLua(uint64_t id, Direction direction, uint16_t type_id);
    sol::as_table_t<std::vector<Ids>> NodeGetRelationshipsIdsByIdForDirectionForTypesViaLua(uint64_t id, Direction direction, const std::vector<std::string> &rel_types);

    sol::as_table_t<std::vector<Ids>> NodeGetRelationshipsIdsByIdForTypeViaLua(uint64_t id, const std::string& rel_type);
    sol::as_table_t<std::vector<Ids>> NodeGetRelationshipsIdsByIdForTypeIdViaLua(uint64_t id, uint16_t type_id);
    sol::as_table_t<std::vector<Ids>> NodeGetRelationshipsIdsByIdForTypesViaLua(uint64_t id, const std::vector<std::string> &rel_types);

    sol::as_table_t<std::vector<Relationship>> NodeGetRelationshipsViaLua(const std::string& type, const std::string& key);
    sol::as_table_t<std::vector<Relationship>> NodeGetRelationshipsForTypeViaLua(const std::string& type, const std::string& key, const std::string& rel_type);
    sol::as_table_t<std::vector<Relationship>> NodeGetRelationshipsForTypeIdViaLua(const std::string& type, const std::string& key, uint16_t type_id);
    sol::as_table_t<std::vector<Relationship>> NodeGetRelationshipsForTypesViaLua(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types);

    sol::as_table_t<std::vector<Relationship>> NodeGetRelationshipsByIdViaLua(uint64_t id);
    sol::as_table_t<std::vector<Relationship>> NodeGetRelationshipsByIdForTypeViaLua(uint64_t id, const std::string& rel_type);
    sol::as_table_t<std::vector<Relationship>> NodeGetRelationshipsByIdForTypeIdViaLua(uint64_t id, uint16_t type_id);
    sol::as_table_t<std::vector<Relationship>> NodeGetRelationshipsByIdForTypesViaLua(uint64_t id, const std::vector<std::string> &rel_types);

    sol::as_table_t<std::vector<Relationship>> NodeGetRelationshipsForDirectionViaLua(const std::string& type, const std::string& key, Direction direction);
    sol::as_table_t<std::vector<Relationship>> NodeGetRelationshipsForDirectionForTypeViaLua(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type);
    sol::as_table_t<std::vector<Relationship>> NodeGetRelationshipsForDirectionForTypeIdViaLua(const std::string& type, const std::string& key, Direction direction, uint16_t type_id);
    sol::as_table_t<std::vector<Relationship>> NodeGetRelationshipsForDirectionForTypesViaLua(const std::string& type, const std::string& key, Direction direction, const std::vector<std::string> &rel_types);

    sol::as_table_t<std::vector<Relationship>> NodeGetRelationshipsByIdForDirectionViaLua(uint64_t id, Direction direction);
    sol::as_table_t<std::vector<Relationship>> NodeGetRelationshipsByIdForDirectionForTypeViaLua(uint64_t id, Direction direction, const std::string& rel_type);
    sol::as_table_t<std::vector<Relationship>> NodeGetRelationshipsByIdForDirectionForTypeIdViaLua(uint64_t id, Direction direction, uint16_t type_id);
    sol::as_table_t<std::vector<Relationship>> NodeGetRelationshipsByIdForDirectionForTypesViaLua(uint64_t id, Direction direction, const std::vector<std::string> &rel_types);

    sol::as_table_t<std::vector<Node>> NodeGetNeighborsViaLua(const std::string& type, const std::string& key);
    sol::as_table_t<std::vector<Node>> NodeGetNeighborsForTypeViaLua(const std::string& type, const std::string& key, const std::string& rel_type);
    sol::as_table_t<std::vector<Node>> NodeGetNeighborsForTypeIdViaLua(const std::string& type, const std::string& key, uint16_t type_id);
    sol::as_table_t<std::vector<Node>> NodeGetNeighborsForTypesViaLua(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types);

    sol::as_table_t<std::vector<Node>> NodeGetNeighborsByIdViaLua(uint64_t id);
    sol::as_table_t<std::vector<Node>> NodeGetNeighborsByIdForTypeViaLua(uint64_t id, const std::string& rel_type);
    sol::as_table_t<std::vector<Node>> NodeGetNeighborsByIdForTypeIdViaLua(uint64_t id, uint16_t type_id);
    sol::as_table_t<std::vector<Node>> NodeGetNeighborsByIdForTypesViaLua(uint64_t id, const std::vector<std::string> &rel_types);

    sol::as_table_t<std::vector<Node>> NodeGetNeighborsForDirectionViaLua(const std::string& type, const std::string& key, Direction direction);
    sol::as_table_t<std::vector<Node>> NodeGetNeighborsForDirectionForTypeViaLua(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type);
    sol::as_table_t<std::vector<Node>> NodeGetNeighborsForDirectionForTypeIdViaLua(const std::string& type, const std::string& key, Direction direction, uint16_t type_id);
    sol::as_table_t<std::vector<Node>> NodeGetNeighborsForDirectionForTypesViaLua(const std::string& type, const std::string& key, Direction direction, const std::vector<std::string> &rel_types);

    sol::as_table_t<std::vector<Node>> NodeGetNeighborsByIdForDirectionViaLua(uint64_t id, Direction direction);
    sol::as_table_t<std::vector<Node>> NodeGetNeighborsByIdForDirectionForTypeViaLua(uint64_t id, Direction direction, const std::string& rel_type);
    sol::as_table_t<std::vector<Node>> NodeGetNeighborsByIdForDirectionForTypeIdViaLua(uint64_t id, Direction direction, uint16_t type_id);
    sol::as_table_t<std::vector<Node>> NodeGetNeighborsByIdForDirectionForTypesViaLua(uint64_t id, Direction direction, const std::vector<std::string> &rel_types);

    // All
    sol::as_table_t<std::vector<uint64_t>> AllNodeIdsViaLua(uint64_t skip = 0, uint64_t limit = 100);
    sol::as_table_t<std::vector<uint64_t>> AllNodeIdsForTypeViaLua(const std::string& type, uint64_t skip = 0, uint64_t limit = 100);
    sol::as_table_t<std::vector<uint64_t>> AllRelationshipIdsViaLua(uint64_t skip = 0, uint64_t limit = 100);
    sol::as_table_t<std::vector<uint64_t>> AllRelationshipIdsForTypeViaLua(const std::string& rel_type, uint64_t skip = 0, uint64_t limit = 100);

    sol::as_table_t<std::vector<Node>> AllNodesViaLua(uint64_t skip = 0, uint64_t limit = 100);
    sol::as_table_t<std::vector<Node>> AllNodesForTypeViaLua(const std::string& type, uint64_t skip = 0, uint64_t limit = 100);
    sol::as_table_t<std::vector<Relationship>> AllRelationshipsViaLua(uint64_t skip = 0, uint64_t limit = 100);
    sol::as_table_t<std::vector<Relationship>> AllRelationshipsForTypeViaLua(const std::string& rel_type, uint64_t skip = 0, uint64_t limit = 100);

  };

} // namespace triton
#endif
