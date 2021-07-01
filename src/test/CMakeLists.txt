# Include Catch2 for Testing
FetchContent_Declare(
        Catch2
        GIT_REPOSITORY https://github.com/catchorg/Catch2.git
        GIT_TAG        v2.13.4)

FetchContent_MakeAvailable(Catch2)

set(SOURCE_FILES
        catch_main.cpp
        shard/RelationshipTypes.cpp shard/Ids.cpp shard/ShardIds.cpp shard/NodeTypes.cpp shard/Shards.cpp shard/Nodes.cpp
        shard/NodeDegrees.cpp shard/NodeProperties.cpp shard/Relationships.cpp shard/RelationshipProperties.cpp
        shard/AllNodes.cpp shard/AllRelationships.cpp)

# Where any include files are
include_directories(../lib/graph /usr/include/luajit-2.1 /usr/local/include/luajit-2.1 ../lib/sol)

add_executable(tests ${SOURCE_FILES})

target_link_libraries(tests
        /usr/local/lib/libluajit-5.1.a
        Graph
        Catch2::Catch2
        project_options)