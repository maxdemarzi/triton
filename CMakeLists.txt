cmake_minimum_required(VERSION 3.17)
project(triton CXX C)

#SET(CC /usr/bin/clang)
#SET(CXX /usr/bin/clang++)

set(CMAKE_CXX_STANDARD 17)
include(cmake/StandardProjectSettings.cmake)
#include(cmake/PreventInSourceBuilds.cmake)

include(FetchContent)

# Link this 'library' to set the c++ standard / compile-time options requested
add_library(project_options INTERFACE)
target_compile_features(project_options INTERFACE cxx_std_17)

if(CMAKE_CXX_COMPILER_ID MATCHES ".*Clang")
    option(ENABLE_BUILD_WITH_TIME_TRACE "Enable -ftime-trace to generate time tracing .json files on clang" OFF)
    if(ENABLE_BUILD_WITH_TIME_TRACE)
        add_compile_definitions(project_options INTERFACE -ftime-trace)
    endif()
endif()

# Link this 'library' to use the warnings specified in CompilerWarnings.cmake
add_library(project_warnings INTERFACE)

# enable cache system
include(cmake/Cache.cmake)

# standard compiler warnings
include(cmake/CompilerWarnings.cmake)
set_project_warnings(project_warnings)

# sanitizer options if supported by compiler
include(cmake/Sanitizers.cmake)
enable_sanitizers(project_options)

# enable doxygen
include(cmake/Doxygen.cmake)
enable_doxygen()

# allow for static analysis options
include(cmake/StaticAnalyzers.cmake)
#
option(BUILD_SHARED_LIBS "Enable compilation of shared libraries" OFF)
option(ENABLE_TESTING "Enable Test Builds" ON)
option(ENABLE_FUZZING "Enable Fuzzing Builds" OFF)

# Very basic PCH example
option(ENABLE_PCH "Enable Precompiled Headers" OFF)
if(ENABLE_PCH)
    # This sets a global PCH parameter, each project will build its own PCH, which is a good idea if any #define's change
    #
    # consider breaking this out per project as necessary
    target_precompile_headers(
            project_options
            INTERFACE
            <vector>
            <string>
            <map>
            <utility>)
endif()

if(ENABLE_TESTING)
    enable_testing()
    message("Building Tests. Be sure to check out test/constexpr_tests for constexpr testing")
    add_subdirectory(src/test)
endif()

if(ENABLE_FUZZING)
    message("Building Fuzz Tests, using fuzzing sanitizer https://www.llvm.org/docs/LibFuzzer.html")
    add_subdirectory(fuzz_test)
endif()

set(seastar_dir "$ENV{HOME}/seastar")
list(APPEND CMAKE_PREFIX_PATH "${seastar_dir}")
list(APPEND CMAKE_PREFIX_PATH "${seastar_dir}/build/release")
list(APPEND CMAKE_PREFIX_PATH "${seastar_dir}/build/release/_cooking/installed")
list(APPEND CMAKE_MODULE_PATH "${seastar_dir}/cmake")

find_package(Seastar REQUIRED)

FetchContent_Declare(
        sparse_map
        GIT_REPOSITORY https://github.com/Tessil/sparse-map.git
        GIT_TAG        v0.6.2
        GIT_SHALLOW TRUE)

FetchContent_MakeAvailable(sparse_map)

# Include SIMDJson for JSON parsing
FetchContent_Declare(
        simdjson
        GIT_REPOSITORY https://github.com/simdjson/simdjson.git
        GIT_TAG        v0.8.1
        GIT_SHALLOW TRUE)

set(SIMDJSON_JUST_LIBRARY ON CACHE INTERNAL "")
set(SIMDJSON_BUILD_STATIC ON CACHE INTERNAL "")

FetchContent_MakeAvailable(simdjson)

# Include RoaringBitmaps for compressed bitmaps
FetchContent_Declare(
        roaring
        GIT_REPOSITORY https://github.com/RoaringBitmap/CRoaring.git
        GIT_TAG v0.2.66
        GIT_SHALLOW TRUE)

set(ENABLE_ROARING_TESTS OFF CACHE INTERNAL "")
set(ROARING_BUILD_STATIC ON CACHE INTERNAL "")

FetchContent_MakeAvailable(roaring)
FetchContent_GetProperties(roaring)
SET(CPP_ROARING_HEADERS ${roaring_SOURCE_DIR}/cpp/roaring64map.hh  ${roaring_SOURCE_DIR}/cpp/roaring.hh)
file(COPY ${CPP_ROARING_HEADERS} DESTINATION ${roaring_SOURCE_DIR}/include/roaring)

# find lua
SET(LuaJIT_FIND_VERSION 5.1)
SET(LUAJIT_INCLUDE_DIR /usr/local/lib)
include(cmake/FindLuaJIT.cmake)
if(LUAJIT_FOUND)
    set(USED_LUA_LIBRARIES, LUAJIT_LIBRARIES)
endif()

# Where any include files are
include_directories(src/lib/graph /usr/include/luajit-2.1 /usr/local/include/luajit-2.1 src/lib/sol)

# Subdirectories to build
add_subdirectory(src/lib/graph)
add_subdirectory(src/lib/sol)

add_executable(triton src/main/main.cpp
        src/lib/seastar/stop_signal.hh
        src/main/server/handl.h
        src/main/server/Nodes.cpp
        src/main/server/Nodes.h src/main/server/JSON.h src/main/server/Degrees.cpp src/main/server/Degrees.h
        src/main/server/NodeProperties.cpp src/main/server/NodeProperties.h src/main/server/Server.cpp
        src/main/server/Server.h src/main/server/RelationshipProperties.cpp src/main/server/RelationshipProperties.h
        src/main/server/Relationships.cpp src/main/server/Relationships.h src/main/server/Lua.h src/main/server/Lua.cpp src/main/server/Neighbors.cpp src/main/server/Neighbors.h)

target_link_libraries(triton PRIVATE ${LUA_LIBRARIES} Graph /usr/local/lib/libluajit-5.1.a)
target_link_libraries(Graph Seastar::seastar)
target_link_libraries(Graph sparse_map)
target_link_libraries(Graph simdjson)
target_link_libraries(Graph roaring)

add_custom_command(
        TARGET triton POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy
        ${CMAKE_SOURCE_DIR}/src/lua/json.lua
        ${CMAKE_CURRENT_BINARY_DIR}/src/lua/json.lua)

# for tests
add_custom_command(
        TARGET triton POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy
        ${CMAKE_SOURCE_DIR}/src/lua/json.lua
        ${CMAKE_CURRENT_BINARY_DIR}/src/test/src/lua/json.lua)