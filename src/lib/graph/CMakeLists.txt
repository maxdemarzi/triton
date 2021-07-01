set(HEADER_FILES
        Graph.h
        )

set(SOURCE_FILES
        Graph.cpp
        utilities/csvmonkey.hpp
        utilities/StringUtils.h
        Ids.cpp Ids.h Types.cpp Types.h Direction.h Node.cpp Node.h Relationship.cpp Relationship.h Shard.h Shard.cpp
        Property.cpp Property.h Group.cpp Group.h)

add_library(Graph ${SOURCE_FILES} ${HEADER_FILES})