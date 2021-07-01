set(HEADER_FILES
        sol.hpp
        config.hpp
        forward.hpp
        )

add_library(sol2 ${HEADER_FILES})
set_target_properties(sol2 PROPERTIES LINKER_LANGUAGE CXX)