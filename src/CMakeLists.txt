cmake_minimum_required (VERSION 2.6)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -fsanitize=address -Wall -Wextra -pedantic")

include_directories(${PROJECT_SRC_DIR}/include)
add_library(database Relation.cpp Parser.cpp Utils.cpp)
target_include_directories(database PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
    PRIVATE src)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/")

set(SOURCE_FILES Relation.cpp Parser.cpp Utils.cpp)
add_executable(Joiner Joiner.cpp ${SOURCE_FILES})
target_link_libraries(Joiner database)

# Interactive command line tool to translate our query format to SQL queries
add_executable(Query2SQL Query2SQL.cpp ${SOURCE_FILES})

