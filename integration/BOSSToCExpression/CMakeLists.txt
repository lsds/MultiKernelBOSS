cmake_minimum_required(VERSION 3.10)
project(bosstoc_utils LANGUAGES CXX)

# Build with C++17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Get all source files from src directory
file(GLOB_RECURSE SOURCES "src/*.cpp")

# Get all header files from include directory
file(GLOB_RECURSE HEADERS "include/*.hpp")

# Create the library
add_library(bosstoc_utils SHARED
    ${SOURCES}
)

# Add include directories
target_include_directories(bosstoc_utils PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/BOSS/Source
    "${CMAKE_SOURCE_DIR}/query-explorer/libgpdbcost/include"
    "${CMAKE_SOURCE_DIR}/query-explorer/libgpopt/include"
    "${CMAKE_SOURCE_DIR}/query-explorer/libnaucrates/include"
    "${CMAKE_SOURCE_DIR}/query-explorer/libgpos/include"
    "${CMAKE_BINARY_DIR}/query-explorer/libgpos/include"
    "${CMAKE_SOURCE_DIR}/query-explorer/server/include"
)

# Link against required libraries
target_link_libraries(bosstoc_utils
    gpopt
    gpdbcost
    naucrates
    gpos
    server_utils
) 