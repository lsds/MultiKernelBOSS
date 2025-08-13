cmake_minimum_required(VERSION 3.10)
project(optimizer LANGUAGES CXX)

# Build with C++17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Get all source files from src directory
file(GLOB_RECURSE SOURCES "src/*.cpp")

# Get all header files from include directory
file(GLOB_RECURSE HEADERS "include/*.hpp")

# Create the library
add_library(optimizer SHARED
    ${SOURCES}
)

# Add server_utils library
add_library(server_utils STATIC
    ${CMAKE_SOURCE_DIR}/query-explorer/server/src/unittest/CTestUtils.cpp
    ${CMAKE_SOURCE_DIR}/query-explorer/server/src/unittest/CSubqueryTestUtils.cpp
)

# Set position independent code for static library used in shared libraries
set_target_properties(server_utils PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)

target_include_directories(server_utils PUBLIC
    "${CMAKE_SOURCE_DIR}/query-explorer/server/include"
    "${CMAKE_SOURCE_DIR}/query-explorer/libgpdbcost/include"
    "${CMAKE_SOURCE_DIR}/query-explorer/libgpopt/include"
    "${CMAKE_SOURCE_DIR}/query-explorer/libnaucrates/include"
    "${CMAKE_SOURCE_DIR}/query-explorer/libgpos/include"
    "${CMAKE_BINARY_DIR}/query-explorer/libgpos/include"
    "${CMAKE_SOURCE_DIR}/query-explorer/libgpoptextender/include"
    ${XERCES_INCLUDE_DIRS}
)

target_link_libraries(server_utils
    gpopt
    gpoptextender
    gpdbcost
    naucrates
    gpos
    ${XERCES_LIBRARIES}
)



# Add include directories
target_include_directories(optimizer PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/BOSS/Source
    "${CMAKE_SOURCE_DIR}/query-explorer/libgpdbcost/include"
    "${CMAKE_SOURCE_DIR}/query-explorer/libgpopt/include"
    "${CMAKE_SOURCE_DIR}/query-explorer/libnaucrates/include"
    "${CMAKE_SOURCE_DIR}/query-explorer/libgpos/include"
    "${CMAKE_BINARY_DIR}/query-explorer/libgpos/include"
    "${CMAKE_SOURCE_DIR}/query-explorer/libgpoptextender/include"
    "${CMAKE_SOURCE_DIR}/integration/BOSSToCExpression/include"
    "${CMAKE_SOURCE_DIR}/integration/CExpressionToBOSS/include"
    "${CMAKE_SOURCE_DIR}/query-explorer/server/include"
    ${XERCES_INCLUDE_DIRS}
)

# Link against required libraries
target_link_libraries(optimizer
    gpopt
    gpdbcost
    naucrates
    gpos
    gpoptextender
    bosstoc_utils
    ctoboss_utils
    server_utils
    ${XERCES_LIBRARIES}
) 