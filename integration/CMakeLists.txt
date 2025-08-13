cmake_minimum_required(VERSION 3.10)
project(integration_lib LANGUAGES CXX)

# Build the integration library with C++17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find Xerces package
# Use the same configuration as in query-explorer
include(${CMAKE_SOURCE_DIR}/query-explorer/cmake/FindXerces.cmake)
include_directories(${XERCES_INCLUDE_DIRS})

# Add BOSSToCExpression subdirectory
add_subdirectory(BOSSToCExpression)
add_subdirectory(CExpressionToBOSS)
add_subdirectory(Optimizer)


add_library(integration_lib SHARED
        main.cpp
)

add_executable(integration_exe main.cpp)

# If BOSS doesn't have its own library, but you need headers:
target_include_directories(integration_lib PRIVATE
        ${CMAKE_SOURCE_DIR}/BOSS/Source
        "${CMAKE_SOURCE_DIR}/query-explorer/libgpdbcost/include"
        "${CMAKE_SOURCE_DIR}/query-explorer/libgpopt/include"
        "${CMAKE_SOURCE_DIR}/query-explorer/libgpoptextender/include"
        "${CMAKE_SOURCE_DIR}/query-explorer/libnaucrates/include"
        "${CMAKE_SOURCE_DIR}/query-explorer/libgpos/include"
        "${CMAKE_BINARY_DIR}/query-explorer/libgpos/include"
        "${CMAKE_SOURCE_DIR}/integration/BOSSToCExpression/include"
        "${CMAKE_SOURCE_DIR}/integration/CExpressionToBOSS/include"
        "${CMAKE_SOURCE_DIR}/integration/Optimizer/include"
)

target_link_libraries(integration_lib
        gpopt
        gpoptextender
        gpdbcost
        naucrates
        gpos
        bosstoc_utils
        ctoboss_utils
        optimizer)

target_include_directories(integration_exe PRIVATE
        ${CMAKE_SOURCE_DIR}/BOSS/Source
        "${CMAKE_SOURCE_DIR}/query-explorer/libgpdbcost/include"
        "${CMAKE_SOURCE_DIR}/query-explorer/libgpopt/include"
        "${CMAKE_SOURCE_DIR}/query-explorer/libgpoptextender/include"
        "${CMAKE_SOURCE_DIR}/query-explorer/libnaucrates/include"
        "${CMAKE_SOURCE_DIR}/query-explorer/libgpos/include"
        "${CMAKE_BINARY_DIR}/query-explorer/libgpos/include"
        "${CMAKE_SOURCE_DIR}/integration/BOSSToCExpression/include"
        "${CMAKE_SOURCE_DIR}/integration/CExpressionToBOSS/include"
        "${CMAKE_SOURCE_DIR}/integration/Optimizer/include"
)
target_link_libraries(integration_exe PRIVATE
        gpopt
        gpoptextender
        gpdbcost
        naucrates
        gpos
        bosstoc_utils
        ctoboss_utils
        optimizer)

