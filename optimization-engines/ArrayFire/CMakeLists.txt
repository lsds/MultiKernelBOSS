cmake_minimum_required(VERSION 3.10)
project(BOSSArrayFire LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Sources
file(GLOB_RECURSE SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")

# Library
add_library(BOSSArrayFire SHARED ${SOURCES})

# Public headers for this lib
target_include_directories(BOSSArrayFire PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${CMAKE_SOURCE_DIR}/BOSS/Source
  ${CMAKE_SOURCE_DIR}/query-explorer/libgpdbcost/include
  ${CMAKE_SOURCE_DIR}/query-explorer/libgpopt/include
  ${CMAKE_SOURCE_DIR}/query-explorer/libgpoptextender/include
  ${CMAKE_SOURCE_DIR}/query-explorer/libnaucrates/include
  ${CMAKE_SOURCE_DIR}/query-explorer/libgpos/include
  ${CMAKE_BINARY_DIR}/query-explorer/libgpos/include
  ${CMAKE_SOURCE_DIR}/integration/CExpressionToBOSS/include
  ${CMAKE_SOURCE_DIR}/integration/BOSSToCExpression/include
)

# IMPORTANT: depend on Velox via target link — enforces build order and
# propagates Velox's PUBLIC include dirs so #include "velox.hpp" works.
target_link_libraries(BOSSArrayFire PUBLIC BOSSVelox)

# If BOSSArrayFire also directly uses these libs, keep them; otherwise omit.
target_link_libraries(BOSSArrayFire PRIVATE
  gpopt gpoptextender gpdbcost naucrates gpos ctoboss_utils bosstoc_utils
)

set_target_properties(BOSSArrayFire PROPERTIES ENABLE_EXPORTS ON)
