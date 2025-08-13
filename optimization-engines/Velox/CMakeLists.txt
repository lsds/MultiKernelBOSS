cmake_minimum_required(VERSION 3.10)
project(BOSSVelox LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Sources
file(GLOB_RECURSE SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")

# Library
add_library(BOSSVelox SHARED ${SOURCES})

# Public headers (propagate to dependents)
target_include_directories(BOSSVelox PUBLIC
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

# Link its own deps (private is fine for a shared lib)
target_link_libraries(BOSSVelox PRIVATE
  gpopt gpoptextender gpdbcost naucrates gpos ctoboss_utils bosstoc_utils
)

# Export symbols as needed
set_target_properties(BOSSVelox PROPERTIES ENABLE_EXPORTS ON)
