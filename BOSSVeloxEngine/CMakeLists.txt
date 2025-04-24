cmake_minimum_required(VERSION 3.10)
project(BOSSVeloxEngine)

############################## Custom build modes ###############################
set(CMAKE_CXX_FLAGS_SANITIZE "-fno-omit-frame-pointer -fsanitize=address,signed-integer-overflow,null,alignment,bounds,function,return,vla-bound -O0 -g"
        CACHE STRING
        "Flags used by the C++ compiler during Sanitize builds."
        FORCE)
set(CMAKE_C_FLAGS_SANITIZE "-fno-omit-frame-pointer -fsanitize=address,signed-integer-overflow,null,alignment,bounds,function,return,vla-bound -O0 -g"
        CACHE STRING
        "Flags used by the C compiler during Sanitize builds."
        FORCE)
set(CMAKE_EXE_LINKER_FLAGS_SANITIZE
        ${CMAKE_EXE_LINKER_FLAGS_DEBUG} CACHE STRING
        "Flags used for linking binaries during Sanitize builds."
        FORCE)
set(CMAKE_SHARED_LINKER_FLAGS_SANITIZE
        ${CMAKE_SHARED_LINKER_FLAGS_DEBUG} CACHE STRING
        "Flags used by the shared libraries linker during Sanitize builds."
        FORCE)
mark_as_advanced(
        CMAKE_CXX_FLAGS_SANITIZE CMAKE_EXE_LINKER_FLAGS_SANITIZE
        CMAKE_C_FLAGS_SANITIZE CMAKE_SHARED_LINKER_FLAGS_SANITIZE
)

set(CMAKE_BUILD_TYPE "${CMAKE_BUILD_TYPE}" CACHE STRING
        "Choose the type of build, options are: None Debug Release RelWithDebInfo MinSizeRel Sanitize."
        FORCE)

############################### External Projects ###############################
if (NOT VELOX_DIRS)
    message(FATAL_ERROR "You need to pre-build Velox Project and set VELOX_DIRS")
endif ()
set(CMAKE_MODULE_PATH ${VELOX_DIRS} ${CMAKE_MODULE_PATH})

set(CMAKE_POSITION_INDEPENDENT_CODE TRUE) # make sure all dependent libraries are compiled with that option

#################################### Targets ####################################

set(ImplementationFiles Source/BOSSVeloxEngine.cpp Source/BridgeVelox.cpp Source/BossConnector.cpp
        ${VELOX_DIRS}/velox/exec/tests/utils/PlanBuilder.cpp
        ${VELOX_DIRS}/velox/exec/tests/utils/Cursor.cpp)

if (MSVC)
    # not making any difference on Windows
    # and helps Visual Studio to parse it correctly as a target
    set(LibraryType SHARED)
else ()
    set(LibraryType MODULE)
endif (MSVC)

add_library(BOSSVeloxEngine ${LibraryType} ${ImplementationFiles})

target_include_directories(BOSSVeloxEngine PUBLIC
        ${VELOX_DIRS}
        ${VELOX_DIRS}/velox/
        ${VELOX_DIRS}/velox/external/xxhash/
        ${VELOX_DIRS}/third_party/
        ${VELOX_DIRS}/third_party/xsimd/
        ${VELOX_DIRS}/third_party/xsimd/include/
        ${BOSSVeloxEngine_BINARY_DIR}/../_deps/protobuf-src/src/)

set(VELOX_BUILD_TESTING ON)
set(VELOX_CODEGEN_SUPPORT OFF)
set(VELOX_ENABLE_HIVE_CONNECTOR ON)
set(VELOX_ENABLE_TPCH_CONNECTOR OFF)
set(VELOX_ENABLE_SPARK_FUNCTIONS OFF)

add_subdirectory(${VELOX_DIRS} velox)
target_link_libraries(
        BOSSVeloxEngine
        velox_tpch_connector
        velox_hive_connector
        velox_dwio_dwrf_proto
        velox_aggregates
        velox_functions_prestosql
        velox_parse_parser
        atomic
)
target_compile_options(BOSSVeloxEngine PUBLIC -DARROW_STATIC -msse4.2 -DFOLLY_CFG_NO_COROUTINES -DFOLLY_HAS_COROUTINES=0)
if (UNIX AND NOT APPLE)
    target_link_libraries(BOSSVeloxEngine uuid)
endif ()
if (NOT WIN32)
    target_link_libraries(BOSSVeloxEngine dl)
endif (NOT WIN32)

set_property(TARGET BOSSVeloxEngine PROPERTY CXX_STANDARD 20)
if (MSVC)
    target_compile_options(BOSSVeloxEngine PUBLIC "/Zc:__cplusplus")
    target_compile_options(BOSSVeloxEngine PUBLIC "/EHsc")
    # to link against all needed symbols for loading BulKBOSS as a plugin
    target_link_libraries(${targetName} ${BOSS_BINARY_DIR}/BOSS.lib)
endif (MSVC)
target_include_directories(BOSSVeloxEngine SYSTEM PUBLIC ${BOSSVeloxEngine_BINARY_DIR}/deps/include)
add_dependencies(BOSSVeloxEngine BOSS)

set_target_properties(BOSSVeloxEngine PROPERTIES INSTALL_RPATH_USE_LINK_PATH TRUE)
install(TARGETS BOSSVeloxEngine LIBRARY DESTINATION lib)
