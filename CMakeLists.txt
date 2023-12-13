cmake_minimum_required(VERSION 3.10)
project(MultiKernelBenchmarks)

############################## Custom build modes ###############################

if(NOT MSVC)
    set(DEFAULT_SANITIZER_COMPILER_FLAGS "-fno-omit-frame-pointer -fsanitize=address,signed-integer-overflow,null,alignment,bounds,function,return,vla-bound -O2 -g")
else()
    set(DEFAULT_SANITIZER_COMPILER_FLAGS "/fsanitize=address /MD /Zi /O2 /Ob1 /DNDEBUG")
endif()

set(CMAKE_CXX_FLAGS_SANITIZE ${DEFAULT_SANITIZER_COMPILER_FLAGS}
  CACHE STRING
  "Flags used by the C++ compiler during Sanitize builds."
  FORCE)
set(CMAKE_C_FLAGS_SANITIZE ${DEFAULT_SANITIZER_COMPILER_FLAGS}
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
  CMAKE_CXX_FLAGS_SANITIZE		  CMAKE_EXE_LINKER_FLAGS_SANITIZE
  CMAKE_C_FLAGS_SANITIZE		  CMAKE_SHARED_LINKER_FLAGS_SANITIZE
  )

set(CMAKE_BUILD_TYPE "${CMAKE_BUILD_TYPE}" CACHE STRING
  "Choose the type of build, options are: None Debug Release RelWithDebInfo MinSizeRel Sanitize."
  FORCE)

if(MSVC)
  # accurate profiling (closer to Release) in RelWithDebInfo: enable automatic inlining
  string(REPLACE "/Ob1" "/Ob2" CMAKE_C_FLAGS_RELWITHDEBINFO_INIT "${CMAKE_C_FLAGS_RELWITHDEBINFO_INIT}")
  string(REPLACE "/Ob1" "/Ob2" CMAKE_CXX_FLAGS_RELWITHDEBINFO_INIT "${CMAKE_CXX_FLAGS_RELWITHDEBINFO_INIT}")
  # better performances in Release
  string(REPLACE "/Ob2" "/Ob3" CMAKE_C_FLAGS_RELEASE_INIT "${CMAKE_C_FLAGS_RELEASE_INIT}")
  string(REPLACE "/Ob2" "/Ob3" CMAKE_CXX_FLAGS_RELEASE_INIT "${CMAKE_CXX_FLAGS_RELEASE_INIT}")
endif(MSVC)

############################### External Projects ###############################

include(ExternalProject)

ExternalProject_Add(googlebenchmark
	URL "https://github.com/google/benchmark/archive/refs/tags/v1.8.3.zip"
  DOWNLOAD_DIR $ENV{HOME}/.cmake-downloads/${CMAKE_PROJECT_NAME}
		CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${MultiKernelBenchmarks_BINARY_DIR}/deps -DBENCHMARK_DOWNLOAD_DEPENDENCIES=ON -DBENCHMARK_ENABLE_GTEST_TESTS=OFF $<IF:$<CONFIG:>,,-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}>
                   -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}  -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER} -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
                   -DCMAKE_CXX_FLAGS_RELEASE=${CMAKE_CXX_FLAGS_RELEASE} -DCMAKE_CXX_FLAGS_RELWITHDEBINFO=${CMAKE_CXX_FLAGS_RELWITHDEBINFO}
  BUILD_BYPRODUCTS ${MultiKernelBenchmarks_BINARY_DIR}/deps/lib/${CMAKE_SHARED_LIBRARY_PREFIX}benchmark${CMAKE_STATIC_LIBRARY_SUFFIX} ${MultiKernelBenchmarks_BINARY_DIR}/deps/lib/${CMAKE_SHARED_LIBRARY_PREFIX}benchmark_main${CMAKE_STATIC_LIBRARY_SUFFIX}
  )
  
# MonetDB
ExternalProject_Add(monetdb
  GIT_REPOSITORY https://github.com/MonetDB/MonetDB
  GIT_TAG Jun2023_SP2_release
  GIT_SHALLOW ON
  DOWNLOAD_DIR $ENV{HOME}/.cmake-downloads/${CMAKE_PROJECT_NAME}
  CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${MultiKernelBenchmarks_BINARY_DIR}/deps $<IF:$<CONFIG:>,,-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}>
    -DCMAKE_CXX_FLAGS_RELEASE=${CMAKE_CXX_FLAGS_RELEASE} -DCMAKE_CXX_FLAGS_RELWITHDEBINFO=${CMAKE_CXX_FLAGS_RELWITHDEBINFO}
    -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}  -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER} -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
    -DCMAKE_C_FLAGS=${CMAKE_C_FLAGS} -DCMAKE_EXE_LINKER_FLAGS=${CMAKE_EXE_LINKER_FLAGS} -DCMAKE_LINKER=${CMAKE_LINKER}
    -DTESTING=OFF -DODBC=OFF
  BUILD_COMMAND "${CMAKE_COMMAND}" --build . --target monetdbe
  BUILD_BYPRODUCTS ${MultiKernelBenchmarks_BINARY_DIR}/deps/lib/${CMAKE_SHARED_LIBRARY_PREFIX}monetdbe${EXTERNAL_PROJECTS_LIB_SUFFIX}
)

if(WIN32)
  ExternalProject_Add_Step(monetdb copyDLL
      DEPENDEES install
      COMMAND ${CMAKE_COMMAND} -E copy <BINARY_DIR>/tools/monetdbe/${CMAKE_SHARED_LIBRARY_PREFIX}monetdbe${CMAKE_SHARED_LIBRARY_SUFFIX}
          ${MultiKernelBenchmarks_BINARY_DIR}/
      BYPRODUCTS ${MultiKernelBenchmarks_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}monetdbe${CMAKE_SHARED_LIBRARY_SUFFIX})
endif(WIN32)

# DuckDB
if(WIN32)
  set(DuckDB_CXX_FLAGS "${CMAKE_CXX_FLAGS} /bigobj")
else()
  set(DuckDB_CXX_FLAGS ${CMAKE_CXX_FLAGS})
endif(WIN32)
ExternalProject_Add(duckdblib
  GIT_REPOSITORY https://github.com/duckdb/duckdb.git
  GIT_TAG v0.8.1
  GIT_SHALLOW ON
  DOWNLOAD_DIR $ENV{HOME}/.cmake-downloads/${CMAKE_PROJECT_NAME}
  CMAKE_ARGS -DBUILD_SHELL=FALSE -DBUILD_UNITTESTS=FALSE -DBUILD_TPCH_EXTENSION=FALSE -DENABLE_SANITIZER=FALSE -DENABLE_UBSAN=FALSE
    -DCMAKE_CXX_FLAGS_RELEASE=${CMAKE_CXX_FLAGS_RELEASE} -DCMAKE_CXX_FLAGS_RELWITHDEBINFO=${CMAKE_CXX_FLAGS_RELWITHDEBINFO}
    -DCMAKE_INSTALL_PREFIX:PATH=${MultiKernelBenchmarks_BINARY_DIR}/deps $<IF:$<CONFIG:>,,-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}>
    -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}  -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER} -DCMAKE_CXX_FLAGS=${DuckDB_CXX_FLAGS}
    -DCMAKE_C_FLAGS=${CMAKE_C_FLAGS} -DCMAKE_EXE_LINKER_FLAGS=${CMAKE_EXE_LINKER_FLAGS} -DCMAKE_LINKER=${CMAKE_LINKER}
  BUILD_BYPRODUCTS ${MultiKernelBenchmarks_BINARY_DIR}/deps/lib/${CMAKE_SHARED_LIBRARY_PREFIX}duckdb${EXTERNAL_PROJECTS_LIB_SUFFIX}
)
if(WIN32)
  ExternalProject_Add_Step(duckdblib copyDLL
      DEPENDEES install
      COMMAND ${CMAKE_COMMAND} -E copy <BINARY_DIR>/src/${CMAKE_SHARED_LIBRARY_PREFIX}duckdb${CMAKE_SHARED_LIBRARY_SUFFIX}
          ${MultiKernelBenchmarks_BINARY_DIR}/
      BYPRODUCTS ${MultiKernelBenchmarks_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}duckdb${CMAKE_SHARED_LIBRARY_SUFFIX})
endif(WIN32)

################################# BOSS and plugins #################################


add_subdirectory(BOSS)
include_directories(${BOSS_SOURCE_DIR}/Source)
link_directories(${BOSS_BINARY_DIR})

add_subdirectory(BOSSArrowStorage)
include_directories(${BOSSArrowStorage_SOURCE_DIR}/Source)
link_directories(${BOSSArrowStorage_BINARY_DIR})

add_subdirectory(BOSSArrayFireEngine)
include_directories(${BOSSArrayFireEngine_SOURCE_DIR}/Source)
link_directories(${BOSSArrayFireEngine_BINARY_DIR})

add_subdirectory(BOSSVeloxEngine)
include_directories(${BOSSVeloxEngine_SOURCE_DIR}/Source)
link_directories(${BOSSVeloxEngine_BINARY_DIR})

if(WIN32)
   add_custom_command(DEPENDS BOSS
        COMMAND ${CMAKE_COMMAND} -E copy ${BOSS_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}BOSS${CMAKE_SHARED_LIBRARY_SUFFIX}
          ${MultiKernelBenchmarks_BINARY_DIR}/
      OUTPUT ${MultiKernelBenchmarks_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}BOSS${CMAKE_SHARED_LIBRARY_SUFFIX})
   add_custom_command(DEPENDS BOSSArrowStorage
        COMMAND ${CMAKE_COMMAND} -E copy ${BOSSArrowStorage_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}BOSSArrowStorage${CMAKE_SHARED_LIBRARY_SUFFIX}
          ${MultiKernelBenchmarks_BINARY_DIR}/
      OUTPUT ${MultiKernelBenchmarks_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}BOSSArrowStorage${CMAKE_SHARED_LIBRARY_SUFFIX})
   add_custom_command(DEPENDS BOSSArrayFireEngineCPU
        COMMAND ${CMAKE_COMMAND} -E copy ${BOSSArrayFireEngine_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}BOSSArrayFireEngineCPU${CMAKE_SHARED_LIBRARY_SUFFIX}
          ${MultiKernelBenchmarks_BINARY_DIR}/
      OUTPUT ${MultiKernelBenchmarks_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}BOSSArrayFireEngineCPU${CMAKE_SHARED_LIBRARY_SUFFIX})
   add_custom_command(DEPENDS BOSSArrayFireEngineGPU
        COMMAND ${CMAKE_COMMAND} -E copy ${BOSSArrayFireEngine_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}BOSSArrayFireEngineGPU${CMAKE_SHARED_LIBRARY_SUFFIX}
          ${MultiKernelBenchmarks_BINARY_DIR}/
      OUTPUT ${MultiKernelBenchmarks_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}BOSSArrayFireEngineGPU${CMAKE_SHARED_LIBRARY_SUFFIX})
   add_custom_command(DEPENDS BOSSVeloxEngine
        COMMAND ${CMAKE_COMMAND} -E copy ${BOSSVeloxEngine_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}BOSSVeloxEngine${CMAKE_SHARED_LIBRARY_SUFFIX}
          ${MultiKernelBenchmarks_BINARY_DIR}/
      OUTPUT ${MultiKernelBenchmarks_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}BOSSVeloxEngine${CMAKE_SHARED_LIBRARY_SUFFIX})
   add_custom_target(copyBossDLLs ALL
    DEPENDS ${MultiKernelBenchmarks_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}BOSS${CMAKE_SHARED_LIBRARY_SUFFIX}
			${MultiKernelBenchmarks_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}BOSSArrowStorage${CMAKE_SHARED_LIBRARY_SUFFIX}
            ${MultiKernelBenchmarks_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}BOSSArrayFireEngineCPU${CMAKE_SHARED_LIBRARY_SUFFIX}
            ${MultiKernelBenchmarks_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}BOSSArrayFireEngineGPU${CMAKE_SHARED_LIBRARY_SUFFIX}
            ${MultiKernelBenchmarks_BINARY_DIR}/${CMAKE_SHARED_LIBRARY_PREFIX}BOSSVeloxEngine${CMAKE_SHARED_LIBRARY_SUFFIX}
	)
endif(WIN32)

####################### Other Options and Library Dependencies #######################

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# required to support GCC < 9 and libstdc++ < 9 (see https://stackoverflow.com/a/33159746)
# still would compile for GCC >= 9 and libstdc++ >= 9 but with unnecessary experimental fs symbols
# we can get rid of it once we do not want to support anymore OS with old libstd (e.g. ubuntu bionic)
if(NOT WIN32)
  set(CXX_FILESYSTEM_LIBRARIES "stdc++fs")
endif()

include_directories(${MultiKernelBenchmarks_BINARY_DIR}/deps/include)
link_directories(${MultiKernelBenchmarks_BINARY_DIR}/deps/lib)

################################ ITT module interface ################################

set(VTune_DIR "" CACHE PATH "Where to look for VTune installation")
find_path(ITT_NOTIFY_INCLUDE_DIR "ittnotify.h" HINTS ${VTune_DIR}/include)

add_library(ITTNotifySupport INTERFACE)
target_sources(ITTNotifySupport INTERFACE ${PROJECT_SOURCE_DIR}/Benchmarks/ITTNotifySupport.hpp)
target_include_directories(ITTNotifySupport INTERFACE ${PROJECT_SOURCE_DIR}/Benchmarks/)
if(ITT_NOTIFY_INCLUDE_DIR)
  message(VERBOSE "found itt notify header in ${ITT_NOTIFY_INCLUDE_DIR}")
  target_include_directories(ITTNotifySupport SYSTEM INTERFACE ${ITT_NOTIFY_INCLUDE_DIR})
  target_link_libraries(ITTNotifySupport INTERFACE ${ITT_NOTIFY_INCLUDE_DIR}/../lib64/libittnotify${CMAKE_STATIC_LIBRARY_SUFFIX})
  if(NOT WIN32)
    target_link_libraries(ITTNotifySupport dl)
  endif(NOT WIN32)
  if (NOT CMAKE_BUILD_TYPE STREQUAL "Release")
    target_compile_options(ITTNotifySupport INTERFACE -DWITH_ITT_NOTIFY)
  else()
    target_compile_options(ITTNotifySupport INTERFACE -DINTEL_NO_ITTNOTIFY_API)
  endif()
endif(ITT_NOTIFY_INCLUDE_DIR)

################################ Benchmark executable ################################

set(BenchmarkFiles Benchmarks/BOSSBenchmarks.cpp)

add_executable(Benchmarks ${ImplementationFiles} ${BenchmarkFiles})
target_link_libraries(Benchmarks Threads::Threads)
target_link_libraries(Benchmarks ${CXX_FILESYSTEM_LIBRARIES})
if(WIN32)
  target_link_libraries(Benchmarks shlwapi.lib)  
endif(WIN32)
target_link_libraries(Benchmarks ITTNotifySupport)
add_dependencies(Benchmarks googlebenchmark)
target_link_libraries(Benchmarks benchmark)
target_link_libraries(Benchmarks benchmark_main)
add_dependencies(Benchmarks monetdb duckdblib)
target_link_libraries(Benchmarks monetdbe duckdb)
add_dependencies(Benchmarks BOSS)
target_link_libraries(Benchmarks BOSS)
add_dependencies(Benchmarks BOSSArrowStorage)
add_dependencies(Benchmarks BOSSArrayFireEngineCPU)
add_dependencies(Benchmarks BOSSArrayFireEngineGPU)
add_dependencies(Benchmarks BOSSVeloxEngine)
if(WIN32)
  add_dependencies(Benchmarks copyBossDLLs)
endif(WIN32)

target_compile_options(Benchmarks PUBLIC -DBENCHMARK_STATIC_DEFINE)
target_link_libraries(Benchmarks Threads::Threads)
set_target_properties(Benchmarks PROPERTIES CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON CXX_EXTENSIONS OFF)
if(MSVC)
  target_compile_options(Benchmarks PUBLIC "/Zc:__cplusplus")
  target_compile_options(Benchmarks PUBLIC "/EHsc")
endif()
if(UNIX AND NOT APPLE)
  target_link_libraries(Benchmarks rt)
  target_link_libraries(Benchmarks dl)
endif() 

#################################### Install ###################################

set_target_properties(Benchmarks PROPERTIES INSTALL_RPATH_USE_LINK_PATH TRUE)
install(TARGETS Benchmarks RUNTIME DESTINATION bin)
if(WIN32)
  FILE(GLOB ALL_DEPENDENT_DLLS "${MultiKernelBenchmarks_BINARY_DIR}/*.dll")
  install(FILES ${ALL_DEPENDENT_DLLS} DESTINATION bin)
else()
  install(FILES ${Benchmarks_BINARY_DIR}/deps/lib/${CMAKE_SHARED_LIBRARY_PREFIX}duckdb${EXTERNAL_PROJECTS_LIB_SUFFIX} DESTINATION lib)
  install(FILES ${Benchmarks_BINARY_DIR}/deps/lib/${CMAKE_SHARED_LIBRARY_PREFIX}monetdbe${EXTERNAL_PROJECTS_LIB_SUFFIX} DESTINATION lib)  
endif(WIN32)
