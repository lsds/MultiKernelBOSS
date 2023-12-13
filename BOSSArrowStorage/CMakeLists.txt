cmake_minimum_required(VERSION 3.10)
project(BOSSArrowStorage)

# Configure CCache if available
find_program(CCACHE_EXECUTABLE ccache)
mark_as_advanced(CCACHE_EXECUTABLE)
if(CCACHE_EXECUTABLE)
  foreach(LANG C CXX)
    if(NOT DEFINED CMAKE_${LANG}_COMPILER_LAUNCHER AND NOT CMAKE_${LANG}_COMPILER MATCHES ".*/ccache")
      message(STATUS "Enabling ccache for ${LANG}")
      set(CMAKE_${LANG}_COMPILER_LAUNCHER ${CCACHE_EXECUTABLE} CACHE STRING "")
    endif()
  endforeach()
endif()

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
  CMAKE_CXX_FLAGS_SANITIZE		  CMAKE_EXE_LINKER_FLAGS_SANITIZE
  CMAKE_C_FLAGS_SANITIZE		  CMAKE_SHARED_LINKER_FLAGS_SANITIZE
  )

set(CMAKE_BUILD_TYPE "${CMAKE_BUILD_TYPE}" CACHE STRING
  "Choose the type of build, options are: None Debug Release RelWithDebInfo MinSizeRel Sanitize."
  FORCE)

############################### External Projects ###############################

set(CMAKE_POSITION_INDEPENDENT_CODE TRUE) # make sure all dependent libraries are compiled with that option

if(MSVC)
    set(BOSS_LINK_LIBRARY_PREFIX ${CMAKE_LINK_LIBRARY_PREFIX})
    set(BOSS_LINK_LIBRARY_SUFFIX ${CMAKE_LINK_LIBRARY_SUFFIX})
else()
    set(BOSS_LINK_LIBRARY_PREFIX ${CMAKE_SHARED_LIBRARY_PREFIX})
    set(BOSS_LINK_LIBRARY_SUFFIX ${CMAKE_SHARED_LIBRARY_SUFFIX})
endif(MSVC)


if("${CMAKE_BUILD_TYPE}" STREQUAL "MinSizeRel")
   set(ARROW_BUILD_TYPE "Release")
 elseif("${CMAKE_BUILD_TYPE}" STREQUAL "Sanitize")
   set(ARROW_BUILD_TYPE "Debug")
 else()
   set(ARROW_BUILD_TYPE ${CMAKE_BUILD_TYPE})
 endif()

if(WIN32)
   set(ARROW_STATIC_LIB_NAME "arrow_static")
   set(ARROW_ADDITIONAL_CMAKE_ARGS -DARROW_MIMALLOC=ON)
   # use arrow_bundled_dependencies only if MIMALLOC is ON
   set(ARROW_BUNDLED_DEPENDENCIES "arrow_bundled_dependencies")
 else()
   set(ARROW_STATIC_LIB_NAME "arrow")
   set(ARROW_ADDITIONAL_CMAKE_ARGS -DARROW_JEMALLOC=ON)
   # use arrow_bundled_dependencies only if JEMALLOC is ON
   set(ARROW_BUNDLED_DEPENDENCIES "arrow_bundled_dependencies")
 endif(WIN32)

 
 ExternalProject_Add(arrow
   URL https://github.com/apache/arrow/archive/apache-arrow-6.0.1.zip
   SOURCE_SUBDIR cpp
   DOWNLOAD_DIR $ENV{HOME}/.cmake-downloads/${CMAKE_PROJECT_NAME}
   CMAKE_ARGS -DARROW_BUILD_SHARED=OFF -DARROW_BUILD_STATIC=ON -DARROW_JSON=ON -DARROW_CSV=ON ${ARROW_ADDITIONAL_CMAKE_ARGS} -DCMAKE_INSTALL_PREFIX=${BOSSArrowStorage_BINARY_DIR}/deps $<IF:$<CONFIG:>,-DCMAKE_BUILD_TYPE=Debug,-DCMAKE_BUILD_TYPE=${ARROW_BUILD_TYPE}> -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}  -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER} -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS} -DCMAKE_C_FLAGS=${CMAKE_C_FLAGS} -DCMAKE_EXE_LINKER_FLAGS=${CMAKE_EXE_LINKER_FLAGS} -DCMAKE_LINKER=${CMAKE_LINKER} -DARROW_ENABLE_TIMING_TESTS=OFF -DARROW_DEPENDENCY_USE_SHARED=OFF -DARROW_ALTIVEC=OFF -DARROW_BOOST_USE_SHARED=OFF -DARROW_PROTOBUF_USE_SHARED=OFF -DARROW_THRIFT_USE_SHARED=OFF
   BUILD_BYPRODUCTS  ${BOSSArrowStorage_BINARY_DIR}/deps/lib/${CMAKE_SHARED_LIBRARY_PREFIX}${ARROW_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX} ${BOSSArrowStorage_BINARY_DIR}/deps/lib/${CMAKE_SHARED_LIBRARY_PREFIX}${ARROW_BUNDLED_DEPENDENCIES}${CMAKE_STATIC_LIBRARY_SUFFIX}
 )

# on Windows, expose all the arrow symbols with a .def file to specify the symbols to include in the executable library
if(WIN32)
  find_program(POWERSHELL_PATH NAMES powershell)
  # get the list of all the Arrow .obj files
  ExternalProject_Add_Step(arrow generateDefObjsList DEPENDEES install
      WORKING_DIRECTORY <BINARY_DIR>
      BYPRODUCTS arrow.def.objs
      COMMAND ${POWERSHELL_PATH} "get-childitem src/arrow/CMakeFiles/arrow_static.dir -recurse | where {$_.extension -eq '.obj'} | % {Write-Output $_.FullName} | Resolve-Path -Relative | % {Write-Output $_.Substring(2)} | out-file -encoding ASCII  -filepath arrow.def.objs")
  # inspect the symbols and them to the Arrow .def file
  ExternalProject_Add_Step(arrow generateDef
      WORKING_DIRECTORY <BINARY_DIR>
      DEPENDS arrow.def.objs
      BYPRODUCTS arrow.def
      COMMAND ${CMAKE_COMMAND} -E rm -f arrow.def && ${CMAKE_COMMAND} -E  __create_def arrow.def arrow.def.objs)
  # install the def file with the other Arrow dependencies
  ExternalProject_Add_Step(arrow installDef
      WORKING_DIRECTORY <BINARY_DIR>
      DEPENDS arrow.def
      BYPRODUCTS ${BOSSArrowStorage_BINARY_DIR}/deps/lib/arrow.def
      COMMAND ${CMAKE_COMMAND} -E copy arrow.def ${BOSSArrowStorage_BINARY_DIR}/deps/lib/arrow.def)
  set(ARROW_DEF_FILE ${BOSSArrowStorage_BINARY_DIR}/deps/lib/arrow.def)
endif(WIN32)


####################### Other Options and Library Dependencies #######################

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

#################################### Targets ####################################

if(MSVC)
  # not making any difference on Windows
  # and helps Visual Studio to parse it correctly as a target
  set(LibraryType SHARED)
else()
  set(LibraryType MODULE)
endif(MSVC)

set(ImplementationFiles Source/BOSSArrowStorageEngine.cpp ${ARROW_DEF_FILE})
set(TestFiles Tests/BOSSArrowStorageTests.cpp)

# Plugin
add_library(BOSSArrowStorage ${LibraryType} ${ImplementationFiles})
add_dependencies(BOSSArrowStorage BOSS)

# Tests
add_executable(BOSSArrowStorageTests ${ImplementationFiles} ${TestFiles})
add_dependencies(BOSSArrowStorageTests catch2)
if(MSVC)
    target_compile_options(BOSSArrowStorageTests PUBLIC /bigobj)
    # On windows, we need to make sure to get the BOSS.dll in the right folder for the tests executable
    add_custom_command(TARGET BOSSArrowStorageTests POST_BUILD
      COMMAND
        ${CMAKE_COMMAND} -E copy_if_different "${BOSS_BINARY_DIR}/BOSS.dll" "${BOSSArrowStorage_BINARY_DIR}/"
    )
endif(MSVC)

list(APPEND AllTargets BOSSArrowStorage BOSSArrowStorageTests)

foreach(Target IN LISTS AllTargets)
  add_dependencies(${Target} arrow)
  target_compile_options(${Target} PUBLIC -DARROW_STATIC)
  if(NOT WIN32)
    target_link_libraries(${Target} dl)
  endif(NOT WIN32)
  set(ARROW_LIBRARY_FILE ${BOSSArrowStorage_BINARY_DIR}/deps/lib/${CMAKE_SHARED_LIBRARY_PREFIX}${ARROW_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX})
  if(ARROW_BUNDLED_DEPENDENCIES)
    set(ARROW_ADDITIONAL_LIBRARY_FILE ${BOSSArrowStorage_BINARY_DIR}/deps/lib/${CMAKE_SHARED_LIBRARY_PREFIX}${ARROW_BUNDLED_DEPENDENCIES}${CMAKE_STATIC_LIBRARY_SUFFIX})
  endif(ARROW_BUNDLED_DEPENDENCIES)
  if(NOT WIN32)
    # make sure to add all symbols when linking with Arrow library (potentially used by plugins)
    # also using -E option to expose all symbols with the symbol table when dynamically loading the plugins (https://stackoverflow.com/a/9445178)
    if(APPLE)
      target_link_libraries(${Target} "-Wl,-export_dynamic")
      target_link_libraries(${Target} "-Wl,-all_load")
      target_link_libraries(${Target} "${ARROW_LIBRARY_FILE}")
      target_link_libraries(${Target} "-Wl,-noall_load")
	  if(ARROW_ADDITIONAL_LIBRARY_FILE)
        target_link_libraries(${Target} "-Wl,-all_load")
        target_link_libraries(${Target} "${ARROW_ADDITIONAL_LIBRARY_FILE}")
        target_link_libraries(${Target} "-Wl,-noall_load")
      endif(ARROW_ADDITIONAL_LIBRARY_FILE)
    else()
      target_link_libraries(${Target} "-Wl,-export-dynamic")
      target_link_libraries(${Target} "-Wl,--whole-archive")
      target_link_libraries(${Target} "${ARROW_LIBRARY_FILE}")
      target_link_libraries(${Target} "-Wl,--no-whole-archive")
	  if(ARROW_ADDITIONAL_LIBRARY_FILE)
        target_link_libraries(${Target} "-Wl,--whole-archive")
        target_link_libraries(${Target} "${ARROW_ADDITIONAL_LIBRARY_FILE}")
        target_link_libraries(${Target} "-Wl,--no-whole-archive")
      endif(ARROW_ADDITIONAL_LIBRARY_FILE)
    endif()
  else()  
    target_link_libraries(${Target} "-WHOLEARCHIVE:${ARROW_LIBRARY_FILE}")
    if(ARROW_ADDITIONAL_LIBRARY_FILE)
	  target_link_libraries(${Target} "-WHOLEARCHIVE:${ARROW_ADDITIONAL_LIBRARY_FILE}")
    endif(ARROW_ADDITIONAL_LIBRARY_FILE)
    # for Windows, we need to generate an "import library" for the plugins to link against it
    # since dlls cannot contain any unresolved symbol
    set_target_properties(${Target} PROPERTIES ENABLE_EXPORTS 1)
  endif()

    target_link_libraries(${Target} Threads::Threads)
    set_target_properties(${Target} PROPERTIES POSITION_INDEPENDENT_CODE ON)
    if(MSVC)
        target_compile_options(${Target} PUBLIC "/Zc:__cplusplus")
        target_compile_options(${Target} PUBLIC "/EHsc")
        # to link against all needed symbols for loading BulKBOSS as a plugin
        target_link_libraries(${Target} ${BOSS_BINARY_DIR}/BOSS.lib)
    endif()
    set_target_properties(${Target} PROPERTIES CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON CXX_EXTENSIONS OFF)
    add_dependencies(${Target} BOSS)
    target_compile_options(${Target} PUBLIC -DARROW_STATIC)
    target_include_directories(${Target} SYSTEM PUBLIC ${BOSSArrowStorage_BINARY_DIR}/deps/include)
    target_include_directories(${Target} SYSTEM PUBLIC ${BOSS_BINARY_DIR}/deps/include)
    target_include_directories(${Target} PUBLIC ${BOSS_SOURCE_DIR}/Source)
    if(UNIX AND NOT APPLE)
        target_link_libraries(${Target} rt)
    else()
        # link against all needed symbols for loading BOSSArrowStorage as a plugin
        target_link_libraries(${Target} ${BOSS_BINARY_DIR}/${BOSS_LINK_LIBRARY_PREFIX}BOSS${BOSS_LINK_LIBRARY_SUFFIX})
    endif()
endforeach()

set_target_properties(BOSSArrowStorage PROPERTIES INSTALL_RPATH_USE_LINK_PATH TRUE)
install(TARGETS BOSSArrowStorage LIBRARY DESTINATION lib)
install(TARGETS BOSSArrowStorageTests RUNTIME DESTINATION bin)
