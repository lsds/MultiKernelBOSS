cmake_minimum_required(VERSION 3.10)
project(BOSS)

########################### C++ Compiler Requirements ###########################

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS "8.0")
    message(FATAL_ERROR "Your GCC version has this bug which affects this project: https://gcc.gnu.org/bugzilla/show_bug.cgi?id=68177")
  endif()
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

include(ExternalProject)

ExternalProject_Add(catch2
	URL https://github.com/catchorg/Catch2/archive/v2.13.6.tar.gz
  DOWNLOAD_DIR $ENV{HOME}/.cmake-downloads/${CMAKE_PROJECT_NAME}
	CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${BOSS_BINARY_DIR}/deps -DCATCH_BUILD_TESTING=NO -DBUILD_TESTING=NO -DCATCH_ENABLE_WERROR=NO -DCATCH_INSTALL_DOCS=NO -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}  -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER} -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
  )

########################## Other Library Dependencies ##########################

set(VTune_DIR "" CACHE PATH "Where to look for VTune installation")
find_path(ITT_NOTIFY_INCLUDE_DIR "ittnotify.h" HINTS ${VTune_DIR}/include)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

############################### Executable Targets ###############################

set(CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS ON)

set(ImplementationFiles ${BOSS_SOURCE_DIR}/Source/BOSS.cpp)
set(TestFiles ${BOSS_SOURCE_DIR}/Tests/BOSSTests.cpp ${BOSS_SOURCE_DIR}/Tests/CAPITests.cpp)

# BOSS
add_library(BOSS SHARED ${ImplementationFiles})

# Tests
add_executable(Tests ${ImplementationFiles} ${TestFiles})
add_dependencies(Tests catch2)
if(WIN32)
    target_compile_options(Tests PUBLIC /bigobj)
endif(WIN32)

list(APPEND AllTargets BOSS Tests)

############################ Targets Common Properties ############################

foreach(Target IN LISTS AllTargets)
  target_link_libraries(${Target} Threads::Threads)
  set_target_properties(${Target} PROPERTIES CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON CXX_EXTENSIONS OFF)
  if(MSVC)
    target_compile_options(${Target} PUBLIC "/Zc:__cplusplus")
    target_compile_options(${Target} PUBLIC "/EHsc")
  endif()
  target_include_directories(${Target} SYSTEM PUBLIC ${BOSS_BINARY_DIR}/deps/include)
  if(UNIX AND NOT APPLE)
    target_link_libraries(${Target} rt)
    target_link_libraries(${Target} dl)
  endif() 
endforeach()


#################################### Install ###################################
set_target_properties(BOSS PROPERTIES PUBLIC_HEADER "${CMAKE_CURRENT_SOURCE_DIR}/Source/BOSS.hpp;${CMAKE_CURRENT_SOURCE_DIR}/Source/Engine.hpp;${CMAKE_CURRENT_SOURCE_DIR}/Source/Expression.hpp;${CMAKE_CURRENT_SOURCE_DIR}/Source/ExpressionUtilities.hpp;${CMAKE_CURRENT_SOURCE_DIR}/Source/Utilities.hpp;${CMAKE_CURRENT_SOURCE_DIR}/Source/Algorithm.hpp")

if(WIN32)
  install(TARGETS BOSS PUBLIC_HEADER DESTINATION include)
  # on Windows, one "import library" for each executable target is needed
  # to link the plugins without leaving any unresolved symbol in the dll
  foreach(Target IN LISTS AllTargets)
    install(FILES ${BOSS_BINARY_DIR}/${Target}.lib DESTINATION lib OPTIONAL)
  endforeach()
else()
  # other OS only need to expose the BOSS library
  install(TARGETS BOSS PUBLIC_HEADER DESTINATION include LIBRARY DESTINATION lib)
endif(WIN32)
install(TARGETS Tests RUNTIME DESTINATION bin)
install(FILES Source/Shims/BOSS.rkt Server/Server.rkt DESTINATION bin)
