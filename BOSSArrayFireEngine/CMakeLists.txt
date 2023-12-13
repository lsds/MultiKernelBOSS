cmake_minimum_required(VERSION 3.10)
project(BOSSArrayFireEngine)

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

include(ExternalProject)

ExternalProject_Add(ArrayFire
  URL https://github.com/arrayfire/arrayfire/releases/download/v3.8.3/arrayfire-full-3.8.3.tar.bz2
  BUILD_IN_SOURCE true # in ArrayFire 3.8.3, the cmake build fails (with span-lite) if not build in source
  DOWNLOAD_DIR $ENV{HOME}/.cmake-downloads/${CMAKE_PROJECT_NAME}
  CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${BOSSArrayFireEngine_BINARY_DIR}/deps $<IF:$<CONFIG:>,,-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}>
             -DCMAKE_VERBOSE_MAKEFILE=${CMAKE_VERBOSE_MAKEFILE}
             -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}  -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER} -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
             -DAF_WITH_LOGGING=OFF -DAF_WITH_IMAGEIO=OFF -DAF_BUILD_DOCS=OFF -DAF_BUILD_EXAMPLES=OFF -DAF_BUILD_OPENCL=OFF -DAF_BUILD_UNIFIED=OFF -DBUILD_TESTING=OFF
			 -DAF_BUILD_CUDA=ON
             -DAF_WITH_EXTERNAL_PACKAGES_ONLY=ON
    BUILD_BYPRODUCTS ${BOSSArrayFireEngine_BINARY_DIR}/deps/lib/${BOSS_LINK_LIBRARY_PREFIX}afcpu${BOSS_LINK_LIBRARY_SUFFIX}
                    ${BOSSArrayFireEngine_BINARY_DIR}/deps/lib/${BOSS_LINK_LIBRARY_PREFIX}afcuda${BOSS_LINK_LIBRARY_SUFFIX}
  )

#################################### Targets ####################################

if(MSVC)
  # not making any difference on Windows
  # and helps Visual Studio to parse it correctly as a target
  set(LibraryType SHARED)
else()
  set(LibraryType MODULE)
endif(MSVC)

if(MSVC)
  set(pluginInstallDir bin)
else()
  set(pluginInstallDir lib)
endif(MSVC)

set(ImplementationFiles Source/BOSSArrayFireEngine.cpp)

function(addEngineTarget targetName backend)
    add_library(${targetName} ${LibraryType} ${ImplementationFiles})
    add_dependencies(${targetName} BOSS)
    add_dependencies(${targetName} ArrayFire)

    set_target_properties(${targetName} PROPERTIES POSITION_INDEPENDENT_CODE ON)
    set_property(TARGET ${targetName} PROPERTY CXX_STANDARD 20)
    if(MSVC)
        target_compile_options(${targetName} PUBLIC "/Zc:__cplusplus")
        target_compile_options(${targetName} PUBLIC "/EHsc")
        target_compile_options(${targetName} PUBLIC "/bigobj")
        # to link against all needed symbols for loading BulKBOSS as a plugin
        target_link_libraries(${targetName} ${BOSS_BINARY_DIR}/BOSS.lib)
    endif()
    target_include_directories(${targetName} SYSTEM PUBLIC ${BOSSArrayFireEngine_BINARY_DIR}/deps/include)
    target_include_directories(${targetName} SYSTEM PUBLIC ${BOSS_BINARY_DIR}/deps/include)
    target_include_directories(${targetName} PUBLIC ${BOSS_SOURCE_DIR}/Source)
    if(UNIX AND NOT APPLE)
        target_link_libraries(${targetName} rt)
    endif()
    target_link_libraries(${targetName} ${BOSSArrayFireEngine_BINARY_DIR}/deps/lib/${BOSS_LINK_LIBRARY_PREFIX}${backend}${BOSS_LINK_LIBRARY_SUFFIX})

    add_custom_command(TARGET ${targetName} POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            ${BOSSArrayFireEngine_BINARY_DIR}/deps/lib/${CMAKE_SHARED_LIBRARY_PREFIX}${backend}${CMAKE_SHARED_LIBRARY_SUFFIX} $<TARGET_FILE_DIR:${targetName}>)

    set_target_properties(${targetName} PROPERTIES INSTALL_RPATH_USE_LINK_PATH TRUE)	
    install(TARGETS ${targetName} LIBRARY DESTINATION lib)
    install(DIRECTORY ${BOSSArrayFireEngine_BINARY_DIR}/deps/lib/ DESTINATION ${pluginInstallDir}
	    FILES_MATCHING PATTERN "${CMAKE_SHARED_LIBRARY_PREFIX}${backend}${CMAKE_SHARED_LIBRARY_SUFFIX}*"
    )
endfunction()

addEngineTarget(BOSSArrayFireEngineCPU afcpu)
addEngineTarget(BOSSArrayFireEngineGPU afcuda)
