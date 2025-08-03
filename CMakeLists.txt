#cmake -B [build directory] -S [source directory] -DCMAKE_TOOLCHAIN_FILE=D:/vcpkg/vcpkg/scripts/buildsystems/vcpkg.cmake

cmake_minimum_required(VERSION 3.10)

# project name
project(ChomoSyncer LANGUAGES CXX)

# C++ standard 17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Find the OpenSSL package
find_package(OpenSSL REQUIRED)

# Find the MONGOCXX package
if(WIN32)
    find_package(bsoncxx CONFIG REQUIRED)
    find_package(mongocxx CONFIG REQUIRED)
else()
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(MONGOCXX REQUIRED libmongocxx)
    pkg_check_modules(BSONCXX REQUIRED libbsoncxx)
    include_directories(${MONGOCXX_INCLUDE_DIRS} ${BSONCXX_INCLUDE_DIRS})
    link_directories(${MONGOCXX_LIBRARY_DIRS} ${BSONCXX_LIBRARY_DIRS})
    add_definitions(${MONGOCXX_CFLAGS_OTHER} ${BSONCXX_CFLAGS_OTHER})

    message("BSONCXX_INCLUDE_DIRS = ${BSONCXX_INCLUDE_DIRS}")
    message("MONGOCXX_INCLUDE_DIRS = ${MONGOCXX_INCLUDE_DIRS}")
    message("BSONCXX_LIBRARIES = ${BSONCXX_LIBRARIES}")
    message("MONGOCXX_LIBRARIES = ${MONGOCXX_LIBRARIES}")
endif()

# Find boost package
find_package(Boost REQUIRED COMPONENTS system thread)
message("Boost_INCLUDE_DIRS = ${Boost_INCLUDE_DIRS}")
message("Boost_LIBRARIES = ${Boost_LIBRARIES}")

# Find hiredis package
find_package(hiredis CONFIG REQUIRED)
message(STATUS "Hiredis include directories: ${hiredis_INCLUDE_DIRS}")
message(STATUS "Hiredis library: ${hiredis_LIBRARIES}")

# Find nlohmann/json
find_package(nlohmann_json 3.2.0 REQUIRED)

# Add your CUDA source files and other source files
set(SOURCES
    src/main.cpp
)

file(GLOB_RECURSE SRC_FILES
    "src/config/*"
    "src/dataSync/*"
    "src/db/*"
    "src/dtos/*"
)
list(APPEND SOURCES ${SRC_FILES})

# Add source to this project's executable.
add_executable(ChomoSyncer ${SOURCES})

# for config file
configure_file(
    ${CMAKE_SOURCE_DIR}/config.ini
    ${CMAKE_BINARY_DIR}/config.ini
    COPYONLY
)

# Include mongo cxx
if(WIN32)
    target_link_libraries(ChomoSyncer PRIVATE $<IF:$<TARGET_EXISTS:mongo::bsoncxx_static>,mongo::bsoncxx_static,mongo::bsoncxx_shared>)
    target_link_libraries(ChomoSyncer PRIVATE $<IF:$<TARGET_EXISTS:mongo::mongocxx_static>,mongo::mongocxx_static,mongo::mongocxx_shared>)

else()
    target_include_directories(ChomoSyncer PRIVATE ${BSONCXX_INCLUDE_DIRS})
    target_include_directories(ChomoSyncer PRIVATE ${MONGOCXX_INCLUDE_DIRS})
    target_link_libraries(ChomoSyncer PRIVATE ${BSONCXX_LIBRARIES} ${MONGOCXX_LIBRARIES})
endif()

# Include boost headers and link boost library
include_directories(${Boost_INCLUDE_DIRS})
target_link_libraries(ChomoSyncer PRIVATE ${Boost_LIBRARIES} OpenSSL::SSL OpenSSL::Crypto)

# include hiredis headers and link hiredis library
target_include_directories(ChomoSyncer PRIVATE ${hiredis_INCLUDE_DIRS})
target_link_libraries(ChomoSyncer PRIVATE hiredis::hiredis)

# Include nlohmann/json headers, header only library
target_link_libraries(ChomoSyncer PRIVATE nlohmann_json::nlohmann_json)

# Add src to path
include_directories(${CMAKE_SOURCE_DIR}/src)

# platform specific settings
if(WIN32)
    # Windows specific settings
    add_definitions(-D_WIN32_WINNT=0x0601) # Windows 7 and later
elseif(UNIX)
    # Linux/Unix
    set(THREADS_PREFER_PTHREAD_FLAG ON)
    find_package(Threads REQUIRED)
    target_link_libraries(ChomoSyncer PRIVATE Threads::Threads)
endif()
