cmake_minimum_required(VERSION 3.10)

# project name
project(RedisTest LANGUAGES CXX)

# C++ standard 17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# find hiredis package
find_package(hiredis CONFIG REQUIRED)
message(STATUS "Hiredis include directories: ${hiredis_INCLUDE_DIRS}")
message(STATUS "Hiredis library: ${hiredis_LIBRARIES}")

# add executable
add_executable(RedisTest ./src/main.cpp)

# include hiredis headers
target_include_directories(RedisTest PRIVATE ${hiredis_INCLUDE_DIRS})

# link hiredis library
target_link_libraries(RedisTest PRIVATE hiredis::hiredis)

# platform specific settings
if(WIN32)
    # Windows specific settings
    add_definitions(-D_WIN32_WINNT=0x0601) # Windows 7 and later
elseif(UNIX)
    # Linux/Unix
    set(THREADS_PREFER_PTHREAD_FLAG ON)
    find_package(Threads REQUIRED)
    target_link_libraries(RedisTest PRIVATE Threads::Threads)
endif()
