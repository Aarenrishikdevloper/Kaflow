cmake_minimum_required(VERSION 3.20)
project(codecrafters-kafka CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

file(GLOB_RECURSE LIB_SOURCES CONFIGURE_DEPENDS src/*.cpp) 
list(REMOVE_ITEM LIB_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/src/main.cpp)

add_library(kafka_lib ${LIB_SOURCES})
target_include_directories(kafka_lib PUBLIC src)
target_compile_options(kafka_lib PRIVATE -Wall -Wextra)

add_executable(kafka src/main.cpp)
target_link_libraries(kafka PRIVATE kafka_lib) 
