cmake_minimum_required(VERSION 3.13)

project(codecrafters-kafka)

set(CMAKE_CXX_STANDARD 23) # Enable the C++23 standard   
set(CMAKE_CXX_STANDARD_REQUIRED ON) # Good practice to enforce it
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

file(GLOB_RECURSE SOURCE_FILES src/*.cpp src/*.hpp)

add_executable(kafka ${SOURCE_FILES})