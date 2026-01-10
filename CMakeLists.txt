cmake_minimum_required(VERSION 3.14)
project(mini-redis)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

file(GLOB SOURCES "src/*.h" "src/*.cc")

add_executable(server
  main.cc
  ${SOURCES}
)

target_compile_definitions(server
  PRIVATE
    BOOST_ASIO_SEPARATE_COMPILATION=1
)

# option(ENABLE_STATIC_LINK OFF)
# if(ENABLE_STATIC_LINK)
#   target_link_options(server
#     PRIVATE
#       -static-libgcc
#       -static-libstdc++
#   )
# endif()
