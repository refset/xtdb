cmake_minimum_required(VERSION 3.16)
project(bespoke_olap CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Arrow REQUIRED)
find_package(nlohmann_json 3.2.0 REQUIRED)
find_package(Protobuf REQUIRED)

add_executable(bespoke_olap
    src/main.cpp
    src/loader.cpp
    src/queries.cpp
)

target_include_directories(bespoke_olap PRIVATE src)
target_link_libraries(bespoke_olap PRIVATE
    Arrow::arrow_shared
    nlohmann_json::nlohmann_json
    protobuf::libprotobuf
)
