# The riders job, a module built against an installed clink. run.sh installs
# the clink checkout into .clink/prefix and points CMAKE_PREFIX_PATH at it;
# clink_add_job_module is the recipe the package ships for an out-of-tree job.
cmake_minimum_required(VERSION 3.24)
project(halftime-riders LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(clink REQUIRED COMPONENTS core)

clink_add_job_module(riders SOURCES riders.cpp SUFFIX ".so")
