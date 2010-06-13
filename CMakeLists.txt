project (mysql-replication-listener)
cmake_minimum_required (VERSION 2.6)

#SET (CMAKE_BUILD_TYPE Debug)

# General settings
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)
include_directories(include)
link_directories(${PROJECT_BINARY_DIR}/lib)

# ---------- Find Boost Headers/Libraries -----------------------
SET(Boost_DEBUG FALSE)
SET(Boost_FIND_REQUIRED TRUE)
SET(Boost_FIND_QUIETLY TRUE)
SET(Boost_USE_STATIC_LIBS FALSE)
SET(Boost_ADDITIONAL_VERSIONS "1.41" "1.41.0")
FIND_PACKAGE(Boost REQUIRED system thread)

# --------- Find crypt
FIND_LIBRARY(LIB_CRYPTO crypto /opt/local/lib /opt/lib /usr/lib /usr/local/lib)
LINK_DIRECTORIES(${Boost_LIBRARY_DIRS})
INCLUDE_DIRECTORIES(${Boost_INCLUDE_DIR})

# Locate Google Test package and enable tests if it is found
find_package(GTest)
if (GTEST_FOUND)
  enable_testing(true)
  include_directories(${GTEST_INCLUDE_DIRS})
  add_subdirectory(tests)
endif(GTEST_FOUND)

# -- Build the libraries
add_subdirectory(src)

# -- Build the examples
add_subdirectory(examples EXCLUDE_FROM_ALL)
