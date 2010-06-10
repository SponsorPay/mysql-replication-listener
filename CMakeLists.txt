project (mysql-replication-listener)
cmake_minimum_required (VERSION 2.6)

#SET (CMAKE_BUILD_TYPE Debug)

# General settings
include_directories(include)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_ARCHIVE_DIRECTORY ${PROJECT_BINARY_DIR}/lib)

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

add_subdirectory(src)
