cmake_minimum_required (VERSION 2.6)
project (libreplication)

#SET (CMAKE_BUILD_TYPE Debug)

# ---------- Find Boost Headers/Libraries -----------------------
SET( Boost_DEBUG FALSE)
SET (Boost_FIND_REQUIRED TRUE)
SET (Boost_FIND_QUIETLY TRUE)
SET (Boost_USE_MULTITHREADED FALSE)
SET (Boost_USE_STATIC_LIBS FALSE)
SET (Boost_ADDITIONAL_VERSIONS "1.41" "1.41.0")

FIND_PACKAGE(Boost COMPONENTS system thread)
LINK_DIRECTORIES(${Boost_LIBRARY_DIRS})
INCLUDE_DIRECTORIES(include ${Boost_INCLUDE_DIR})

# -- build dynamic lib
SET(CMAKE_LIBRARY_OUTPUT_DIRECTORY "./lib")
ADD_LIBRARY (replication SHARED src/access_method_factory.cpp src/field_iterator.cpp src/row_of_fields.cpp src/basic_transaction_parser.cpp src/tcp_driver.cpp src/binary_log.cpp src/protocol.cpp src/value_adapter.cpp src/binlog_event.cpp src/resultset_iterator.cpp)
TARGET_LINK_LIBRARIES(replication crypto ${Boost_LIBRARIES})

# -- build static lib
#ADD_LIBRARY (replication-s STATIC src/access_method_factory.cpp src/field_iterator.cpp src/row_of_fields.cpp src/basic_transaction_parser.cpp src/tcp_driver.cpp src/binary_log.cpp src/protocol.cpp src/value_adapter.cpp src/binlog_event.cpp src/resultset_iterator.cpp)
#TARGET_LINK_LIBRARIES(replication-s crypto ${Boost_LIBRARIES})

ADD_EXECUTABLE(maintest src/maintest.cpp)
TARGET_LINK_LIBRARIES(maintest replication)

