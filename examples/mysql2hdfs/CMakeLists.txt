project (mysql2hdfs)

find_package(HDFS)

find_library(HDFS_LIBRARY libhdfs)
find_path(Replication_INCLUDE_DIR binlog_api.h)
include_directories(${HDFS_INCLUDE_DIR})
include_directories(${HDFS_LIB_PATHS})
set(LIBS ${LIBS} ${HDFS_LIBS})

ADD_EXECUTABLE(happlier mysql2hdfs.cpp table_index.cpp hdfs_schema.cpp table_insert.cpp) 
TARGET_LINK_LIBRARIES(happlier ${LIBS})
TARGET_LINK_LIBRARIES(happlier replication_static mysqlclient)

