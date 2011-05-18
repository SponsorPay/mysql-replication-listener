find_package(CLucene)

if (CLucene_FOUND)
  add_executable(mysql2lucene
    main.cpp table_delete.cpp table_index.cpp
    table_insert.cpp table_update.cpp)
  include_directories(${CLUCENE_INCLUDE_DIR} ${CLUCENE_LIBRARY_DIR})
  target_link_libraries(mysql2lucene ${CLUCENE_LIBRARY} replication_static)
else(CLucene_FOUND)
  message("CLucene not found. Building without mysql2lucene")
endif(CLucene_FOUND)