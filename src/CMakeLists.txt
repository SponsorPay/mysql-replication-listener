add_library(replication STATIC
  access_method_factory.cpp field_iterator.cpp row_of_fields.cpp
  basic_transaction_parser.cpp tcp_driver.cpp binary_log.cpp
  protocol.cpp value_adapter.cpp binlog_event.cpp
  resultset_iterator.cpp)

target_link_libraries(replication crypto ${Boost_LIBRARIES})
