# Right now, the tests are built using the static version of the
# replication listener, but we need to decide which one to use.
add_executable(test-basic test-basic.cpp)
target_link_libraries(test-basic replication_static ${GTEST_BOTH_LIBRARIES})

add_executable(replaybinlog replaybinlog.cpp)
target_link_libraries(replaybinlog replication_static)

add_test(AllTests test-basic)

