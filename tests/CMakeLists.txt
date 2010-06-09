add_executable(test-basic test-basic.cpp)
target_link_libraries(test-basic replication ${GTEST_BOTH_LIBRARIES})

add_executable(replaybinlog replaybinlog.cpp)
target_link_libraries(replaybinlog replication)

add_test(AllTests test-basic)

