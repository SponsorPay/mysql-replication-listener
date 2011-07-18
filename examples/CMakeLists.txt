project(examples)

link_directories(${PROJECT_BUILD_DIR}/lib)
include_directories(${PROJECT_BUILD_DIR}/include)
ADD_EXECUTABLE(example_1 example_1.cpp)
TARGET_LINK_LIBRARIES(example_1 replication)

add_subdirectory(mysql2lucene EXCLUDE_FROM_ALL)
