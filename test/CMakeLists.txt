enable_testing()

find_package(GTest REQUIRED)

include_directories(${GTEST_INCLUDE_DIRS})

# add_executable(
#   bigquery_utils_test
#   cpp/bigquery_utils_test.cpp
# )

# target_link_libraries(
# bigquery_utils_test
# ${LOADABLE_EXTENSION_NAME} 
# ${GTEST_LIBRARIES}
# )

# add_test(NAME bigquery_utils_test COMMAND bigquery_utils_test)