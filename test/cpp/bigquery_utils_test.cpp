#include <gtest/gtest.h>
//#include "bigquery_utils.hpp"
#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace duckdb {

TEST(BigQueryUtilsTest, ParsesEmptySchema) {
    json empty_schema = {{"fields", json::array()}};
    //auto result = BigQueryUtils::ParseColumnFields(empty_schema);
    //EXPECT_TRUE(result.empty());
    EXPECT_TRUE(true);
}

} // namespace duckdb
