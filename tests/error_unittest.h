#include "tests/common.h"
#include "db/error.h"

using namespace keyvadb;

TEST(ErrorTests, General)
{
    auto err = make_error_condition(db_error::key_not_found);
    ASSERT_EQ("Key not found", err.message());
    auto err1 = std::generic_category().default_error_condition(1);
    ASSERT_EQ("Operation not permitted", err1.message());
    auto err6 = std::generic_category().default_error_condition(6);
    ASSERT_TRUE("Device not configured" == err6.message() ||
                "No such device or address" == err6.message());
}
