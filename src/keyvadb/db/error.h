#pragma once

#include <string>
#include <system_error>

namespace keyvadb
{
enum db_error
{
    key_not_found = 1,
    value_not_found,
    key_wrong_length,
    value_too_long,
    zero_length_value,
    short_read,
    short_write,
    bad_commit,
};

class db_category : public std::error_category
{
   public:
    const char* name() const noexcept override { return "db"; };
    std::string message(int ev) const noexcept override
    {
        switch (ev)
        {
        case db_error::key_not_found:
            return "Key not found";
        case db_error::key_wrong_length:
            return "Key wrong length";
        case db_error::value_too_long:
            return "Value too long";
        case db_error::zero_length_value:
            return "Zero length value";
        case db_error::value_not_found:
            return "Value not found";
        case db_error::short_read:
            return "Short Read";
        case db_error::short_write:
            return "Short Write";
        case db_error::bad_commit:
            return "Bad Commit";
        default:
            return "Unknown error";
        }
    }
    bool equivalent(const std::error_code&, int) const noexcept override
    {
        // TODO(DH) mappings here
        return false;
    }
};

const std::error_category& db_category()
{
    static class db_category instance;
    return instance;
}

std::error_condition make_error_condition(db_error e)
{
    return std::error_condition(static_cast<int>(e), db_category());
}

}  // namespace keyvadb

namespace std
{
template <>
struct is_error_condition_enum<keyvadb::db_error> : public true_type
{
};
}
