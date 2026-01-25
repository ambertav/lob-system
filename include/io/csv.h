#pragma once

#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "core/dataframe.h"

namespace df {
DataFrame from_csv(
    const std::string& csv,
    const std::unordered_map<std::string, ColumnType>& types = {},
    char delimiter = ',');

void to_csv(const DataFrame& df, const std::string& csv, char delimiter = ',');
}  // namespace df