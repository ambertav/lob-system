#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include "core/column.h"
#include "core/dataframe.h"

namespace df {
DataFrame from_bytes(const std::vector<std::byte>& bytes);
std::vector<std::byte> to_bytes(const DataFrame& df);

DataFrame from_binary(const std::string& path);
void to_binary(const DataFrame& df, const std::string& path);
}  // namespace df