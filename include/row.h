#pragma once

#include <iomanip>
#include <iostream>
#include <optional>
#include <string>
#include <unordered_map>
#include <variant>

using RowVariant = std::variant<std::optional<int>, std::optional<double>,
                                std::optional<std::string>>;

struct Row {
  std::unordered_map<std::string, RowVariant> data;

  template <typename T>
  std::optional<T> get(const std::string& column_name) const {
    auto it{data.find(column_name)};
    if (it == data.end()) {
      return std::nullopt;
    }

    auto* value{std::get_if<std::optional<T>>(&it->second)};
    if (value.has_value()) {
      return *value;
    } else {
      return std::nullopt;
    }
  }

  const RowVariant& operator[](const std::string& column_name) const {
    return data.at(column_name);
  }

  RowVariant& operator[](const std::string& column_name) {
    return data.at(column_name);
  }

  friend std::ostream& operator<<(std::ostream& os, const Row& row) {
    size_t count{};

    os << "{ ";

    for (const auto& [key, value] : row.data) {
      os << key << ": ",

          std::visit(
              [&os](const auto& val) {
                if (val.has_value()) {
                  os << *val;
                } else {
                  os << "NULL";
                }
              },
              value);

      ++count;
      if (count < row.data.size()) {
        os << ", ";
      }
    }

    os << " }";
    return os;
  }
};