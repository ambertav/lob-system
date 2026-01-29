#pragma once

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <optional>
#include <ranges>
#include <string>
#include <unordered_map>
#include <variant>

#include "utils.h"
#include "column.h"

namespace df {
using RowVariant = std::variant<int64_t, double, std::string>;

struct Row {
  std::unordered_map<std::string, RowVariant> data;

  Row() = default;
  explicit Row(const std::unordered_map<std::string, RowVariant>& d)
      : data(d) {}
  explicit Row(std::unordered_map<std::string, RowVariant>&& d)
      : data(std::move(d)) {};

  Row(std::initializer_list<std::pair<const std::string, RowVariant>> il)
      : data(il.begin(), il.end()) {}

  template <Storable T>
  std::optional<T> get(const std::string& column_name) const {
    auto it{data.find(column_name)};
    if (it == data.end()) {
      return std::nullopt;
    }

    const T* value{std::get_if<T>(&it->second)};
    if (!value) {
      throw std::bad_variant_access();
    }

    return *value;
  }

  template <Storable T>
  T at(const std::string& column_name) const {
    const T* value{std::get_if<T>(&data.at(column_name))};
    if (!value) {
      throw std::bad_variant_access();
    }
    return *value;
  }

  template <Storable T>
  Row& set(const std::string& column_name, T value) {
    auto it{data.find(column_name)};
    if (it != data.end()) {
      if (!std::holds_alternative<T>(
              it->second)) {  // if existing, restrict to same data type
        throw std::bad_variant_access();
      }
      it->second = std::move(value);
    } else {
      data[column_name] = std::move(value);
    }

    return *this;
  }

  template <Storable T>
  Row& update(const std::string& column_name, T value) {
    auto it{data.find(column_name)};
    if (it == data.end()) {
      throw std::out_of_range("column: " + column_name + " not found");
    } else {
      if (!std::holds_alternative<T>(
              it->second)) {  // restrict to same data type
        throw std::bad_variant_access();
      }
      it->second = std::move(value);
      return *this;
    }
  }

  std::vector<std::string> column_names() const {
    std::vector<std::string> output{};
    output.reserve(data.size());

    std::ranges::transform(data, std::back_inserter(output),
                           [&](const auto& pair) { return pair.first; });

    return output;
  }

  bool contains(const std::string& column_name) const {
    return data.contains(column_name);
  }
  bool empty() { return data.empty(); }
  size_t size() { return data.size(); }

  using iterator =
      typename std::unordered_map<std::string, RowVariant>::iterator;
  using const_iterator =
      typename std::unordered_map<std::string, RowVariant>::const_iterator;

  iterator begin() { return data.begin(); }
  const_iterator begin() const { return data.begin(); }
  const_iterator cbegin() const noexcept { return data.cbegin(); }
  iterator end() { return data.end(); }
  const_iterator end() const { return data.end(); }
  const_iterator cend() const noexcept { return data.cend(); }

  friend std::ostream& operator<<(std::ostream& os, const Row& row) {
    size_t count{};

    os << "{ ";

    for (const auto& [key, value] : row.data) {
      os << key << ": ",

          std::visit(
              [&os](const auto& val) {
                if (utils::is_null(val)) {
                  os << "NULL";
                } else {
                  os << val;
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
}  // namespace df