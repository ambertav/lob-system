#pragma once

#include "dataframe.h"

namespace df {

// =====================================
// constructors
// =====================================

template <Storable T>
DataFrame::DataFrame(std::vector<std::string> cn,
                     std::vector<std::vector<T>>&& d) {
  if (d.size() != cn.size()) {
    throw std::runtime_error("column count does not match");
  }

  for (size_t i{}; i < cn.size(); ++i) {
    columns[cn[i]] = Column<T>(std::move(d[i]));
  }
  column_info = std::move(cn);

  rows = d[0].size();
  bool equal_lengths{true};
  for (size_t i{1}; i < d.size(); ++i) {
    if (rows < d[i].size()) {
      equal_lengths = false;  // flag to normalize lengths
      rows = d[i].size();     // assign rows to max column length
    }
  }
  cols = column_info.size();

  if (!equal_lengths) {
    normalize_length();
  }
}

// =====================================
// column methods
// =====================================

template <Storable T>
void DataFrame::add_column(const std::string& column_name,
                           const std::vector<T>& data) {
  auto it{std::ranges::find(column_info, column_name)};
  if (it != column_info.end()) {
    throw std::runtime_error("column already exists in dataframe");
  }

  column_info.emplace_back(column_name);
  columns[column_name] = Column<T>(std::forward<decltype(data)>(data));
  ++cols;

  if (rows == data.size()) {
    return;
  }

  if (rows < data.size()) {
    rows = data.size();  // new max length column
  }

  normalize_length();
}

template <Storable T>
const Column<T>* DataFrame::get_column(const std::string& column_name) const {
  auto it{columns.find(column_name)};
  if (it == columns.end()) {
    return nullptr;
  }

  return std::get_if<Column<T>>(&it->second);
}

template <Storable T>
Column<T>* DataFrame::get_column(const std::string& column_name) {
  auto it{columns.find(column_name)};
  if (it == columns.end()) {
    return nullptr;
  }

  return std::get_if<Column<T>>(&it->second);
}

// =====================================
// row methods
// =====================================

template <Storable T>
void DataFrame::update(size_t index, const std::string& column_name,
                       const T& value) {
  if (index >= rows) {
    throw std::out_of_range("index out of range");
  }

  if (!columns.contains(column_name)) {
    throw std::invalid_argument("column not found: " + column_name);
  }

  auto& column{columns.at(column_name)};

  std::visit(
      [&](auto& col) {
        using U = std::decay_t<decltype(col)>::value_type;

        if constexpr (std::is_same_v<U, T>) {
          col[index] = value;
        } else {
          throw std::invalid_argument("type mismatch, column '" + column_name +
                                      "' expects a different type");
        }
      },
      column);
}

// =====================================
// cleaning methods
// =====================================

template <Storable T>
DataFrame& DataFrame::fillna(const T& value, const std::vector<std::string>& subset) {
  const std::vector<std::string>* target_columns{};

  if (subset.empty()) {
    target_columns = &column_info;
  } else {
    validate_subset(subset);
    target_columns = &subset;
  }

  for (size_t i{0}; i < rows; ++i) {
    for (const auto& column_name : *target_columns) {
      auto* column{get_column<T>(column_name)};

      if (column == nullptr) {
        continue;
      }

      if (utils::is_null<T>(*column[i])) {
        *column[i] = value;
        column->decrement_null();
      }
    }
  }

  return *this;
}

// =====================================
// statistical methods
// =====================================

template <Storable T>
T DataFrame::maximum(const std::string& column_name) const {
  auto it{columns.find(column_name)};
  if (it == columns.end()) {
    throw std::invalid_argument("column not found: " + column_name);
  }
  const auto& target{it->second};

  const auto* col_ptr{std::get_if<Column<T>>(&target)};
  if (col_ptr == nullptr) {
    throw std::invalid_argument("type mismatch, column '" + column_name +
                                "' expects a different type");
  }

  return col_ptr->maximum();
}

template <Storable T>
T DataFrame::minimum(const std::string& column_name) const {
  auto it{columns.find(column_name)};
  if (it == columns.end()) {
    throw std::invalid_argument("column not found: " + column_name);
  }
  const auto& target{it->second};

  const auto* col_ptr{std::get_if<Column<T>>(&target)};
  if (col_ptr == nullptr) {
    throw std::invalid_argument("type mismatch, column '" + column_name +
                                "' expects a different type");
  }

  return col_ptr->minimum();
}

template <Storable T>
std::vector<T> DataFrame::mode(const std::string& column_name) const {
  auto it{columns.find(column_name)};
  if (it == columns.end()) {
    throw std::invalid_argument("column not found: " + column_name);
  }
  const auto& target{it->second};

  const auto* col_ptr{std::get_if<Column<T>>(&target)};
  if (col_ptr == nullptr) {
    throw std::invalid_argument("type mismatch, column '" + column_name +
                                "' expects a different type");
  }

  return col_ptr->mode();
}

// =====================================
// helper methods
// =====================================

template <typename Func>
double DataFrame::call_statistical_column_method(const std::string& column_name,
                                                 Func func) const {
  if (rows == 0) {
    throw std::invalid_argument("cannot get median of empty column");
  }
  auto it{columns.find(column_name)};
  if (it == columns.end()) {
    throw std::invalid_argument("column not found: " + column_name);
  }
  return std::visit(func, it->second);
}

}  // namespace df