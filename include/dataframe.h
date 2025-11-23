#pragma once

#include <algorithm>
#include <chrono>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

#include "column.h"

using ColumnVariant =
    std::variant<Column<int>, Column<double>, Column<std::string>>;

class DataFrame {
 private:
  std::unordered_map<std::string, ColumnVariant> columns;
  std::vector<std::string> column_info;

  size_t rows;
  size_t cols;

 public:
  DataFrame() = default;

  explicit DataFrame(std::vector<std::string> cn);
  template <typename T>
  explicit DataFrame(std::vector<std::string> cn,
                     const std::vector<std::vector<std::optional<T>>>& d) {
    if (d.size() != cn.size()) {
      throw std::runtime_error("column count does not match");
    }

    for (size_t i{}; i < cn.size(); ++i) {
      columns[cn[i]] = Column<T>(d[i]);
    }

    column_info = std::move(cn);

    // assume input has same number of rows for now
    rows = d[0].size();
    cols = column_info.size();
  }

  // void read_csv(const std::string& csv);

  size_t size() const;
  bool empty() const;

  std::pair<size_t, size_t> shape() const;
  size_t nrows() const;
  size_t ncols() const;

  void head(size_t n = 5) const;
  void tail(size_t n = 5) const;

  void info() const;

  template <typename T>
  const Column<T>* get_column(const std::string& col_name) const {
    auto it{columns.find(col_name)};
    if (it == columns.end()) {
      return nullptr;
    }

    return std::get_if<Column<T>>(&it->second);
  }

  template <typename T>
  void add_column(const std::string& col_name,
                  const std::vector<std::optional<T>>& data) {
    auto it{std::ranges::find(column_info, col_name)};
    if (it != column_info.end()) {
      throw std::runtime_error("column already exists in dataframe");
    }

    columns[col_name] = Column<T>(data);
    cols++;
  }

 private:
  void print(size_t start, size_t end) const;
};
