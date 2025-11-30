#pragma once

#include <algorithm>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

#include "column.h"

enum class ColumnType {
  Integer,
  Double,
  String,
};

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

  void read_csv(const std::string& csv,
                const std::unordered_map<std::string, ColumnType>& types = {});

  // TO-DO: simple custom json parser if necessary
  // void read_json(const std::string& json);

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

    column_info.emplace_back(col_name);
    columns[col_name] = Column<T>(data);
    cols++;

    if (rows == data.size()) {
      return;
    }

    if (rows < data.size()) {
      rows = data.size();  // new max length column
    }
    normalize_length();
  }

 private:
  void normalize_length();
  std::unordered_map<std::string, ColumnType> infer_types(
      std::string_view data, const std::vector<std::string_view>& headers,
      const std::unordered_map<std::string, ColumnType>& types) const;
  void create_columns(const std::vector<std::string_view>& headers,
                      const std::unordered_map<std::string, ColumnType>& types,
                      size_t size);
  void print(size_t start, size_t end) const;
};
