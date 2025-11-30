#pragma once

#include <algorithm>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

#include "column.h"
#include "row.h"

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
  // =========================
  // constructors
  // =========================

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

  // =========================
  // file i/o methods
  // =========================

  void from_csv(const std::string& csv,
                const std::unordered_map<std::string, ColumnType>& types = {});
  void to_csv(const std::string& csv) const;

  // TO-DO: simple custom json parser if necessary
  // void read_json(const std::string& json);

  // =========================
  // size methods
  // =========================

  size_t size() const;
  bool empty() const;

  std::pair<size_t, size_t> shape() const;
  size_t nrows() const;
  size_t ncols() const;

  // =========================
  // column methods
  // =========================

  std::vector<std::string> column_names() const;
  bool has_column(const std::string& column_name) const;

  template <typename T>
  const Column<T>* get_column(const std::string& column_name) const {
    auto it{columns.find(column_name)};
    if (it == columns.end()) {
      return nullptr;
    }

    return std::get_if<Column<T>>(&it->second);
  }

  template <typename T>
  Column<T>* get_column(const std::string& column_name) {
    auto it{columns.find(column_name)};
    if (it == columns.end()) {
      return nullptr;
    }

    return std::get_if<Column<T>>(&it->second);
  }

  template <typename T>
  void add_column(const std::string& column_name,
                  const std::vector<std::optional<T>>& data) {
    auto it{std::ranges::find(column_info, column_name)};
    if (it != column_info.end()) {
      throw std::runtime_error("column already exists in dataframe");
    }

    column_info.emplace_back(column_name);
    columns[column_name] = Column<T>(data);
    ++cols;

    if (rows == data.size()) {
      return;
    }

    if (rows < data.size()) {
      rows = data.size();  // new max length column
    }

    normalize_length();
  }

  void drop_column(const std::string& column_name);

  // =========================
  // row methods
  // =========================

  template <typename T>
  void add_row(std::unordered_map<std::string, T>&& data) {
    std::unordered_map<std::string, std::optional<T>> optional_data{};
    optional_data.reserve(data.size());

    for (auto& [col, val] : data) {
      optional_data.emplace(std::move(col), std::move(val));
    }
    add_row(std::move(optional_data));
  }

  template <typename T>
  void add_row(const std::unordered_map<std::string, std::optional<T>>& data) {
    for (const auto& [column_name, val] : data) {
      if (!columns.contains(column_name)) {
        throw std::invalid_argument("invalid data column '" + column_name +
                                    "' not found in columns");
      }
    }

    for (const auto& column_name : column_info) {
      auto it{data.find(column_name)};
      std::optional<T> value{(it == data.end()) ? std::nullopt : it->second};

      auto& column{columns.at(column_name)};

      std::visit(
          [&](auto& col) {
            using U = std::decay_t<decltype(col)>::value_type;

            if constexpr (std::is_same_v<U, T>) {
              col.append(value);
            } else {
              throw std::invalid_argument(
                  "type mismatch  in update(), column '" + column_name +
                  "' expects a different type");
            }
          },
          column);
    }

    ++rows;
  }

  template <typename T>
  void update(size_t index, const std::string& column_name, const T& value) {
    update(index, column_name, std::optional<T>(value));
  }

  template <typename T>
  void update(size_t index, const std::string& column_name,
              const std::optional<T>& value) {
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
            throw std::invalid_argument("type mismatch  in update(), column '" +
                                        column_name +
                                        "' expects a different type");
          }
        },
        column);
  }

  Row get_row(size_t index) const;

  void drop_row(size_t index);


  // =========================
  // display methods
  // =========================

  void head(size_t n = 5) const;
  void tail(size_t n = 5) const;

  void display(size_t index) const;
  void display(size_t start, size_t end) const;

  void info() const;

  // =========================
  // helper methods
  // =========================
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
