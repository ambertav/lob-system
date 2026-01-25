#pragma once

#include <algorithm>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>

#include "core/column.h"
#include "core/row.h"

namespace df {
using ColumnVariant =
    std::variant<Column<int64_t>, Column<double>, Column<std::string>>;

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
  template <Storable T>
  explicit DataFrame(std::vector<std::string> cn,
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

  explicit DataFrame(size_t r, size_t c, std::vector<std::string> cn,
                     std::unordered_map<std::string, ColumnVariant> d);

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

  template <Storable T>
  void add_column(const std::string& column_name, const std::vector<T>& data) {
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
  const Column<T>* get_column(const std::string& column_name) const {
    auto it{columns.find(column_name)};
    if (it == columns.end()) {
      return nullptr;
    }

    return std::get_if<Column<T>>(&it->second);
  }

  template <Storable T>
  Column<T>* get_column(const std::string& column_name) {
    auto it{columns.find(column_name)};
    if (it == columns.end()) {
      return nullptr;
    }

    return std::get_if<Column<T>>(&it->second);
  }

  const ColumnVariant* get_column(const std::string& column_name) const;

  ColumnVariant* get_column(const std::string& column_name);

  void drop_column(const std::string& column_name);

  // =========================
  // row methods
  // =========================

  void add_row(const Row& row);
  void add_row(const std::unordered_map<std::string, RowVariant>& data);

  template <Storable T>
  void update(size_t index, const std::string& column_name, const T& value) {
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
            throw std::invalid_argument("type mismatch, column '" +
                                        column_name +
                                        "' expects a different type");
          }
        },
        column);
  }

  size_t update(size_t index, const Row& row);

  Row get_row(size_t index) const;

  void drop_row(size_t index);

  // =========================
  // operator methods
  // =========================

  bool operator==(const DataFrame& other) const;

  bool operator!=(const DataFrame& other) const;

  // =========================
  // cleaning methods
  // =========================

  DataFrame& dropna(const std::vector<std::string>& subset = {},
                    int threshold = 0);
  DataFrame& drop_duplicates(const std::vector<std::string>& subset = {});

//   template <Storable T>
//   DataFrame& fillna(const std::vector<std::string>& subset = {},
//                     const T& value) {
//     const std::vector<std::string>* target_columns{};

//     if (subset.empty()) {
//       target_columns = &column_info;
//     } else {
//       validate_subset(subset);
//       target_columns = &subset;
//     }

//     for (size_t i{0}; i < rows; ++i) {
//       for (const auto& column_name : *target_columns) {
//         const auto* column{get_column<T>(column_name)};

//         if (!column) {
//           continue;
//         }

//         if (Utils::is_null<T>(*column[i])) {
//           *column[i] = value;
//           // DECREMENT NULL
//         }
//       }
//     }

//     return *this;
//   }

//   DataFrame& ffill(const std::vector<std::string>& subset = {});
//   DataFrame& bfill(const std::vector<std::string>& subset = {});

  // =========================
  // selection and sorting methods
  // =========================

  DataFrame& sort_by(const std::string& column_name, bool ascending = true);

  DataFrame select(const std::vector<std::string>& subset) const;
  DataFrame get_last(size_t start) const;

  // =========================
  // statistical methods
  // =========================

  void describe() const;

  template <Storable T>
  T maximum(const std::string& column_name) const {
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
  T minimum(const std::string& column_name) const {
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
  std::vector<T> mode(const std::string& column_name) const {
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

  double sum(const std::string& column_name) const;
  double median(const std::string& column_name) const;
  double mean(const std::string& column_name) const;
  double standard_deviation(const std::string& column_name) const;
  double variance(const std::string& column_name) const;

  // =========================
  // time-series methods
  // =========================

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
  void compact_rows(const std::vector<size_t>& removal_indices);
  void validate_subset(const std::vector<std::string>& subset) const;
  void combine_hash(size_t& row_hash, size_t value_hash) const;
  void print(size_t start, size_t end) const;

  template <typename Func>
  double call_statistical_column_method(const std::string& column_name,
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
};
}  // namespace df