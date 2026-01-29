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

#include "column.h"
#include "row.h"

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
  // =====================================
  // constructors
  // =====================================

  DataFrame() = default;

  explicit DataFrame(std::vector<std::string> cn);

  template <Storable T>
  explicit DataFrame(std::vector<std::string> cn,
                     std::vector<std::vector<T>>&& d);

  explicit DataFrame(size_t r, size_t c, std::vector<std::string> cn,
                     std::unordered_map<std::string, ColumnVariant> d);

  // =====================================
  // i/o and serialization methods
  // =====================================

  void from_csv(const std::string& csv,
                const std::unordered_map<std::string, ColumnType>& types = {},
                char delimiter = ',');
  void to_csv(const std::string& csv, char delimiter = ',') const;

  static DataFrame from_bytes(const std::vector<std::byte>& bytes);
  static DataFrame from_binary(const std::string& path);

  std::vector<std::byte> to_bytes() const;
  void to_binary(const std::string& path) const;

  // =====================================
  // size methods
  // =====================================

  size_t size() const;
  bool empty() const;

  std::pair<size_t, size_t> shape() const;
  size_t nrows() const;
  size_t ncols() const;

  // =====================================
  // column methods
  // =====================================

  std::vector<std::string> column_names() const;
  bool has_column(const std::string& column_name) const;

  template <Storable T>
  void add_column(const std::string& column_name, const std::vector<T>& data);

  template <Storable T>
  const Column<T>* get_column(const std::string& column_name) const;

  template <Storable T>
  Column<T>* get_column(const std::string& column_name);

  const ColumnVariant* get_column(const std::string& column_name) const;
  ColumnVariant* get_column(const std::string& column_name);

  void drop_column(const std::string& column_name);

  // =====================================
  // row methods
  // =====================================

  void add_row(const Row& row);
  void add_row(const std::unordered_map<std::string, RowVariant>& data);

  template <Storable T>
  void update(size_t index, const std::string& column_name, const T& value);
  size_t update(size_t index, const Row& row);

  Row get_row(size_t index) const;

  void drop_row(size_t index);

  // =====================================
  // operator methods
  // =====================================

  bool operator==(const DataFrame& other) const;

  bool operator!=(const DataFrame& other) const;

  // =====================================
  // cleaning methods
  // =====================================

  DataFrame& dropna(const std::vector<std::string>& subset = {},
                    int threshold = 0);
  DataFrame& drop_duplicates(const std::vector<std::string>& subset = {});

  template <Storable T>
  DataFrame& fillna(const T& value, const std::vector<std::string>& subset = {});

  DataFrame& ffill(const std::vector<std::string>& subset = {});
  DataFrame& bfill(const std::vector<std::string>& subset = {});

  // =====================================
  // selection and sorting methods
  // =====================================

  DataFrame& sort_by(const std::string& column_name, bool ascending = true);

  DataFrame select(const std::vector<std::string>& subset) const;
  DataFrame get_last(size_t start) const;

  // =====================================
  // statistical methods
  // =====================================

  void describe() const;

  template <Storable T>
  T maximum(const std::string& column_name) const;

  template <Storable T>
  T minimum(const std::string& column_name) const;

  template <Storable T>
  std::vector<T> mode(const std::string& column_name) const;

  double sum(const std::string& column_name) const;
  double median(const std::string& column_name) const;
  double mean(const std::string& column_name) const;
  double standard_deviation(const std::string& column_name) const;
  double variance(const std::string& column_name) const;

  // =====================================
  // time-series methods
  // =====================================

  // =====================================
  // display methods
  // =====================================

  void head(size_t n = 5) const;
  void tail(size_t n = 5) const;

  void display(size_t index) const;
  void display(size_t start, size_t end) const;

  void info() const;

  // =====================================
  // helper methods
  // =====================================
 private:
  void normalize_length();
  std::unordered_map<std::string, ColumnType> infer_types(
      std::string_view data, const std::vector<std::string>& headers,
      const std::unordered_map<std::string, ColumnType>& types,
      char delimiter) const;
  void compact_rows(const std::vector<size_t>& removal_indices);
  void validate_subset(const std::vector<std::string>& subset) const;
  void combine_hash(size_t& row_hash, size_t value_hash) const;
  void print(size_t start, size_t end) const;

  template <typename Func>
  double call_statistical_column_method(const std::string& column_name,
                                        Func func) const;
};
}  // namespace df

#include "dataframe.inl"