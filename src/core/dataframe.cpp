#include "core/dataframe.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <ranges>

#include "utils.h"

namespace df {
// =========================
// constructors
// =========================

DataFrame::DataFrame(std::vector<std::string> cn)
    : cols(cn.size()), column_info(std::move(cn)) {}

DataFrame::DataFrame(size_t r, size_t c, std::vector<std::string> cn,
                     std::unordered_map<std::string, ColumnVariant> d)
    : rows(r),
      cols(c),
      column_info(std::move(cn)),
      columns(std::move(d)) {
  normalize_length();
}

// =========================
// size methods
// =========================

size_t DataFrame::size() const { return rows * cols; }

bool DataFrame::empty() const { return rows == 0; }

std::pair<size_t, size_t> DataFrame::shape() const { return {rows, cols}; }

size_t DataFrame::nrows() const { return rows; }

size_t DataFrame::ncols() const { return cols; }

// =========================
// column methods
// =========================

std::vector<std::string> DataFrame::column_names() const { return column_info; }

bool DataFrame::has_column(const std::string& column_name) const {
  return columns.contains(column_name);
}

const ColumnVariant* DataFrame::get_column(
    const std::string& column_name) const {
  auto it{columns.find(column_name)};
  if (it == columns.end()) {
    return nullptr;
  }
  return &it->second;
}

ColumnVariant* DataFrame::get_column(const std::string& column_name) {
  auto it{columns.find(column_name)};
  if (it == columns.end()) {
    return nullptr;
  }
  return &it->second;
}

void DataFrame::drop_column(const std::string& column_name) {
  auto it{columns.find(column_name)};
  if (it == columns.end()) {
    throw std::invalid_argument("column not found: " + column_name);
  }

  columns.erase(it);

  std::erase_if(column_info,
                [&](const auto& col) { return col == column_name; });

  --cols;
}

// =========================
// row methods
// =========================

void DataFrame::add_row(const Row& row) { add_row(row.data); }

void DataFrame::add_row(
    const std::unordered_map<
        std::string, std::variant<int64_t, double, std::string>>& data) {
  for (const auto& [column_name, val] : data) {
    if (!columns.contains(column_name)) {
      throw std::invalid_argument("invalid data column '" + column_name +
                                  "' not found in columns");
    }
  }

  for (const auto& column_name : column_info) {
    auto& column{columns.at(column_name)};

    auto it{data.find(column_name)};

    std::visit(
        [&](auto& col) {
          using T = std::decay_t<decltype(col)>::value_type;

          if (it == data.end()) {
            col.append(utils::get_null<T>());
          } else {
            auto* value{std::get_if<T>(&it->second)};

            if (!value) {
              throw std::invalid_argument("invalid data type for column " +
                                          column_name);
            } else {
              col.append(*value);
            }
          }
        },
        column);
  }

  ++rows;
}

size_t DataFrame::update(size_t index, const Row& row) {
  if (index >= rows) {
    throw std::out_of_range("index is out of range");
  }

  validate_subset(row.column_names());

  size_t count{};
  for (const auto& [column_name, value] : row) {
    auto& col{columns.at(column_name)};

    std::visit(
        [&](auto& column) {
          using T = std::decay_t<decltype(column)>::value_type;

          if (!std::holds_alternative<T>(value)) {
            throw std::bad_variant_access();
          } else {
            column[index] = *(std::get_if<T>(&value));
            ++count;
          }
        },
        col);
  }

  return count;
}

Row DataFrame::get_row(size_t index) const {
  if (index >= rows) {
    throw std::out_of_range("index out of range");
  }

  Row row{};
  for (const auto& column_name : column_info) {
    const auto& column{columns.at(column_name)};

    std::visit(
        [&column_name, &row, &index](const auto& col) {
          row.data.emplace(column_name, col[index]);
        },
        column);
  }

  return row;
}

void DataFrame::drop_row(size_t index) {
  if (index >= rows) {
    throw std::out_of_range("index out of range");
  }

  for (const auto& column_name : column_info) {
    auto& column{columns.at(column_name)};

    std::visit([&](auto& col) { col.erase(index); }, column);
  }

  --rows;
}

// =========================
// operator methods
// =========================

bool DataFrame::operator==(const DataFrame& other) const {
  if (this->nrows() != other.nrows()) {
    return false;
  }

  const std::vector<std::string> other_column_names{other.column_names()};
  if (this->column_info.size() != other_column_names.size()) {
    return false;
  }

  for (size_t i{0}; i < this->column_info.size(); ++i) {
    if (this->column_info[i] != other_column_names[i]) {
      return false;
    }

    const auto* this_variant_column{this->get_column(column_info[i])};
    const auto* other_variant_column{other.get_column(column_info[i])};
    if (*this_variant_column != *other_variant_column) {
      return false;
    }
  }

  return true;
}

bool DataFrame::operator!=(const DataFrame& other) const {
  return !(*this == other);
}

// =========================
// cleaning methods
// =========================

DataFrame& DataFrame::dropna(const std::vector<std::string>& subset,
                             int threshold) {
  const std::vector<std::string>* target_columns{};

  if (subset.empty()) {
    target_columns = &column_info;
  } else {
    validate_subset(subset);
    target_columns = &subset;
  }

  std::vector<size_t> removal_indices{};

  for (size_t i{0}; i < rows; ++i) {
    bool remove{false};
    size_t count{};

    for (const auto& column_name : *target_columns) {
      const auto& col{columns.at(column_name)};
      std::visit(
          [&](const auto& column) {
            using T = std::decay_t<decltype(column)>::value_type;
            if (utils::is_null<T>(column[i])) {
              ++count;
            }
          },
          col);

      if (count > threshold) {
        remove = true;
      }
    }

    if (remove) {
      removal_indices.push_back(i);
    }
  }

  compact_rows(removal_indices);

  return *this;
}

DataFrame& DataFrame::drop_duplicates(const std::vector<std::string>& subset) {
  const std::vector<std::string>* target_columns{};
  if (subset.empty()) {
    target_columns = &column_info;
  } else {
    validate_subset(subset);
    target_columns = &subset;
  }

  std::unordered_set<size_t> seen{};
  std::vector<size_t> removal_indices{};

  for (size_t i{0}; i < rows; ++i) {
    size_t row_hash{};

    for (const auto& column_name : *target_columns) {
      const auto& col{columns.at(column_name)};
      std::visit(
          [&](const auto& column) {
            using T = std::decay_t<decltype(column)>::value_type;
            combine_hash(row_hash, std::hash<T>{}(column[i]));
          },
          col);
    }

    if (seen.contains(row_hash)) {
      removal_indices.push_back(i);
    } else {
      seen.insert(row_hash);
    }
  }

  compact_rows(removal_indices);

  return *this;
}

// DataFrame& DataFrame::ffill(const std::vector<std::string>& subset = {}) {}

// DataFrame& DataFrame::bfill(const std::vector<std::string>& subset = {}) {}

// =========================
// selection and sorting methods
// =========================

DataFrame& DataFrame::sort_by(const std::string& column_name, bool ascending) {
  std::vector<size_t> indices{};
  for (size_t i{0}; i < rows; ++i) {
    indices.push_back(i);
  }

  auto it{columns.find(column_name)};
  if (it == columns.end()) {
    throw std::invalid_argument("column not found: " + column_name);
  }
  auto& target{it->second};

  std::ranges::sort(indices, [&target, ascending](size_t a, size_t b) {
    return std::visit(
        [&](const auto& column) {
          const auto& val_a = column[a];
          const auto& val_b = column[b];

          if (ascending) {
            return val_a < val_b;
          } else {
            return val_a > val_b;
          }
        },
        target);
  });

  for (auto& [col, column] : columns) {
    auto sorted_column{std::visit(
        [&](const auto& old_column) {
          using T = std::decay_t<decltype(old_column)>::value_type;

          Column<T> new_column{Column<T>(indices.size())};
          for (size_t i{0}; i < indices.size(); ++i) {
            new_column.append(old_column[indices[i]]);
          }

          return ColumnVariant(std::in_place_type<Column<T>>, new_column);
        },
        column)};

    columns[col] = std::move(sorted_column);
  }

  return *this;
}

DataFrame DataFrame::select(const std::vector<std::string>& subset) const {
  if (subset.empty()) {
    throw std::invalid_argument("no columns indicated for selection");
  }

  validate_subset(subset);

  DataFrame df{};

  for (const auto& column_name : subset) {
    const auto& col{columns.at(column_name)};

    std::visit(
        [&](const auto& column) {
          using T = std::decay_t<decltype(column)>::value_type;
          std::vector<T> copy(column.begin(), column.end());

          df.add_column<T>(column_name, copy);
        },
        col);
  }

  return df;
}

DataFrame DataFrame::get_last(size_t start) const {
  if (start >= rows) {
    throw std::out_of_range("index out of range");
  }

  DataFrame df{};
  for (const auto& [column_name, col] : columns) {
    std::visit(
        [&](const auto& column) {
          using T = std::decay_t<decltype(column)>::value_type;
          std::vector<T> copy(column.begin() + start, column.end());

          df.add_column<T>(column_name, copy);
        },
        col);
  }

  return df;
}

// =========================
// statistical methods
// =========================

void DataFrame::describe() const {
  if (rows == 0) {
    std::cout << "empty dataframe\n";
    return;
  }

  std::unordered_map<std::string, std::vector<double>> stats_by_row{};
  std::vector<std::string_view> col_names{};
  col_names.reserve(column_info.size());

  for (size_t i{}; i < column_info.size(); ++i) {
    const auto& col{columns.at(column_info[i])};
    std::visit(
        [&](const auto& column) {
          using T = std::decay_t<decltype(column)>::value_type;
          if constexpr (std::is_arithmetic_v<T>) {
            col_names.emplace_back(column_info[i]);

            stats_by_row["count"].emplace_back(column.nrows() -
                                               column.get_null_count());
            stats_by_row["mean"].emplace_back(column.mean());
            stats_by_row["std"].emplace_back(column.standard_deviation());
            stats_by_row["min"].emplace_back(column.minimum());
            stats_by_row["25%"].emplace_back(column.percentile(0.25));
            stats_by_row["50%"].emplace_back(column.percentile(0.5));
            stats_by_row["75%"].emplace_back(column.percentile(0.75));
            stats_by_row["max"].emplace_back(column.maximum());
          }
        },
        col);
  }

  if (col_names.empty()) {
    std::cout << "no numerical columns to describe\n";
    return;
  }

  std::cout << std::setw(10) << "";
  for (const auto& name : col_names) {
    std::cout << std::setw(12) << name;
  }
  std::cout << '\n';

  for (const auto& stat : utils::describe_order) {
    std::cout << std::setw(10) << stat;
    for (const auto& value : stats_by_row[stat]) {
      std::cout << std::setw(12) << std::fixed << std::setprecision(2) << value;
    }
    std::cout << '\n';
  }
}

double DataFrame::sum(const std::string& column_name) const {
  return call_statistical_column_method(
      column_name, [](const auto& col) { return col.sum(); });
}

double DataFrame::median(const std::string& column_name) const {
  return call_statistical_column_method(
      column_name, [](const auto& col) { return col.median(); });
}

double DataFrame::mean(const std::string& column_name) const {
  return call_statistical_column_method(
      column_name, [](const auto& col) { return col.mean(); });
}

double DataFrame::standard_deviation(const std::string& column_name) const {
  return call_statistical_column_method(
      column_name, [](const auto& col) { return col.standard_deviation(); });
}

double DataFrame::variance(const std::string& column_name) const {
  return call_statistical_column_method(
      column_name, [](const auto& col) { return col.variance(); });
}

// =========================
// display methods
// =========================

void DataFrame::head(size_t n) const {
  if (rows < n) {
    print(0, rows);
  } else {
    print(0, n);
  }
}

void DataFrame::tail(size_t n) const {
  if (rows < n) {
    print(0, rows);
  } else {
    print(rows - n, rows);
  }
}

void DataFrame::display(size_t index) const {
  if (rows == 0) {
    return;
  }

  if (index >= rows) {
    throw std::out_of_range("index out of range");
  }

  print(index, index + 1);
}

void DataFrame::display(size_t start, size_t end) const {
  if (rows == 0) {
    return;
  }

  if (start >= rows || end > rows) {
    throw std::out_of_range("index out of range");
  }

  if (start >= end) {
    throw std::invalid_argument("invalid range input");
  }

  print(start, end);
}

void DataFrame::info() const {
  std::cout << "------------- summary -------------\n"
            << "rows: " << rows << "\n"
            << "columns: " << cols << "\n";

  std::vector<int> widths{};  // for formating
  widths.reserve(column_info.size() + 1);

  std::array<std::string, 4> header{" ", "column", "null", "type"};
  for (size_t i{0}; i < header.size(); ++i) {
    widths.push_back(header[i].size() + 5);
    std::cout << std::setw(widths.back()) << header[i];
  }

  std::cout << '\n';

  for (size_t i{0}; i < column_info.size(); ++i) {
    int w{0};

    std::cout << std::setw(widths[w++]) << i;
    std::cout << std::setw(widths[w++]) << column_info[i];

    const auto& column{columns.at(column_info[i])};

    std::visit(
        [&w, &widths](const auto& col) {
          using T = std::decay_t<decltype(col)>;

          std::cout << std::setw(widths[w++]);
          std::cout << col.get_null_count();

          std::cout << std::setw(widths[w++]);
          if constexpr (std::is_same_v<T, Column<int64_t>>) {
            std::cout << "integer";
          } else if constexpr (std::is_same_v<T, Column<double>>) {
            std::cout << "double";
          } else if constexpr (std::is_same_v<T, Column<std::string>>) {
            std::cout << "string";
          }
        },
        column);

    std::cout << '\n';
  }

  std::cout << "memory usage: " << this->size() << " bytes\n"
            << "-----------------------------------\n";
}

// =========================
// private helper methods
// =========================

void DataFrame::normalize_length() {
  for (auto& column : std::views::values(columns)) {
    std::visit(
        [&](auto& col) {
          using T = std::decay_t<decltype(col)>::value_type;
          size_t diff{rows - col.nrows()};
          if (diff == 0) {
            return;
          }

          for (size_t i{0}; i < diff; ++i) {
            col.append(utils::get_null<T>());
          }
        },
        column);
  }
}

void DataFrame::compact_rows(const std::vector<size_t>& removal_indices) {
  // build removal true / false vector
  std::vector<bool> remove(rows, false);
  for (auto& index : removal_indices) {
    remove[index] = true;
  }

  for (auto& [_, col] : columns) {
    std::visit(
        [&](auto& c) {
          auto shifter = [&](auto& column) {
            size_t write_position{};
            for (size_t i{0}; i < column.nrows(); ++i) {
              if (remove[i] == false) {
                column[write_position++] = std::move(column[i]);
              }
            }

            column.resize(write_position);
          };

          shifter(c);
        },
        col);
  }

  rows -= removal_indices.size();
}

void DataFrame::validate_subset(const std::vector<std::string>& subset) const {
  for (const auto& col : subset) {
    if (std::ranges::find(column_info, col) == column_info.end()) {
      throw std::invalid_argument(
          "specified column input contains invalid column: " + col);
    }
  }
}

void DataFrame::combine_hash(size_t& row_hash, size_t value_hash) const {
  row_hash ^= value_hash + 0x9e3779b9 + (row_hash << 6) + (row_hash >> 2);
}

void DataFrame::print(size_t start, size_t end) const {
  std::vector<int> widths{};  // for formatting
  widths.reserve(column_info.size() + 1);

  widths.push_back(2);
  std::cout << std::setw(widths.back()) << " ";

  for (const auto& column_name : column_info) {
    // set widths according to column name size
    widths.push_back(column_name.size() + 8);
    std::cout << std::setw(widths.back()) << column_name;
  }

  std::cout << "\n";

  for (size_t i{start}; i < end; ++i) {
    int w{0};  // align widths
    std::cout << std::setw(widths[w++]) << i;
    for (const auto& column_name : column_info) {
      const auto& column{columns.at(column_name)};
      std::visit(
          [&](const auto& col) {
            const auto& value{col[i]};

            // use widths set from column name
            std::cout << std::setw(widths[w++]);

            if (!utils::is_null(value)) {
              std::cout << value;
            } else {
              std::cout << "NULL";
            }
          },
          column);
    }
    std::cout << "\n";
  }
}
}