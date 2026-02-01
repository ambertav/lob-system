#include "dataframe.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <ranges>

#include "utils.h"

namespace df {
// =====================================
// constructors
// =====================================

DataFrame::DataFrame(std::vector<std::string> cn)
    : cols(cn.size()), column_info(std::move(cn)) {}

DataFrame::DataFrame(size_t r, size_t c, std::vector<std::string> cn,
                     std::unordered_map<std::string, ColumnVariant> d)
    : rows(r), cols(c), column_info(std::move(cn)), columns(std::move(d)) {
  normalize_length();
}

// =====================================
// i/o and serialization methods
// =====================================

void DataFrame::from_csv(
    const std::string& csv,
    const std::unordered_map<std::string, ColumnType>& types, char delimiter) {
  std::ifstream file{csv};
  if (!file.is_open()) {
    throw std::runtime_error("failed to open csv file: " + csv);
  }

  file.seekg(0, std::ios::end);
  std::streamsize size(file.tellg());
  file.seekg(0, std::ios::beg);

  std::string buffer(size, '\0');
  file.read(buffer.data(), size);

  size_t header_end(buffer.find('\n'));
  if (header_end == std::string::npos) {
    throw std::invalid_argument("missing header in file: " + csv);
  }

  std::string_view header_sv{buffer.data(), header_end};
  std::vector<std::string_view> headers_sv{
      utils::to_tokens(header_sv, delimiter)};

  // to reserve column vector sizes
  size_t row_count{static_cast<size_t>(
      std::count(buffer.begin() + header_end, buffer.end(), '\n'))};

  // compare types with headers
  for (const auto& [col, _] : types) {
    if (std::ranges::find(headers_sv, col) == headers_sv.end()) {
      throw std::invalid_argument(
          "specified input types contains invalid column:" + col);
    }
  }

  column_info.reserve(headers_sv.size());
  for (const auto& header : headers_sv) {
    column_info.emplace_back(header);
  }

  std::unordered_map<std::string, ColumnType> all_types{};
  if (types.size() != column_info.size()) {
    std::string_view data{buffer};
    all_types = infer_types(data, column_info, types, delimiter);
  } else {
    all_types = types;
  }

  for (size_t i{}; i < column_info.size(); ++i) {
    const std::string& column_name{column_info[i]};
    const ColumnType type{all_types.at(column_name)};

    switch (type) {
      case ColumnType::Int64:
        columns[column_name] = Column<int64_t>(row_count);
        break;
      case ColumnType::Double:
        columns[column_name] = Column<double>(row_count);
        break;
      case ColumnType::String:
        columns[column_name] = Column<std::string>(row_count);
        break;
    }
  }

  size_t line_start{header_end + 1};
  size_t line_number{2};

  while (line_start < buffer.size()) {
    size_t line_end{buffer.find('\n', line_start)};
    if (line_end == std::string::npos) {
      line_end = buffer.size();
    }

    std::string_view line{buffer.data() + line_start, line_end - line_start};
    if (utils::trim(line).empty()) {
      line_start = line_end + 1;
      continue;
    }

    std::vector<std::string_view> tokens{utils::to_tokens(line, delimiter)};
    if (tokens.size() != column_info.size()) {
      throw std::runtime_error(
          "malformed line " + std::to_string(line_number) + ": expected " +
          std::to_string(column_info.size()) + " columns, got " +
          std::to_string(tokens.size()));
    }

    for (size_t i{}; i < column_info.size(); ++i) {
      const std::string& column_name{column_info[i]};
      const std::string_view value{utils::trim(tokens[i])};

      ColumnVariant& col{columns.at(column_name)};

      std::visit(
          [&](auto& column) {
            using T = std::decay_t<decltype(column)>::value_type;

            if constexpr (std::is_same_v<T, int64_t>) {
              column.append(utils::parse<int64_t>(value));
            } else if constexpr (std::is_same_v<T, double>) {
              column.append(utils::parse<double>(value));
            } else if constexpr (std::is_same_v<T, std::string>) {
              column.append(std::string(value));
            }
          },
          col);
    }

    line_start = line_end + 1;
    ++line_number;
  }

  rows = row_count;
  cols = column_info.size();
}

void DataFrame::to_csv(const std::string& csv, char delimiter) const {
  std::ofstream file{csv};
  if (!file.is_open()) {
    throw std::runtime_error("failed to open csv file: " + csv);
  }

  file << std::setprecision(17);

  for (size_t i{}; i < column_info.size(); ++i) {
    file << column_info[i];
    if (i < column_info.size() - 1) {
      file << delimiter;
    }
  }
  file << '\n';

  for (size_t i{}; i < rows; ++i) {
    for (size_t j{}; j < column_info.size(); ++j) {
      const auto& col{columns.at(column_info[j])};
      std::visit(
          [&](const auto& column) {
            using T = std::decay_t<decltype(column)>::value_type;
            if (!utils::is_null<T>(column[i])) {
              file << column[i];
            }
          },
          col);

      if (j < column_info.size() - 1) {
        file << delimiter;
      }
    }
    if (i < rows - 1) {
      file << '\n';
    }
  }
}

DataFrame DataFrame::from_bytes(const std::vector<std::byte>& bytes) {
  if (bytes.size() < sizeof(size_t) * 2) {
    throw std::runtime_error("invalid number of bytes");
  }
  size_t offset{};

  size_t nr{};
  std::memcpy(&nr, bytes.data() + offset, sizeof(size_t));
  offset += sizeof(size_t);

  size_t nc{};
  std::memcpy(&nc, bytes.data() + offset, sizeof(size_t));
  offset += sizeof(size_t);

  std::vector<std::string> column_names{};
  column_names.reserve(nc);

  for (size_t i{}; i < nc; ++i) {
    if (offset + sizeof(uint32_t) > bytes.size()) {
      throw std::runtime_error(
          "truncated data, cannot read column name length");
    }

    uint32_t length{};
    std::memcpy(&length, bytes.data() + offset, sizeof(uint32_t));
    offset += sizeof(uint32_t);

    if (offset + length > bytes.size()) {
      throw std::runtime_error("truncated data, cannot read column name");
    }

    const char* name_data{reinterpret_cast<const char*>(bytes.data() + offset)};
    column_names.emplace_back(name_data, length);
    offset += length;
  }

  std::unordered_map<std::string, ColumnVariant> column_map{};
  column_map.reserve(nc);

  for (size_t i{}; i < nc; ++i) {
    const std::string& column_name{column_names[i]};

    if (offset + sizeof(ColumnType) > bytes.size()) {
      throw std::runtime_error("truncated data, cannot read column type");
    }

    ColumnType type{};
    std::memcpy(&type, bytes.data() + offset, sizeof(ColumnType));
    offset += sizeof(ColumnType);

    size_t data_size{};
    if (type == ColumnType::Int64) {
      data_size = nr * sizeof(int64_t);
    } else if (type == ColumnType::Double) {
      data_size = nr * sizeof(double);
    } else if (type == ColumnType::String) {
      size_t temp_offset{offset};
      for (size_t row{}; row < nr; ++row) {
        if (temp_offset + sizeof(uint32_t) > bytes.size()) {
          throw std::runtime_error("truncated data, cannot read string length");
        }

        uint32_t length{};
        std::memcpy(&length, bytes.data() + temp_offset, sizeof(uint32_t));
        temp_offset += sizeof(uint32_t);

        if (temp_offset + length > bytes.size()) {
          throw std::runtime_error("truncated data, cannot read string data");
        }

        temp_offset += length;
      }

      data_size = temp_offset - offset;
    } else {
      throw std::runtime_error("unknown column type during deserialization");
    }

    if (offset + data_size > bytes.size()) {
      throw std::runtime_error("truncated data, cannot read column data");
    }

    std::vector<std::byte> column_bytes{bytes.begin() + offset,
                                        bytes.begin() + offset + data_size};
    offset += data_size;

    switch (type) {
      case ColumnType::Int64:
        column_map[column_name] = Column<int64_t>::from_bytes(column_bytes);
        break;
      case ColumnType::Double:
        column_map[column_name] = Column<double>::from_bytes(column_bytes);
        break;
      case ColumnType::String:
        column_map[column_name] = Column<std::string>::from_bytes(column_bytes);
        break;
    }
  }

  return DataFrame(nr, column_names.size(), std::move(column_names),
                   std::move(column_map));
}

DataFrame DataFrame::from_binary(const std::string& path) {
  std::ifstream file{path, std::ios::binary};
  if (!file.is_open()) {
    throw std::runtime_error("failed to open binary file: " + path);
  }

  file.seekg(0, std::ios::end);
  std::streamsize size{file.tellg()};
  file.seekg(0, std::ios::beg);

  std::vector<std::byte> bytes(size);
  file.read(reinterpret_cast<char*>(bytes.data()), size);

  file.close();
  return from_bytes(bytes);
}

std::vector<std::byte> DataFrame::to_bytes() const {
  size_t metadata_size{sizeof(size_t) * 2};

  for (const auto& name : column_info) {
    metadata_size += sizeof(uint32_t) + name.size();
  }
  metadata_size += cols * sizeof(ColumnType);

  std::vector<std::byte> result{};
  result.reserve(metadata_size);

  const std::byte* rows_bytes{reinterpret_cast<const std::byte*>(&rows)};
  result.insert(result.end(), rows_bytes, rows_bytes + sizeof(size_t));

  const std::byte* cols_bytes{reinterpret_cast<const std::byte*>(&cols)};
  result.insert(result.end(), cols_bytes, cols_bytes + sizeof(size_t));

  for (const auto& name : column_info) {
    uint32_t length{static_cast<uint32_t>(name.size())};
    const std::byte* length_bytes{reinterpret_cast<const std::byte*>(&length)};
    result.insert(result.end(), length_bytes, length_bytes + sizeof(uint32_t));

    const std::byte* name_bytes{
        reinterpret_cast<const std::byte*>(name.data())};
    result.insert(result.end(), name_bytes, name_bytes + name.size());
  }

  for (const auto& name : column_info) {
    const auto& column{columns.at(name)};
    ColumnType type{std::visit(
        [](const auto& col) -> ColumnType { return col.get_type(); }, column)};

    const std::byte* type_bytes{reinterpret_cast<const std::byte*>(&type)};
    result.insert(result.end(), type_bytes, type_bytes + sizeof(ColumnType));

    std::vector<std::byte> column_bytes{std::visit(
        [](const auto& col) -> std::vector<std::byte> {
          return col.to_bytes();
        },
        column)};
    result.insert(result.end(), column_bytes.begin(), column_bytes.end());
  }

  return result;
}

void DataFrame::to_binary(const std::string& path) const {
  std::ofstream file{path, std::ios::binary};
  if (!file.is_open()) {
    throw std::runtime_error("failed to open binary file: " + path);
  }

  std::vector<std::byte> data{to_bytes()};
  file.write(reinterpret_cast<const char*>(data.data()), data.size());
  file.close();
}

// =====================================
// size methods
// =====================================

size_t DataFrame::size() const { return rows * cols; }

bool DataFrame::empty() const { return rows == 0; }

std::pair<size_t, size_t> DataFrame::shape() const { return {rows, cols}; }

size_t DataFrame::nrows() const { return rows; }

size_t DataFrame::ncols() const { return cols; }

// =====================================
// column methods
// =====================================

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

// =====================================
// row methods
// =====================================

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

// =====================================
// operator methods
// =====================================

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

// =====================================
// cleaning methods
// =====================================

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
    size_t row_hash{compute_row_hash(*this, i, *target_columns)};
    if (seen.contains(row_hash)) {
      removal_indices.push_back(i);
    } else {
      seen.insert(row_hash);
    }
  }

  compact_rows(removal_indices);

  return *this;
}

DataFrame& DataFrame::ffill(const std::vector<std::string>& subset) {
  const std::vector<std::string>* target_columns{};
  if (subset.empty()) {
    target_columns = &column_info;
  } else {
    validate_subset(subset);
    target_columns = &subset;
  }

  for (const auto& column_name : *target_columns) {
    auto& col{columns.at(column_name)};
    std::visit(
        [&](auto& column) {
          using T = std::decay_t<decltype(column)>::value_type;

          T* last_non_null{nullptr};
          for (size_t i{}; i < rows; ++i) {
            if (!utils::is_null<T>(column[i])) {
              last_non_null = &column[i];
            } else if (last_non_null != nullptr) {
              column[i] = *last_non_null;
              column.decrement_null();
            }
          }
        },
        col);
  }

  return *this;
}

DataFrame& DataFrame::bfill(const std::vector<std::string>& subset) {
  const std::vector<std::string>* target_columns{};
  if (subset.empty()) {
    target_columns = &column_info;
  } else {
    validate_subset(subset);
    target_columns = &subset;
  }

  for (const auto& column_name : *target_columns) {
    auto& col{columns.at(column_name)};
    std::visit(
        [&](auto& column) {
          using T = std::decay_t<decltype(column)>::value_type;

          T* last_non_null{nullptr};
          for (size_t i{rows}; i > 0; --i) {
            if (!utils::is_null<T>(column[i - 1])) {
              last_non_null = &column[i - 1];
            } else if (last_non_null != nullptr) {
              column[i - 1] = *last_non_null;
              column.decrement_null();
            }
          }
        },
        col);
  }

  return *this;
}

// =====================================
// selection and sorting methods
// =====================================

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

DataFrame DataFrame::slice(size_t start, size_t end) const {
  if (end == 0 || end > rows) {
    end = rows;
  }

  if (start >= end) {
    throw std::out_of_range("invalid range");
  }

  DataFrame df{};
  df.column_info = column_info;

  for (const auto& column_name : column_info) {
    const auto& col{columns.at(column_name)};
    std::visit(
        [&](const auto& column) {
          using T = std::decay_t<decltype(column)>::value_type;
          std::vector<T> copy{column.begin() + start, column.begin() + end};
          df.columns[column_name] = Column<T>(std::move(copy));
        },
        col);
  }

  df.rows = end - start;
  df.cols = column_info.size();

  return df;
}

// =====================================
// join methods
// =====================================

DataFrame DataFrame::inner_join(const DataFrame& left, const DataFrame& right,
                                const std::vector<std::string>& on) {
  left.validate_subset(on);
  right.validate_subset(on);

  auto [all_column_names, right_only_column_names, result] =
      setup_join(left, right, on, left.nrows());

  std::unordered_map<size_t, std::vector<Row>> hashes{
      build_row_hash_map(right, on)};

  auto append_left = [&](size_t i) {
    for (const auto& column_name : left.column_names()) {
      const ColumnVariant* left_col{left.get_column(column_name)};
      std::visit(
          [&](const auto& column) {
            using T = std::decay_t<decltype(column)>::value_type;
            T value = column[i];
            std::get<Column<T>>(result[column_name]).append(value);
          },
          *left_col);
    }
  };

  auto append_right = [&](const Row& row) {
    for (const auto& column_name : right_only_column_names) {
      std::visit(
          [&](auto& column) {
            using T = std::decay_t<decltype(column)>::value_type;
            T value{row.at<T>(column_name)};
            std::get<Column<T>>(result[column_name]).append(value);
          },
          result[column_name]);
    }
  };

  size_t total_rows{};
  for (size_t i{}; i < left.nrows(); ++i) {
    size_t row_hash{compute_row_hash(left, i, on)};
    auto it{hashes.find(row_hash)};

    if (it != hashes.end()) {
      total_rows += it->second.size();
      for (const Row& right_row : it->second) {
        append_left(i);
        append_right(right_row);
      }
    }
  }

  return DataFrame(total_rows, all_column_names.size(), all_column_names,
                   result);
}

DataFrame DataFrame::left_join(const DataFrame& left, const DataFrame& right,
                               const std::vector<std::string>& on) {
  left.validate_subset(on);
  right.validate_subset(on);

  auto [all_column_names, right_only_column_names, result] =
      setup_join(left, right, on, left.nrows());

  std::unordered_map<size_t, std::vector<Row>> hashes{
      build_row_hash_map(right, on)};

  auto append_left = [&](size_t i) {
    for (const auto& column_name : left.column_names()) {
      const ColumnVariant* left_col{left.get_column(column_name)};
      std::visit(
          [&](const auto& column) {
            using T = std::decay_t<decltype(column)>::value_type;
            T value = column[i];
            std::get<Column<T>>(result[column_name]).append(value);
          },
          *left_col);
    }
  };

  auto append_right = [&](const Row* row) {
    for (const auto& column_name : right_only_column_names) {
      std::visit(
          [&](auto& column) {
            using T = std::decay_t<decltype(column)>::value_type;
            if (row != nullptr) {
              T value{row->at<T>(column_name)};
              std::get<Column<T>>(result[column_name]).append(value);
            } else {
              std::get<Column<T>>(result[column_name])
                  .append(utils::get_null<T>());
            }
          },
          result[column_name]);
    }
  };

  size_t total_rows{};
  for (size_t i{}; i < left.nrows(); ++i) {
    size_t row_hash{compute_row_hash(left, i, on)};
    auto it{hashes.find(row_hash)};

    if (it != hashes.end()) {
      total_rows += it->second.size();
      for (const Row& right_row : it->second) {
        append_left(i);
        append_right(&right_row);
      }
    } else {  // append nulls for right
      ++total_rows;
      append_left(i);
      append_right(nullptr);
    }
  }

  return DataFrame(total_rows, all_column_names.size(), all_column_names,
                   result);
}

DataFrame DataFrame::right_join(const DataFrame& left, const DataFrame& right,
                                const std::vector<std::string>& on) {
  return DataFrame::left_join(right, left, on);
}

// =====================================
// statistical methods
// =====================================

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

// =====================================
// display methods
// =====================================

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

// =====================================
// private helper methods
// =====================================

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

std::unordered_map<std::string, ColumnType> DataFrame::infer_types(
    std::string_view data, const std::vector<std::string>& headers,
    const std::unordered_map<std::string, ColumnType>& types,
    char delimiter) const {
  std::unordered_map<std::string, ColumnType> all_types{types};

  // mapping to track parseable states and column index
  struct InferenceState {
    size_t index;
    bool as_int{true};
    bool as_double{true};
  };
  std::unordered_map<std::string, InferenceState> column_states{};

  for (size_t i{}; i < headers.size(); ++i) {
    const std::string& column{headers[i]};
    if (!all_types.contains(column)) {
      column_states[column] = InferenceState{i};
    }
  }

  size_t line_start{data.find('\n') + 1};
  size_t line_number{2};

  while (line_start < data.size() && line_number < 100) {
    size_t line_end{data.find('\n', line_start)};
    if (line_end == std::string_view::npos) {
      line_end = data.size();
    }

    std::string_view line{data.data() + line_start, line_end - line_start};
    if (utils::trim(line).empty()) {
      line_start = line_end + 1;
      continue;
    }

    std::vector<std::string_view> tokens{utils::to_tokens(line, delimiter)};

    for (auto& [column, state] : column_states) {
      if (!state.as_int && !state.as_double) {
        continue;
      }

      if (state.index >= tokens.size()) {
        continue;
      }

      const std::string_view& value{tokens[state.index]};

      if (value.empty()) {
        continue;
      }

      if (state.as_int && !utils::try_parse<int64_t>(value)) {
        state.as_int = false;
      }

      if (state.as_double && !utils::try_parse<double>(value)) {
        state.as_double = false;
      }
    }

    line_start = line_end + 1;
    ++line_number;
  }

  for (const auto& [column, state] : column_states) {
    if (state.as_int) {
      all_types[column] = ColumnType::Int64;
    } else if (state.as_double) {
      all_types[column] = ColumnType::Double;
    } else {
      all_types[column] = ColumnType::String;
    }
  }

  return all_types;
}

void DataFrame::validate_subset(const std::vector<std::string>& subset) const {
  for (const auto& col : subset) {
    if (std::ranges::find(column_info, col) == column_info.end()) {
      throw std::invalid_argument(
          "specified column input contains invalid column: " + col);
    }
  }
}

void DataFrame::combine_hash(size_t& row_hash, size_t value_hash) {
  row_hash ^= value_hash + 0x9e3779b9 + (row_hash << 6) + (row_hash >> 2);
}

size_t DataFrame::compute_row_hash(const DataFrame& df, size_t index,
                                   const std::vector<std::string>& on) {
  size_t row_hash{};
  for (const auto& column_name : on) {
    const ColumnVariant* col{df.get_column(column_name)};
    std::visit(
        [&](const auto& column) {
          using T = std::decay_t<decltype(column)>::value_type;
          combine_hash(row_hash, std::hash<T>{}(column[index]));
        },
        *col);
  }
  return row_hash;
}

std::unordered_map<size_t, std::vector<Row>> DataFrame::build_row_hash_map(
    const DataFrame& df, const std::vector<std::string>& on) {
  std::unordered_map<size_t, std::vector<Row>> hashes{};
  for (size_t i{}; i < df.nrows(); ++i) {
    size_t row_hash{compute_row_hash(df, i, on)};
    hashes[row_hash].push_back(df.get_row(i));
  }

  return hashes;
}

std::tuple<std::vector<std::string>, std::vector<std::string>,
           std::unordered_map<std::string, ColumnVariant>>
DataFrame::setup_join(const DataFrame& left, const DataFrame& right,
                      const std::vector<std::string>& on, size_t size) {
  std::vector<std::string> all_names{};
  std::vector<std::string>
      right_names{};  // to easily access which values to append
  std::unordered_map<std::string, ColumnVariant> result{};

  // initialize left df columns
  for (const auto& column_name : left.column_names()) {
    all_names.push_back(column_name);
    const ColumnVariant* col{left.get_column(column_name)};
    std::visit(
        [&](const auto& column) {
          using T = std::decay_t<decltype(column)>::value_type;
          result[column_name] =
              Column<T>{size};  // initializes columns with approx size
        },
        *col);
  }

  // initialize right df columns
  for (const auto& column_name : right.column_names()) {
    auto it{std::ranges::find(on, column_name)};
    if (it == on.end()) {
      all_names.push_back(column_name);
      right_names.push_back(column_name);
      const ColumnVariant* col{right.get_column(column_name)};
      std::visit(
          [&](const auto& column) {
            using T = std::decay_t<decltype(column)>::value_type;
            result[column_name] =
                Column<T>{size};  // initializes columns with approx size
          },
          *col);
    }
  }

  return {all_names, right_names, result};
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
}  // namespace df