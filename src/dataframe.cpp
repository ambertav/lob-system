#include "dataframe.h"

#include <array>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <ranges>

#include "utils.h"

// =========================
// constructors
// =========================

DataFrame::DataFrame(std::vector<std::string> cn)
    : column_info(std::move(cn)) {}

// =========================
// file i/o methods
// =========================

void DataFrame::from_csv(
    const std::string& csv,
    const std::unordered_map<std::string, ColumnType>& types) {
  std::ifstream file{csv};
  if (!file.is_open()) {
    throw std::runtime_error("failed to open file: " + csv);
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

  std::string_view header{buffer.data(), header_end};
  auto headers{Utils::split(header, ',')};

  // to reserve column vector sizes
  size_t row_count{static_cast<size_t>(
      std::count(buffer.begin() + header_end, buffer.end(), '\n'))};

  // compare types with headers
  bool invalid_columns{false};
  for (const auto& [col, _] : types) {
    if (std::ranges::find(headers, col) == headers.end()) {
      invalid_columns = true;
      break;
    }
  }
  if (invalid_columns) {
    throw std::invalid_argument(
        "specified input types contains invalid columns");
  }

  if (types.size() != headers.size()) {
    std::string_view data{buffer};
    const std::unordered_map<std::string, ColumnType> all_types{
        infer_types(data, headers, types)};
    create_columns(headers, all_types, row_count);
  } else {
    create_columns(headers, types, row_count);
  }

  size_t line_start{header_end + 1};
  int line_number{2};

  while (line_start < buffer.size()) {
    size_t line_end{buffer.find('\n', line_start)};
    if (line_end == std::string::npos) {
      line_end = buffer.size();
    }

    std::string_view line(buffer.data() + line_start, line_end - line_start);
    if (Utils::trim(line).empty()) {
      line_start = line_end + 1;
      continue;
    }

    auto tokens{Utils::split(line, ',')};

    if (tokens.size() != column_info.size()) {
      std::cerr << "line " << line_number << " is malformed, skipping...\n";
      line_start = line_end + 1;
      ++line_number;
      continue;
    }

    for (size_t i{0}; i < column_info.size(); ++i) {
      std::string_view value{Utils::trim(tokens[i])};
      auto& column{columns.at(column_info[i])};
      std::visit(
          [&](auto& col) {
            using T = std::decay_t<decltype(col)>;
            if constexpr (std::is_same_v<T, Column<int>>) {
              col.append(Utils::parse<int>(value));
            } else if constexpr (std::is_same_v<T, Column<double>>) {
              col.append(Utils::parse<double>(value));
            } else if constexpr (std::is_same_v<T, Column<std::string>>) {
              std::optional<std::string> parsed_value{};
              if (!value.empty()) {
                parsed_value.emplace(value);
              }
              col.append(parsed_value);
            }
          },
          column);
    }

    ++rows;
    line_start = line_end + 1;
    ++line_number;
  }
}

// TO-DO: simple custom json parser if necessary
// void DataFrame::read_json(const std::string& json) {}

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

void DataFrame::drop_column(const std::string& column_name) {
  auto it{columns.find(column_name)};
  if (it == columns.end()) {
    throw std::invalid_argument("column not found: " + column_name);
  }

  columns.erase(it);

  std::erase_if(column_info, [&](const auto& col) { return col == column_name; });

  --cols;
}

// =========================
// row methods
// =========================

Row DataFrame::get_row(size_t index) const {
  if (index >= rows) {
    throw std::out_of_range("index out of range");
  }

  Row row{};
  for (const auto& column_name : column_info) {
    const auto& column{columns.at(column_name)};

    std::visit(
        [&column_name, &row, &index](const auto& col) {
          const auto& val_opt{col[index]};

          row.data.emplace(column_name, val_opt);
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

  std::cout << "\n";

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
          if constexpr (std::is_same_v<T, Column<int>>) {
            std::cout << "integer";
          } else if constexpr (std::is_same_v<T, Column<double>>) {
            std::cout << "double";
          } else if constexpr (std::is_same_v<T, Column<std::string>>) {
            std::cout << "string";
          }
        },
        column);

    std::cout << "\n";
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
          size_t diff{rows - col.size()};
          if (diff == 0) {
            return;
          }

          for (size_t i{0}; i < diff; ++i) {
            col.append(std::nullopt);
          }

          col.set_null_count(col.get_null_count() + diff);
        },
        column);
  }
}

std::unordered_map<std::string, ColumnType> DataFrame::infer_types(
    std::string_view data, const std::vector<std::string_view>& headers,
    const std::unordered_map<std::string, ColumnType>& types) const {
  std::unordered_map<std::string, ColumnType> all_types{types};

  // mapping to track parseable states and column index
  struct InferenceState {
    size_t index;
    bool as_int{true};
    bool as_double{true};
  };
  std::unordered_map<std::string, InferenceState> column_states{};

  // track states for columns not specified by input mapping
  for (size_t i{0}; i < headers.size(); ++i) {
    std::string col{std::string(headers[i])};
    if (!all_types.contains(col)) {
      column_states[col] = InferenceState{i};
    }
  }

  size_t line_start{data.find('\n') + 1};
  int line_number{2};

  while (line_start < data.size() &&
         line_number < 100) {  // read first 100 lines
    size_t line_end{data.find('\n', line_start)};
    if (line_end == std::string_view::npos) {
      line_end = data.size();
    }

    std::string_view line(data.data() + line_start, line_end - line_start);

    if (Utils::trim(line).empty()) {
      line_start = line_end + 1;
      continue;
    }

    auto tokens{Utils::split(line, ',')};

    for (auto& [col, state] : column_states) {
      if (!state.as_int && !state.as_double) {
        continue;
      }

      if (state.index >= tokens.size()) {
        continue;
      }
      std::string_view value{Utils::trim(tokens[state.index])};

      if (value.empty()) {
        continue;
      }

      if (state.as_int && !Utils::try_parse<int>(value)) {
        state.as_int = false;
      }
      if (state.as_double && !Utils::try_parse<double>(value)) {
        state.as_double = false;
      }
    }

    line_start = line_end + 1;
    ++line_number;
  }

  for (const auto& [col, state] : column_states) {
    if (state.as_int) {
      all_types[col] = ColumnType::Integer;
    } else if (state.as_double) {
      all_types[col] = ColumnType::Double;
    } else {
      all_types[col] = ColumnType::String;
    }
  }

  return all_types;
}

void DataFrame::create_columns(
    const std::vector<std::string_view>& headers,
    const std::unordered_map<std::string, ColumnType>& types, size_t size) {
  cols = headers.size();
  column_info.reserve(headers.size());

  for (const auto& col : headers) {
    column_info.emplace_back(col);
    const std::string& column{column_info.back()};

    ColumnType type{types.at(column)};

    switch (type) {
      case ColumnType::Integer:
        columns[column] = Column<int>(size);
        break;
      case ColumnType::Double:
        columns[column] = Column<double>(size);
        break;
      case ColumnType::String:
      default:
        columns[column] = Column<std::string>(size);
        break;
    }
  }
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
            const auto& val_opt{col[i]};

            // use widths set from column name
            std::cout << std::setw(widths[w++]);

            if (val_opt.has_value()) {
              std::cout << val_opt.value();
            } else {
              std::cout << "NULL";
            }
          },
          column);
    }
    std::cout << "\n";
  }
}
