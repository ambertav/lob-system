#include "io/csv.h"

#include <fstream>
#include <stdexcept>

#include "utils.h"

namespace df {

// forward declarations
namespace {
std::vector<std::string_view> to_tokens(std::string_view line, char delimiter);
std::unordered_map<std::string, ColumnType> infer_types(
    std::string_view data, const std::vector<std::string>& headers,
    const std::unordered_map<std::string, ColumnType>& types, char delimiter);
}  // namespace

DataFrame from_csv(const std::string& csv,
                   const std::unordered_map<std::string, ColumnType>& types,
                   char delimiter) {
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
  std::vector<std::string_view> headers_sv{to_tokens(header_sv, delimiter)};

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

  std::vector<std::string> headers{};
  headers.reserve(headers_sv.size());
  for (const auto& header : headers_sv) {
    headers.emplace_back(header);
  }

  std::unordered_map<std::string, ColumnType> all_types{};
  if (types.size() != headers.size()) {
    std::string_view data{buffer};
    all_types = infer_types(data, headers, types, delimiter);
  } else {
    all_types = types;
  }

  std::unordered_map<std::string, ColumnVariant> columns{};
  for (size_t i{}; i < headers.size(); ++i) {
    const std::string& column_name{headers[i]};
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

    std::vector<std::string_view> tokens{to_tokens(line, delimiter)};
    if (tokens.size() != headers.size()) {
      throw std::runtime_error("malformed line " + std::to_string(line_number) +
                               ": expected " + std::to_string(headers.size()) +
                               " columns, got " +
                               std::to_string(tokens.size()));
    }

    for (size_t i{}; i < headers.size(); ++i) {
      const std::string& column_name{headers[i]};
      const std::string_view value{tokens[i]};

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

  return DataFrame(row_count, std::move(headers), std::move(columns));
}

void to_csv(const DataFrame& df, const std::string& csv, char delimiter) {
  std::ofstream file{csv};
  if (!file.is_open()) {
    throw std::runtime_error("failed to open csv file: " + csv);
  }

  const std::vector<std::string> column_names{df.column_names()};
  for (size_t i{}; i < column_names.size(); ++i) {
    file << column_names[i];
    if (i < column_names.size() - 1) {
      file << delimiter;
    }
  }
  file << '\n';

  std::vector<const ColumnVariant*> columns{};
  columns.reserve(column_names.size());
  for (const auto& name : column_names) {
    const ColumnVariant* column_variant{df.get_column(name)};
    if (column_variant == nullptr) {
      throw std::runtime_error("column not found: " + name);
    }
    columns.push_back(column_variant);
  }

  for (size_t i{}; i < df.nrows(); ++i) {
    for (size_t j{}; j < column_names.size(); ++j) {
      std::visit(
          [&](const auto& column) {
            using T = std::decay_t<decltype(column)>::value_type;
            if (!utils::is_null<T>(column[i])) {
              file << column[i];
            }
          },
          *columns[j]);

      if (j < column_names.size() - 1) {
        file << delimiter;
      }
    }
    file << '\n';
  }
}

namespace {
std::vector<std::string_view> to_tokens(std::string_view line, char delimiter) {
  std::vector<std::string_view> tokens{};
  size_t start{};
  bool in_quotes{false};

  for (size_t i{}; i <= line.size(); ++i) {
    if (i < line.size() && line[i] == '"') {
      in_quotes = !in_quotes;
    }

    bool at_delimiter{false};
    if (i < line.size() && line[i] == delimiter && !in_quotes) {
      at_delimiter = true;
    }

    if (at_delimiter || i == line.size()) {
      std::string_view token{line.substr(start, i - start)};

      while (!token.empty() &&
             std::isspace(static_cast<unsigned char>(token.front()))) {
        token.remove_prefix(1);
      }

      while (!token.empty() &&
             std::isspace(static_cast<unsigned char>(token.back()))) {
        token.remove_suffix(1);
      }

      if (token.size() >= 2 && token.front() == '"' && token.back() == '"') {
        token.remove_prefix(1);
        token.remove_suffix(1);
      }

      tokens.push_back(token);
      start = i + 1;
    }
  }

  return tokens;
}

std::unordered_map<std::string, ColumnType> infer_types(
    std::string_view data, const std::vector<std::string>& headers,
    const std::unordered_map<std::string, ColumnType>& types, char delimiter) {
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

    std::vector<std::string_view> tokens{to_tokens(line, delimiter)};

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
}  // namespace
}  // namespace df