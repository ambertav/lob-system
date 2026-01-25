#include "io/binary.h"

#include <fstream>
#include <stdexcept>

namespace df {
DataFrame from_bytes(const std::vector<std::byte>& bytes) {
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
      throw std::runtime_error("unknonw column type during deserialization");
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

std::vector<std::byte> to_bytes(const DataFrame& df) {
  size_t rows{df.nrows()};
  size_t cols{df.ncols()};

  size_t metadata_size{sizeof(size_t) * 2};

  const std::vector<std::string> column_names{df.column_names()};
  for (const auto& name : column_names) {
    metadata_size += sizeof(uint32_t) + name.size();
  }
  metadata_size += cols * sizeof(ColumnType);

  std::vector<std::byte> result{};
  result.reserve(metadata_size);

  const std::byte* rows_bytes{reinterpret_cast<const std::byte*>(&rows)};
  result.insert(result.end(), rows_bytes, rows_bytes + sizeof(size_t));

  const std::byte* cols_bytes{reinterpret_cast<const std::byte*>(&cols)};
  result.insert(result.end(), cols_bytes, cols_bytes + sizeof(size_t));

  for (const auto& name : column_names) {
    uint32_t length{static_cast<uint32_t>(name.size())};
    const std::byte* length_bytes{reinterpret_cast<const std::byte*>(&length)};
    result.insert(result.end(), length_bytes, length_bytes + sizeof(uint32_t));

    const std::byte* name_bytes{
        reinterpret_cast<const std::byte*>(name.data())};
    result.insert(result.end(), name_bytes, name_bytes + name.size());
  }

  for (const auto& name : column_names) {
    const ColumnVariant* column{df.get_column(name)};
    ColumnType type{std::visit(
        [](const auto& col) -> ColumnType { return col.get_type(); }, *column)};

    const std::byte* type_bytes{reinterpret_cast<const std::byte*>(&type)};
    result.insert(result.end(), type_bytes, type_bytes + sizeof(ColumnType));

    std::vector<std::byte> column_bytes{std::visit(
        [](const auto& col) -> std::vector<std::byte> {
          return col.to_bytes();
        },
        *column)};
    result.insert(result.end(), column_bytes.begin(), column_bytes.end());
  }

  return result;
}

DataFrame from_binary(const std::string& path) {
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

void to_binary(const DataFrame& df, const std::string& path) {
  std::ofstream file{path, std::ios::binary};
  if (!file.is_open()) {
    throw std::runtime_error("failed to open binary file: " + path);
  }

  std::vector<std::byte> data{to_bytes(df)};
  file.write(reinterpret_cast<const char*>(data.data()), data.size());
  file.close();
}
}  // namespace df