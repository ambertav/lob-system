#pragma once

#include <algorithm>
#include <cmath>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <unordered_map>
#include <vector>

#include "utils.h"

namespace df {
enum class ColumnType { Int64, Double, String };

template <typename T>
concept Storable = std::is_same_v<T, int64_t> || std::is_same_v<T, double> ||
                   std::is_same_v<T, std::string>;

template <Storable T>
class Column {
 public:
  using value_type = T;

 private:
  std::vector<T> data;
  ColumnType type{[]() constexpr {
    if constexpr (std::is_same_v<T, int64_t>) {
      return ColumnType::Int64;
    } else if constexpr (std::is_same_v<T, double>) {
      return ColumnType::Double;
    } else if constexpr (std::is_same_v<T, std::string>) {
      return ColumnType::String;
    }
  }()};
  size_t null_count{};

 public:
  Column() = default;
  Column(size_t size_reserve) { data.reserve(size_reserve); }

  Column(const std::vector<T>& d) {
    data.reserve(d.size());

    for (const auto& x : d) {
      if (utils::is_null(x)) {
        ++null_count;
      }
      data.emplace_back(x);
    }
  }

  size_t get_null_count() const { return null_count; }

  void set_null_count(size_t n) { null_count = n; }

  size_t nrows() const { return data.size(); }

  bool empty() const { return data.empty(); }

  void clear() {
    data.clear();
    null_count = 0;
  }

  void append(T value) {
    if (utils::is_null(value)) {
      ++null_count;
    }
    data.emplace_back(std::move(value));
  }

  ColumnType get_type() const { return type; }

  void describe() const {
    if (data.empty()) {
      std::cout << "column is empty\n";
      return;
    }

    if constexpr (std::is_arithmetic_v<T>) {
      std::unordered_map<std::string, double> stats_by_row{};
      stats_by_row["count"] = nrows() - get_null_count();
      stats_by_row["mean"] = mean();
      stats_by_row["std"] = standard_deviation();
      stats_by_row["min"] = minimum();
      stats_by_row["25%"] = percentile(0.25);
      stats_by_row["50%"] = percentile(0.5);
      stats_by_row["75%"] = percentile(0.75);
      stats_by_row["max"] = maximum();

      for (const auto& stat : utils::describe_order) {
        std::cout << std::setw(10) << stat;
        std::cout << std::setw(12) << std::fixed << std::setprecision(2)
                  << stats_by_row[stat] << '\n';
      }
    } else {
      throw std::invalid_argument("cannot describe a non-numerical column");
    }
  }

  // =========================
  // serialization methods
  // =========================

  std::vector<std::byte> to_bytes() const {
    if constexpr (std::is_arithmetic_v<T>) {
      const std::byte* byte_ptr{
          reinterpret_cast<const std::byte*>(data.data())};
      const size_t total_bytes{data.size() * sizeof(T)};
      return std::vector<std::byte>(byte_ptr, byte_ptr + total_bytes);
    } else {
      size_t total_bytes{};
      for (const auto& value : data) {
        total_bytes += sizeof(uint32_t) + value.size();
      }

      std::vector<std::byte> result{};
      result.reserve(total_bytes);

      for (const auto& value : data) {
        uint32_t length{static_cast<uint32_t>(value.size())};
        const std::byte* length_bytes{
            reinterpret_cast<const std::byte*>(&length)};
        result.insert(result.end(), length_bytes,
                      length_bytes + sizeof(uint32_t));

        const std::byte* value_bytes{
            reinterpret_cast<const std::byte*>(value.data())};
        result.insert(result.end(), value_bytes, value_bytes + value.size());
      }
      return result;
    }
  }

  static Column<T> from_bytes(const std::vector<std::byte>& bytes) {
    if (bytes.empty()) {
      throw std::runtime_error("cannot deserialize from empty bytes");
    }

    if constexpr (std::is_arithmetic_v<T>) {
      if (bytes.size() % sizeof(T) != 0) {
        throw std::runtime_error("invalid byte vector size for type");
      }

      const size_t n{bytes.size() / sizeof(T)};
      std::vector<T> d(n);
      std::memcpy(d.data(), bytes.data(), bytes.size());
      return Column<T>(std::move(d));

    } else {
      std::vector<std::string> d{};
      size_t offset{};

      while (offset < bytes.size()) {
        if (offset + sizeof(uint32_t) > bytes.size()) {
          throw std::runtime_error("truncated data, cannot read string length");
        }

        uint32_t length{};
        std::memcpy(&length, bytes.data() + offset, sizeof(uint32_t));
        offset += sizeof(uint32_t);

        if (offset + length > bytes.size()) {
          throw std::runtime_error("truncated data, cannot read string data");
        }

        const char* str_data{
            reinterpret_cast<const char*>(bytes.data() + offset)};
        d.emplace_back(str_data, length);
        offset += length;
      }

      return Column<T>(std::move(d));
    }
  }

  // =========================
  // statistical methods
  // =========================

  T maximum() const {
    if (data.empty()) {
      throw std::invalid_argument("cannot get maximum of empty column");
    }

    size_t start{0};
    while (start < data.size() && utils::is_null(data[start])) {
      ++start;
    }

    if (start == data.size()) {
      throw std::invalid_argument("all values are null");
    }

    T max{data[start]};
    for (size_t i{start + 1}; i < data.size(); ++i) {
      if (!utils::is_null(data[i]) && data[i] > max) {  // disregard null values
        max = data[i];
      }
    }

    return max;
  }

  T minimum() const {
    if (data.empty()) {
      throw std::invalid_argument("cannot get minimum of empty column");
    }

    size_t start{0};
    while (start < data.size() && utils::is_null(data[start])) {
      ++start;
    }

    if (start == data.size()) {
      throw std::invalid_argument("all values are null");
    }

    T min{data[start]};
    for (size_t i{start + 1}; i < data.size(); ++i) {
      if (!utils::is_null(data[i]) && data[i] < min) {  // disregard null values
        min = data[i];
      }
    }

    return min;
  }

  std::vector<T> mode() const {
    if (data.empty()) {
      throw std::invalid_argument("cannot get mode of empty column");
    }

    std::vector<T> modes{};
    std::unordered_map<T, size_t> frequency{};
    for (size_t i{0}; i < data.size(); ++i) {
      if (!utils::is_null<T>(data[i])) {
        ++frequency[data[i]];
      }
    }

    if (frequency.empty()) {
      throw std::invalid_argument("all values are nulls, cannot compute mode");
    }

    size_t max_frequency{2};  // skips count of 1
    for (const auto& [value, count] : frequency) {
      if (count > max_frequency) {
        max_frequency = count;

        modes.clear();
        modes.push_back(value);
      } else if (count == max_frequency) {
        modes.push_back(value);
      }
    }

    return modes;
  }

  double percentile(double p = 0.0) const {
    if (data.empty()) {
      throw std::invalid_argument("cannot get percentile of empty column");
    }

    size_t non_null{data.size() - null_count};
    if (non_null == 0) {
      throw std::invalid_argument("cannot get percentile: no non-null values");
    }

    if (p < 0.0 || p > 1.0) {
      throw std::invalid_argument("percentile must be between 0 and 1");
    }

    if constexpr (std::is_arithmetic_v<T>) {
      std::vector<T> copy{};
      copy.reserve(data.size() - null_count);
      for (const auto& value : data) {
        if (!utils::is_null(value)) {
          copy.push_back(value);
        }
      }

      if (copy.size() == 1) {
        return static_cast<double>(copy[0]);
      }

      std::sort(copy.begin(), copy.end());

      double index{p * (copy.size() - 1)};
      size_t lower{static_cast<size_t>(std::floor(index))};
      size_t upper{static_cast<size_t>(std::ceil(index))};

      if (lower == upper) {
        return static_cast<double>(copy[lower]);
      }

      double fraction{index - lower};
      return static_cast<double>(copy[lower]) * (1 - fraction) +
             static_cast<double>(copy[upper]) * fraction;
    } else {
      throw std::invalid_argument("column is not numeric type");
    }
  }

  double sum() const {
    if (data.empty()) {
      throw std::invalid_argument("cannot get sum of empty column");
    }

    size_t non_null{data.size() - null_count};
    if (non_null == 0) {
      throw std::invalid_argument("cannot get median: no non-null values");
    }

    double sum{};
    if constexpr (std::is_arithmetic_v<T>) {
      for (size_t i{0}; i < data.size(); ++i) {
        if (!utils::is_null(data[i])) {
          sum += data[i];
        }
      }
    } else {
      throw std::invalid_argument("column is not numeric type");
    }

    return sum;
  }

  double median() const {
    if (data.empty()) {
      throw std::invalid_argument("cannot get median of empty column");
    }

    size_t non_null{data.size() - null_count};
    if (non_null == 0) {
      throw std::invalid_argument("cannot get median: no non-null values");
    }

    if constexpr (std::is_arithmetic_v<T>) {
      // create copy without null values
      std::vector<T> copy{};
      copy.reserve(data.size() - null_count);
      for (const auto& value : data) {
        if (!utils::is_null(value)) {
          copy.push_back(value);
        }
      }
      size_t n{copy.size()};

      if (n % 2 == 1) {
        std::nth_element(copy.begin(), copy.begin() + n / 2, copy.end());
        return copy[n / 2];
      } else {
        std::nth_element(copy.begin(), copy.begin() + n / 2 - 1, copy.end());

        double left{static_cast<double>(copy[n / 2 - 1])};
        double right{static_cast<double>(
            *std::min_element(copy.begin() + n / 2, copy.end()))};

        return (left + right) / 2.0;
      }
    } else {
      throw std::invalid_argument("column is not numeric type");
    }
  }

  double mean() const {
    if (data.empty()) {
      throw std::invalid_argument("cannot get mean of empty column");
    }

    size_t non_null{data.size() - null_count};
    if (non_null == 0) {
      throw std::invalid_argument("cannot get mean: no non-null values");
    }
    return sum() / (data.size() - null_count);
  }

  double standard_deviation() const {
    if (data.empty()) {
      throw std::invalid_argument(
          "cannot get standard deviation of empty column");
    }

    return std::sqrt(variance());
  }

  double variance() const {
    if (data.empty()) {
      throw std::invalid_argument("cannot get variance of empty column");
    }

    double summation{};
    size_t non_null{data.size() - null_count};
    if (non_null == 0) {
      throw std::invalid_argument("cannot get variance: no non-null values");
    }

    if constexpr (std::is_arithmetic_v<T>) {
      double mu{mean()};
      for (size_t i{0}; i < data.size(); ++i) {
        if (!utils::is_null(data[i])) {
          summation += (data[i] - mu) * (data[i] - mu);
        }
      }
    } else {
      throw std::invalid_argument("column is not numeric type");
    }

    return summation / (non_null - 1);
  }

  // =========================
  // accessor and iterators
  // =========================

  bool operator==(const Column<T>& other) const { return data == other.data; }

  bool operator!=(const Column<T>& other) const { return data != other.data; }

  T& operator[](size_t i) {
    if (i >= data.size()) {
      throw std::out_of_range("column index out of range");
    }

    return data[i];
  }

  const T& operator[](size_t i) const {
    if (i >= data.size()) {
      throw std::out_of_range("column index out of range");
    }

    return data[i];
  }

  void erase(size_t index) {
    if (index >= data.size()) {
      throw std::out_of_range("column index out of range");
    }

    if (utils::is_null<T>(data[index])) {
      --null_count;
    }

    data.erase(data.begin() + index);
  }

  void reserve(size_t capacity) { data.reserve(capacity); }
  void resize(size_t count) { data.resize(count); }

  using iterator = typename std::vector<T>::iterator;
  using const_iterator = typename std::vector<T>::const_iterator;

  iterator begin() { return data.begin(); }
  const_iterator begin() const { return data.begin(); }
  const_iterator cbegin() const noexcept { return data.cbegin(); }
  iterator end() { return data.end(); }
  const_iterator end() const { return data.end(); }
  const_iterator cend() const noexcept { return data.cend(); }

  T& front() { return data.front(); }
  const T& front() const { return data.front(); }
  T& back() { return data.back(); }
  const T& back() const { return data.back(); }
};
}  // namespace df