#pragma once

#include <algorithm>
#include <cmath>
#include <iostream>
#include <limits>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <unordered_map>
#include <vector>

#include "utils.h"

enum class ColumnType { Int64, Double, String };

template <typename T>
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
      if (Utils::is_null(x)) {
        ++null_count;
      }
      data.emplace_back(x);
    }
  }

  using iterator = typename std::vector<T>::iterator;
  using const_iterator = typename std::vector<T>::const_iterator;

  size_t get_null_count() const { return null_count; }

  void set_null_count(size_t n) { null_count = n; }

  size_t size() const { return data.size(); }

  bool empty() const { return data.empty(); }

  void append(T value) {
    if (Utils::is_null(value)) {
      ++null_count;
    }
    data.emplace_back(std::move(value));
  }

  ColumnType get_type() { return type; }

  // =========================
  // statistical methods
  // =========================

  T maximum() const {
    if (data.empty()) {
      throw std::invalid_argument("cannot get maximum of empty column");
    }

    size_t start{0};
    while (start < data.size() && Utils::is_null(data[start])) {
      ++start;
    }

    if (start == data.size()) {
      throw std::invalid_argument("all values are null");
    }

    T max{data[start]};
    for (size_t i{start + 1}; i < data.size(); ++i) {
      if (!Utils::is_null(data[i]) && data[i] > max) {  // disregard null values
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
    while (start < data.size() && Utils::is_null(data[start])) {
      ++start;
    }

    if (start == data.size()) {
      throw std::invalid_argument("all values are null");
    }

    T min{data[start]};
    for (size_t i{start + 1}; i < data.size(); ++i) {
      if (!Utils::is_null(data[i]) && data[i] < min) {  // disregard null values
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
      if (!Utils::is_null<T>(data[i])) {
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

  double sum() const {
    if (data.empty()) {
      throw std::invalid_argument("cannot get sum of empty column");
    }

    double sum{};
    if constexpr (std::is_arithmetic_v<T>) {
      for (size_t i{0}; i < data.size(); ++i) {
        if (!Utils::is_null(data[i])) {
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
        if (!Utils::is_null(value)) {
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
        if (!Utils::is_null(data[i])) {
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

    if (Utils::is_null<T>(data[index])) {
      --null_count;
    }

    data.erase(data.begin() + index);
  }

  void resize(size_t count) { data.resize(count); }

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
