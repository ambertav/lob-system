#pragma once

#include <cmath>
#include <iostream>
#include <limits>
#include <optional>
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
