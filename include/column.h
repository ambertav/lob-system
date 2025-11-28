#pragma once

#include <iostream>
#include <optional>
#include <vector>

template <typename T>
class Column {
 private:
  std::vector<std::optional<T>> data;
  size_t null_count{};

 public:
  Column() = default;
  Column(size_t size_reserve) { data.reserve(size_reserve); }

  Column(const std::vector<std::optional<T>>& d) {
    data.reserve(d.size());

    for (const auto& x : d) {
      data.emplace_back(x);
      if (!x.has_value()) {
        ++null_count;
      }
    }
  }

  using iterator = typename std::vector<std::optional<T>>::iterator;
  using const_iterator = typename std::vector<std::optional<T>>::const_iterator;

  size_t get_null_count() const { return null_count; }

  void set_null_count(size_t n) { null_count = n; }

  void increment_null_count() { ++null_count; }

  size_t size() const { return data.size(); }

  bool empty() const { return data.empty(); }

  void append(std::optional<T> value) { data.emplace_back(value); }

  std::optional<T>& operator[](size_t i) {
    if (i >= data.size()) {
      throw std::out_of_range("column index out of range");
    }

    return data[i];
  }

  const std::optional<T>& operator[](size_t i) const {
    if (i >= data.size()) {
      throw std::out_of_range("column index out of range");
    }

    return data[i];
  }

  iterator begin() { return data.begin(); }

  const_iterator begin() const { return data.begin(); }

  const_iterator cbegin() const noexcept { return data.cbegin(); }

  iterator end() { return data.end(); }

  const_iterator end() const { return data.end(); }

  const_iterator cend() const noexcept { return data.cend(); }

  std::optional<T>& front() { return data.front(); }

  const std::optional<T>& front() const { return data.front(); }

  std::optional<T>& back() { return data.back(); }

  const std::optional<T>& back() const { return data.back(); }
};
