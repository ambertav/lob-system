#pragma once

#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "column.h"

class DataFrame {
 private:
  std::unordered_map<std::string, std::unique_ptr<ColumnBase>> columns;
  std::vector<std::string> order;

  size_t rows;
  size_t cols;

 public:
  DataFrame() = default;

  explicit DataFrame(std::vector<std::string> cn);
  template <typename T>
  explicit DataFrame(std::vector<std::string> cn,
                     const std::vector<std::vector<std::optional<T>>>& d) {
    if (d.size() != cn.size()) {
      throw std::runtime_error("column count does not match");
    }

    for (size_t i{}; i < cn.size(); ++i) {
        columns[cn[i]] = std::make_unique<Column<T>>(d[i]);
    }

    order = std::move(cn);
    
    // assume input has same number of rows for now
    rows = d[0].size();
    cols = order.size();
  }

  // void read_csv(const std::string& csv);

  size_t size() const;
  bool empty() const;

  std::pair<size_t, size_t> shape() const;
  size_t nrows() const;
  size_t ncols() const;

  void head() const;
  void tail() const;

  void info() const;

  template <typename T>
  Column<T>* get_column(const std::string& col_name) const {
    auto it {columns.find(col_name)};
    if (it == columns.end()) {
        return nullptr;
    }
    return dynamic_cast<Column<T>*>(it->second.get());
  }
};
