#include "dataframe.h"

#include <iomanip>
#include <iostream>

DataFrame::DataFrame(std::vector<std::string> cn)
    : column_info(std::move(cn)) {}

/*
    DataFrame::read_csv(const std::string& csv)
{

}
*/

size_t DataFrame::size() const { return rows * cols; }

bool DataFrame::empty() const { return rows == 0; }

std::pair<size_t, size_t> DataFrame::shape() const { return {rows, cols}; }

size_t DataFrame::nrows() const { return rows; }

size_t DataFrame::ncols() const { return cols; }

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

void DataFrame::info() const {
  // row number (0 index range)
  // columns info (number, types) in order
  // memory usage
}

void DataFrame::print(size_t start, size_t end) const {
  std::vector<int> widths{};  // for formating
  widths.reserve(column_info.size() + 1);

  widths.push_back(2);
  std::cout << std::setw(widths.back()) << " ";

  for (const auto& column_name : column_info) {
    // set widths according to column name size
    widths.push_back(column_name.size() + 5);
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
