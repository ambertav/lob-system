#include "dataframe.h"

DataFrame::DataFrame(std::vector<std::string> cn)
    : order(std::move(cn)) {}

/*
    DataFrame::read_csv(const std::string& csv)
{

}
*/

size_t DataFrame::size() const { 
    return rows * cols; 
}

bool DataFrame::empty() const { return rows == 0; }

std::pair<size_t, size_t> DataFrame::shape() const { return {rows, cols}; }

size_t DataFrame::nrows() const { return rows; }

size_t DataFrame::ncols() const { return cols; }

void DataFrame::head() const {
  // get top 5 rows and print
}

void DataFrame::tail() const {
  // get last 5 rows and print
}

void DataFrame::info() const {
  // row number (0 index range)
  // columns info (number, types) in order
  // memory usage
}
