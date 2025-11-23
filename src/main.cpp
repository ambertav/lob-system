#include <iostream>

#include "dataframe.h"

int main() {
    std::vector<std::string> names {"ask", "sell"};

    std::vector<std::vector<std::optional<double>>> data {{3.5, 5.0, 10.25, 11, 12, 13, 15}, {3.4, std::nullopt, 11, 5, 4, 3, 2}};
  DataFrame df{names, data};

  std::cout << df.size() << std::endl;

  std::string new_column;
  std::vector<std::optional<std::string>> new_data {"11/20/2025", "11/21/2025", "11/22/2025"};
  df.add_column(new_column, new_data);

  const auto dates {df.get_column<std::string>(new_column)};

  if (dates != nullptr) {
      for (const auto& d : *dates) {
        if (d.has_value())
        {
            std::cout << *d << std::endl;
        }
      }
  }

  df.head();

  std::cout << "NOW CHECKING TAIL:\n";
  df.tail();

  return 0;
}
