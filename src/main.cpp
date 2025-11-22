#include <iostream>

#include "dataframe.h"

int main() {
    std::vector<std::string> names {"ask", "sell"};

    std::vector<std::vector<std::optional<double>>> data {{3.5, 5.0, 10.25}, {3.4, std::nullopt, 11}};
  DataFrame df{names, data};

  std::cout << df.size() << std::endl;

  auto ask {df.get_column<double>("ask")};

  if (ask != nullptr) {
      for (const auto& a : *ask) {
        if (a.has_value())
        {
            std::cout << *a << std::endl;
        }
      }
  }

  return 0;
}
