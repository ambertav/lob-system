#pragma once

#include <charconv>
#include <string_view>
#include <vector>

namespace Utils {
inline std::string_view trim(std::string_view sv) {
  while (!sv.empty() && (std::isspace(static_cast<unsigned char>(sv.front())) ||
                         sv.front() == '\r' || sv.front() == '\n')) {
    sv.remove_prefix(1);
  }

  while (!sv.empty() && (std::isspace(static_cast<unsigned char>(sv.back())) ||
                         sv.back() == '\r' || sv.back() == '\n')) {
    sv.remove_suffix(1);
  }

  return sv;
}

inline std::vector<std::string_view> split(std::string_view sv,
                                           char delimiter) {
  size_t start{};
  std::vector<std::string_view> tokens{};

  while (true) {
    auto pos{sv.find(delimiter, start)};
    std::string_view token{};

    if (pos == std::string_view::npos) {
      token = trim(sv.substr(start));
      tokens.emplace_back(token);
      break;
    }

    tokens.emplace_back(sv.substr(start, pos - start));
    start = pos + 1;
  }

  return tokens;
}

/*
NOTE: using min values as null for int64 and double
should be out of range for data and preserves sort
*/
template <typename T>
inline T get_null() {
  if constexpr (std::is_same_v<T, int64_t>) {
    return std::numeric_limits<int64_t>::min();
  } else if constexpr (std::is_same_v<T, double>) {
    return std::numeric_limits<double>::lowest();
  } else if constexpr (std::is_same_v<T, std::string>) {
    return "";
  }
}

template <typename T>
inline bool is_null(const T& value) {
  if constexpr (std::is_same_v<T, int64_t>) {
    return value == std::numeric_limits<int64_t>::min();
  } else if constexpr (std::is_same_v<T, double>) {
    return value == std::numeric_limits<double>::lowest();
  } else if constexpr (std::is_same_v<T, std::string>) {
    return value.empty();
  }
}

template <typename T>
inline bool try_parse(std::string_view sv) {
  T result{};
  auto [ptr, ec]{std::from_chars(sv.data(), sv.data() + sv.size(), result)};
  return ec == std::errc{} && ptr == sv.data() + sv.size();
}

template <typename T>
inline T parse(std::string_view sv) {
  T result{};
  auto [ptr, ec]{std::from_chars(sv.data(), sv.data() + sv.size(), result)};

  if (ec == std::errc{} && ptr == sv.data() + sv.size()) {
    return result;
  } else {
    return get_null<T>();
  }
}

}  // namespace Utils