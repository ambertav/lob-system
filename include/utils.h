#pragma once

#include <array>
#include <charconv>
#include <string_view>
#include <vector>

namespace df {
namespace utils {
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

inline std::vector<std::string_view> to_tokens(std::string_view sv,
                                           char delimiter) {
  std::vector<std::string_view> tokens{};
  size_t start{};
  bool in_quotes{false};

  for (size_t i{}; i <= sv.size(); ++i) {
    if (i < sv.size() && sv[i] == '"') {
      in_quotes = !in_quotes;
    }

    bool at_delimiter{false};
    if (i < sv.size() && sv[i] == delimiter && !in_quotes) {
      at_delimiter = true;
    }

    if (at_delimiter || i == sv.size()) {
      std::string_view token{sv.substr(start, i - start)};

      // Trim whitespace
      while (!token.empty() &&
             std::isspace(static_cast<unsigned char>(token.front()))) {
        token.remove_prefix(1);
      }
      while (!token.empty() &&
             std::isspace(static_cast<unsigned char>(token.back()))) {
        token.remove_suffix(1);
      }

      // Remove surrounding quotes if present
      if (token.size() >= 2 && token.front() == '"' && token.back() == '"') {
        token.remove_prefix(1);
        token.remove_suffix(1);
      }

      tokens.push_back(token);
      start = i + 1;
    }
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

inline constexpr std::array<const char*, 8> describe_order{
    "count", "mean", "std", "min", "25%", "50%", "75%", "max"};

}  // namespace utils
}  // namespace df