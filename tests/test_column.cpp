#include <gtest/gtest.h>

#include "column.h"

template <typename T>
class ColumnTypedTest : public ::testing::Test {
 protected:
  using Col = Column<T>;

  T get_test_value(int index) {
    if constexpr (std::is_same_v<T, int>) {
      return 10 + index;
    } else if constexpr (std::is_same_v<T, double>) {
      return 1.5 * (index + 1);
    } else if constexpr (std::is_same_v<T, std::string>) {
      return "test" + std::to_string(index);
    }
  }
};

using MyTypes = ::testing::Types<int, double, std::string>;
TYPED_TEST_SUITE(ColumnTypedTest, MyTypes);

TYPED_TEST(ColumnTypedTest, DefaultConstructor) {
  typename TestFixture::Col col;
  EXPECT_EQ(col.size(), 0);
  EXPECT_TRUE(col.empty());
  EXPECT_EQ(col.get_null_count(), 0);
}

TYPED_TEST(ColumnTypedTest, VectorConstructor) {
  int n{3};
  std::vector<std::optional<TypeParam>> vec{};

  for (int i{0}; i < n; ++i) {
    vec.push_back(this->get_test_value(i));
  }

  typename TestFixture::Col col(vec);

  EXPECT_EQ(col.size(), vec.size());
  EXPECT_EQ(col.get_null_count(), 0);

  for (int i{0}; i < n; ++i) {
    EXPECT_EQ(col[i], this->get_test_value(i));
  }
}

TYPED_TEST(ColumnTypedTest, VectorConstructorWithNullOpt) {
  int n{4};
  std::vector<std::optional<TypeParam>> vec{};

  for (int i{0}; i < n; ++i) {
    if (i % 2 == 0) {
      vec.push_back(this->get_test_value(i));
    } else {
      vec.push_back(std::nullopt);
    }
  }

  typename TestFixture::Col col(vec);

  EXPECT_EQ(col.size(), 4);
  EXPECT_EQ(col.get_null_count(), 2);

  for (int i{0}; i < n; ++i) {
    if (i % 2 == 0) {
      EXPECT_EQ(col[i].value(), this->get_test_value(i));
    } else {
      EXPECT_FALSE(col[i].has_value());
    }
  }
}

TYPED_TEST(ColumnTypedTest, AppendValue) {
  typename TestFixture::Col col{};

  auto value{this->get_test_value(0)};

  col.append(value);

  EXPECT_EQ(col.size(), 1);
  EXPECT_EQ(col.get_null_count(), 0);
  EXPECT_EQ(col[0].value(), value);
}

TYPED_TEST(ColumnTypedTest, AppendNull) {
  typename TestFixture::Col col{};

  col.append(std::nullopt);

  EXPECT_EQ(col.size(), 1);
  EXPECT_EQ(col.get_null_count(), 1);
  EXPECT_FALSE(col[0].has_value());
}

TYPED_TEST(ColumnTypedTest, OperatorThrowsOutOfRange) {
  typename TestFixture::Col col{};
  col.append(this->get_test_value(0));

  EXPECT_THROW(col[5], std::out_of_range);
}

TYPED_TEST(ColumnTypedTest, NullCountManagement) {
  typename TestFixture::Col col{};

  size_t nulls{5};
  col.set_null_count(nulls);
  EXPECT_EQ(col.get_null_count(), nulls);
}