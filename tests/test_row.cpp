#include <gtest/gtest.h>

#include "row.h"

using namespace df;

class RowTest : public ::testing::Test {
 protected:
  std::unordered_map<std::string, RowVariant> row_data{{"name", "test"},
                                                       {"age", 100}};

  Row row{row_data};
  Row empty_row{};

  Row row_via_initializer{{"name", "Amber"},
                          {"age", 28},
                          {"gpa", 3.98},
                          {"profession", "Software Engineer"}};
};

// constructor with empty, single, multiple, null
TEST_F(RowTest, ConstructorInitializesCorrectly) {
  EXPECT_TRUE(empty_row.empty());
  EXPECT_FALSE(row.empty());

  for (const auto& [key, value] : row_data) {
    EXPECT_TRUE(row.contains(key));

    std::visit(
        [&](const auto& expected) {
          using T = std::decay_t<decltype(expected)>;
          EXPECT_EQ(row.at<T>(key), expected);
        },
        value);
  }
}

TEST_F(RowTest, InitializerListConstructsCorrectly) {
  EXPECT_EQ(row_via_initializer.at<std::string>("name"), "Amber");
  EXPECT_EQ(row_via_initializer.at<int64_t>("age"), 28);
  EXPECT_EQ(row_via_initializer.at<double>("gpa"), 3.98);
  EXPECT_EQ(row_via_initializer.at<std::string>("profession"),
            "Software Engineer");
}

TEST_F(RowTest, GetReturnsOptional) {
  auto value_opt{row.get<std::string>("name")};
  EXPECT_TRUE(value_opt.has_value());

  EXPECT_THROW(row.get<std::string>("age"), std::bad_variant_access);

  auto not_found_opt{row.get<std::string>("profession")};
  EXPECT_FALSE(not_found_opt.has_value());
}

TEST_F(RowTest, AtThrowsOrReturnsValue) {
  auto name{row.at<std::string>("name")};
  EXPECT_EQ(name, std::get<std::string>(row_data.at("name")));

  EXPECT_THROW(row.at<std::string>("age"), std::bad_variant_access);

  EXPECT_THROW(row.at<std::string>("profession"), std::out_of_range);
}

TEST_F(RowTest, SetAddsOrUpdatesAValue) {
  std::string new_name{"testing"};
  row.set<std::string>("name", new_name);
  EXPECT_EQ(row.at<std::string>("name"), new_name);

  double salary{75000.75};
  row.set<double>("salary", salary);
  EXPECT_TRUE(row.contains("salary"));
  EXPECT_EQ(row.at<double>("salary"), salary);
}

TEST_F(RowTest, UpdateThrowsOrUpdates) {
  std::string new_name{"testing"};
  row.update<std::string>("name", new_name);
  EXPECT_EQ(row.at<std::string>("name"), new_name);

  EXPECT_THROW(row.update<double>("salary", 100.00), std::out_of_range);
  EXPECT_FALSE(row.contains("salary"));
}

TEST_F(RowTest, SetAndUpdateDoNotModifyExistingType) {
  int64_t invalid_type{5};

  EXPECT_THROW(row.set<int64_t>("name", invalid_type), std::bad_variant_access);
  EXPECT_THROW(row.update<int64_t>("name", invalid_type),
               std::bad_variant_access);
}
