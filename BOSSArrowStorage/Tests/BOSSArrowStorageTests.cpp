#include "../Source/BOSSArrowStorageEngine.hpp"
#define CATCH_CONFIG_MAIN
#include <ExpressionUtilities.hpp>
#include <catch2/catch.hpp>
using boss::utilities::operator""_;

TEST_CASE("Empty Table", "[empty]") {
  boss::engines::arrow_storage::Engine engine;

  auto result = engine.evaluate("CreateTable"_("DummyTable"_, "ColA"_, "ColB"_, "ColC"_));
  REQUIRE(result == boss::Expression(true));

  auto emptyResult = engine.evaluate("DummyTable"_);
  REQUIRE(emptyResult == "Table"_("Column"_("ColA"_, "List"_()), "Column"_("ColB"_, "List"_()),
                                  "Column"_("ColC"_, "List"_())));

  auto rewriteResult = engine.evaluate(
      "Select"_("Project"_("DummyTable"_, "As"_("ColA"_, "ColA"_, "ColB"_, "ColB"_)),
                "Greater"_("ColA"_, 10))); // NOLINT
  REQUIRE(rewriteResult == "Select"_("Project"_("Table"_("Column"_("ColA"_, "List"_()),
                                                         "Column"_("ColB"_, "List"_()),
                                                         "Column"_("ColC"_, "List"_())),
                                                "As"_("ColA"_, "ColA"_, "ColB"_, "ColB"_)),
                                     "Greater"_("ColA"_, 10)));
}

TEST_CASE("Create a TPC-H table", "[tpch]") {
  boss::engines::arrow_storage::Engine engine;

  auto createResult = engine.evaluate(
      "CreateTable"_("NATION"_, "N_NATIONKEY"_, "N_NAME"_, "N_REGIONKEY"_, "N_COMMENT"_));
  REQUIRE(createResult == boss::Expression(true));

  auto emptyResult = engine.evaluate("NATION"_);
  REQUIRE(emptyResult ==
          "Table"_("Column"_("N_NATIONKEY"_, "List"_()), "Column"_("N_NAME"_, "List"_()),
                   "Column"_("N_REGIONKEY"_, "List"_()), "Column"_("N_COMMENT"_, "List"_())));

  auto loadResult = engine.evaluate("Load"_("NATION"_, "../Tests/nation.tbl"));
  REQUIRE(loadResult == boss::Expression(true));

  auto loadedResult = engine.evaluate("NATION"_);
  auto result = std::move(loadedResult); // check if the spans can be moved
  REQUIRE(boss::get<boss::ComplexExpression>(result).getHead() == "Table"_);

  INFO(result);

  for(int i = 0; i < 4; ++i) {
    REQUIRE(boss::get<boss::ComplexExpression>(
                boss::get<boss::ComplexExpression>(result).getArguments()[i])
                .getHead() == "Column"_);
    REQUIRE(boss::get<boss::ComplexExpression>(
                boss::get<boss::ComplexExpression>(
                    boss::get<boss::ComplexExpression>(result).getArguments()[i])
                    .getArguments()[1])
                .getHead() == "List"_);
    REQUIRE(boss::get<boss::ComplexExpression>(
                boss::get<boss::ComplexExpression>(
                    boss::get<boss::ComplexExpression>(result).getArguments()[i])
                    .getArguments()[1])
                .getArguments()
                .size() > 1);
  }

  auto rewriteStrings = engine.evaluate("Select"_(
      "Project"_("NATION"_, "As"_("N_NAME"_, "N_NAME"_)), "StringContainsQ"_("N_NAME"_, "BRAZIL")));
  INFO(rewriteStrings);
  REQUIRE(boss::get<boss::ComplexExpression>(rewriteStrings).getDynamicArguments()[1] ==
          "Equal"_("N_NAME"_, 2));
}

TEST_CASE("Test string columns", "[strings]") {
  boss::engines::arrow_storage::Engine engine;

  REQUIRE(engine.evaluate("Set"_("UseAutoDictionaryEncoding"_, false)) == boss::Expression(true));

  auto createResult = engine.evaluate(
      "CreateTable"_("NATION"_, "N_NATIONKEY"_, "N_NAME"_, "N_REGIONKEY"_, "N_COMMENT"_));
  REQUIRE(createResult == boss::Expression(true));

  auto loadResult = engine.evaluate("Load"_("NATION"_, "../Tests/nation.tbl"));
  REQUIRE(loadResult == boss::Expression(true));

  auto result = engine.evaluate("NATION"_);
  REQUIRE(boss::get<boss::ComplexExpression>(result).getHead() == "Table"_);

  auto nameColumn = boss::get<boss::ComplexExpression>(result).getArguments()[1];
  REQUIRE(boss::get<boss::ComplexExpression>(nameColumn) ==
          "Column"_("N_NAME"_, "DictionaryEncodedList"_("List"_(0, 7, 16, 22, 28), "ALGERIA"
                                                                                   "ARGENTINA"
                                                                                   "BRAZIL"
                                                                                   "CANADA")));

  auto noRewriteStrings = engine.evaluate("Select"_(
      "Project"_("NATION"_, "As"_("N_NAME"_, "N_NAME"_)), "StringContainsQ"_("N_NAME"_, "BRAZIL")));
  INFO(noRewriteStrings);
  REQUIRE(boss::get<boss::ComplexExpression>(noRewriteStrings).getDynamicArguments()[1] ==
          "StringContainsQ"_("N_NAME"_, "BRAZIL"));
}

TEST_CASE("Test column type specification", "[columnTypes]") {
  boss::engines::arrow_storage::Engine engine;

  REQUIRE(engine.evaluate("Set"_("LoadToMemoryMappedFiles"_, false)) == boss::Expression(true));

  boss::Symbol nationKeyType = GENERATE("BIGINT"_, "INTEGER"_);
  boss::Symbol regionKeyType = GENERATE("BIGINT"_, "DOUBLE"_);

  auto createResult =
      engine.evaluate("CreateTable"_("NATION"_, "N_NATIONKEY"_, "As"_(nationKeyType), "N_NAME"_,
                                     "N_REGIONKEY"_, "As"_(regionKeyType), "N_COMMENT"_));
  REQUIRE(createResult == boss::Expression(true));

  auto loadResult = engine.evaluate("Load"_("NATION"_, "../Tests/nation.tbl"));
  REQUIRE(loadResult == boss::Expression(true));

  auto result = engine.evaluate("NATION"_);
  REQUIRE(boss::get<boss::ComplexExpression>(result).getArguments().size() == 4);

  auto nationKeyColumn = boss::get<boss::ComplexExpression>(result).getArguments()[0];
  REQUIRE(boss::get<boss::ComplexExpression>(nationKeyColumn) ==
          "Column"_("N_NATIONKEY"_,
                    (nationKeyType == "BIGINT"_
                         ? boss::ComplexExpression("List"_(1LL, 2LL, 3LL, 4LL)) // NOLINT
                         : boss::ComplexExpression("List"_(1, 2, 3, 4)))));     // NOLINT

  auto regionKeyColumn = boss::get<boss::ComplexExpression>(result).getArguments()[2];
  REQUIRE(boss::get<boss::ComplexExpression>(regionKeyColumn) ==
          "Column"_("N_REGIONKEY"_,
                    (regionKeyType == "BIGINT"_
                         ? boss::ComplexExpression("List"_(0LL, 1LL, 1LL, 1LL))     // NOLINT
                         : boss::ComplexExpression("List"_(0.0, 1.0, 1.0, 1.0))))); // NOLINT
}

TEST_CASE("Test PKs and FKs", "[constraints]") {
  boss::engines::arrow_storage::Engine engine;

  REQUIRE(engine.evaluate("Set"_("LoadToMemoryMappedFiles"_, false)) == boss::Expression(true));

  boss::Symbol primaryAndForeignKeyType = GENERATE("BIGINT"_, "INTEGER"_);

  auto createNationResult =
      engine.evaluate("CreateTable"_("NATION"_, "N_NATIONKEY"_, "As"_(primaryAndForeignKeyType),
                                     "N_NAME"_, "N_REGIONKEY"_, "N_COMMENT"_));
  REQUIRE(createNationResult == boss::Expression(true));

  auto createSupplierResult = engine.evaluate(
      "CreateTable"_("SUPPLIER"_, "S_SUPPKEY"_, "S_NAME"_, "S_ADDRESS"_, "S_NATIONKEY"_,
                     "As"_(primaryAndForeignKeyType), "S_PHONE"_, "S_ACCTBAL"_, "S_COMMENT"_));
  REQUIRE(createSupplierResult == boss::Expression(true));

  auto nationPKResult = engine.evaluate("AddConstraint"_("NATION"_, "PrimaryKey"_("N_NATIONKEY"_)));
  REQUIRE(nationPKResult == boss::Expression(true));

  auto supplierPKResult =
      engine.evaluate("AddConstraint"_("SUPPLIER"_, "PrimaryKey"_("S_SUPPKEY"_)));
  REQUIRE(supplierPKResult == boss::Expression(true));

  auto nationFKResult =
      engine.evaluate("AddConstraint"_("SUPPLIER"_, "ForeignKey"_("NATION"_, "S_NATIONKEY"_)));
  REQUIRE(nationFKResult == boss::Expression(true));

  auto emptyResult = engine.evaluate("SUPPLIER"_);
  REQUIRE(emptyResult ==
          "Table"_("Column"_("S_SUPPKEY"_, "List"_()), "Column"_("S_NAME"_, "List"_()),
                   "Column"_("S_ADDRESS"_, "List"_()), "Column"_("S_NATIONKEY"_, "List"_()),
                   "Column"_("S_PHONE"_, "List"_()), "Column"_("S_ACCTBAL"_, "List"_()),
                   "Column"_("S_COMMENT"_, "List"_()), "Index"_("N_NATIONKEY"_, "List"_())));

  auto loadNationResult = engine.evaluate("Load"_("NATION"_, "../Tests/nation.tbl"));
  REQUIRE(loadNationResult == boss::Expression(true));

  auto loadSupplierResult = engine.evaluate("Load"_("SUPPLIER"_, "../Tests/supplier.tbl"));
  REQUIRE(loadSupplierResult == boss::Expression(true));

  auto result = engine.evaluate("SUPPLIER"_);

  INFO(result);

  auto const& indexExpr = boss::get<boss::ComplexExpression>(
      boss::get<boss::ComplexExpression>(result).getDynamicArguments().back());
  REQUIRE(indexExpr == "Index"_("N_NATIONKEY"_,
                                // index is int32 even if the key is BIGINT
                                boss::ComplexExpression("List"_(0, 0, 3, 2, 2, 1, 2, 0, 1, 3))));
}
