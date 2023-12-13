#include "DuckDB.hpp"
#include "ITTNotifySupport.hpp"
#include "MonetDB.hpp"
#include <BOSS.hpp>
#include <ExpressionUtilities.hpp>
#include <benchmark/benchmark.h>
#include <iostream>

using namespace std;

using namespace boss::utilities;
using boss::expressions::CloneReason;
using boss::expressions::ComplexExpression;

namespace utilities {
static boss::Expression injectDebugInfoToSpans(boss::Expression&& expr) {
  return std::visit(
      boss::utilities::overload(
          [&](boss::ComplexExpression&& e) -> boss::Expression {
            auto [head, unused_, dynamics, spans] = std::move(e).decompose();
            boss::ExpressionArguments debugDynamics;
            debugDynamics.reserve(dynamics.size() + spans.size());
            std::transform(std::make_move_iterator(dynamics.begin()),
                           std::make_move_iterator(dynamics.end()),
                           std::back_inserter(debugDynamics), [](auto&& arg) {
                             return injectDebugInfoToSpans(std::forward<decltype(arg)>(arg));
                           });
            std::transform(
                std::make_move_iterator(spans.begin()), std::make_move_iterator(spans.end()),
                std::back_inserter(debugDynamics), [](auto&& span) {
                  return std::visit(
                      [](auto&& typedSpan) -> boss::Expression {
                        using Element = typename std::decay_t<decltype(typedSpan)>::element_type;
                        return boss::ComplexExpressionWithStaticArguments<std::string, int64_t>(
                            "Span"_, {typeid(Element).name(), typedSpan.size()}, {}, {});
                      },
                      std::forward<decltype(span)>(span));
                });
            return boss::ComplexExpression(std::move(head), {}, std::move(debugDynamics), {});
          },
          [](auto&& otherTypes) -> boss::Expression { return otherTypes; }),
      std::move(expr));
}
}; // namespace utilities

static auto const vtune = VTuneAPIInterface{"BOSS"};

static bool VERBOSE_QUERY_OUTPUT = false;
static bool MORE_VERBOSE_OUTPUT = false;
static bool VERIFY_OUTPUT = false;
static bool EXPLAIN_QUERY_OUTPUT = false;
static int VERBOSE_QUERY_OUTPUT_MAX_LINES = 1000;

static bool IGNORE_ERRORS = false;
static int BENCHMARK_NUM_WARMPUP_ITERATIONS = 3;

static bool MONETDB_MULTITHREADING = true;
static int DUCKDB_MAX_THREADS = 100;

static bool USE_FIXED_POINT_NUMERIC_TYPE = false;

static bool DISABLE_MMAP_CACHE = false;
static bool DISABLE_GPU_CACHE = false;
static bool DISABLE_CONSTRAINTS = false;
static bool DISABLE_GATHER_OPERATOR = false;
static bool DISABLE_AUTO_DICTIONARY_ENCODING = false;
static bool ALL_STRINGS_AS_INTEGERS = false;
static bool BENCHMARK_STORAGE_BLOCK_SIZE = false;
static int64_t DEFAULT_STORAGE_BLOCK_SIZE = -1; // keep storage's default
static int64_t MAX_GPU_MEMORY_CACHE = -1;

static bool BENCHMARK_DATA_COPY_IN = false;
static bool BENCHMARK_DATA_COPY_OUT = false;

enum DB_ENGINE { ENGINE_START = 0, BOSS = ENGINE_START, MONETDB, DUCKDB, ENGINE_END };
static auto const DBEngineNames = std::vector<string>{"BOSS", "MonetDB", "DuckDB"};

static auto& librariesToTest() {
  static std::vector<string> libraries;
  return libraries;
};

static void resetBOSSEngine() {
  auto eval = [](auto&& expression) {
    boss::expressions::ExpressionSpanArguments spans;
    spans.emplace_back(boss::expressions::Span<std::string>(librariesToTest()));
    return boss::evaluate("EvaluateInEngines"_(ComplexExpression("List"_, {}, {}, std::move(spans)),
                                               std::move(expression)));
  };

  eval("DropTable"_("REGION"_));
  eval("DropTable"_("NATION"_));
  eval("DropTable"_("PART"_));
  eval("DropTable"_("SUPPLIER"_));
  eval("DropTable"_("PARTSUPP"_));
  eval("DropTable"_("CUSTOMER"_));
  eval("DropTable"_("ORDERS"_));
  eval("DropTable"_("LINEITEM"_));
}

static auto& lastDataSize() {
  static int dataSize = 0;
  return dataSize;
}

static auto& lastDataset() {
  static std::string ext;
  return ext;
}

static auto& lastStorageBlockSize() {
  static int64_t blockSize = 0;
  return blockSize;
}

static void initBOSSEngine_TPCH(int dataSize, int64_t storageBlockSize) {
  static auto dataset = std::string("TPCH");

  if(lastDataset() != dataset || dataSize != lastDataSize() ||
     storageBlockSize != lastStorageBlockSize()) {
    resetBOSSEngine();

    lastDataset() = dataset;
    lastDataSize() = dataSize;
    lastStorageBlockSize() = storageBlockSize;

    auto eval = [](auto&& expression) {
      boss::expressions::ExpressionSpanArguments spans;
      spans.emplace_back(boss::expressions::Span<std::string>(librariesToTest()));
      return boss::evaluate("EvaluateInEngines"_(
          ComplexExpression("List"_, {}, {}, std::move(spans)), std::move(expression)));
    };

    auto checkForErrors = [](auto&& output) {
      auto* maybeComplexExpr = std::get_if<boss::ComplexExpression>(&output);
      if(maybeComplexExpr == nullptr) {
        return;
      }
      if(maybeComplexExpr->getHead() == "ErrorWhenEvaluatingExpression"_) {
        std::cout << "Error: " << output << std::endl;
      }
    };

    checkForErrors(eval("Set"_("LoadToMemoryMappedFiles"_, !DISABLE_MMAP_CACHE)));
    checkForErrors(eval("Set"_("UseAutoDictionaryEncoding"_, !DISABLE_AUTO_DICTIONARY_ENCODING)));
    checkForErrors(eval("Set"_("AllStringColumnsAsIntegers"_, ALL_STRINGS_AS_INTEGERS)));
    if(storageBlockSize > 0) {
      checkForErrors(eval("Set"_("FileLoadingBlockSize"_, storageBlockSize)));
    }
    if(MAX_GPU_MEMORY_CACHE >= 0) {
      checkForErrors(eval("Set"_("MaxGPUMemoryCacheMB"_, MAX_GPU_MEMORY_CACHE)));
    }
    checkForErrors(eval("Set"_("ArrayFireEngineCopyDataIn"_, BENCHMARK_DATA_COPY_IN)));
    checkForErrors(eval("Set"_("ArrayFireEngineCopyDataOut"_, BENCHMARK_DATA_COPY_OUT)));
    checkForErrors(eval("Set"_("DisableGatherOperator"_, DISABLE_GATHER_OPERATOR)));

    checkForErrors(
        eval("CreateTable"_("LINEITEM"_, "l_orderkey"_, "l_partkey"_, "l_suppkey"_, "l_linenumber"_,
                            "l_quantity"_, "l_extendedprice"_, "l_discount"_, "l_tax"_,
                            "l_returnflag"_, "l_linestatus"_, "l_shipdate"_, "l_commitdate"_,
                            "l_receiptdate"_, "l_shipinstruct"_, "l_shipmode"_, "l_comment"_)));
    checkForErrors(eval(
        "CreateTable"_("REGION"_, "r_regionkey"_, "As"_("INTEGER"_), "r_name"_, "r_comment"_)));
    checkForErrors(eval("CreateTable"_("NATION"_, "n_nationkey"_, "As"_("INTEGER"_), "n_name"_,
                                       "n_regionkey"_, "As"_("INTEGER"_), "n_comment"_)));
    checkForErrors(eval("CreateTable"_("PART"_, "p_partkey"_, "p_name"_, "p_mfgr"_, "p_brand"_,
                                       "p_type"_, "p_size"_, "As"_("INTEGER"_), "p_container"_,
                                       "p_retailprice"_, "p_comment"_)));
    checkForErrors(
        eval("CreateTable"_("SUPPLIER"_, "s_suppkey"_, "s_name"_, "s_address"_, "s_nationkey"_,
                            "As"_("INTEGER"_), "s_phone"_, "s_acctbal"_, "s_comment"_)));
    checkForErrors(eval("CreateTable"_("PARTSUPP"_, "ps_partkey"_, "ps_suppkey"_, "ps_availqty"_,
                                       "ps_supplycost"_, "ps_comment"_)));
    checkForErrors(eval("CreateTable"_("CUSTOMER"_, "c_custkey"_, "c_name"_, "c_address"_,
                                       "c_nationkey"_, "As"_("INTEGER"_), "c_phone"_, "c_acctbal"_,
                                       "c_mktsegment"_, "c_comment"_)));
    checkForErrors(eval("CreateTable"_(
        "ORDERS"_, "o_orderkey"_, "o_custkey"_, "o_orderstatus"_, "o_totalprice"_, "o_orderdate"_,
        "o_orderpriority"_, "o_clerk"_, "o_shippriority"_, "As"_("INTEGER"_), "o_comment"_)));

    if(!DISABLE_GPU_CACHE) {
      // cached columns on the GPU
      // Q1
      checkForErrors(eval("Set"_("CachedColumn"_, "l_quantity"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "l_discount"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "l_shipdate"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "l_extendedprice"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "l_returnflag"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "l_linestatus"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "l_tax"_)));
      // Q3
      checkForErrors(eval("Set"_("CachedColumn"_, "c_custkey"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "c_mktsegment"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "o_orderkey"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "o_orderdate"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "o_custkey"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "o_shippriority"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "l_orderkey"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "l_discount"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "l_shipdate"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "l_extendedprice"_)));
      // Q6
      checkForErrors(eval("Set"_("CachedColumn"_, "l_quantity"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "l_discount"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "l_shipdate"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "l_extendedprice"_)));
      // Q9
      checkForErrors(eval("Set"_("CachedColumn"_, "o_orderkey"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "o_orderdate"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "p_partkey"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "p_retailprice"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "n_name"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "n_nationkey"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "s_suppkey"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "s_nationkey"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "ps_partkey"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "ps_suppkey"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "ps_supplycost"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "l_partkey"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "l_suppkey"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "l_orderkey"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "l_extendedprice"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "l_discount"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "l_quantity"_)));
      // Q18
      checkForErrors(eval("Set"_("CachedColumn"_, "l_orderkey"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "l_quantity"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "c_custkey"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "o_orderkey"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "o_custkey"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "o_orderdate"_)));
      checkForErrors(eval("Set"_("CachedColumn"_, "o_totalprice"_)));
    }

    auto filenamesAndTables = std::vector<std::pair<std::string, boss::Symbol>>{
        {"lineitem"s, "LINEITEM"_}, {"region"s, "REGION"_},     {"nation"s, "NATION"_},
        {"part"s, "PART"_},         {"supplier"s, "SUPPLIER"_}, {"partsupp"s, "PARTSUPP"_},
        {"customer"s, "CUSTOMER"_}, {"orders"s, "ORDERS"_},
    };

    for(auto const& [filename, table] : filenamesAndTables) {
      std::string path = "../data/tpch_" + std::to_string(dataSize) + "MB/" + filename + ".tbl";
      checkForErrors(eval("Load"_(table, path)));
    }

    if(!DISABLE_CONSTRAINTS) {
      // primary key constraints
      checkForErrors(eval("AddConstraint"_("PART"_, "PrimaryKey"_("p_partkey"_))));
      checkForErrors(eval("AddConstraint"_("SUPPLIER"_, "PrimaryKey"_("s_suppkey"_))));
      checkForErrors(
          eval("AddConstraint"_("PARTSUPP"_, "PrimaryKey"_("ps_partkey"_, "ps_suppkey"_))));
      checkForErrors(eval("AddConstraint"_("CUSTOMER"_, "PrimaryKey"_("c_custkey"_))));
      checkForErrors(eval("AddConstraint"_("ORDERS"_, "PrimaryKey"_("o_orderkey"_))));
      checkForErrors(
          eval("AddConstraint"_("LINEITEM"_, "PrimaryKey"_("l_orderkey"_, "l_linenumber"_))));
      checkForErrors(eval("AddConstraint"_("NATION"_, "PrimaryKey"_("n_nationkey"_))));
      checkForErrors(eval("AddConstraint"_("REGION"_, "PrimaryKey"_("r_regionkey"_))));

      // foreign key constraints
      checkForErrors(eval("AddConstraint"_("SUPPLIER"_, "ForeignKey"_("NATION"_, "s_nationkey"_))));
      checkForErrors(eval("AddConstraint"_("PARTSUPP"_, "ForeignKey"_("PART"_, "ps_partkey"_))));
      checkForErrors(
          eval("AddConstraint"_("PARTSUPP"_, "ForeignKey"_("SUPPLIER"_, "ps_suppkey"_))));
      checkForErrors(eval("AddConstraint"_("CUSTOMER"_, "ForeignKey"_("NATION"_, "c_nationkey"_))));
      checkForErrors(eval("AddConstraint"_("ORDERS"_, "ForeignKey"_("CUSTOMER"_, "o_custkey"_))));
      checkForErrors(eval("AddConstraint"_("LINEITEM"_, "ForeignKey"_("ORDERS"_, "l_orderkey"_))));
      checkForErrors(eval(
          "AddConstraint"_("LINEITEM"_, "ForeignKey"_("PARTSUPP"_, "l_partkey"_, "l_suppkey"_))));
      checkForErrors(eval("AddConstraint"_("NATION"_, "ForeignKey"_("REGION"_, "n_regionkey"_))));
    }
  }
};

static void releaseBOSSEngine() {
  // make sure to release engines in reverse order of evaluation
  // (important for data ownership across engines)
  auto reversedLibraries = librariesToTest();
  std::reverse(reversedLibraries.begin(), reversedLibraries.end());
  boss::expressions::ExpressionSpanArguments spans;
  spans.emplace_back(boss::expressions::Span<std::string>(reversedLibraries));
  boss::evaluate("ReleaseEngines"_(ComplexExpression("List"_, {}, {}, std::move(spans))));
}

enum TPCH_QUERIES { TPCH_Q1 = 1, TPCH_Q3 = 3, TPCH_Q6 = 6, TPCH_Q9 = 9, TPCH_Q18 = 18 };

enum TPCH_VARIANTS {
  TPCH_Q1_POSTFILTER = 50,     // projection before selection
  TPCH_Q3_POSTFILTER_1JOIN,    // post-filter 1st join (when only 1st join fits in GPU memory)
  TPCH_Q3_POSTFILTER_2JOINS,   // post-filter both joins (when both joins fit in GPU memory)
  TPCH_Q6_NESTED_SELECT,       // nest select ops with single predicates
  TPCH_Q9_POSTFILTER_PRIORITY, // post-filter 3rd join + priority on lineitem x order
};

static auto& queryNames() {
  static std::map<int, std::string> names;
  if(names.empty()) {
    // TPC-H
    names.try_emplace(TPCH_Q1, "TPC-H_Q1");
    names.try_emplace(TPCH_Q3, "TPC-H_Q3");
    names.try_emplace(TPCH_Q6, "TPC-H_Q6");
    names.try_emplace(TPCH_Q9, "TPC-H_Q9");
    names.try_emplace(TPCH_Q18, "TPC-H_Q18");
    // GPU Kernel Heuristics
    names.try_emplace(TPCH_Q1_POSTFILTER, "TPC-H_Q1V_POST-FILTER");
    names.try_emplace(TPCH_Q3_POSTFILTER_1JOIN, "TPC-H_Q3V_POST-FILTER-1JOIN");
    names.try_emplace(TPCH_Q3_POSTFILTER_2JOINS, "TPC-H_Q3V_POST-FILTER-2JOINS");
    names.try_emplace(TPCH_Q6_NESTED_SELECT, "TPC-H_Q6V_NESTED-SELECT");
    names.try_emplace(TPCH_Q9_POSTFILTER_PRIORITY, "TPC-H_Q9V_POST-FILTER-AND-PRIORITY");
  }
  return names;
}

static auto& bossQueries() {
  static std::map<int, boss::ComplexExpression> queries;
  if(queries.empty()) {
    queries.try_emplace(
        TPCH_Q1,
        "Order"_(
            "Group"_(
                "Project"_(
                    "Project"_(
                        "Project"_(
                            "Select"_(
                                "Project"_("LINEITEM"_,
                                           "As"_("l_quantity"_, "l_quantity"_, "l_discount"_,
                                                 "l_discount"_, "l_shipdate"_, "l_shipdate"_,
                                                 "l_extendedprice"_,
                                                 "l_extendedprice"_,
                                                 "l_returnflag"_, "l_returnflag"_, "l_linestatus"_,
                                                 "l_linestatus"_, "l_tax"_, "l_tax"_)),
                                "Where"_("Greater"_("DateObject"_("1998-08-31"), "l_shipdate"_))),
                            "As"_("l_returnflag"_, "l_returnflag"_, "l_linestatus"_,
                                  "l_linestatus"_, "l_quantity"_, "l_quantity"_, "l_extendedprice"_,
                                  "l_extendedprice"_, "l_discount"_, "l_discount"_, "calc1"_,
                                  "Minus"_(1.0, "l_discount"_), "calc2"_, "Plus"_("l_tax"_, 1.0))),
                        "As"_("l_returnflag"_, "l_returnflag"_, "l_linestatus"_, "l_linestatus"_,
                              "l_quantity"_, "l_quantity"_, "l_extendedprice"_, "l_extendedprice"_,
                              "l_discount"_, "l_discount"_, "disc_price"_,
                              "Times"_("l_extendedprice"_, "calc1"_), "calc2"_, "calc2"_)),
                    "As"_("l_returnflag"_, "l_returnflag"_, "l_linestatus"_, "l_linestatus"_,
                          "l_quantity"_, "l_quantity"_, "l_extendedprice"_, "l_extendedprice"_,
                          "l_discount"_, "l_discount"_, "disc_price"_, "disc_price"_, "calc"_,
                          "Times"_("disc_price"_, "calc2"_))),
                "By"_("l_returnflag"_, "l_linestatus"_),
                "As"_("sum_qty"_, "Sum"_("l_quantity"_), "sum_base_price"_,
                      "Sum"_("l_extendedprice"_), "sum_disc_price"_, "Sum"_("disc_price"_),
                      "sum_charges"_, "Sum"_("calc"_), "avg_qty"_, "Avg"_("l_quantity"_),
                      "avg_price"_, "Avg"_("l_extendedprice"_), "avg_disc"_, "Avg"_("l_discount"_),
                      "count_order"_, "Count"_("*"_))),
            "By"_("l_returnflag"_, "l_linestatus"_)));

    queries.try_emplace(
        TPCH_Q3,
        "Top"_(
            "Group"_(
                "Project"_(
                    "Join"_("Project"_(
                                "Join"_("Select"_(
                                            "Project"_("ORDERS"_,
                                                       "As"_("o_orderkey"_, "o_orderkey"_,
                                                             "o_orderdate"_, "o_orderdate"_,
                                                             "o_custkey"_, "o_custkey"_,
                                                             "o_shippriority"_, "o_shippriority"_)),
                                            "Where"_("Greater"_("DateObject"_("1995-03-15"),
                                                                "o_orderdate"_))),
                                        "Project"_(
                                            "Select"_(
                                                "Project"_("CUSTOMER"_,
                                                           "As"_("c_custkey"_, "c_custkey"_,
                                                                 "c_mktsegment"_, "c_mktsegment"_)),
                                                "Where"_("StringContainsQ"_("c_mktsegment"_,
                                                                            "BUILDING"))),
                                            "As"_("c_custkey"_, "c_custkey"_, "c_mktsegment"_,
                                                  "c_mktsegment"_)),
                                        "Where"_("Equal"_("c_custkey"_, "o_custkey"_))),
                                "As"_("o_orderkey"_, "o_orderkey"_, "o_orderdate"_, "o_orderdate"_,
                                      "o_custkey"_, "o_custkey"_, "o_shippriority"_,
                                      "o_shippriority"_)),
                            "Project"_(
                                "Select"_(
                                    "Project"_("LINEITEM"_,
                                               "As"_("l_orderkey"_, "l_orderkey"_, "l_discount"_,
                                                     "l_discount"_, "l_shipdate"_, "l_shipdate"_,
                                                     "l_extendedprice"_, "l_extendedprice"_)),
                                    "Where"_(
                                        "Greater"_("l_shipdate"_, "DateObject"_("1993-03-15")))),
                                "As"_("l_orderkey"_, "l_orderkey"_, "l_discount"_, "l_discount"_,
                                      "l_extendedprice"_, "l_extendedprice"_)),
                            "Where"_("Equal"_("o_orderkey"_, "l_orderkey"_))),
                    "As"_("expr1009"_, "Times"_("l_extendedprice"_, "Minus"_(1.0, "l_discount"_)),
                          "l_extendedprice"_, "l_extendedprice"_, "l_orderkey"_, "l_orderkey"_,
                          "o_orderdate"_, "o_orderdate"_, "o_shippriority"_, "o_shippriority"_)),
                "By"_("l_orderkey"_, "o_orderdate"_, "o_shippriority"_),
                "As"_("revenue"_, "Sum"_("expr1009"_))),
            "By"_("revenue"_, "desc"_, "o_orderdate"_), 10));

    queries.try_emplace(
        TPCH_Q6,
        "Group"_(
            "Project"_(
                "Select"_("Project"_("LINEITEM"_, "As"_("l_quantity"_, "l_quantity"_, "l_discount"_,
                                                        "l_discount"_, "l_shipdate"_, "l_shipdate"_,
                                                        "l_extendedprice"_, "l_extendedprice"_)),
                          "Where"_("And"_("Greater"_(24, "l_quantity"_),      // NOLINT
                                          "Greater"_("l_discount"_, 0.0499),  // NOLINT
                                          "Greater"_(0.07001, "l_discount"_), // NOLINT
                                          "Greater"_("DateObject"_("1995-01-01"), "l_shipdate"_),
                                          "Greater"_("l_shipdate"_, "DateObject"_("1993-12-31"))))),
                "As"_("revenue"_, "Times"_("l_extendedprice"_, "l_discount"_))),
            "Sum"_("revenue"_)));

    queries.try_emplace(
        TPCH_Q9,
        "Order"_(
            "Group"_(
                "Project"_(
                    "Join"_(
                        "Project"_("ORDERS"_, "As"_("o_orderkey"_, "o_orderkey"_, "o_orderdate"_,
                                                    "o_orderdate"_)),
                        "Project"_(
                            "Join"_(
                                "Project"_(
                                    "Join"_(
                                        "Project"_(
                                            "Select"_(
                                                "Project"_("PART"_,
                                                           "As"_("p_partkey"_, "p_partkey"_,
                                                                 "p_retailprice"_,
                                                                 "p_retailprice"_)),
                                                "Where"_("And"_("Greater"_("p_retailprice"_,
                                                                           1006.05), // NOLINT
                                                                "Greater"_(1080.1,   // NOLINT
                                                                           "p_retailprice"_)))),
                                            "As"_("p_partkey"_, "p_partkey"_, "p_retailprice"_,
                                                  "p_retailprice"_)),
                                        "Project"_(
                                            "Join"_(
                                                "Project"_(
                                                    "Join"_(
                                                        "Project"_("NATION"_,
                                                                   "As"_("n_name"_, "n_name"_,
                                                                         "n_nationkey"_,
                                                                         "n_nationkey"_)),
                                                        "Project"_("SUPPLIER"_,
                                                                   "As"_("s_suppkey"_, "s_suppkey"_,
                                                                         "s_nationkey"_,
                                                                         "s_nationkey"_)),
                                                        "Where"_("Equal"_("n_nationkey"_,
                                                                          "s_nationkey"_))),
                                                    "As"_("n_name"_, "n_name"_, "s_suppkey"_,
                                                          "s_suppkey"_)),
                                                "Project"_("PARTSUPP"_,
                                                           "As"_("ps_partkey"_, "ps_partkey"_,
                                                                 "ps_suppkey"_, "ps_suppkey"_,
                                                                 "ps_supplycost"_,
                                                                 "ps_supplycost"_)),
                                                "Where"_("Equal"_("s_suppkey"_, "ps_suppkey"_))),
                                            "As"_("n_name"_, "n_name"_, "ps_partkey"_,
                                                  "ps_partkey"_, "ps_suppkey"_, "ps_suppkey"_,
                                                  "ps_supplycost"_, "ps_supplycost"_)),
                                        "Where"_("Equal"_("p_partkey"_, "ps_partkey"_))),
                                    "As"_("n_name"_, "n_name"_, "ps_partkey"_, "ps_partkey"_,
                                          "ps_suppkey"_, "ps_suppkey"_, "ps_supplycost"_,
                                          "ps_supplycost"_)),
                                "Project"_("LINEITEM"_,
                                           "As"_("l_partkey"_, "l_partkey"_, "l_suppkey"_,
                                                 "l_suppkey"_, "l_orderkey"_, "l_orderkey"_,
                                                 "l_extendedprice"_, "l_extendedprice"_,
                                                 "l_discount"_, "l_discount"_, "l_quantity"_,
                                                 "l_quantity"_)),
                                "Where"_("Equal"_("List"_("ps_partkey"_, "ps_suppkey"_),
                                                  "List"_("l_partkey"_, "l_suppkey"_)))),
                            "As"_("n_name"_, "n_name"_, "ps_supplycost"_, "ps_supplycost"_,
                                  "l_orderkey"_, "l_orderkey"_, "l_extendedprice"_,
                                  "l_extendedprice"_, "l_discount"_, "l_discount"_, "l_quantity"_,
                                  "l_quantity"_)),
                        "Where"_("Equal"_("o_orderkey"_, "l_orderkey"_))),
                    "As"_("nation"_, "n_name"_, "o_year"_, "Year"_("o_orderdate"_), "amount"_,
                          "Minus"_("Times"_("l_extendedprice"_, "Minus"_(1.0, "l_discount"_)),
                                   "Times"_("ps_supplycost"_, "l_quantity"_)))),
                "By"_("nation"_, "o_year"_), "Sum"_("amount"_)),
            "By"_("nation"_, "o_year"_, "desc"_)));

    queries.try_emplace(
        TPCH_Q18,
        "Top"_(
            "Group"_(
                "Project"_(
                    "Join"_(
                        /* Following our join order heuristics, LINEITEM should be on the probe side
                         * and not the build side. However, because Velox engine does not support an
                         * aggregated relation on the probe side, we need to put the
                         * aggregated relation on the build side.
                         */
                        "Select"_(
                            "Group"_("Project"_("LINEITEM"_, "As"_("l_orderkey"_, "l_orderkey"_,
                                                                   "l_quantity"_, "l_quantity"_)),
                                     "By"_("l_orderkey"_),
                                     "As"_("sum_l_quantity"_, "Sum"_("l_quantity"_))),
                            "Where"_("Greater"_("sum_l_quantity"_, 300))), // NOLINT
                        "Project"_(
                            "Join"_("Project"_("CUSTOMER"_, "As"_("c_custkey"_, "c_custkey"_)),
                                    "Project"_("ORDERS"_, "As"_("o_orderkey"_, "o_orderkey"_,
                                                                "o_custkey"_, "o_custkey"_,
                                                                "o_orderdate"_, "o_orderdate"_,
                                                                "o_totalprice"_, "o_totalprice"_)),
                                    "Where"_("Equal"_("c_custkey"_, "o_custkey"_))),
                            "As"_("o_orderkey"_, "o_orderkey"_, "o_custkey"_, "o_custkey"_,
                                  "o_orderdate"_, "o_orderdate"_, "o_totalprice"_,
                                  "o_totalprice"_)),
                        "Where"_("Equal"_("l_orderkey"_, "o_orderkey"_))),
                    "As"_("o_orderkey"_, "o_orderkey"_, "o_orderdate"_, "o_orderdate"_,
                          "o_totalprice"_, "o_totalprice"_, "o_custkey"_, "o_custkey"_,
                          "sum_l_quantity"_, "sum_l_quantity"_)),
                "By"_("o_custkey"_, "o_orderkey"_, "o_orderdate"_, "o_totalprice"_),
                "Sum"_("sum_l_quantity"_)),
            "By"_("o_totalprice"_, "desc"_, "o_orderdate"_), 100));

    // GPU kernel Heuristics
    queries.try_emplace(
        TPCH_Q1_POSTFILTER,
        "Order"_(
            "Group"_(
                "Select"_(
                    "Project"_(
                        "Project"_(
                            "Project"_("LINEITEM"_,
                                       "As"_("l_shipdate"_, "l_shipdate"_, "l_returnflag"_,
                                             "l_returnflag"_, "l_linestatus"_, "l_linestatus"_,
                                             "l_quantity"_, "l_quantity"_, "l_extendedprice"_,
                                             "l_extendedprice"_, "l_discount"_, "l_discount"_,
                                             "calc1"_, "Minus"_(1.0, "l_discount"_), "calc2"_,
                                             "Plus"_("l_tax"_, 1.0))),
                            "As"_("l_shipdate"_, "l_shipdate"_, "l_returnflag"_, "l_returnflag"_,
                                  "l_linestatus"_, "l_linestatus"_, "l_quantity"_, "l_quantity"_,
                                  "l_extendedprice"_, "l_extendedprice"_, "l_discount"_,
                                  "l_discount"_, "disc_price"_,
                                  "Times"_("l_extendedprice"_, "calc1"_), "calc2"_, "calc2"_)),
                        "As"_("l_shipdate"_, "l_shipdate"_, "l_returnflag"_, "l_returnflag"_,
                              "l_linestatus"_, "l_linestatus"_, "l_quantity"_, "l_quantity"_,
                              "l_extendedprice"_, "l_extendedprice"_, "l_discount"_, "l_discount"_,
                              "disc_price"_, "disc_price"_, "calc"_,
                              "Times"_("disc_price"_, "calc2"_))),
                    "Where"_("Greater"_("DateObject"_("1998-08-31"), "l_shipdate"_))),
                "By"_("l_returnflag"_, "l_linestatus"_),
                "As"_("sum_qty"_, "Sum"_("l_quantity"_), "sum_base_price"_,
                      "Sum"_("l_extendedprice"_), "sum_disc_price"_, "Sum"_("disc_price"_),
                      "sum_charges"_, "Sum"_("calc"_), "avg_qty"_, "Avg"_("l_quantity"_),
                      "avg_price"_, "Avg"_("l_extendedprice"_), "avg_disc"_, "Avg"_("l_discount"_),
                      "count_order"_, "Count"_("*"_))),
            "By"_("l_returnflag"_, "l_linestatus"_)));
    queries.try_emplace(
        TPCH_Q3_POSTFILTER_1JOIN,
        "Top"_(
            "Group"_(
                "Project"_(
                    "Join"_(
                        "Project"_(
                            "Select"_(
                                "Project"_(
                                    "Join"_("Project"_("CUSTOMER"_,
                                                       "As"_("c_custkey"_, "c_custkey"_,
                                                             "c_mktsegment"_, "c_mktsegment"_)),
                                            "Project"_("ORDERS"_,
                                                       "As"_("o_orderkey"_, "o_orderkey"_,
                                                             "o_orderdate"_, "o_orderdate"_,
                                                             "o_custkey"_, "o_custkey"_,
                                                             "o_shippriority"_, "o_shippriority"_)),
                                            "Where"_("Equal"_("c_custkey"_, "o_custkey"_))),
                                    "As"_("c_mktsegment"_, "c_mktsegment"_, "o_orderkey"_,
                                          "o_orderkey"_, "o_orderdate"_, "o_orderdate"_,
                                          "o_custkey"_, "o_custkey"_, "o_shippriority"_,
                                          "o_shippriority"_)),
                                "Where"_("And"_(
                                    "StringContainsQ"_("c_mktsegment"_, "BUILDING"),
                                    "Greater"_("DateObject"_("1995-03-15"), "o_orderdate"_)))),
                            "As"_("o_orderkey"_, "o_orderkey"_, "o_orderdate"_, "o_orderdate"_,
                                  "o_shippriority"_, "o_shippriority"_)),
                        "Project"_(
                            "Select"_(
                                "Project"_("LINEITEM"_,
                                           "As"_("l_orderkey"_, "l_orderkey"_, "l_discount"_,
                                                 "l_discount"_, "l_shipdate"_, "l_shipdate"_,
                                                 "l_extendedprice"_, "l_extendedprice"_)),
                                "Where"_("Greater"_("l_shipdate"_, "DateObject"_("1993-03-15")))),
                            "As"_("l_orderkey"_, "l_orderkey"_, "l_discount"_, "l_discount"_,
                                  "l_extendedprice"_, "l_extendedprice"_)),
                        "Where"_("Equal"_("o_orderkey"_, "l_orderkey"_))),
                    "As"_("expr1009"_, "Times"_("l_extendedprice"_, "Minus"_(1.0, "l_discount"_)),
                          "l_extendedprice"_, "l_extendedprice"_, "l_orderkey"_, "l_orderkey"_,
                          "o_orderdate"_, "o_orderdate"_, "o_shippriority"_, "o_shippriority"_)),
                "By"_("l_orderkey"_, "o_orderdate"_, "o_shippriority"_),
                "As"_("revenue"_, "Sum"_("expr1009"_))),
            "By"_("revenue"_, "desc"_, "o_orderdate"_), 10));
    queries.try_emplace(
        TPCH_Q3_POSTFILTER_2JOINS,
        "Top"_(
            "Group"_(
                "Project"_(
                    "Select"_(
                        "Select"_(
                            "Select"_(
                                "Project"_(
                                    "Join"_("Project"_(
                                                "Join"_("Project"_("CUSTOMER"_,
                                                                   "As"_("c_custkey"_, "c_custkey"_,
                                                                         "c_mktsegment"_,
                                                                         "c_mktsegment"_)),
                                                        "Project"_(
                                                            "ORDERS"_,
                                                            "As"_("o_orderkey"_, "o_orderkey"_,
                                                                  "o_orderdate"_, "o_orderdate"_,
                                                                  "o_custkey"_, "o_custkey"_,
                                                                  "o_shippriority"_,
                                                                  "o_shippriority"_)),
                                                        "Where"_(
                                                            "Equal"_("c_custkey"_, "o_custkey"_))),
                                                "As"_("c_mktsegment"_, "c_mktsegment"_,
                                                      "o_orderkey"_, "o_orderkey"_, "o_orderdate"_,
                                                      "o_orderdate"_, "o_custkey"_, "o_custkey"_,
                                                      "o_shippriority"_, "o_shippriority"_)),
                                            "Project"_(
                                                "LINEITEM"_,
                                                "As"_("l_orderkey"_, "l_orderkey"_, "l_discount"_,
                                                      "l_discount"_, "l_shipdate"_, "l_shipdate"_,
                                                      "l_extendedprice"_, "l_extendedprice"_)),
                                            "Where"_("Equal"_("o_orderkey"_, "l_orderkey"_))),
                                    "As"_("l_shipdate"_, "l_shipdate"_, "l_orderkey"_,
                                          "l_orderkey"_, "l_discount"_, "l_discount"_,
                                          "l_extendedprice"_, "l_extendedprice"_, "o_orderdate"_,
                                          "o_orderdate"_, "o_shippriority"_, "o_shippriority"_,
                                          "c_mktsegment"_, "c_mktsegment"_)),
                                "Where"_("Greater"_("DateObject"_("1995-03-15"), "o_orderdate"_))),
                            "Where"_("StringContainsQ"_("c_mktsegment"_, "BUILDING"))),
                        "Where"_("Greater"_("l_shipdate"_, "DateObject"_("1993-03-15")))),
                    "As"_("expr1009"_, "Times"_("l_extendedprice"_, "Minus"_(1.0, "l_discount"_)),
                          "l_extendedprice"_, "l_extendedprice"_, "l_orderkey"_, "l_orderkey"_,
                          "o_orderdate"_, "o_orderdate"_, "o_shippriority"_, "o_shippriority"_)),
                "By"_("l_orderkey"_, "o_orderdate"_, "o_shippriority"_),
                "As"_("revenue"_, "Sum"_("expr1009"_))),
            "By"_("revenue"_, "desc"_, "o_orderdate"_), 10));
    queries.try_emplace(
        TPCH_Q6_NESTED_SELECT,
        "Group"_(
            "Project"_(
                "Select"_("Select"_("Select"_("Project"_(
                                                  "LINEITEM"_,
                                                  "As"_("l_quantity"_, "l_quantity"_, "l_discount"_,
                                                        "l_discount"_, "l_shipdate"_, "l_shipdate"_,
                                                        "l_extendedprice"_, "l_extendedprice"_)),
                                              "Where"_("Greater"_(24, "l_quantity"_))),    // NOLINT
                                    "Where"_("And"_("Greater"_("l_discount"_, 0.0499),     // NOLINT
                                                    "Greater"_(0.07001, "l_discount"_)))), // NOLINT
                          "Where"_("And"_("Greater"_("DateObject"_("1995-01-01"), "l_shipdate"_),
                                          "Greater"_("l_shipdate"_, "DateObject"_("1993-12-31"))))),
                "As"_("revenue"_, "Times"_("l_extendedprice"_, "l_discount"_))),
            "Sum"_("revenue"_)));
    queries.try_emplace(
        TPCH_Q9_POSTFILTER_PRIORITY,
        "Order"_(
            "Group"_(
                "Project"_(
                    "Join"_(
                        "Project"_(
                            "Select"_(
                                "Project"_(
                                    "Join"_("Project"_("PART"_,
                                                       "As"_("p_partkey"_, "p_partkey"_,
                                                             "p_retailprice"_, "p_retailprice"_)),
                                            "Project"_(
                                                "Join"_(
                                                    "Project"_(
                                                        "Join"_(
                                                            "Project"_("NATION"_,
                                                                       "As"_("n_name"_, "n_name"_,
                                                                             "n_nationkey"_,
                                                                             "n_nationkey"_)),
                                                            "Project"_("SUPPLIER"_,
                                                                       "As"_("s_suppkey"_,
                                                                             "s_suppkey"_,
                                                                             "s_nationkey"_,
                                                                             "s_nationkey"_)),
                                                            "Where"_("Equal"_("n_nationkey"_,
                                                                              "s_nationkey"_))),
                                                        "As"_("n_name"_, "n_name"_, "s_suppkey"_,
                                                              "s_suppkey"_)),
                                                    "Project"_("PARTSUPP"_,
                                                               "As"_("ps_partkey"_, "ps_partkey"_,
                                                                     "ps_suppkey"_, "ps_suppkey"_,
                                                                     "ps_supplycost"_,
                                                                     "ps_supplycost"_)),
                                                    "Where"_(
                                                        "Equal"_("s_suppkey"_, "ps_suppkey"_))),
                                                "As"_("n_name"_, "n_name"_, "ps_partkey"_,
                                                      "ps_partkey"_, "ps_suppkey"_, "ps_suppkey"_,
                                                      "ps_supplycost"_, "ps_supplycost"_)),
                                            "Where"_("Equal"_("p_partkey"_, "ps_partkey"_))),
                                    "As"_("n_name"_, "n_name"_, "ps_partkey"_, "ps_partkey"_,
                                          "ps_suppkey"_, "ps_suppkey"_, "ps_supplycost"_,
                                          "ps_supplycost"_, "p_retailprice"_, "p_retailprice"_)),
                                "Where"_("And"_("Greater"_("p_retailprice"_,
                                                           1006.05), // NOLINT
                                                "Greater"_(1080.1,   // NOLINT
                                                           "p_retailprice"_)))),
                            "As"_("n_name"_, "n_name"_, "ps_partkey"_, "ps_partkey"_, "ps_suppkey"_,
                                  "ps_suppkey"_, "ps_supplycost"_, "ps_supplycost"_)),
                        "Project"_(
                            "Join"_("Project"_("ORDERS"_, "As"_("o_orderkey"_, "o_orderkey"_,
                                                                "o_orderdate"_, "o_orderdate"_)),
                                    "Project"_("LINEITEM"_,
                                               "As"_("l_partkey"_, "l_partkey"_, "l_suppkey"_,
                                                     "l_suppkey"_, "l_orderkey"_, "l_orderkey"_,
                                                     "l_extendedprice"_, "l_extendedprice"_,
                                                     "l_discount"_, "l_discount"_, "l_quantity"_,
                                                     "l_quantity"_)),
                                    "Where"_("Equal"_("o_orderkey"_, "l_orderkey"_))),
                            "As"_("o_orderdate"_, "o_orderdate"_, "l_extendedprice"_,
                                  "l_extendedprice"_, "l_discount"_, "l_discount"_, "l_quantity"_,
                                  "l_quantity"_, "l_partkey"_, "l_partkey"_, "l_suppkey"_,
                                  "l_suppkey"_)),
                        "Where"_("Equal"_("List"_("ps_partkey"_, "ps_suppkey"_),
                                          "List"_("l_partkey"_, "l_suppkey"_)))),
                    "As"_("nation"_, "n_name"_, "o_year"_, "Year"_("o_orderdate"_), "amount"_,
                          "Minus"_("Times"_("l_extendedprice"_, "Minus"_(1.0, "l_discount"_)),
                                   "Times"_("ps_supplycost"_, "l_quantity"_)))),
                "By"_("nation"_, "o_year"_), "Sum"_("amount"_)),
            "By"_("nation"_, "o_year"_, "desc"_)));
  }
  return queries;
}

static auto& monetdbQueries() {
  static std::map<int, std::string> queries = {
      {TPCH_Q1, "select "s
                "    l_returnflag, "s
                "    l_linestatus, "s
                "    sum(l_quantity) as sum_qty, "s
                "    sum(l_extendedprice) as sum_base_price, "s
                "    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, "s
                "    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, "s
                "    avg(l_quantity) as avg_qty, "s
                "    avg(l_extendedprice) as avg_price, "s
                "    avg(l_discount) as avg_disc, "s
                "    count(*) as count_order "s
                "from "s
                "    lineitem "s
                "where "s
                "    l_shipdate <= date '1998-12-01' - interval '90' day (3) "s
                "group by "s
                "    l_returnflag, "s
                "    l_linestatus "s
                "order by "s
                "    l_returnflag, "s
                "    l_linestatus; "s},
      {TPCH_Q3, "select"s
                "       l_orderkey,"s
                "       o_orderdate,"s
                "       o_shippriority,"s
                "       sum(l_extendedprice * (1 - l_discount)) as revenue"s
                "   from"s
                "       customer,"s
                "       orders,"s
                "       lineitem"s
                "   where"s
                "       c_mktsegment = 'BUILDING'"s
                "       and c_custkey = o_custkey"s
                "       and l_orderkey = o_orderkey"s
                "       and o_orderdate < date '1995-03-15'"s
                "       and l_shipdate > date '1995-03-15'"s
                "   group by"s
                "       l_orderkey,"s
                "       o_orderdate,"s
                "       o_shippriority"s
                "   order by"s
                "       revenue desc,"s
                "       o_orderdate"s
                "   limit 10;"s},
      {TPCH_Q6, "select "s
                "    sum(l_extendedprice * l_discount) as revenue "s
                "from "s
                "    lineitem "s
                "where "s
                "    l_shipdate >= date '1994-01-01' "s
                "    and l_shipdate < date '1995-01-01'"s
                "    and l_discount between 0.05 and 0.07"s
                "    and l_quantity < 24;"s},
      {TPCH_Q9,
       "select"s
       "      nation,"s
       "      o_year,"s
       "      sum(amount) as sum_profit"s
       "  from"s
       "      ("s
       "          select"s
       "              n_name as nation,"s
       "              extract(year from o_orderdate) as o_year,"s
       "              l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount"s
       "          from"s
       "              part,"s
       "              supplier,"s
       "              lineitem,"s
       "              partsupp,"s
       "              orders,"s
       "              nation"s
       "          where"s
       "              s_suppkey = l_suppkey"s
       "              and ps_suppkey = l_suppkey"s
       "              and ps_partkey = l_partkey"s
       "              and p_partkey = l_partkey"s
       "              and o_orderkey = l_orderkey"s
       "              and s_nationkey = n_nationkey"s
       "              and p_retailprice > 1006.05"s // modified Q9 predicate: use p_retailprice
       "              and p_retailprice < 1080.1"s  // instead of p_name like '%green%
       "      ) as profit"s
       "  group by"s
       "      nation,"s
       "      o_year"s
       "  order by"s
       "      nation,"s
       "      o_year desc;"s},
      {TPCH_Q18, "select"s
                 "     c_name,"s
                 "     c_custkey,"s
                 "     o_orderkey,"s
                 "     o_orderdate,"s
                 "     o_totalprice,"s
                 "     sum(l_quantity)"s
                 " from"s
                 "     customer,"s
                 "     orders,"s
                 "     lineitem"s
                 " where"s
                 "     o_orderkey in ("s
                 "         select"s
                 "             l_orderkey"s
                 "         FROM"s
                 "             lineitem"s
                 "         group by"s
                 "             l_orderkey having"s
                 "                 sum(l_quantity) > 300"s
                 "     )"s
                 "     and c_custkey = o_custkey"s
                 "     and o_orderkey = l_orderkey"s
                 " group by"s // modified Q18: removed c_name from group by
                 "     c_custkey,"s
                 "     o_orderkey,"s
                 "     o_orderdate,"s
                 "     o_totalprice"s
                 " order by"s
                 "     o_totalprice desc,"s
                 "     o_orderdate"s
                 " limit 100;"s},
  };
  return queries;
}

static auto& duckdbQueries() {
  static std::map<int, std::string> queries = {
      {TPCH_Q1, "SELECT"
                "    l_returnflag, "
                "    l_linestatus, "
                "    sum(l_quantity) AS sum_qty, "
                "    sum(l_extendedprice) AS sum_base_price, "
                "    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price, "
                "    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, "
                "    avg(l_quantity) AS avg_qty, "
                "    avg(l_extendedprice) AS avg_price, "
                "    avg(l_discount) AS avg_disc, "
                "    count(*) AS count_order "
                " FROM"
                "    lineitem"
                " WHERE"
                "    l_shipdate <= DATE '1998-09-02'"
                " GROUP BY"
                "     l_returnflag,"
                "     l_linestatus"
                " ORDER BY"
                "     l_returnflag,"
                "     l_linestatus;"},
      {TPCH_Q3, "SELECT"
                "       l_orderkey,"
                "       o_orderdate,"
                "       o_shippriority,"
                "       sum(l_extendedprice * (1 - l_discount)) AS revenue"
                " FROM"
                "       customer,"
                "       orders,"
                "       lineitem"
                " WHERE"
                "       c_mktsegment = 'BUILDING'"
                "       AND c_custkey = o_custkey"
                "       AND l_orderkey = o_orderkey"
                "       AND o_orderdate < date '1995-03-15'"
                "       AND l_shipdate > date '1995-03-15'"
                " GROUP BY"
                "       l_orderkey,"
                "       o_orderdate,"
                "       o_shippriority"
                " ORDER BY"
                "       revenue DESC,"
                "       o_orderdate"
                " LIMIT 10;"},
      {TPCH_Q6, "SELECT"
                "    sum(l_extendedprice * l_discount) AS revenue"
                " FROM"
                "    lineitem"
                " WHERE"
                "    l_shipdate >= CAST('1994-01-01' AS date)"
                "    AND l_shipdate < CAST('1995-01-01' AS date)"
                "    AND l_discount BETWEEN 0.05"
                "    AND 0.07"
                "    AND l_quantity < 24;"},
      {TPCH_Q9,
       "SELECT"
       "     nation,"
       "     o_year,"
       "     sum(amount) AS sum_profit"
       " FROM ("
       "     SELECT"
       "         n_name AS nation,"
       "         extract(year FROM o_orderdate) AS o_year,"
       "         l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount"
       "     FROM"
       "         part,"
       "         supplier,"
       "         lineitem,"
       "         partsupp,"
       "         orders,"
       "         nation"
       "     WHERE"
       "         s_suppkey = l_suppkey"
       "         AND ps_suppkey = l_suppkey"
       "         AND ps_partkey = l_partkey"
       "         AND p_partkey = l_partkey"
       "         AND o_orderkey = l_orderkey"
       "         AND s_nationkey = n_nationkey"
       "         AND p_retailprice > 1006.50"           // modified Q9 predicate: use p_retailprice
       "         AND p_retailprice < 1080.1) AS profit" // instead of p_name like '%green%
       " GROUP BY"
       "     nation,"
       "     o_year"
       " ORDER BY"
       "     nation,"
       "     o_year DESC;"},
      {TPCH_Q18, "SELECT"
                 "       c_name,"
                 "       c_custkey,"
                 "       o_orderkey,"
                 "       o_orderdate,"
                 "       o_totalprice,"
                 "       sum(l_quantity)"
                 "   FROM"
                 "       customer,"
                 "       orders,"
                 "       lineitem"
                 "   WHERE"
                 "       o_orderkey IN ("
                 "           SELECT"
                 "               l_orderkey"
                 "           FROM"
                 "               lineitem"
                 "           GROUP BY"
                 "               l_orderkey"
                 "           HAVING"
                 "               sum(l_quantity) > 300)"
                 "       AND c_custkey = o_custkey"
                 "       AND o_orderkey = l_orderkey"
                 "   GROUP BY" // modified Q18: removed c_name from group by
                 "       c_custkey,"
                 "       o_orderkey,"
                 "       o_orderdate,"
                 "       o_totalprice"
                 "   ORDER BY"
                 "       o_totalprice DESC,"
                 "       o_orderdate"
                 "   LIMIT 100;"},
  };
  return queries;
}

static void TPCH_BOSS(benchmark::State& state, int queryIdx, int dataSize, int64_t blockSize) {
  initBOSSEngine_TPCH(dataSize, blockSize);

  auto eval = [](auto const& expression) {
    boss::expressions::ExpressionSpanArguments spans;
    spans.emplace_back(boss::expressions::Span<std::string>(librariesToTest()));
    return boss::evaluate(
        "EvaluateInEngines"_(ComplexExpression("List"_, {}, {}, std::move(spans)),
                             expression.clone(CloneReason::EVALUATE_CONST_EXPRESSION)));
  };

  auto evalFirstLastOnly = [](auto const& expression) {
    // assuming the first library is always the storage engine
    return boss::evaluate(
        "EvaluateInEngines"_("List"_(librariesToTest()[0], librariesToTest().back()),
                             expression.clone(CloneReason::EVALUATE_CONST_EXPRESSION)));
  };

  auto error_handling = [](auto&& result, auto const& queryName) {
    if(IGNORE_ERRORS) {
      return false;
    }
    if(!holds_alternative<boss::ComplexExpression>(result)) {
      return false;
    }
    if(get<boss::ComplexExpression>(result).getHead() == "Table"_) {
      return false;
    }
    if(get<boss::ComplexExpression>(result).getHead() == "List"_) {
      return false;
    }
    std::cout << queryName << " Error: "
              << (!MORE_VERBOSE_OUTPUT ? utilities::injectDebugInfoToSpans(std::move(result))
                                       : std::move(result))
              << std::endl;
    return true;
  };

  bool failed = false;

  auto const& queryName = queryNames().find(queryIdx)->second;
  auto const& query = bossQueries().find(queryIdx)->second;

  if(!failed) {
    auto result = eval(query);
    failed = error_handling(result, queryName);

    if(VERBOSE_QUERY_OUTPUT && !failed) {
      std::cout << "BOSS " << queryName << "BOSS output = "
                << (!MORE_VERBOSE_OUTPUT ? utilities::injectDebugInfoToSpans(std::move(result))
                                         : std::move(result))
                << std::endl;
    }
  }

  for(auto i = 1; !failed && i < BENCHMARK_NUM_WARMPUP_ITERATIONS; ++i) {
    failed = error_handling(eval(query), queryName);
  }

  if(!failed && VERIFY_OUTPUT) {
    auto result1 = eval(query);
    auto result2 = evalFirstLastOnly(query);
    if(result1 != result2) {
      std::cout << queryName << ": ouputs are not matching." << std::endl;
      std::cout << "output1 = " << result1 << std::endl;
      std::cout << "output2 = " << result2 << std::endl;
      failed = true;
    }
  }

  vtune.startSampling(queryName + " - BOSS");
  for(auto _ : state) { // NOLINT
    if(!failed) {
      state.PauseTiming();
      state.ResumeTiming();
      auto dummyResult = eval(query);
      if(error_handling(dummyResult, queryName)) {
        failed = true;
      }
      benchmark::DoNotOptimize(dummyResult);
    }
  }
  vtune.stopSampling();
}

static void TPCH_monetdb(benchmark::State& state, int queryIdx, int dataSize) {
  auto& db = initMonetDB(dataSize, !DISABLE_CONSTRAINTS, MONETDB_MULTITHREADING,
                         USE_FIXED_POINT_NUMERIC_TYPE);
  monetdbe_result* result = nullptr;

  auto& query = monetdbQueries().find(queryIdx)->second;
  auto const& queryName = queryNames().find(queryIdx)->second;

  auto error_handling = [&db, &queryName](auto&& result, bool failed) {
    if(failed) {
      std::cout << queryName << " Error: " << monetdbe_error(db) << std::endl;
      return true;
    } else if(!result) {
      std::cout << queryName << " Error: NULL output" << std::endl;
      return true;
    }
    return false;
  };

  if(VERBOSE_QUERY_OUTPUT) {
    std::cout << "MonetDB ";
    std::string verboseQuery = EXPLAIN_QUERY_OUTPUT ? "EXPLAIN " + query : query;
    auto failed = (bool)monetdbe_query(db, verboseQuery.data(), &result, NULL);
    if(!error_handling(result, failed)) {
      std::cout << queryName << " output = ";
      printResult(db, result, VERBOSE_QUERY_OUTPUT_MAX_LINES);
    }
    monetdbe_cleanup_result(db, result);
  }

  bool failed = false;

  for(auto i = 0; i < BENCHMARK_NUM_WARMPUP_ITERATIONS; ++i) {
    auto failed = (bool)monetdbe_query(db, query.data(), &result, NULL);
    if(error_handling(result, failed)) {
      failed = true;
      break;
    }
  }

  vtune.startSampling(queryName + " - MonetDB");
  for(auto _ : state) { // NOLINT
    if(!failed) {
      auto failed = (bool)monetdbe_query(db, query.data(), &result, NULL);
      if(error_handling(result, failed)) {
        failed = true;
      }
      benchmark::DoNotOptimize(result);
    }
  }
  vtune.stopSampling();

  monetdbe_cleanup_result(db, result);
}

static void TPCH_duckdb(benchmark::State& state, int queryIdx, int dataSize) {
  auto& connection =
      initDuckDB(dataSize, !DISABLE_CONSTRAINTS, DUCKDB_MAX_THREADS, USE_FIXED_POINT_NUMERIC_TYPE);

  auto const& query = duckdbQueries().find(queryIdx)->second;
  auto const& queryName = queryNames().find(queryIdx)->second;

  auto error_handling = [&queryName](auto&& result) {
    if(result->HasError()) {
      std::cout << queryName << " Error: " << result->GetError() << std::endl;
      return true;
    }
    return false;
  };

  if(VERBOSE_QUERY_OUTPUT) {
    std::cout << "DuckDB ";
    auto result =
        EXPLAIN_QUERY_OUTPUT ? connection.Query("EXPLAIN " + query) : connection.Query(query);
    if(!error_handling(result)) {
      std::cout << queryName << " output = ";
      result->Print();
    }
  }

  bool failed = false;

  for(auto i = 0; i < BENCHMARK_NUM_WARMPUP_ITERATIONS; ++i) {
    auto result = connection.Query(query);
    if(error_handling(result)) {
      failed = true;
      break;
    }
  }

  vtune.startSampling(queryName + " - DuckDB");
  for(auto _ : state) { // NOLINT
    if(!failed) {
      auto result = connection.Query(query);
      if(error_handling(result)) {
        failed = true;
      }
      benchmark::DoNotOptimize(result);
    }
  }
  vtune.stopSampling();
}

static void TPCH_test(benchmark::State& state, int engine, int query, int dataSize, int blockSize) {
  static auto lastEngine = engine;
  if(lastEngine != engine) {
    switch(lastEngine) {
    case BOSS:
      resetBOSSEngine();
      break;
    case MONETDB:
      releaseMonetDB();
      break;
    case DUCKDB:
      releaseDuckDB();
      break;
    default:
      break;
    }
  }
  lastEngine = engine;

  switch(engine) {
  case BOSS: {
    TPCH_BOSS(state, query, dataSize, blockSize);
  } break;
  case MONETDB: {
    TPCH_monetdb(state, query, dataSize);
  } break;
  case DUCKDB: {
    TPCH_duckdb(state, query, dataSize);
  } break;
  default:
    throw std::runtime_error("unknown engine");
  }
}

template <typename... Args>
benchmark::internal::Benchmark* RegisterBenchmarkNolint([[maybe_unused]] Args... args) {
#ifdef __clang_analyzer__
  // There is not way to disable clang-analyzer-cplusplus.NewDeleteLeaks
  // even though it is perfectly safe. Let's just please clang analyzer.
  return nullptr;
#else
  return benchmark::RegisterBenchmark(args...);
#endif
}

void initAndRunBenchmarks(int argc, char** argv) {
  // read custom arguments
  for(int i = 0; i < argc; ++i) {
    if(std::string("--verbose") == argv[i] || std::string("-v") == argv[i]) {
      VERBOSE_QUERY_OUTPUT = true;
    } else if(std::string("--very-verbose") == argv[i] || std::string("-vv") == argv[i]) {
      MORE_VERBOSE_OUTPUT = true;
    } else if(std::string("--verify-output") == argv[i]) {
      VERIFY_OUTPUT = true;
    } else if(std::string("--explain") == argv[i]) {
      EXPLAIN_QUERY_OUTPUT = true;
    } else if(std::string("--ignore-errors") == argv[i]) {
      IGNORE_ERRORS = true;
    } else if(std::string("--disable-mmap-cache") == argv[i]) {
      DISABLE_MMAP_CACHE = true;
    } else if(std::string("--disable-gpu-cache") == argv[i]) {
      DISABLE_GPU_CACHE = true;
    } else if(std::string("--disable-constraints") == argv[i]) {
      DISABLE_CONSTRAINTS = true;
    } else if(std::string("--disable-gather-operator") == argv[i]) {
      DISABLE_GATHER_OPERATOR = true;
    } else if(std::string("--disable-auto-dictionary-encoding") == argv[i]) {
      DISABLE_AUTO_DICTIONARY_ENCODING = true;
    } else if(std::string("--all-strings-as-integers") == argv[i]) {
      ALL_STRINGS_AS_INTEGERS = true;
    } else if(std::string("--benchmark-data-copy-in") == argv[i]) {
      BENCHMARK_DATA_COPY_IN = true;
    } else if(std::string("--benchmark-data-copy-out") == argv[i]) {
      BENCHMARK_DATA_COPY_OUT = true;
    } else if(std::string("--benchmark-storage-block-size") == argv[i]) {
      BENCHMARK_STORAGE_BLOCK_SIZE = true;
    } else if(std::string("--default-storage-block-size") == argv[i]) {
      if(++i < argc) {
        DEFAULT_STORAGE_BLOCK_SIZE = atoi(argv[i]);
      }
    } else if(std::string("--max-gpu-memory-cache") == argv[i]) {
      if(++i < argc) {
        MAX_GPU_MEMORY_CACHE = atoi(argv[i]);
      }
    } else if(std::string("--duckdb-max-threads") == argv[i]) {
      if(++i < argc) {
        DUCKDB_MAX_THREADS = atoi(argv[i]);
      }
    } else if(std::string("--monetdb-enable-multithreading") == argv[i]) {
      MONETDB_MULTITHREADING = true;
    } else if(std::string("--fixed-point-numeric-type") == argv[i]) {
      USE_FIXED_POINT_NUMERIC_TYPE = true;
    } else if(std::string("--benchmark-num-warmup-iterations") == argv[i]) {
      if(++i < argc) {
        BENCHMARK_NUM_WARMPUP_ITERATIONS = atoi(argv[i]);
      }
    } else if(std::string("--library") == argv[i]) {
      if(++i < argc) {
        librariesToTest().emplace_back(argv[i]);
      }
    }
  }
  // register TPC-H benchmarks
  for(int dataSize :
           std::vector<int>{1, 10, 100, 1000, 2000, 5000, 10000, 20000, 50000, 100000}) {
    for(int engine = ENGINE_START; engine < ENGINE_END; ++engine) {
      for(int64_t blockSize :
          (BENCHMARK_STORAGE_BLOCK_SIZE && engine == BOSS
               ? std::vector<int64_t>{1 << 25, 1 << 26, 1 << 27, 1 << 28, 1 << 29, 1 << 30,
                                      std::numeric_limits<int32_t>::max()}
               : std::vector<int64_t>{DEFAULT_STORAGE_BLOCK_SIZE})) {
        for(int queryIdx : std::vector<int>{TPCH_Q1, TPCH_Q3, TPCH_Q6, TPCH_Q9, TPCH_Q18}) {
          std::ostringstream testName;
          auto const& queryName = queryNames()[queryIdx];
          testName << queryName << "/";
          testName << DBEngineNames[engine] << "/";
          testName << dataSize << "MB";
          if(BENCHMARK_STORAGE_BLOCK_SIZE && engine == BOSS) {
            testName << "/";
            testName << (blockSize >> 20) << "MB";
          }
          RegisterBenchmarkNolint(testName.str().c_str(), TPCH_test, engine, queryIdx, dataSize,
                                  blockSize);
        }
      }
    }
  }
  // register TPC-H benchmarks with GPU kernel heuristics for the query plans
  for(int dataSize : std::vector<int>{1, 10, 100, 1000, 2000, 5000, 10000, 20000, 50000, 100000}) {
    for(int64_t blockSize :
        (BENCHMARK_STORAGE_BLOCK_SIZE
             ? std::vector<int64_t>{1 << 25, 1 << 26, 1 << 27, 1 << 28, 1 << 29, 1 << 30,
                                    std::numeric_limits<int32_t>::max()}
             : std::vector<int64_t>{DEFAULT_STORAGE_BLOCK_SIZE})) {
      for(auto queryIdx :
          std::vector<int>{TPCH_Q1_POSTFILTER, TPCH_Q3_POSTFILTER_1JOIN, TPCH_Q3_POSTFILTER_2JOINS,
                           TPCH_Q6_NESTED_SELECT, TPCH_Q9_POSTFILTER_PRIORITY}) {
        std::ostringstream testName;
        auto const& queryName = queryNames()[queryIdx];
        testName << queryName << "/";
        testName << dataSize << "MB";
        if(BENCHMARK_STORAGE_BLOCK_SIZE) {
          testName << "/";
          testName << (blockSize >> 20) << "MB";
        }
        RegisterBenchmarkNolint(testName.str().c_str(), TPCH_test, BOSS, queryIdx, dataSize,
                                blockSize);
      }
    }
  }
  // initialise and run google benchmark
  ::benchmark::Initialize(&argc, argv, ::benchmark::PrintDefaultHelp);
  ::benchmark::RunSpecifiedBenchmarks();

  releaseBOSSEngine();
}

int main(int argc, char** argv) {
  try {
    initAndRunBenchmarks(argc, argv);
  } catch(std::exception& e) {
    std::cerr << "caught exception in main: " << e.what() << std::endl;
    return EXIT_FAILURE;
  } catch(...) {
    std::cerr << "unhandled exception." << std::endl;
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
