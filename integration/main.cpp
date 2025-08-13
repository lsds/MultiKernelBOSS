#include "Optimizer.hpp"

int main() {
  // 3, just push select through the joins.
  // auto expr1 = "Top"_(
  //     "Group"_(
  //         "Project"_(
  //             "Join"_(
  //                 "Join"_(
  //                     "Select"_("ORDERS"_(33333),
  //                               "Where"_("Greater"_("DateObject"_("1995-03-15"),
  //                                                   "o_orderdate"_))),
  //                     "Select"_("CUSTOMER"_(22222),
  //                               "Where"_("StringContainsQ"_("c_mktsegment"_,
  //                                                           "BUILDING"))),
  //                     "Where"_("Equal"_("c_custkey"_, "o_custkey"_))),
  //                 "Select"_("LINEITEM"_(11111),
  //                           "Where"_("Greater"_("l_shipdate"_,
  //                                               "DateObject"_("1993-03-15")))),
  //                 "Where"_("Equal"_("o_orderkey"_, "l_orderkey"_))),
  //             "As"_("revenue"_,
  //                   "Times"_("l_extendedprice"_, "Minus"_(1.0, "l_discount"_)),
  //                   "l_orderkey"_, "l_orderkey"_, "o_orderdate"_,
  //                   "o_orderdate"_, "o_shippriority"_, "o_shippriority"_)),
  //         "By"_("l_orderkey"_, "o_orderdate"_, "o_shippriority"_),
  //         "As"_("total_revenue"_, "Sum"_("revenue"_))),
  //     "By"_("total_revenue"_, "desc"_, "o_orderdate"_, "asc"_), 10);

  // 6 (nested select when big)
  auto q6 = "Group"_(
      "Project"_(
          "Select"_(
              "LINEITEM"_(11111),
              "Where"_("And"_(
                  "Greater"_(24, "l_quantity"_),       // l_quantity < 24
                  "Greater"_("l_discount"_, 0.0499),   // l_discount > 0.0499
                  "Greater"_(0.07001, "l_discount"_),  // l_discount < 0.07001
                  "Greater"_("DateObject"_("1995-01-01"),
                             "l_shipdate"_),  // l_shipdate < '1995-01-01'
                  "Greater"_("l_shipdate"_,
                             "DateObject"_("1993-12-31"))))),  // l_shipdate >
                                                               // '1993-12-31'
          "As"_("revenue"_, "Times"_("l_extendedprice"_, "l_discount"_))),
      "As"_("sum_revenue"_, "Sum"_("revenue"_)));

  // 9 cooked
  // auto q9 = "Order"_(
  //     "Group"_(
  //         "Project"_(
  //             "Join"_(
  //                 "Select"_(
  //                     "Join"_(
  //                         "PART"_(66666),
  //                         "Join"_(
  //                             "Join"_("NATION"_(55555), "SUPPLIER"_(77777),
  //                                     "Where"_("Equal"_("n_nationkey"_,
  //                                                       "s_nationkey"_))),
  //                             "PARTSUPP"_(88888),
  //                             "Where"_("Equal"_("s_suppkey"_, "ps_suppkey"_))),
  //                         "Where"_("Equal"_("p_partkey"_, "ps_partkey"_))),
  //                     "Where"_("And"_("Greater"_("p_retailprice"_,
  //                                                1006),  // NOLINT
  //                                     "Greater"_(1080,    // NOLINT
  //                                                "p_retailprice"_)))),
  //                 "Join"_("ORDERS"_(33333), "LINEITEM"_(11111),
  //                         "Where"_("Equal"_("o_orderkey"_, "l_orderkey"_))),
  //                 "Where"_("And"_("Equal"_("ps_partkey"_, "l_partkey"_),
  //                                 "Equal"_("ps_suppkey"_, "l_suppkey"_)))),
  //             "As"_("nation"_, "n_name"_, "o_year"_, "Year"_("o_orderdate"_),
  //                   "amount"_,
  //                   "Minus"_("Times"_("l_extendedprice"_,
  //                                     "Minus"_(1, "l_discount"_)),
  //                            "Times"_("ps_supplycost"_, "l_quantity"_)))),
  //         "By"_("nation"_, "o_year"_), "As"_("sum_amount"_, "Sum"_("amount"_))),
  //     "By"_("nation"_, "asc"_, "o_year"_, "desc"_));

  auto q9 = "Order"_(
            "Group"_(
                "Project"_(
                    "Join"_("ORDERS"_(33333),
                            "Join"_(
                                    "Join"_(
                                            "Select"_("PART"_(66666),
                                                "Where"_("And"_("Greater"_("p_retailprice"_,
                                                                           1006.05), // NOLINT
                                                                "Greater"_(1080.1,   // NOLINT
                                                                           "p_retailprice"_)))),
                                            "Join"_(
                                                    "Join"_("NATION"_(55555), "SUPPLIER"_(77777),
                                                        "Where"_("Equal"_("n_nationkey"_,
                                                                          "s_nationkey"_))),
                                                "PARTSUPP"_(88888),
                                                "Where"_("Equal"_("s_suppkey"_, "ps_suppkey"_))),
                                        "Where"_("Equal"_("p_partkey"_, "ps_partkey"_))),
                                "LINEITEM"_(11111),
                                 "Where"_("And"_("Equal"_("ps_partkey"_, "l_partkey"_),
                                  "Equal"_("ps_suppkey"_, "l_suppkey"_)))),
                        "Where"_("Equal"_("o_orderkey"_, "l_orderkey"_))),
                    "As"_("nation"_, "n_name"_, "o_year"_, "Year"_("o_orderdate"_), "amount"_,
                          "Minus"_("Times"_("l_extendedprice"_, "Minus"_(1.0, "l_discount"_)),
                                   "Times"_("ps_supplycost"_, "l_quantity"_)))),
                "By"_("nation"_, "o_year"_), "As"_("sum_amount"_, "Sum"_("amount"_))),
            "By"_("nation"_, "asc"_,"o_year"_, "desc"_));

  // 1

  auto q1 = "Order"_(
                "Group"_(
                    "Project"_(
                        "Project"_(
                            "Project"_(
                                "Select"_("LINEITEM"_(11111),
                                    "Where"_("Greater"_("DateObject"_("1998-08-31"),
                                    "l_shipdate"_))),
                                "As"_("l_returnflag"_, "l_returnflag"_,
                                "l_linestatus"_,
                                      "l_linestatus"_, "l_quantity"_,
                                      "l_quantity"_, "l_extendedprice"_,
                                      "l_extendedprice"_, "l_discount"_,
                                      "l_discount"_, "calc1"_, "Minus"_(1.0,
                                      "l_discount"_), "calc2"_,
                                      "Plus"_("l_tax"_, 1.0))),
                        "As"_("l_returnflag"_, "l_returnflag"_,
                        "l_linestatus"_, "l_linestatus"_,
                              "l_quantity"_, "l_quantity"_,
                              "l_extendedprice"_, "l_extendedprice"_,
                              "l_discount"_, "l_discount"_, "disc_price"_,
                              "Times"_("l_extendedprice"_, "calc1"_),
                              "calc2"_, "calc2"_)),
                    "As"_("l_returnflag"_, "l_returnflag"_, "l_linestatus"_,
                    "l_linestatus"_,
                          "l_quantity"_, "l_quantity"_, "l_extendedprice"_,
                          "l_extendedprice"_, "l_discount"_, "l_discount"_,
                          "disc_price"_, "disc_price"_, "calc"_,
                          "Times"_("disc_price"_, "calc2"_))),
                "By"_("l_returnflag"_, "l_linestatus"_),
                "As"_("sum_qty"_, "Sum"_("l_quantity"_), "sum_base_price"_,
                      "Sum"_("l_extendedprice"_), "sum_disc_price"_,
                      "Sum"_("disc_price"_), "sum_charges"_, "Sum"_("calc"_),
                      "avg_qty"_, "Avg"_("l_quantity"_), "avg_price"_,
                      "Avg"_("l_extendedprice"_), "avg_disc"_,
                      "Avg"_("l_discount"_), "count_order"_,
                      "Count"_("*"_))),
            "By"_("l_returnflag"_, "asc"_, "l_linestatus"_, "asc"_));

  // Q3

  auto q3 = "Top"_(
                "Group"_(
                    "Project"_(
                        "Join"_("Join"_("Select"_("ORDERS"_(33333),
                                                "Where"_("Greater"_("DateObject"_("1995-03-15"),
                                                                    "o_orderdate"_))),
                                                "Select"_("CUSTOMER"_(22222),
                                                    "Where"_("StringContainsQ"_("c_mktsegment"_,
                                                                                "BUILDING"))),
                                            "Where"_("Equal"_("c_custkey"_,
                                            "o_custkey"_))),
                                    "Select"_("LINEITEM"_(11111),
                                        "Where"_(
                                            "Greater"_("l_shipdate"_,
                                            "DateObject"_("1993-03-15")))),
                                "Where"_("Equal"_("o_orderkey"_,
                                "l_orderkey"_))),
                        "As"_("revenue"_, "Times"_("l_extendedprice"_,
                        "Minus"_(1.0, "l_discount"_)),
                              "l_orderkey"_, "l_orderkey"_,
                              "o_orderdate"_, "o_orderdate"_,
                              "o_shippriority"_, "o_shippriority"_)),
                    "By"_("l_orderkey"_, "o_orderdate"_, "o_shippriority"_),
                    "As"_("total_revenue"_, "Sum"_("revenue"_))),
                "By"_("total_revenue"_, "desc"_, "o_orderdate"_, "asc"_),
                10);

  // Q18
  auto q18 = "Top"_(
                  "Group"_(
                          "Join"_(
                              // Aggregated LINEITEM with filter
                              "Select"_(
                                  "Group"_("LINEITEM"_(11111),
                                           "By"_("l_orderkey"_),
                                           "As"_("sum_l_quantity"_,
                                           "Sum"_("l_quantity"_))),
                                  "Where"_("Greater"_("sum_l_quantity"_,
                                  300))),
                              // CUSTOMER-ORDERS join
                                  "Join"_("CUSTOMER"_(22222),
                                          "ORDERS"_(33333),
                                          "Where"_("Equal"_("c_custkey"_,
                                          "o_custkey"_))),
                              "Where"_("Equal"_("l_orderkey"_,
                              "o_orderkey"_))),
                      "By"_("o_custkey"_, "o_orderkey"_, "o_orderdate"_,
                      "o_totalprice"_), "As"_("sum_sum_l_quantity"_,
                      "Sum"_("sum_l_quantity"_))),
                  "By"_("o_totalprice"_, "desc"_, "o_orderdate"_, "asc"_),
                  100);

  auto newQ = "Group"_("LINEITEM"_(11111), "By"_("l_orderkey"_), "As"_("sum_l_quantity"_, "Sum"_("l_quantity"_)));
  auto x = "Project"_("Select"_("LINEITEM"_(11111), "Where"_("Greater"_("l_quantity"_, 100))), "As"_("l_orderkey"_, "l_orderkey"_));

  Optimizer optimizer;
  optimizer.Init();
  optimizer.AddLoadLibraryTask("../../../build_velox/libveloxboss.so");
  optimizer.AddLoadLibraryTask("../../../build_arrayfire/libarrayfire.so");
//   optimizer.AddLoadLibraryTask("../../../build_dummy/libdummyengine.dylib");
//   optimizer.AddUnloadLibraryTask("../../../build_arrayfire/libarrayfire.dylib");
  optimizer.AddGetSupportedOperatorsTask();
  optimizer.AddOptimizeQueryTask(std::move(q1));
  optimizer.AddOptimizeQueryTask(std::move(q3));
  optimizer.AddOptimizeQueryTask(std::move(q6));
  optimizer.AddOptimizeQueryTask(std::move(q9));
  optimizer.AddOptimizeQueryTask(std::move(q18));
//   optimizer.AddOptimizeQueryTask(std::move(newQ));
//   optimizer.AddOptimizeQueryTask(std::move(x));
  std::vector<Expression> resultExprs = optimizer.ExecuteTasks("../data/dxl/metadata/md.xml");
  for (auto &expr : resultExprs) {
    std::cout << "--------------------------------" << std::endl;
    std::cout << expr << std::endl;
  }

  // std::cout << resultExpr << std::endl;
  optimizer.Cleanup();

  return 0;
}
