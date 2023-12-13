#pragma once
#include <duckdb.hpp>
#include <iostream>
#include <string>

using namespace std::string_literals;

class DuckDBHandling {
public:
  DuckDBHandling(size_t size, bool enableIndexes, int maxThreads, bool useFixedPoint)
      : config(createConfig(maxThreads)), db(":memory:", config.get()), connection(db) {
    std::string numeric_type = useFixedPoint ? "DECIMAL(15,2)" : "DOUBLE";
    std::vector<std::string> createCmds{
        " CREATE TABLE region  ( r_regionkey  INTEGER NOT NULL,"
        "                             r_name       CHAR(25) NOT NULL,"
        "                             r_comment    VARCHAR(152),"
        "                              dummy           VARCHAR " +
            (!enableIndexes ? ");"s : "                           , PRIMARY KEY(r_regionkey));"s),
        " CREATE TABLE nation  ( n_nationkey  INTEGER NOT NULL,"
        "                             n_name       CHAR(25) NOT NULL,"
        "                             n_regionkey  INTEGER NOT NULL,"
        "                             n_comment    VARCHAR(152),"
        "                              dummy           VARCHAR " +
            (!enableIndexes
                 ? ");"s
                 : "                           , PRIMARY KEY(n_nationkey), "
                   "                             FOREIGN KEY (n_regionkey) REFERENCES region(r_regionkey));"s),
        " CREATE TABLE part  ( p_partkey     BIGINT NOT NULL,"
        "                           p_name        VARCHAR(55) NOT NULL,"
        "                           p_mfgr        CHAR(25) NOT NULL,"
        "                           p_brand       CHAR(10) NOT NULL,"
        "                           p_type        VARCHAR(25) NOT NULL,"
        "                           p_size        INTEGER NOT NULL,"
        "                           p_container   CHAR(10) NOT NULL,"
        "                           p_retailprice " +
            numeric_type +
            " NOT NULL,"
            "                           p_comment     VARCHAR(23) NOT NULL,"
            "                              dummy           VARCHAR " +
            (!enableIndexes ? ");"s : "                         , PRIMARY KEY(p_partkey));"s),
        " CREATE TABLE supplier ( s_suppkey     BIGINT NOT NULL,"
        "                              s_name        CHAR(25) NOT NULL,"
        "                              s_address     VARCHAR(40) NOT NULL,"
        "                              s_nationkey   INTEGER NOT NULL,"
        "                              s_phone       CHAR(15) NOT NULL,"
        "                              s_acctbal     " +
            numeric_type +
            " NOT NULL,"
            "                              s_comment     VARCHAR(101) NOT NULL,"
            "                              dummy           VARCHAR " +
            (!enableIndexes
                 ? ");"s
                 : "                            , PRIMARY KEY(s_suppkey), "
                   "                              FOREIGN KEY (s_nationkey) REFERENCES nation(n_nationkey));"s),
        " CREATE TABLE partsupp ( ps_partkey     BIGINT NOT NULL,"
        "                              ps_suppkey     BIGINT NOT NULL,"
        "                              ps_availqty    BIGINT NOT NULL,"
        "                              ps_supplycost  " +
            numeric_type +
            "  NOT NULL,"
            "                              ps_comment     VARCHAR(199) NOT NULL,"
            "                              dummy           VARCHAR " +
            (!enableIndexes
                 ? ");"s
                 : "                            , PRIMARY KEY(ps_partkey, ps_suppkey), "
                   "                              FOREIGN KEY (ps_partkey) REFERENCES "
                   "part(p_partkey), "
                   "                              FOREIGN KEY (ps_suppkey) REFERENCES supplier(s_suppkey));"s),
        " CREATE TABLE customer ( c_custkey     BIGINT NOT NULL,"
        "                              c_name        VARCHAR(25) NOT NULL,"
        "                              c_address     VARCHAR(40) NOT NULL,"
        "                              c_nationkey   INTEGER NOT NULL,"
        "                              c_phone       CHAR(15) NOT NULL,"
        "                              c_acctbal     " +
            numeric_type +
            "   NOT NULL,"
            "                              c_mktsegment  CHAR(10) NOT NULL,"
            "                              c_comment     VARCHAR(117) NOT NULL,"
            "                              dummy           VARCHAR " +
            (!enableIndexes
                 ? ");"s
                 : "                            , PRIMARY KEY(c_custkey), "
                   "                              FOREIGN KEY (c_nationkey) REFERENCES nation(n_nationkey));"s),
        " CREATE TABLE orders  ( o_orderkey       BIGINT NOT NULL,"
        "                            o_custkey        BIGINT NOT NULL,"
        "                            o_orderstatus    CHAR(1) NOT NULL,"
        "                            o_totalprice     " +
            numeric_type +
            " NOT NULL,"
            "                            o_orderdate      DATE NOT NULL,"
            "                            o_orderpriority  CHAR(15) NOT NULL,  "
            "                            o_clerk          CHAR(15) NOT NULL, "
            "                            o_shippriority   INTEGER NOT NULL,"
            "                            o_comment        VARCHAR(79) NOT NULL,"
            "                              dummy           VARCHAR " +
            (!enableIndexes
                 ? ");"s
                 : "                          , PRIMARY KEY(o_orderkey), "
                   "                            FOREIGN KEY (o_custkey) REFERENCES customer(c_custkey));"s),
        " CREATE TABLE lineitem ( l_orderkey    BIGINT NOT NULL,"
        "                              l_partkey     BIGINT NOT NULL,"
        "                              l_suppkey     BIGINT NOT NULL,"
        "                              l_linenumber  BIGINT NOT NULL,"
        "                              l_quantity    " +
            numeric_type +
            " NOT NULL,"
            "                              l_extendedprice  " +
            numeric_type +
            " NOT NULL,"
            "                              l_discount    " +
            numeric_type +
            " NOT NULL,"
            "                              l_tax         " +
            numeric_type +
            " NOT NULL,"
            "                              l_returnflag  CHAR(1) NOT NULL,"
            "                              l_linestatus  CHAR(1) NOT NULL,"
            "                              l_shipdate    DATE NOT NULL,"
            "                              l_commitdate  DATE NOT NULL,"
            "                              l_receiptdate DATE NOT NULL,"
            "                              l_shipinstruct CHAR(25) NOT NULL,"
            "                              l_shipmode     CHAR(10) NOT NULL,"
            "                              l_comment      VARCHAR(44) NOT NULL,"
            "                              dummy           VARCHAR " +
            (!enableIndexes
                 ? ");"s
                 : "                            , PRIMARY KEY(l_orderkey, l_linenumber), "
                   "                              FOREIGN KEY (l_orderkey) REFERENCES "
                   "orders(o_orderkey), "
                   "                              FOREIGN KEY (l_partkey,l_suppkey) REFERENCES partsupp(ps_partkey,ps_suppkey));"s)};
    for(auto const& cmd : createCmds) {
      auto result = connection.Query(cmd);
      if(result->HasError()) {
        std::cerr << result->GetError();
      }
    }
    std::vector<std::string> tables{"region",   "nation",   "part",   "supplier",
                                    "partsupp", "customer", "orders", "lineitem"};
    for(auto const& table : tables) {
      std::string path = "../data/tpch_" + std::to_string(size) + "MB/" + table + ".tbl";
      auto insertCmd = "INSERT INTO " + table + " SELECT * FROM read_csv_auto('" + path + "');";
      auto result = connection.Query(insertCmd);
      if(result->HasError()) {
        std::cerr << result->GetError();
      }
    }
  }

  ~DuckDBHandling() {}

  duckdb::Connection& getConnection() { return connection; }

private:
  std::unique_ptr<duckdb::DBConfig> config;
  duckdb::DuckDB db;
  duckdb::Connection connection;

  std::unique_ptr<duckdb::DBConfig> createConfig(int maxThreads) {
    auto config = std::make_unique<duckdb::DBConfig>();
    config->options.maximum_threads = maxThreads;
    config->options.temporary_directory = "duckdbtemp";
    // config->options.maximum_memory = duckdb::DBConfig::ParseMemoryLimit("176GB");
    return config;
  }
};

static auto& duckDBHandlingPtr() {
  static std::unique_ptr<DuckDBHandling> duckDBhandling;
  return duckDBhandling;
}

static void releaseDuckDB() { duckDBHandlingPtr().reset(); }

static auto& initDuckDB(size_t size, bool enableIndexes, int maxThreads, bool useFixedPoint) {
  static auto lastSize = size;
  static auto lastEnableIndexes = enableIndexes;
  static auto lastMaxThreads = maxThreads;
  static auto lastUseFixedPoint = useFixedPoint;
  if(!duckDBHandlingPtr() || size != lastSize || enableIndexes != lastEnableIndexes ||
     maxThreads != lastMaxThreads || useFixedPoint != lastUseFixedPoint) {
    lastSize = size;
    lastEnableIndexes = enableIndexes;
    lastMaxThreads = maxThreads;
    lastUseFixedPoint = useFixedPoint;
    duckDBHandlingPtr().reset();
    duckDBHandlingPtr() =
        std::make_unique<DuckDBHandling>(size, enableIndexes, maxThreads, useFixedPoint);
  }
  return duckDBHandlingPtr()->getConnection();
}