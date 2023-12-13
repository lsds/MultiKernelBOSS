#pragma once
#include "Expression.hpp"
namespace boss {
namespace engines {
class Engine {
public:
  expressions::Expression evaluate(expressions::Expression const& e);
};
} // namespace engines
using boss::engines::Engine; // NOLINT(misc-unused-using-decls)
} // namespace boss
