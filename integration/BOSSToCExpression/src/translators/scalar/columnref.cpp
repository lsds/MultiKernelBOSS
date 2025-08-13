#include "translators/scalar/columnref.hpp"
#include "Translator.hpp"

namespace bosstocexpression {

std::pair<bool, Expression> ColumnRefTranslator::Match(Expression &&bossExpr) {
  return std::visit(
      boss::utilities::overload(
          [&](Symbol&& sym) -> std::pair<bool, Expression> {
            return std::make_pair(true, std::move(sym));
          },
          [&](auto &&val) -> std::pair<bool, Expression> { return std::make_pair(false, std::move(val)); }),
      std::move(bossExpr));
}

RetType<EmptyStruct> ColumnRefTranslator::Translate(Expression &&bossExpr, BOSSToCExpressionConverter<EmptyStruct, EmptyStruct, EmptyStruct, ColRefMap> &converter, ColRefMap const& colMap) {
  return std::visit(
      boss::utilities::overload(
          [&](Symbol&& sym) -> RetType<EmptyStruct> {
            // Convert column reference - use existing column reference from map
            // if available
            std::string colName = sym.getName();

            // Look up the column in our mapping
            auto it = colMap.find(colName);
            if (it != colMap.end()) {
              // Found the column in the map - use the existing column reference
              CColRef* colref = it->second;
              return {GPOS_NEW(mp)
                  CExpression(mp, GPOS_NEW(mp) CScalarIdent(mp, colref)), true};
            } else {
              // Column not found in map - throw an error
              std::cerr << "Column '" + colName +
                               "' not found in column mapping"
                        << std::endl;
              throw std::runtime_error("Column '" + colName +
                                       "' not found in column mapping");
            }
          },
          [&](auto&&) -> RetType<EmptyStruct> {
            __builtin_unreachable();
          }),
      std::move(bossExpr));
}

int ColumnRefTranslator::GetPriority() {
  return 0;
}

}  // namespace bosstocexpression
