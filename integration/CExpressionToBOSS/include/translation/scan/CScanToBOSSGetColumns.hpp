#pragma once

#include "translation/Translator.hpp"

namespace cexpressiontoboss::translation {

class CScanToBOSSGetColumns : public Translator {
 public:
  CScanToBOSSGetColumns() = default;
  ~CScanToBOSSGetColumns() override = default;
  bool Match(const CExpression* expr) override;

  // helper functions for GetColumns
  static const CTableDescriptor* GetTableDescriptor(const CExpression* expr);
  static Expression GetTableNameExpr(const CExpression* expr,
                                     const CTableDescriptor* tableDesc);
  static Expression GetColumnListExpr(const CExpression* expr,
                                      const CTableDescriptor* tableDesc);
  // Check if a column list contains all columns for a table
  static bool IsFullColumnList(const CExpression* expr, const CTableDescriptor* tableDesc);
};

}  // namespace cexpressiontoboss::translation
