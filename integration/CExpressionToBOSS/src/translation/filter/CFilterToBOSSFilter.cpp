#include "translation/filter/CFilterToBOSSFilter.hpp"

#include "CExpressionToBOSS.hpp"

namespace cexpressiontoboss::translation {
bool CFilterToBOSSFilter::Match(const CExpression* expr) {
  return expr->Pop()->Eopid() == COperator::EopPhysicalFilter;
}

// assumes scalar predicate
RetTypeC2B<ColSet> CFilterToBOSSFilter::GetPredicateExpr(
    const CExpression* expr, CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter) {
  CExpression* predicateExpr = (*expr)[1];
  return converter.ConvertScalar(predicateExpr, {});
}

}  // namespace cexpressiontoboss::translation
