#include "c2btranslators/limit/CPhysicalSortLimitToVeloxOrderBy.hpp"

#include "CExpressionToBOSS.hpp"
#include "translation/limit/CPhysicalLimitToBOSSPhysicalLimit.hpp"
#include "translation/sort/CSortToBOSSSort.hpp"

namespace cexpressiontoboss::translation {

bool CPhysicalSortLimitToVeloxOrderBy::Match(const CExpression* expr) {
  if (expr->Pop()->Eopid() != COperator::EopPhysicalLimit) {
    return false;
  }
  
  if (expr->Arity() < 1) {
    return false;
  }

  CExpression* pexprChild = (*expr)[0];
  while (pexprChild->Pop()->Eopid() == COperator::EopPhysicalMotionGather) {
    pexprChild = (*pexprChild)[0];
  }
  return pexprChild->Pop()->Eopid() == COperator::EopPhysicalSort;
}

// Helper function to safely extract numeric values from variants
template <typename T = int64_t>
T get_numeric_value(const Expression& expr) {
  return std::visit([](auto&& arg) -> T {
    using ArgType = std::decay_t<decltype(arg)>;
    if constexpr (std::is_arithmetic_v<ArgType>) {
      return static_cast<T>(arg);
    } else {
      throw std::runtime_error("Expression does not contain a numeric value");
    }
  }, expr);
}

RetTypeC2B<EmptyStruct> CPhysicalSortLimitToVeloxOrderBy::Translate(
    const CExpression* expr, CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter, ProjectInfo const& aux) {
  ColSet newRequiredColumns;
  bool success = true;
  newRequiredColumns.insert(aux.requiredColumns.begin(), aux.requiredColumns.end());
  // Get the child Sort expression
  CExpression* pexprSort = (*expr)[0];

  orcaextender::DynamicRegistry* dynamicRegistry = orcaextender::DynamicRegistry::GetInstance();
  auto engineType = dynamicRegistry->GetEngineType("Velox");
  std::vector<COperator::EOperatorId> passthroughOperators = {
    dynamicRegistry->GetOperatorId(engineType, "CVeloxGather"),
    COperator::EopPhysicalMotionGather, 
    COperator::EopPhysicalMotionBroadcast, 
    COperator::EopPhysicalMotionHashDistribute, 
    COperator::EopPhysicalSpool, 
    COperator::EopScalarCast, 
    COperator::EopPhysicalEngineTransform,
    COperator::EopPhysicalMotionRandom
  };
  while (std::find(passthroughOperators.begin(), passthroughOperators.end(), pexprSort->Pop()->Eopid()) != passthroughOperators.end()) {
    pexprSort = (*pexprSort)[0];
  }
  
  // Translate the sort columns - use the Sort translator's helper function
  auto [sortColumns, sortColumnsRequiredColumns] = CSortToBOSSSort::GetSortKeyList(pexprSort);
  newRequiredColumns.insert(sortColumnsRequiredColumns.begin(), sortColumnsRequiredColumns.end());

  RetTypeC2B<ColSet> limitOffsetExpr = CPhysicalLimitToBOSSPhysicalLimit::GetOffsetExpr(expr, converter);
  success &= limitOffsetExpr.success;
  newRequiredColumns.insert(limitOffsetExpr.aux.begin(), limitOffsetExpr.aux.end());

  auto [_, __, offsetArgs, ___] = std::move(get<ComplexExpression>(limitOffsetExpr.expr)).decompose();

  RetTypeC2B<ColSet> limitCountExpr = CPhysicalLimitToBOSSPhysicalLimit::GetLimitCountExpr(expr, converter);
  success &= limitCountExpr.success;
  newRequiredColumns.insert(limitCountExpr.aux.begin(), limitCountExpr.aux.end());
  auto [____, _____, countArgs, ______] = std::move(std::get<ComplexExpression>(limitCountExpr.expr)).decompose();

  // Use the visitor pattern to safely extract the numeric value, regardless of its exact type
  int64_t offsetValue = get_numeric_value<int64_t>(offsetArgs[0]);
  if (offsetValue != 0) {
    throw std::runtime_error("Limit offset is not supported in MultiKernelBOSS");
  }

  ProjectInfo newAux;
  newAux.parentOp = expr->Pop()->Eopid();
  newAux.requiredColumns = newRequiredColumns;
  RetTypeC2B<EmptyStruct> childExpr = utils::GetChildExprUnary<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>(pexprSort, converter, newAux);
  success &= childExpr.success;

  int64_t countValue = get_numeric_value<int64_t>(countArgs[0]);

  if (countValue == gpos::ulong_max) {
    return {"Order"_(
      std::move(childExpr.expr),
      std::move(sortColumns)
    ), success};
  }

  return {"Top"_(
    std::move(childExpr.expr),
    std::move(sortColumns),
    std::move(countArgs[0])  // Keep the original variant to maintain type information
  ), success};
}


int CPhysicalSortLimitToVeloxOrderBy::GetPriority() {
  return 1;
}

}  // namespace cexpressiontoboss::translation 