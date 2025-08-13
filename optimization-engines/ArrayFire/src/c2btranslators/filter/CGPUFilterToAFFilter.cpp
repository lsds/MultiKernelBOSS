#include "c2btranslators/filter/CGPUFilterToAFFilter.hpp"

#include "CExpressionToBOSS.hpp"

namespace cexpressiontoboss::translation {
    using namespace orcaextender;
bool CGPUFilterToAFFilter::Match(const CExpression* expr) {
  DynamicRegistry* dynamicRegistry = DynamicRegistry::GetInstance();
  auto engineType = dynamicRegistry->GetEngineType("ArrayFire");
  return expr->Pop()->Eopid() == dynamicRegistry->GetOperatorId(engineType, "CPhysicalGPUPartialSelect") || expr->Pop()->Eopid() == dynamicRegistry->GetOperatorId(engineType, "CPhysicalGPUFullSelect");
}
  
RetTypeC2B<EmptyStruct> CGPUFilterToAFFilter::Translate(
    const CExpression* expr, CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter, ProjectInfo const& aux) {
  ColSet newRequiredColumns;
  bool success = true;
  newRequiredColumns.insert(aux.requiredColumns.begin(), aux.requiredColumns.end());

  RetTypeC2B<ColSet> predicate = CFilterToBOSSFilter::GetPredicateExpr(expr, converter);
  success &= predicate.success;
  newRequiredColumns.insert(predicate.aux.begin(), predicate.aux.end());
  if (std::move(predicate.expr) == true) {
    ProjectInfo newAux;
    newAux.parentOp = expr->Pop()->Eopid();
    newAux.requiredColumns = newRequiredColumns;
    return utils::GetChildExprUnary(expr, converter, newAux);
  }

  RetTypeC2B<ColSet> predicate2 = CFilterToBOSSFilter::GetPredicateExpr(expr, converter);
  ProjectInfo newAux;
  newAux.parentOp = expr->Pop()->Eopid();
  newAux.requiredColumns = newRequiredColumns;
  RetTypeC2B<EmptyStruct> childExpr = utils::GetChildExprUnary<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>(expr, converter, newAux);
  success &= childExpr.success;

  DynamicRegistry* dynamicRegistry = DynamicRegistry::GetInstance();
  auto engineType = dynamicRegistry->GetEngineType("ArrayFire");
  Expression selectExpr;
  if (expr->Pop()->Eopid() == dynamicRegistry->GetOperatorId(engineType, "CPhysicalGPUPartialSelect")) {
    selectExpr = "SelectToGather"_(std::move(childExpr.expr), "Where"_(std::move(predicate2.expr)));
  } else {
    selectExpr = "Select"_(std::move(childExpr.expr), "Where"_(std::move(predicate2.expr)));
  }

  if (!(aux.parentOp == COperator::EopPhysicalComputeScalar || aux.parentOp == dynamicRegistry->GetOperatorId(dynamicRegistry->GetEngineType("ArrayFire"), "CPhysicalGPUProject") || aux.parentOp == dynamicRegistry->GetOperatorId(dynamicRegistry->GetEngineType("ArrayFire"), "CPhysicalGPUPartialSelect") || aux.parentOp == dynamicRegistry->GetOperatorId(dynamicRegistry->GetEngineType("ArrayFire"), "CPhysicalGPUPartialSelect") ||  aux.parentOp == dynamicRegistry->GetOperatorId(dynamicRegistry->GetEngineType("ArrayFire"), "CPhysicalGPUFullSelect"))) {
    ColSet outputColumns = utils::GetOutputColumns(const_cast<CExpression*>(expr));
    ColSet projectColumns = utils::GetSetIntersection(outputColumns, aux.requiredColumns);
    if (projectColumns != outputColumns) {
      Expression projectList = utils::CreateProjectListFromColumns(projectColumns);
      return {"Project"_(std::move(selectExpr), std::move(projectList)), success};
    }
  }

  return {std::move(selectExpr), success};
}

int CGPUFilterToAFFilter::GetPriority() {
  return 0;
}
}  // namespace cexpressiontoboss::translation
