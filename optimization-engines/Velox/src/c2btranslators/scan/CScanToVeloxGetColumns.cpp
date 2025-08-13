#include "c2btranslators/scan/CScanToVeloxGetColumns.hpp"

#include "CExpressionToBOSS.hpp"

namespace cexpressiontoboss::translation {

RetTypeC2B<EmptyStruct> CScanToVeloxGetColumns::Translate(
    const CExpression* expr, CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter, ProjectInfo const& aux) {
  ColSet newRequiredColumns;
  newRequiredColumns.insert(aux.requiredColumns.begin(), aux.requiredColumns.end());
  const CTableDescriptor* tableDesc = GetTableDescriptor(expr);

  if (IsFullColumnList(expr, tableDesc)) {
    auto scanExpr = GetTableNameExpr(expr, tableDesc);
    if (!(aux.parentOp == COperator::EopPhysicalComputeScalar)) {
      ColSet outputColumns = utils::GetOutputColumns(const_cast<CExpression*>(expr));
      ColSet projectColumns = utils::GetSetIntersection(outputColumns, aux.requiredColumns);
      if (projectColumns != outputColumns) {
        Expression projectList = utils::CreateProjectListFromColumns(projectColumns);
        return {"Project"_(std::move(scanExpr), std::move(projectList)), true};
      }
    }
    return {std::move(scanExpr), true};    
  }
  // Create BOSS Physical Get expression with columns
  return {"GetColumns"_(GetTableNameExpr(expr, tableDesc),
                                      GetColumnListExpr(expr, tableDesc)), true};
}

int CScanToVeloxGetColumns::GetPriority() {
  return 0;
}

}  // namespace cexpressiontoboss::translation
