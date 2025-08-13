#include "translation/scalar/CScalarProjectListToBOSSProjectList.hpp"

#include "CExpressionToBOSS.hpp"

namespace cexpressiontoboss::translation {

bool CScalarProjectListToBOSSProjectList::Match(const CExpression* expr) {
  return expr->Pop()->Eopid() == COperator::EopScalarProjectList;
}

RetTypeC2B<ColSet> CScalarProjectListToBOSSProjectList::Translate(
    const CExpression* expr, CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter, EmptyStruct const& aux) {
  ColSet requiredColumns;
  bool success = true;
  // Extract and convert each project element
  ExpressionArguments projElements;

  for (ULONG i = 0; i < expr->Arity(); i++) {
    CExpression* projElem = (*expr)[i];

    // Get the defined column
    CScalarProjectElement* pProjElem =
        CScalarProjectElement::PopConvert(projElem->Pop());
    const CColRef* pcrDefined = pProjElem->Pcr();

    // Get the column name
    std::string colName = cexpressiontoboss::utils::WStringToString(
        pcrDefined->Name().Pstr()->GetBuffer());

    // Get and convert the scalar expression
    CExpression* pexprScalar = (*projElem)[0];
    RetTypeC2B<ColSet> scalarBossExpr = converter.ConvertScalar(pexprScalar, aux);
    success &= scalarBossExpr.success;
    requiredColumns.insert(scalarBossExpr.aux.begin(), scalarBossExpr.aux.end());

    // Create a projection element: "as"_(colName, scalarExpr)
    projElements.push_back(Symbol{colName});
    projElements.push_back(std::move(scalarBossExpr.expr));
  }

  // Create and return a project list expression
  return {ComplexExpression{"As"_, {}, std::move(projElements), {}}, success, requiredColumns};
}

int CScalarProjectListToBOSSProjectList::GetPriority() {
  return 0;
}

}  // namespace cexpressiontoboss::translation
