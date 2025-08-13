#include "translation/scalar/CScalarIdentToBOSSScalarIdent.hpp"

#include "CExpressionToBOSS.hpp"

namespace cexpressiontoboss::translation {

bool CScalarIdentToBOSSScalarIdent::Match(const CExpression* expr) {
  return expr->Pop()->Eopid() == COperator::EopScalarIdent;
}

RetTypeC2B<ColSet> CScalarIdentToBOSSScalarIdent::Translate(
    const CExpression* expr, CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter, EmptyStruct const& aux) {
  CScalarIdent* scalarIdent = CScalarIdent::PopConvert(expr->Pop());
  const CColRef* colRef = scalarIdent->Pcr();
  const CName& colNameObj = colRef->Name();
  const CWStringConst* colNameStr = colNameObj.Pstr();

  if (!colNameStr) {
    throw std::runtime_error("Column reference has no name");
  }

  std::string colName =
      cexpressiontoboss::utils::WStringToString(colNameStr->GetBuffer());
  ColSet requiredColumns;
  requiredColumns.insert(colName);
  return {Symbol{colName}, true, requiredColumns};
}

int CScalarIdentToBOSSScalarIdent::GetPriority() {
  return 0;
}

}  // namespace cexpressiontoboss::translation