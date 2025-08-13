#include "translators/getcolumns.hpp"

namespace bosstocexpression {

std::pair<bool, ComplexExpression> GetColumnsTranslator::Match(ComplexExpression &&bossExpr) {
  auto [head, arg1, arg2, arg3] = std::move(bossExpr).decompose();
  if (head.getName() == "GetColumns") {
    return std::make_pair(true, ComplexExpression{std::move(head), std::move(arg1), std::move(arg2), std::move(arg3)});
  }

  return std::make_pair(false, ComplexExpression{std::move(head),std::move(arg1), std::move(arg2), std::move(arg3)});
}



RetType<EmptyStruct> GetColumnsTranslator::Translate(ComplexExpression &&bossExpr, BOSSToCExpressionConverter<EmptyStruct, EmptyStruct, EmptyStruct, ColRefMap> &converter, EmptyStruct const& _) {
  auto [head, unused1_, dyns, unused2_] = std::move(bossExpr).decompose();

  if (dyns.size() < 2) {
    std::cerr << "GetColumns expression requires at least 2 arguments"
              << std::endl;
    throw std::runtime_error(
        "GetColumns expression requires at least 2 arguments");
  }

  // Extract relation information
  if (!std::holds_alternative<ComplexExpression>(dyns[0])) {
    std::cerr << "First argument of GetColumns must be a relation expression"
              << std::endl;
    throw std::runtime_error(
        "First argument of GetColumns must be a relation expression");
  }

  ComplexExpression relExpr = std::get<ComplexExpression>(std::move(dyns[0]));
  auto [relHead, unused3_, relArgs, unused4_] = std::move(relExpr).decompose();

  // Get relation name
  std::string relName = relHead.getName();
  std::wstring wRelName = utils::StringToWString(relName);
  CWStringConst tableName(wRelName.c_str());
  CWStringConst alias(tableName);

  // Extract OID
  if (relArgs.size() < 1 || !std::holds_alternative<int>(relArgs[0])) {
    std::cerr << "Expected integer argument for OID value" << std::endl;
    throw std::runtime_error("Expected integer argument for OID value");
  }

  int oidValue = std::get<int>(relArgs[0]);
  if (oidValue < 0) {
    std::cerr << "OID value cannot be negative" << std::endl;
    throw std::runtime_error("OID value cannot be negative");
  }
  ULONG ulTableId = static_cast<ULONG>(oidValue);

  // Extract column list
  if (!std::holds_alternative<ComplexExpression>(dyns[1])) {
    std::cerr
        << "Second argument of GetColumns must be a column list expression"
        << std::endl;
    throw std::runtime_error(
        "Second argument of GetColumns must be a column list expression");
  }

  ComplexExpression columnList =
      std::get<ComplexExpression>(std::move(dyns[1]));
  std::vector<std::string> columnNames =
      utils::extractColumnNames(std::move(columnList));

  // Convert std::vector<std::string> to CDynamicPtrArray<CWStringConst,
  // CleanupRelease>
  CDynamicPtrArray<CWStringConst, CleanupNULL>* pdrgpsColumnNames =
      GPOS_NEW(mp) CDynamicPtrArray<CWStringConst, CleanupNULL>(mp);
  std::vector<CWStringConst*> stringsToCleanup;

  for (const auto& colName : columnNames) {
    std::wstring wColName = utils::StringToWString(colName);
    CWStringConst* pstrColName =
        GPOS_NEW(mp) CWStringConst(mp, wColName.c_str());
    pdrgpsColumnNames->Append(pstrColName);
    stringsToCleanup.push_back(pstrColName);
  }

  // Create the expression using PexprLogicalGetWithMetadataKeys
  CExpression* pexpr = utils::PexprLogicalGetWithMetadataKeys(mp,
      &tableName, &alias, ulTableId, pdrgpsColumnNames);
  for (CWStringConst* pstrColName : stringsToCleanup) {
    GPOS_DELETE(pstrColName);
  }
  pdrgpsColumnNames->Release();
  return {pexpr, true};
}

int GetColumnsTranslator::GetPriority() {
  return 0;
}

}  // namespace bosstocexpression
