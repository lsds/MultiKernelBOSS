#include "translation/scan/CScanToBOSSGetColumns.hpp"

#include "CExpressionToBOSS.hpp"

namespace cexpressiontoboss::translation {
bool CScanToBOSSGetColumns::Match(const CExpression* expr) {
  return expr->Pop()->Eopid() == COperator::EopPhysicalTableScan;
}

const CTableDescriptor* CScanToBOSSGetColumns::GetTableDescriptor(
    const CExpression* expr) {
  const CPhysicalTableScan* tableScan =
      dynamic_cast<const CPhysicalTableScan*>(expr->Pop());
  if (!tableScan) {
    throw std::runtime_error("Not a table scan operator");
  }
  // Get the table name
  const CTableDescriptor* tableDesc = tableScan->Ptabdesc();
  if (!tableDesc) {
    throw std::runtime_error("Missing table descriptor");
  }
  return tableDesc;
}

Expression CScanToBOSSGetColumns::GetTableNameExpr(
    const CExpression* expr, const CTableDescriptor* tableDesc) {
  const CName& tableNameObj = tableDesc->Name();
  const CWStringConst* tableNameStr = tableNameObj.Pstr();

  // Convert wide string to standard string
  std::string tableName;
  if (tableNameStr) {
    tableName =
        cexpressiontoboss::utils::WStringToString(tableNameStr->GetBuffer());
  } else {
    throw std::runtime_error("Missing table name");
  }

  return Symbol(tableName);
}

Expression CScanToBOSSGetColumns::GetColumnListExpr(
    const CExpression* expr, const CTableDescriptor* tableDesc) {
  // Get the list of columns from the table descriptor
  ExpressionArguments columnList;
  ULONG ulColumns = tableDesc->ColumnCount();

  // Get the column array
  const CDynamicPtrArray<CColumnDescriptor, CleanupRelease>* colArray =
      tableDesc->Pdrgpcoldesc();

  for (ULONG ul = 0; ul < ulColumns; ul++) {
    const CColumnDescriptor* colDesc = colArray->operator[](ul);
    if (colDesc) {
      const CName& colNameObj = colDesc->Name();
      const CWStringConst* colNameStr = colNameObj.Pstr();
      if (colNameStr) {
        std::string colName =
            cexpressiontoboss::utils::WStringToString(colNameStr->GetBuffer());
        columnList.push_back(Symbol{colName});
      }
    }
  }

  return ComplexExpression("List"_, {}, std::move(columnList), {});
}

bool CScanToBOSSGetColumns::IsFullColumnList(const CExpression* expr, const CTableDescriptor* tableDesc) {
  auto scanOp = dynamic_cast<const CPhysicalScan*>(expr->Pop());
  const CColRefArray* pdrgpcrOutput = scanOp->PdrgpcrOutput();
  if (pdrgpcrOutput == nullptr) {
    return false;
  }
  
  ULONG tableColumnCount = tableDesc->ColumnCount();
  return pdrgpcrOutput->Size() == tableColumnCount;
}

}  // namespace cexpressiontoboss::translation
