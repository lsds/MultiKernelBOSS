#include "translation/sort/CSortToBOSSSort.hpp"

#include "CExpressionToBOSS.hpp"

namespace cexpressiontoboss::translation {

bool CSortToBOSSSort::Match(const CExpression* expr) {
  return expr && expr->Pop()->Eopid() == COperator::EopPhysicalSort;
}

std::pair<Expression, std::unordered_set<std::string>> CSortToBOSSSort::GetSortKeyList(const CExpression* expr) {
  std::unordered_set<std::string> requiredColumns;

  // Get the sort keys from the CPhysicalSort operator
  CPhysicalSort* sortOp = CPhysicalSort::PopConvert(expr->Pop());
  COrderSpec* orderSpec = const_cast<COrderSpec*>(sortOp->Pos());

  // Create a list of sort keys
  ExpressionArguments sortKeyArgs{};

  // Extract each sort column and add it to the key list
  for (ULONG i = 0; i < orderSpec->UlSortColumns(); i++) {
    const CColRef* colRef = orderSpec->Pcr(i);
    std::string colName = cexpressiontoboss::utils::WStringToString(
        colRef->Name().Pstr()->GetBuffer());

    // Add the column name to the sort key arguments
    sortKeyArgs.push_back(Symbol{colName});
    requiredColumns.insert(colName);
    // For descending order: add "desc"_
    COrderSpec::ENullTreatment nullTreatment = orderSpec->Ent(i);
    
    // Determine direction based on null treatment
    // We know from BOSSToCExpression that EntLast is used for ascending order
    bool isDescending = (nullTreatment == COrderSpec::EntFirst);
    
    // Add the appropriate sort direction
    if (isDescending) {
      sortKeyArgs.push_back("desc"_);
    }
  }

  // Create a list of sort keys with directions
  return std::make_pair(ComplexExpression{"By"_, {}, std::move(sortKeyArgs), {}}, requiredColumns);
}
}  // namespace cexpressiontoboss::translation
