#include "translators/groupby.hpp"

namespace bosstocexpression {

std::pair<bool, ComplexExpression> GroupByTranslator::Match(ComplexExpression &&bossExpr) {
  auto [head, arg1, arg2, arg3] = std::move(bossExpr).decompose();
  if (head.getName() == "Group") {
    return std::make_pair(true, ComplexExpression{std::move(head), std::move(arg1), std::move(arg2), std::move(arg3)});
  }

  return std::make_pair(false, ComplexExpression{std::move(head),std::move(arg1), std::move(arg2), std::move(arg3)});
}

RetType<EmptyStruct> GroupByTranslator::Translate(ComplexExpression &&bossExpr, BOSSToCExpressionConverter<EmptyStruct, EmptyStruct, EmptyStruct, ColRefMap> &converter, EmptyStruct const& _) {
  auto [head, unused1_, dyns, unused2_] = std::move(bossExpr).decompose();
  // NOTE THIS DOESN'T ACCOUNT FOR DISTINCT AGGREGATES
  // Get underlying data
  RetType<EmptyStruct> ret = converter.Convert(std::move(dyns[0]), {});
  if (!ret.success) {
    return {nullptr, false};
  }
  CExpression* undlData = ret.expr;

  if (!undlData) {
    std::cerr << "Failed to convert underlying data in GroupBy" << std::endl;
    throw std::runtime_error("Failed to convert underlying data in GroupBy");
  }

  // Get the mapping of column names to column references
  std::unordered_map<std::string, CColRef*> colMap =
      utils::CreateColumnMapping(undlData);

  // Create array of grouping columns
  CColRefArray* pdrgpcrGroupingCols = GPOS_NEW(mp) CColRefArray(mp);

  // Check if we have a "By" expression (index 1 in dyns)
  bool hasByClause = false;
  size_t asIndex = 1;  // Default to 1 if no "By" clause

  if (dyns.size() > 1 && std::holds_alternative<ComplexExpression>(dyns[1])) {
    // We need to check if this is a "By" expression without moving it yet
    const ComplexExpression& byExprCandidate =
        std::get<ComplexExpression>(dyns[1]);
    Symbol byHeadCandidate = byExprCandidate.getHead();

    if (byHeadCandidate.getName() == "By") {
      // This is indeed a "By" expression
      hasByClause = true;
      asIndex = 2;  // "As" is at index 2 if we have a "By" clause

      // Now we can safely move and process the "By" expression
      auto byExpr = std::get<ComplexExpression>(std::move(dyns[1]));
      auto [byHead, by___, byArgs, by____] = std::move(byExpr).decompose();

      // Process all arguments in the "By" expression as grouping columns
      for (const auto& groupCol : byArgs) {
        if (!std::holds_alternative<Symbol>(groupCol)) {
          utils::safeRelease(undlData, pdrgpcrGroupingCols);
          std::cerr << "Expected Symbol for grouping column in GroupBy"
                    << std::endl;
          throw std::runtime_error(
              "Expected Symbol for grouping column in GroupBy");
        }

        std::string colName = std::get<Symbol>(groupCol).getName();
        if (colMap.find(colName) != colMap.end()) {
          pdrgpcrGroupingCols->Append(colMap[colName]);
        } else {
          utils::safeRelease(undlData, pdrgpcrGroupingCols);
          std::cerr << "Grouping column not found in column mapping: "
                    << colName << std::endl;
          throw std::runtime_error(
              "Grouping column not found in column mapping: " + colName);
        }
      }
    }
  }

  // If no "By" clause was found, we'll use an empty group key (global
  // aggregation) pdrgpcrGroupingCols is already initialized as an empty array

  // Get aggregate expressions from "As" component
  if (dyns.size() <= asIndex ||
      !std::holds_alternative<ComplexExpression>(dyns[asIndex])) {
    utils::safeRelease(undlData, pdrgpcrGroupingCols);
    std::cerr << "Expected ComplexExpression for 'As' in GroupBy" << std::endl;
    throw std::runtime_error("Expected ComplexExpression for 'As' in GroupBy");
  }

  auto asExpr = std::get<ComplexExpression>(std::move(dyns[asIndex]));
  auto [asHead, as___, asArgs, as____] = std::move(asExpr).decompose();

  if (asHead.getName() != "As") {
    utils::safeRelease(undlData, pdrgpcrGroupingCols);
    std::cerr << "Expected 'As' head for aggregate expressions in GroupBy"
              << std::endl;
    throw std::runtime_error(
        "Expected 'As' head for aggregate expressions in GroupBy");
  }

  // Validate that "As" has pairs of arguments (column name, expression)
  if (asArgs.size() < 2 || asArgs.size() % 2 != 0) {
    utils::safeRelease(undlData, pdrgpcrGroupingCols);
    std::cerr << "Invalid 'As' expression: must have an even number of "
                 "arguments (column name, aggregate expression pairs)"
              << std::endl;
    throw std::runtime_error(
        "Invalid 'As' expression: must have an even number of arguments "
        "(column name, aggregate expression pairs)");
  }

  // Create array of project elements for aggregates
  CExpressionArray* pdrgpexprPrjElems = GPOS_NEW(mp) CExpressionArray(mp);

  // Process pairs of arguments in "As" (column name, aggregate expression)
  for (size_t i = 0; i < asArgs.size(); i += 2) {
    // First argument must be a symbol (column name)
    if (!std::holds_alternative<Symbol>(asArgs[i])) {
      utils::safeRelease(undlData, pdrgpcrGroupingCols, pdrgpexprPrjElems);
      std::cerr << "Expected Symbol for aggregate output column in GroupBy"
                << std::endl;
      throw std::runtime_error(
          "Expected Symbol for aggregate output column in GroupBy");
    }

    std::string colName = std::get<Symbol>(asArgs[i]).getName();
    std::wstring wcolName = utils::StringToWString(colName);
    CWStringConst strColName(wcolName.c_str());
    CName name(mp, &strColName);

    // Create column reference for output HARDCODED INT4
    IMDId* mdid = GPOS_NEW(mp) CMDIdGPDB(GPDB_INT4_OID);
    const IMDType* pmdtype = mda->RetrieveType(mdid);
    mdid->Release();
    CColRef* colref = COptCtxt::PoctxtFromTLS()->Pcf()->PcrCreate(
        pmdtype, default_type_modifier, name);

    // Second argument must be the aggregate expression
    if (!std::holds_alternative<ComplexExpression>(asArgs[i + 1])) {
      utils::safeRelease(undlData, pdrgpcrGroupingCols, pdrgpexprPrjElems);
      std::cerr
          << "Expected ComplexExpression for aggregate function in GroupBy"
          << std::endl;
      throw std::runtime_error(
          "Expected ComplexExpression for aggregate function in GroupBy");
    }

    auto aggExpr = std::get<ComplexExpression>(std::move(asArgs[i + 1]));
    auto [aggHead, agg___, aggArgs, agg____] = std::move(aggExpr).decompose();

    // Get the aggregate function name
    std::string aggFuncName = aggHead.getName();

    // Validate the aggregate function
    if (aggArgs.size() < 1) {
      utils::safeRelease(undlData, pdrgpcrGroupingCols, pdrgpexprPrjElems);
      throw std::runtime_error(
          "Aggregate function expression requires at least 1 argument");
    }

    if (aggFuncName == "Count") {
      if (std::holds_alternative<Symbol>(aggArgs[0])) {
        auto symbol = std::get<Symbol>(aggArgs[0]);
        if (symbol.getName() == "*") {
          // This is COUNT(*), use the special function for it
          CExpression* pexprCountStar = CUtils::PexprCountStar(mp);

          // Create a project element using the existing colref
          CExpression* pexprPrjElem =
              CUtils::PexprScalarProjectElement(mp, colref, pexprCountStar);

          pdrgpexprPrjElems->Append(pexprPrjElem);
          continue;
        }
      }
    }

    // Get the input column/expression
    CExpression* pexprInput;
    try {
      RetType<EmptyStruct> ret = converter.ConvertScalar(std::move(aggArgs[0]), colMap);
      if (!ret.success) {
        utils::safeRelease(undlData, pdrgpcrGroupingCols, pdrgpexprPrjElems);
        return {nullptr, false};
      }
      pexprInput = ret.expr;
      if (!pexprInput) {
        std::cerr << "Failed to convert aggregate input expression in GroupBy"
                  << std::endl;
        throw std::runtime_error(
            "Failed to convert aggregate input expression in GroupBy");
      }
    } catch (const std::exception& e) {
      utils::safeRelease(undlData, pdrgpcrGroupingCols, pdrgpexprPrjElems);
      std::cerr << "Failed to convert aggregate input expression in GroupBy: "
                << e.what() << std::endl;
      throw std::runtime_error(
          "Failed to convert aggregate input expression in GroupBy: " +
          std::string(e.what()));
    }

    // Create the aggregate function
    CExpression* pexprAgg;
    try {
      pexprAgg = utils::CreateAggregateFunction(mp, aggFuncName, pexprInput, colref);
      if (!pexprAgg) {
        std::cerr << "Failed to create aggregate function in GroupBy"
                  << std::endl;
        throw std::runtime_error(
            "Failed to create aggregate function in GroupBy");
      }
    } catch (const std::exception& e) {
      utils::safeRelease(undlData, pdrgpcrGroupingCols, pdrgpexprPrjElems,
                         pexprInput);
      std::cerr << "Failed to create aggregate function in GroupBy: "
                << e.what() << std::endl;
      throw std::runtime_error(
          "Failed to create aggregate function in GroupBy: " +
          std::string(e.what()));
    }

    pdrgpexprPrjElems->Append(pexprAgg);
  }

  // Create project list
  CExpression* pexprPrjList = GPOS_NEW(mp)
      CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp), pdrgpexprPrjElems);

  // Create group by aggregate
  return {CUtils::PexprLogicalGbAgg(mp, pdrgpcrGroupingCols, undlData, pexprPrjList,
							 COperator::EgbaggtypeGlobal), true};
}

int GroupByTranslator::GetPriority() {
  return 0;
}

}  // namespace bosstocexpression
