#include "translators/scan.hpp"
#include "gpoptextender/DynamicRegistry/DynamicRegistry.hpp"

namespace bosstocexpression {

std::pair<bool, ComplexExpression> ScanTranslator::Match(ComplexExpression &&bossExpr) {
  auto [head, arg1, arg2, arg3] = std::move(bossExpr).decompose();
  std::vector<std::string> op_list = {"GetColumns", "Project", "Join", "LeftJoin", "OuterJoin", "SemiJoin", "AntiSemiJoin", "AntiSemiJoinNotIn", "NAryJoin", "Order", "Top", "Group", "Select"};
  if (std::find(op_list.begin(), op_list.end(), head.getName()) == op_list.end()) {
    return std::make_pair(true, ComplexExpression{std::move(head), std::move(arg1), std::move(arg2), std::move(arg3)});
  }

  return std::make_pair(false, ComplexExpression{std::move(head),std::move(arg1), std::move(arg2), std::move(arg3)});
}

RetType<EmptyStruct> ScanTranslator::Translate(ComplexExpression &&bossExpr, BOSSToCExpressionConverter<EmptyStruct, EmptyStruct, EmptyStruct, ColRefMap> &converter, EmptyStruct const& _) {
  auto [head, unused1_, unused2_, unused3_] = std::move(bossExpr).decompose();
  std::wstring relName = utils::StringToWString(head.getName());
  CWStringConst tableName(relName.c_str());
  CWStringConst alias(tableName);

  // if (!std::holds_alternative<int>(args[0])) {
  //   std::cerr << "Expected integer arguments for OID value" << std::endl;
  //   throw std::runtime_error("Expected integer arguments for OID value");
  // }

  orcaextender::DynamicRegistry *dynamicRegistry = orcaextender::DynamicRegistry::GetInstance();
  ULONG ulTableId = dynamicRegistry->GetOID(head.getName());
  
  // std::get<int>(args[0]);
  return {utils::PexprLogicalGetWithMetadataKeys(mp, &tableName, &alias, ulTableId), true};
}

int ScanTranslator::GetPriority() {
  return 0;
}

}  // namespace bosstocexpression
