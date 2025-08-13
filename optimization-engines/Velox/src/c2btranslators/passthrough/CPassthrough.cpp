#include "c2btranslators/passthrough/CPassthrough.hpp"

#include "CExpressionToBOSS.hpp"

namespace cexpressiontoboss::translation {
    using namespace orcaextender;
bool CPassthrough::Match(const CExpression* expr) {
  DynamicRegistry* dynamicRegistry = DynamicRegistry::GetInstance();
  auto engineType = dynamicRegistry->GetEngineType("Velox");

  std::vector<COperator::EOperatorId> passthroughOperators = {
    dynamicRegistry->GetOperatorId(engineType, "CVeloxGather"),
    COperator::EopPhysicalMotionGather, 
    COperator::EopPhysicalMotionBroadcast, 
    COperator::EopPhysicalMotionHashDistribute, 
    COperator::EopPhysicalSpool, 
    COperator::EopScalarCast, 
    COperator::EopPhysicalMotionRandom,
    COperator::EopPhysicalEngineTransform
  };

  return std::find(passthroughOperators.begin(), passthroughOperators.end(), expr->Pop()->Eopid()) != passthroughOperators.end();
}
  
RetTypeC2B<EmptyStruct> CPassthrough::Translate(
    const CExpression* expr, CExpressionToBOSSConverter<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>& converter, ProjectInfo const& aux) {
  return utils::GetChildExprUnary<EmptyStruct, ProjectInfo, ColSet, EmptyStruct>(expr, converter, aux);
}

int CPassthrough::GetPriority() {
  return 0;
}
}  // namespace cexpressiontoboss::translation
