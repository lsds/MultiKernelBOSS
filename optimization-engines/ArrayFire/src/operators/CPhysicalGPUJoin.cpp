#include "operators/CPhysicalGPUJoin.hpp"
#include "gpoptextender/DynamicRegistry/DynamicRegistry.hpp"
#include "gpoptextender/EngineProperty/CEngineSpec.hpp"

#include "gpos/base.h"

#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CDistributionSpecSingleton.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"

using namespace orcaextender;


CPhysicalGPUJoin::CPhysicalGPUJoin(
	CMemoryPool *mp, CExpressionArray *pdrgpexprOuterKeys,
	CExpressionArray *pdrgpexprInnerKeys)
	: CPhysicalInnerHashJoin(mp, pdrgpexprOuterKeys, pdrgpexprInnerKeys)
{
}


CPhysicalGPUJoin::~CPhysicalGPUJoin()
{
}

gpopt::CDistributionSpec *CPhysicalGPUJoin::PdsDerive(CMemoryPool *mp,
												CExpressionHandle &exprhdl) const
{
  return PdsDerivePassThruOuter(exprhdl);
}

gpopt::CDistributionSpec *CPhysicalGPUJoin::PdsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, gpopt::CDistributionSpec *pdsRequired, ULONG child_index, CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) const {
	return PdsPassThru(mp, exprhdl, pdsRequired, child_index);
}

// ident accessors
COperator::EOperatorId
CPhysicalGPUJoin::Eopid() const
{
	DynamicRegistry* registry = DynamicRegistry::GetInstance();
	EEngineType engine = registry->GetEngineType(AFEngineKeys::ArrayFire);
  return registry->GetOperatorId(engine, SzId());
}

// EOF
