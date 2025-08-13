#include "operators/CPhysicalGPUPartialSelect.hpp"
#include "gpoptextender/DynamicRegistry/DynamicRegistry.hpp"
#include "gpoptextender/EngineProperty/CEngineSpec.hpp"

#include "gpos/base.h"

#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CDistributionSpecSingleton.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"

using namespace orcaextender;



CPhysicalGPUPartialSelect::CPhysicalGPUPartialSelect(
	CMemoryPool *mp)
	: CPhysicalFilter(mp)
{
}


CPhysicalGPUPartialSelect::~CPhysicalGPUPartialSelect()
{
}

// ident accessors
COperator::EOperatorId
CPhysicalGPUPartialSelect::Eopid() const
{
	DynamicRegistry* registry = DynamicRegistry::GetInstance();
	EEngineType engine = registry->GetEngineType(AFEngineKeys::ArrayFire);
  return registry->GetOperatorId(engine, SzId());
}

// EOF
