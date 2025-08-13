#include "operators/CPhysicalGPUFullSelect.hpp"
#include "gpoptextender/DynamicRegistry/DynamicRegistry.hpp"
#include "gpoptextender/EngineProperty/CEngineSpec.hpp"

#include "gpos/base.h"

#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CDistributionSpecSingleton.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CExpressionHandle.h"

using namespace orcaextender;



CPhysicalGPUFullSelect::CPhysicalGPUFullSelect(
	CMemoryPool *mp)
	: CPhysicalFilter(mp)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalGPUFullSelect::~CPhysicalHashAgg
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalGPUFullSelect::~CPhysicalGPUFullSelect()
{
}

// ident accessors
COperator::EOperatorId
CPhysicalGPUFullSelect::Eopid() const
{
	DynamicRegistry* registry = DynamicRegistry::GetInstance();
	EEngineType engine = registry->GetEngineType(AFEngineKeys::ArrayFire);
  return registry->GetOperatorId(engine, SzId());
}

// EOF
