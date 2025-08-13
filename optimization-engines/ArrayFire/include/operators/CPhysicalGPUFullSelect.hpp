#pragma once

#include "gpos/base.h"

#include "gpopt/operators/CPhysicalFilter.h"
#include "gpoptextender/DynamicRegistry/DynamicRegistry.hpp"
#include "afnamestore.hpp"
namespace orcaextender
{
using namespace gpopt;
// fwd declaration
class CDistributionSpec;


class CPhysicalGPUFullSelect : public CPhysicalFilter
{
private:
	// private copy ctor
	CPhysicalGPUFullSelect(const CPhysicalGPUFullSelect &);

public:
	// ctor
	CPhysicalGPUFullSelect(CMemoryPool *mp);

	// dtor
	virtual ~CPhysicalGPUFullSelect();

  virtual EOperatorId
Eopid() const;

	// return a string for operator name
	virtual const CHAR *
	SzId() const
	{
		return AFOpKeys::CPhysicalGPUFullSelect;
	}

	virtual CEngineSpec *PesDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const {
		DynamicRegistry* registry = DynamicRegistry::GetInstance();
		EEngineType engine = registry->GetEngineType(AFEngineKeys::ArrayFire);
		return GPOS_NEW(mp) CEngineSpec(engine);
	}

	virtual CEngineSpec *PesRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, CEngineSpec *pesRequired, ULONG child_index, CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) const {
		DynamicRegistry* registry = DynamicRegistry::GetInstance();
		EEngineType engine = registry->GetEngineType(AFEngineKeys::ArrayFire);
		return GPOS_NEW(mp) CEngineSpec(engine);
	}



	// conversion function
	static CPhysicalGPUFullSelect *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		DynamicRegistry* registry = DynamicRegistry::GetInstance();
		EEngineType engine = registry->GetEngineType(AFEngineKeys::ArrayFire);
		GPOS_ASSERT(registry->GetOperatorId(engine, AFOpKeys::CPhysicalGPUFullSelect) == pop->Eopid() ||
					EopPhysicalFilter == pop->Eopid());
          // wait this shouldn't be the case??? CHECK THIS
		return reinterpret_cast<CPhysicalGPUFullSelect *>(pop);
	}

};	// class CPhysicalGPUFullSelect

}  // namespace orcaextender

// EOF
