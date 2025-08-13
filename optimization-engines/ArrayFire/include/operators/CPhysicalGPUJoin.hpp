#pragma once

#include "gpos/base.h"

#include "gpopt/operators/CPhysicalInnerHashJoin.h"
#include "gpoptextender/DynamicRegistry/DynamicRegistry.hpp"
#include "afnamestore.hpp"

namespace orcaextender
{
using namespace gpopt;
// fwd declaration
class CDistributionSpec;


class CPhysicalGPUJoin : public CPhysicalInnerHashJoin
{
private:
	// private copy ctor
	CPhysicalGPUJoin(const CPhysicalGPUJoin &);

public:
	// ctor
	CPhysicalGPUJoin(CMemoryPool *mp, CExpressionArray *pdrgpexprOuterKeys,
					  CExpressionArray *pdrgpexprInnerKeys);

	// dtor
	virtual ~CPhysicalGPUJoin();

  virtual EOperatorId
Eopid() const;

	// return a string for operator name
	virtual const CHAR *
	SzId() const
	{
		return AFOpKeys::CPhysicalGPUJoin;
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

	virtual gpopt::CDistributionSpec *PdsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const;
	virtual gpopt::CDistributionSpec *PdsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, gpopt::CDistributionSpec *pdsRequired, ULONG child_index, CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) const;



	// conversion function
	static CPhysicalGPUJoin *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		DynamicRegistry* registry = DynamicRegistry::GetInstance();
		EEngineType engine = registry->GetEngineType("ArrayFire");
		GPOS_ASSERT(registry->GetOperatorId(engine, "CPhysicalGPUJoin") == pop->Eopid() ||
					EopPhysicalInnerHashJoin == pop->Eopid());
          // wait this shouldn't be the case??? CHECK THIS
		return reinterpret_cast<CPhysicalGPUJoin *>(pop);
	}

};	// class CPhysicalGPUJoin

}  // namespace orcaextender

// EOF
