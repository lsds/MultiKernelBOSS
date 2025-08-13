#pragma once

#include "gpos/base.h"

#include "gpopt/operators/CPhysicalComputeScalar.h"
#include "gpoptextender/DynamicRegistry/DynamicRegistry.hpp"
#include "afnamestore.hpp"
namespace orcaextender
{
using namespace gpopt;
// fwd declaration
class CDistributionSpec;


class CPhysicalGPUProject : public CPhysicalComputeScalar
{
private:
	// private copy ctor
	CPhysicalGPUProject(const CPhysicalGPUProject &);

public:
	// ctor
	CPhysicalGPUProject(CMemoryPool *mp);

	// dtor
	virtual ~CPhysicalGPUProject();

  virtual EOperatorId
Eopid() const;

	// return a string for operator name
	virtual const CHAR *
	SzId() const
	{
		return AFOpKeys::CPhysicalGPUProject;
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
	static CPhysicalGPUProject *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		DynamicRegistry* registry = DynamicRegistry::GetInstance();
		EEngineType engine = registry->GetEngineType("ArrayFire");
		GPOS_ASSERT(registry->GetOperatorId(engine, "CPhysicalGPUProject") == pop->Eopid() ||
					EopPhysicalComputeScalar == pop->Eopid());
          // wait this shouldn't be the case??? CHECK THIS
		return reinterpret_cast<CPhysicalGPUProject *>(pop);
	}

};	// class CPhysicalGPUProject

}  // namespace orcaextender

// EOF
