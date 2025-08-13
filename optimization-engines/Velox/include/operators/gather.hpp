#pragma once

#include "gpos/base.h"

#include "gpoptextender/GenericOps/CBasePhysicalUnaryOp.hpp"
#include "gpoptextender/EngineProperty/CEngineSpec.hpp"
#include "gpoptextender/EngineProperty/CEnfdEngine.hpp"
#include "gpoptextender/DynamicRegistry/DynamicRegistry.hpp"
#include "gpoptextender/CostModel/BOSSCostModel.hpp"
#include "veloxnamestore.hpp"
namespace orcaextender {
using namespace gpos;
using namespace gpmd;
using namespace gpopt;

// Operator to handle engine transitions when needed
class CVeloxGather : public CBasePhysicalUnaryOp
{
public:
    // Ctor
    CVeloxGather(CMemoryPool *mp)
        : CBasePhysicalUnaryOp(mp)
    {
    }

	// dtor
	virtual ~CVeloxGather(){
	};

    // ident accessors
	virtual EOperatorId
	Eopid() const {
		return DynamicRegistry::GetInstance()->GetOperatorId(DynamicRegistry::GetInstance()->GetEngineType("Velox"), "CVeloxGather");
	}

    	// return a string for operator name
	virtual const CHAR *
	SzId() const
	{
		return VeloxOpKeys::CPhysicalVeloxGather;
	}

  	// match function
	virtual BOOL Matches(COperator *pop) const {
		return pop->Eopid() == Eopid();
	}


	BOOL FInputOrderSensitive() const {
		return false;
	}

	// conversion function
	static CVeloxGather *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopPhysicalEngineTransform == pop->Eopid());

		return dynamic_cast<CVeloxGather *>(pop);
	}

	virtual CEngineSpec* PesDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const {
    DynamicRegistry* registry = DynamicRegistry::GetInstance();
		EEngineType engine = registry->GetEngineType(VeloxEngineKeys::Velox);
		return GPOS_NEW(mp) CEngineSpec(engine);
  }

	virtual CEngineSpec* PesRequired(CMemoryPool *mp, CExpressionHandle &exprhdl, CEngineSpec *pesRequired, ULONG child_index, CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq) const {
    DynamicRegistry* registry = DynamicRegistry::GetInstance();
		EEngineType engine = registry->GetEngineType(VeloxEngineKeys::Velox);
		return GPOS_NEW(mp) CEngineSpec(engine);
  }  

};

}