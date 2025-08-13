#pragma once

#include "gpos/base.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalUnary.h"
#include "gpopt/operators/CLogicalSelect.h"
#include "gpoptextender/DynamicRegistry/DynamicRegistry.hpp"
#include "gpoptextender/GenericOps/CBaseLogicalOp.hpp"

#include "veloxnamestore.hpp"

namespace orcaextender
{
  using namespace gpopt;


class VeloxLogicalGather : public CBaseLogicalOp
{
private:
	// private copy ctor
	VeloxLogicalGather(const VeloxLogicalGather &);

public:
	// ctor
	explicit VeloxLogicalGather(CMemoryPool *mp);

	// dtor
	virtual ~VeloxLogicalGather();

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return DynamicRegistry::GetInstance()->GetOperatorId(DynamicRegistry::GetInstance()->GetEngineType(VeloxEngineKeys::Velox), VeloxOpKeys::CLogicalVeloxGather);
	}

	virtual const CHAR *
	SzId() const
	{
		return VeloxOpKeys::CLogicalVeloxGather;
	}

	// candidate set of xforms
	virtual CXformSet *PxfsCandidates(CMemoryPool *mp) const;


  // promise level for stat derivation
	virtual EStatPromise Esp(CExpressionHandle &exprhdl) const;


	BOOL FInputOrderSensitive() const {
		return false;
	}

	// conversion function
	static VeloxLogicalGather *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);

		return reinterpret_cast<VeloxLogicalGather *>(pop);
	}
};	// class VeloxLogicalGather

}  // namespace orcaextender
