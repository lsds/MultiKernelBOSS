#pragma once

#include "gpos/base.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalUnary.h"
#include "gpopt/operators/CLogicalSelect.h"
#include "gpoptextender/DynamicRegistry/DynamicRegistry.hpp"
#include "afnamestore.hpp"

namespace orcaextender
{
  using namespace gpopt;


class CLogicalPartialSelect : public CLogicalSelect
{
private:
	// private copy ctor
	CLogicalPartialSelect(const CLogicalPartialSelect &);

public:
	// ctor
	explicit CLogicalPartialSelect(CMemoryPool *mp);

	// ctor
	CLogicalPartialSelect(CMemoryPool *mp, CTableDescriptor *ptabdesc);

	// dtor
	virtual ~CLogicalPartialSelect();

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return DynamicRegistry::GetInstance()->GetOperatorId(DynamicRegistry::GetInstance()->GetEngineType(AFEngineKeys::ArrayFire), AFOpKeys::CLogicalPartialSelect);
	}

	virtual const CHAR *
	SzId() const
	{
		return AFOpKeys::CLogicalPartialSelect;
	}

	// candidate set of xforms
	virtual CXformSet *PxfsCandidates(CMemoryPool *) const;

	// conversion function
	static CLogicalSelect *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);

		return reinterpret_cast<CLogicalPartialSelect *>(pop);
	}

};	// class CLogicalSelect

}  // namespace orcaextender
