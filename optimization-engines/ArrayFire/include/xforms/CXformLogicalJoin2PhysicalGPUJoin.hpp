#pragma once

#include "gpos/base.h"
#include "gpoptextender/DynamicRegistry/DynamicRegistry.hpp"

#include "gpopt/xforms/CXformImplementation.h"
#include "afnamestore.hpp"

namespace orcaextender
{
using namespace gpos;
using namespace gpopt;


class CXformLogicalJoin2PhysicalGPUJoin: public CXformImplementation
{
private:
	// private copy ctor
	CXformLogicalJoin2PhysicalGPUJoin(const CXformLogicalJoin2PhysicalGPUJoin &);

protected:
	// check if the transformation is applicable
	BOOL FApplicable(CExpression *pexpr) const;

public:
	// ctor
	CXformLogicalJoin2PhysicalGPUJoin(CMemoryPool *mp);

	// ctor
	explicit CXformLogicalJoin2PhysicalGPUJoin(CExpression *pexprPattern);

	// dtor
	virtual ~CXformLogicalJoin2PhysicalGPUJoin()
	{
	}

	// ident accessors
	virtual EXformId
	Exfid() const
	{
		return DynamicRegistry::GetInstance()->GetTransformId(SzId());
	}

	// return a string for xform name
	virtual const CHAR *
	SzId() const
	{
		return AFTransformKeys::CXformLogicalJoin2PhysicalGPUJoin;
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformLogicalJoin2PhysicalGPUJoin

}  // namespace gpopt

// EOF
