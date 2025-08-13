#pragma once

#include "gpos/base.h"
#include "gpoptextender/DynamicRegistry/DynamicRegistry.hpp"

#include "gpopt/xforms/CXformExploration.h"
#include "afnamestore.hpp"

namespace orcaextender
{
using namespace gpos;
using namespace gpopt;


class CXformGbLogicalSelectGather2Select: public CXformExploration
{
private:
	// private copy ctor
	CXformGbLogicalSelectGather2Select(const CXformGbLogicalSelectGather2Select &);

protected:
	// check if the transformation is applicable
	BOOL FApplicable(CExpression *pexpr) const;

public:
	// ctor
	CXformGbLogicalSelectGather2Select(CMemoryPool *mp);

	// ctor
	explicit CXformGbLogicalSelectGather2Select(CExpression *pexprPattern);

	// dtor
	virtual ~CXformGbLogicalSelectGather2Select()
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
		return AFTransformKeys::CXformGbLogicalSelectGather2Select;
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformGbLogicalSelectGather2Select

}  // namespace gpopt

// EOF
