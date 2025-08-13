#pragma once

#include "gpos/base.h"
#include "gpoptextender/DynamicRegistry/DynamicRegistry.hpp"

#include "gpopt/xforms/CXformExploration.h"
#include "afnamestore.hpp"

namespace orcaextender
{
using namespace gpos;
using namespace gpopt;


class CXformGbSelect2SelectGather: public CXformExploration
{
private:
	// private copy ctor
	CXformGbSelect2SelectGather(const CXformGbSelect2SelectGather &);

protected:
	// check if the transformation is applicable
	BOOL FApplicable(CExpression *pexpr) const;

public:
	// ctor
	CXformGbSelect2SelectGather(CMemoryPool *mp);

	// ctor
	explicit CXformGbSelect2SelectGather(CExpression *pexprPattern);

	// dtor
	virtual ~CXformGbSelect2SelectGather()
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
		return AFTransformKeys::CXformGbSelect2SelectGather;
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformGbSelect2SelectGather

}  // namespace gpopt

// EOF
