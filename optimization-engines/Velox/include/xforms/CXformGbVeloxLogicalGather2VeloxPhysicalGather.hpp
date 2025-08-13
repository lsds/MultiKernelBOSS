#pragma once

#include "gpos/base.h"
#include "gpoptextender/DynamicRegistry/DynamicRegistry.hpp"

#include "gpopt/xforms/CXformImplementation.h"
#include "veloxnamestore.hpp"

namespace orcaextender
{
using namespace gpos;
using namespace gpopt;


class CXformGbLogicalGather2VeloxPhysicalGather: public CXformImplementation
{
private:
	// private copy ctor
	CXformGbLogicalGather2VeloxPhysicalGather(const CXformGbLogicalGather2VeloxPhysicalGather &);

protected:
	// check if the transformation is applicable
	BOOL FApplicable(CExpression *pexpr) const;

public:
	// ctor
	CXformGbLogicalGather2VeloxPhysicalGather(CMemoryPool *mp);

	// ctor
	explicit CXformGbLogicalGather2VeloxPhysicalGather(CExpression *pexprPattern);

	// dtor
	virtual ~CXformGbLogicalGather2VeloxPhysicalGather()
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
		return VeloxTransformKeys::CXformGbVeloxLogicalGather2VeloxPhysicalGather;
	}

	// compute xform promise for a given expression handle
	virtual EXformPromise Exfp(CExpressionHandle &exprhdl) const;

	// actual transform
	void Transform(CXformContext *pxfctxt, CXformResult *pxfres,
				   CExpression *pexpr) const;

};	// class CXformGbLogicalGather2VeloxPhysicalGather

}  // namespace orcaextender

// EOF
