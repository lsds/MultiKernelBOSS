#include "operators/CLogicalPartialSelect.hpp"

namespace orcaextender
{
  using namespace gpopt;

  CLogicalPartialSelect::CLogicalPartialSelect(CMemoryPool *mp)
    : CLogicalSelect(mp)
  {
  }

  CLogicalPartialSelect::CLogicalPartialSelect(CMemoryPool *mp, CTableDescriptor *ptabdesc)
    : CLogicalSelect(mp, ptabdesc)
  {
  }

  CLogicalPartialSelect::~CLogicalPartialSelect()
  {
  }

  CXformSet *CLogicalPartialSelect::PxfsCandidates(CMemoryPool *mp) const
  {
    CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);
    (void) xform_set->ExchangeSet(DynamicRegistry::GetInstance()->GetTransformId(AFTransformKeys::CXformGbLogicalPartialSelect2PhysicalGPUPartialSelect));
    (void) xform_set->ExchangeSet(DynamicRegistry::GetInstance()->GetTransformId(AFTransformKeys::CXformGbLogicalSelectGather2Select));
    DynamicRegistry *registry = DynamicRegistry::GetInstance();
    registry->AddTransformsToXFormSet(Eopid(), xform_set);
    return xform_set;
  }

}
