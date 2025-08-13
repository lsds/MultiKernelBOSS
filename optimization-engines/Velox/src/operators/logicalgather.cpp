#include "operators/logicalgather.hpp"

namespace orcaextender {
  using namespace gpopt;
  
  VeloxLogicalGather::VeloxLogicalGather(CMemoryPool *mp) : CBaseLogicalOp(mp) {
  }

  VeloxLogicalGather::~VeloxLogicalGather() {
  }

  CXformSet *VeloxLogicalGather::PxfsCandidates(CMemoryPool *mp) const {
    CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);
    (void) xform_set->ExchangeSet(DynamicRegistry::GetInstance()->GetTransformId(VeloxTransformKeys::CXformGbVeloxLogicalGather2VeloxPhysicalGather));
    
    DynamicRegistry *registry = DynamicRegistry::GetInstance();
    registry->AddTransformsToXFormSet(Eopid(), xform_set);
    
    return xform_set;
  }

  CLogical::EStatPromise VeloxLogicalGather::Esp(CExpressionHandle &exprhdl) const {
    return EStatPromise::EspLow;
  }
}
