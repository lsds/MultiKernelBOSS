#include "gpoptextender/EngineInterface/Engine.hpp"
#include "gpoptextender/DynamicRegistry/DynamicOperatorArgs.hpp"
#include "gpoptextender/CostModel/BOSSCostModel.hpp"
#include "costmodel/CGPUCostModelParams.hpp"
#include "CExpressionToBOSS.hpp"
#include "BOSSToCExpression.hpp"
#include "xforms/CXformGbSelect2SelectGather.hpp"
#include "xforms/CXformGbLogicalPartialSelect2PhysicalGPUPartialSelect.hpp"
#include "xforms/CXformGbLogicalSelectGather2Select.hpp"
#include "xforms/CXformLogicalJoin2PhysicalGPUJoin.hpp"
#include "xforms/CXformGbPostFilterLeft.hpp"
#include "xforms/CXformGbPostFilterRight.hpp"
#include "xforms/CXformGbFSelectConjunct2NestedFSelect.hpp"
#include "xforms/CXformGbSwapGPUSelect.hpp"
#include "xforms/CXformGbLogicalSelect2PhysicalGPUSelect.hpp"
#include "operators/CPhysicalGPUJoin.hpp"
#include "operators/CPhysicalGPUPartialSelect.hpp"
#include "operators/CPhysicalGPUProject.hpp"
#include "gpos/base.h"
#include "afnamestore.hpp"


class ArrayFire : public orcaextender::Engine {
    public:
        ArrayFire(std::string engineName);
        ~ArrayFire();
        void RegisterCostModelParams() override;
        void RegisterOperators() override;
        void RegisterTransforms() override;
        void RegisterTranslators() override;
        void RemoveTransforms() override;
        void RegisterEngineTransforms() override;
        void RegisterMetadataFilePath() override;
};

extern "C" orcaextender::Engine *CreateEngine();