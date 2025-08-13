#include "gpoptextender/EngineInterface/Engine.hpp"
#include "gpoptextender/DynamicRegistry/DynamicOperatorArgs.hpp"
#include "gpoptextender/CostModel/BOSSCostModel.hpp"
#include "xforms/CXformGbVeloxLogicalGather2VeloxPhysicalGather.hpp"
#include "gpos/base.h"
#include "veloxnamestore.hpp"


class Velox : public orcaextender::Engine {
    public:
        Velox(std::string engineName);
        ~Velox();
        void ConfigureCostModel() override;
        void RegisterCostModelParams() override;
        void RegisterOperators() override;
        void RegisterTransforms() override;
        void RegisterTranslators() override;
        void RemoveTransforms() override;
        void RegisterEngineTransforms() override;
        void RegisterMetadataFilePath() override;
};

extern "C" orcaextender::Engine *CreateEngine();