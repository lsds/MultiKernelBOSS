#pragma once


namespace VeloxOpKeys { 
  inline constexpr auto CPhysicalVeloxGather = "CVeloxGather";
  inline constexpr auto CLogicalVeloxGather = "VeloxLogicalGather";
  inline constexpr auto VeloxPhysicalHashJoin = "VeloxPhysicalHashJoin";
}

namespace VeloxTransformKeys {
  inline constexpr auto CXformGbVeloxLogicalGather2VeloxPhysicalGather = "CXformGbVeloxLogicalGather2VeloxPhysicalGather";
  inline constexpr auto CXformGbJoin2VeloxPhysicalJoin = "CXformGbJoin2VeloxPhysicalJoin";
}

namespace VeloxEngineKeys {
    inline constexpr auto Velox = "Velox";
}