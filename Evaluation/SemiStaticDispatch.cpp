operators["Greater"_] =
    []<NumericOrSymbol FirstArgument, NumericOrSymbol SecondArgument>(
        ComplexExpressionWithStaticArguments<FirstArgument, SecondArgument>&& input) -> Expression {
  if constexpr(std::is_arithmetic_v<FirstArgument> && std::is_arithmetic_v<SecondArgument>) {
    // compare two numerical values (returns a numerical value)
    return get<0>(e.getStaticArguments()) > get<1>(e.getStaticArguments());
  } else {
    // compare a numerical value and a Symbol (returns a lambda function)
    return [getValueOrSpan = createLambda(get<0>(e.getStaticArguments())),
            getValueOrSpan = createLambda(get<1>(e.getStaticArguments()))](
               Columns& columns) mutable -> std::optional<SpanArgument> {
      auto arg0 = getValueOrSpan(columns);
      auto arg1 = getValueOrSpan(columns);
      if(!arg0 || !arg1) {
        return {}; // that was the last span
      }
      // calling ArrayFire's greater operator and convert to span
      auto afResult =
          getArrayFireWrapperOrValue(std::move(arg0)) > getArrayFireWrapperOrValue(std::move(arg1));
      return static_cast<SpanArgument>(std::move(afResult));
    };
  }
};