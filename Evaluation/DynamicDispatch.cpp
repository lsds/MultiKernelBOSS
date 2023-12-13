operators["Greater"_] = [](Expression&& arg0, Expression&& arg1) {
  return std::visit(
      []<template FirstArgument>(FirstArgument&& typedArg0) -> Expression {
        if constexpr(std::is_arithmetic_v<FirstArgument>) {
          return std::visit(
              [&arg1]<template SecondArgument>(SecondArgument&& typedArg1) -> Expression {
                if constexpr(std::is_arithmetic_v<SecondArgument>) {
                  // compare two numerical values (returns a numerical value)
                  return typedArg0 > typedArg1;
                } else if constexpr(std::is_same_v<SecondArgument, Symbol>) {
                  // compare a numerical value and a Symbol (returns a lambda function)
                  return [&typedArg0, nextSpanLambdaFunction = createLambdaArgument(typedArg1)](
                             Columns& columns) mutable -> std::optional<SpanArgument> {
                    auto nextSpan = nextSpanLambdaFunction(columns);
                    if(!nextSpan) {
                      return {}; // that was the last span
                    }
                    return std::visit(
                        [&typedArg0]<template ElementType>(
                            Span<ElementType>&& typedSpan) -> Expression {
                          if constexpr(std::is_arithmetic_v<ElementType>) {
                            // calling ArrayFire's greater operator and convert to span
                            auto afResult =
                                typedArg0 >
                                ArrayFireWrapper{std::get<Span<ElementType>>(std::move(span))};
                            return static_cast<Span<ElementType>>(std::move(afResult));
                          } else {
                            // unsupported span type, return unevaluated
                            return "Greater"_(typedArg0, typedArg1);
                          }
                        },
                        std::move(nextSpan));
                  };
                } else {
                  // unsupported type for the 2nd argument
                  // return unevaluated
                  return "Greater"_(typedArg0, typedArg1);
                }
              },
              arg1);
        } else if constexpr(std::is_same_v<FirstArgument, Symbol>) {
          return std::visit(
              []<template SecondArgument>(auto const& typedArg1) -> Expression {
                if constexpr(std::is_arithmetic_v<SecondArgument>) {
                  // compare a numerical value and a Symbol (returns a lambda function)
                  return [&typedArg0, nextSpanFunction = createLambdaArgument(typedArg1),
                          value = get<1>(e.getStaticArguments())](
                             Columns& columns) mutable -> std::optional<SpanArgument> {
                    auto nextSpan = nextSpanFunction(columns);
                    if(!nextSpan) {
                      return {}; // that was the last span
                    }
                    return std::visit(
                        [&typedArg0, &typedArg1]<template ElementType>(
                            Span<ElementType>&& typedSpan) -> Expression {
                          if constexpr(std::is_arithmetic_v<ElementType>) {
                            // calling ArrayFire's greater operator and convert to span
                            auto afResult = ArrayFireWrapper{std::get<Span<ElementType>>(
                                                std::move(typedSpan))} >
                                typedArg1;
                            return static_cast<Span<FirstArgument>>(std::move(afResult));
                          } else {
                            // unsupported span type, return unevaluated
                            return "Greater"_(typedArg0, typedArg1);
                          }
                        },
                        std::move(nextSpan));
                  };
                } else {
                  // unsupported type for the 2nd argument, return unevaluated
                  return "Greater"_(typedArg0, typedArg1);
                }
              },
              arg1);
        } else {
          // unsupported type for the 1st argument
          // return unevaluated
          return "Greater"_(typedArg0, arg1);
        }
      },
      arg0);
  return ComplexExpression{};
};