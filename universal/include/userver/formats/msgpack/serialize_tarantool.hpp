#pragma once

/// @file userver/formats/msgpack/serialize_tarantool.hpp
/// @brief ADL Parse / Serialize hooks for Tarantool ext types.
///
/// Include this header to enable:
/// - `value.As<TntUuid>()`, `value.As<DatetimeTz>()`, etc. via ADL for generic
///   container parsers (e.g. `value.As<std::vector<TntUuid>>()`).
/// - `ValueBuilder(TntUuid)` etc. via the Serialize ADL hook.
///
/// The explicit `template<> Value::As<T>()` specialisations in value.hpp
/// already handle direct calls; this header adds the `Parse()` free functions
/// that the generic `formats::parse::common_containers` uses.

#include <userver/formats/msgpack/value.hpp>
#include <userver/formats/msgpack/value_builder.hpp>
#include <userver/formats/parse/to.hpp>
#include <userver/formats/serialize/to.hpp>
#include <userver/utils/datetime/date.hpp>

USERVER_NAMESPACE_BEGIN

namespace formats::msgpack {

// ---- Parse ADL hooks --------------------------------------------------------
// Found by ADL when T lives in formats::msgpack.

inline TntUuid Parse(const Value& v, formats::parse::To<TntUuid>) {
    return v.AsUuid();
}

inline utils::datetime::Date Parse(const Value& v,
                                   formats::parse::To<utils::datetime::Date>) {
    return v.AsDate();
}

inline DatetimeTz Parse(const Value& v, formats::parse::To<DatetimeTz>) {
    return v.AsDatetimeTz();
}

inline DatetimeWithoutTz Parse(const Value& v,
                               formats::parse::To<DatetimeWithoutTz>) {
    return v.AsDatetimeWithoutTz();
}

inline TimestampTz Parse(const Value& v, formats::parse::To<TimestampTz>) {
    return v.AsTimestampTz();
}

inline TimestampWithoutTz Parse(const Value& v,
                                formats::parse::To<TimestampWithoutTz>) {
    return v.AsTimestampWithoutTz();
}

inline TntInterval Parse(const Value& v, formats::parse::To<TntInterval>) {
    return v.AsInterval();
}

inline std::string Parse(const Value& v, formats::parse::To<std::string>) {
    return v.As<std::string>();
}

// ---- Serialize ADL hooks ----------------------------------------------------
// Found by ADL when T lives in formats::msgpack.

inline ValueBuilder Serialize(const TntUuid& uuid,
                              formats::serialize::To<ValueBuilder>) {
    return ValueBuilder{uuid};
}

inline ValueBuilder Serialize(utils::datetime::Date date,
                              formats::serialize::To<ValueBuilder>) {
    return ValueBuilder{date};
}

inline ValueBuilder Serialize(const DatetimeTz& dt,
                              formats::serialize::To<ValueBuilder>) {
    return ValueBuilder{dt};
}

inline ValueBuilder Serialize(DatetimeWithoutTz dt,
                              formats::serialize::To<ValueBuilder>) {
    return ValueBuilder{dt};
}

inline ValueBuilder Serialize(const TimestampTz& ts,
                              formats::serialize::To<ValueBuilder>) {
    return ValueBuilder{ts};
}

inline ValueBuilder Serialize(TimestampWithoutTz ts,
                              formats::serialize::To<ValueBuilder>) {
    return ValueBuilder{ts};
}

inline ValueBuilder Serialize(const TntInterval& iv,
                              formats::serialize::To<ValueBuilder>) {
    return ValueBuilder{iv};
}

}  // namespace formats::msgpack

USERVER_NAMESPACE_END
