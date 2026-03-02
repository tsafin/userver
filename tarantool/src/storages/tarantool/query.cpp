#include <userver/storages/tarantool/query.hpp>

#include <userver/tracing/span.hpp>
#include <userver/tracing/tags.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool {

Query::Query(Type type, std::string space_or_func,
             formats::json::Value args, formats::json::Value ops,
             std::uint32_t limit)
    : type_{type},
      space_or_func_{std::move(space_or_func)},
      args_{std::move(args)},
      ops_{std::move(ops)},
      limit_{limit} {}

Query Query::Call(std::string func_name, formats::json::Value args) {
    return Query{Type::kCall, std::move(func_name), std::move(args)};
}

Query Query::Select(std::string space, formats::json::Value key,
                    std::uint32_t limit) {
    return Query{Type::kSelect, std::move(space), std::move(key), {}, limit};
}

Query Query::Insert(std::string space, formats::json::Value tuple) {
    return Query{Type::kInsert, std::move(space), std::move(tuple)};
}

Query Query::Replace(std::string space, formats::json::Value tuple) {
    return Query{Type::kReplace, std::move(space), std::move(tuple)};
}

Query Query::Delete(std::string space, formats::json::Value key) {
    return Query{Type::kDelete, std::move(space), std::move(key)};
}

Query Query::Update(std::string space, formats::json::Value key,
                    formats::json::Value ops) {
    return Query{Type::kUpdate, std::move(space), std::move(key),
                 std::move(ops)};
}

Query Query::Upsert(std::string space, formats::json::Value tuple,
                    formats::json::Value ops) {
    return Query{Type::kUpsert, std::move(space), std::move(tuple),
                 std::move(ops)};
}

void Query::FillSpanTags(tracing::Span& span) const {
    span.AddTag(tracing::kDatabaseStatement, space_or_func_);
}

}  // namespace storages::tarantool

USERVER_NAMESPACE_END
