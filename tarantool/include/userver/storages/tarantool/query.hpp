#pragma once

/// @file userver/storages/tarantool/query.hpp
/// @brief @copybrief storages::tarantool::Query

#include <cstdint>
#include <string>
#include <string_view>

#include <userver/formats/json/value.hpp>

#include <userver/storages/tarantool/options.hpp>

USERVER_NAMESPACE_BEGIN

namespace tracing {
class Span;
}

namespace storages::tarantool {

/// @brief Represents a Tarantool operation (CALL or CRUD).
///
/// Use the static factory methods to create queries.
class Query final {
 public:
  enum class Type {
    kCall,
    kSelect,
    kInsert,
    kReplace,
    kDelete,
    kUpdate,
    kUpsert,
  };

  static Query Call(std::string func_name, formats::json::Value args);
  static Query Select(std::string space, formats::json::Value key,
                      std::uint32_t limit = 0xFFFFFFFFu);
  static Query Insert(std::string space, formats::json::Value tuple);
  static Query Replace(std::string space, formats::json::Value tuple);
  static Query Delete(std::string space, formats::json::Value key);
  static Query Update(std::string space, formats::json::Value key,
                      formats::json::Value ops);
  static Query Upsert(std::string space, formats::json::Value tuple,
                      formats::json::Value ops);

  Type GetType() const noexcept { return type_; }
  const std::string& GetSpaceOrFunc() const noexcept { return space_or_func_; }
  const formats::json::Value& GetArgs() const noexcept { return args_; }
  const formats::json::Value& GetOps() const noexcept { return ops_; }
  std::uint32_t GetLimit() const noexcept { return limit_; }

  void FillSpanTags(tracing::Span&) const;

 private:
  Query(Type type, std::string space_or_func, formats::json::Value args,
        formats::json::Value ops = {}, std::uint32_t limit = 0xFFFFFFFFu);

  Type type_;
  std::string space_or_func_;
  formats::json::Value args_;
  formats::json::Value ops_;
  std::uint32_t limit_{0xFFFFFFFFu};
};

}  // namespace storages::tarantool

USERVER_NAMESPACE_END
