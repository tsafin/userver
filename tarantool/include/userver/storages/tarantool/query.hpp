#pragma once

/// @file userver/storages/tarantool/query.hpp
/// @brief @copybrief storages::tarantool::Query

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include <userver/formats/msgpack/value_builder.hpp>

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

  static Query Call(std::string func_name, formats::msgpack::ValueBuilder args);
  static Query Select(std::string space, formats::msgpack::ValueBuilder key,
                      std::uint32_t limit = 0xFFFFFFFFu);
  static Query Insert(std::string space, formats::msgpack::ValueBuilder tuple);
  static Query Replace(std::string space, formats::msgpack::ValueBuilder tuple);
  static Query Delete(std::string space, formats::msgpack::ValueBuilder key);
  static Query Update(std::string space, formats::msgpack::ValueBuilder key,
                      formats::msgpack::ValueBuilder ops);
  static Query Upsert(std::string space, formats::msgpack::ValueBuilder tuple,
                      formats::msgpack::ValueBuilder ops);

  /// @brief Create a query with pre-encoded msgpack bytes, bypassing
  ///        ValueBuilder entirely.  Use this from the typed mpp API
  ///        (`storages::tarantool::typed`) to avoid intermediate allocations.
  static Query WithRawArgs(Type type, std::string space,
                           std::vector<uint8_t> args_bytes,
                           std::uint32_t limit = 0xFFFFFFFFu);

  Type GetType() const noexcept { return type_; }
  const std::string& GetSpaceOrFunc() const noexcept { return space_or_func_; }
  const std::vector<uint8_t>& GetArgBytes() const noexcept { return args_bytes_; }
  const std::vector<uint8_t>& GetOpsBytes() const noexcept { return ops_bytes_; }
  std::uint32_t GetLimit() const noexcept { return limit_; }

  void FillSpanTags(tracing::Span&) const;

 private:
  Query(Type type, std::string space_or_func, formats::msgpack::ValueBuilder args,
        formats::msgpack::ValueBuilder ops = {}, std::uint32_t limit = 0xFFFFFFFFu);

  // Raw-bytes ctor: args_bytes already serialised (e.g. via mpp::encode).
  Query(Type type, std::string space_or_func,
        std::vector<uint8_t> args_bytes, std::uint32_t limit);

  Type type_;
  std::string space_or_func_;
  std::vector<uint8_t> args_bytes_;
  std::vector<uint8_t> ops_bytes_;
  std::uint32_t limit_{0xFFFFFFFFu};
};

}  // namespace storages::tarantool

USERVER_NAMESPACE_END
