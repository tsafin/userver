#pragma once

#include <string>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::impl::scopes {

inline const std::string kConnect = "tarantool_connect";
inline const std::string kCall    = "tarantool_call";
inline const std::string kSelect  = "tarantool_select";
inline const std::string kInsert  = "tarantool_insert";
inline const std::string kReplace = "tarantool_replace";
inline const std::string kDelete  = "tarantool_delete";
inline const std::string kUpdate  = "tarantool_update";
inline const std::string kUpsert  = "tarantool_upsert";

}  // namespace storages::tarantool::impl::scopes

USERVER_NAMESPACE_END
