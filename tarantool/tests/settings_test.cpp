#include <userver/utest/utest.hpp>

#include <userver/formats/json/serialize.hpp>

#include <storages/tarantool/impl/settings.hpp>

USERVER_NAMESPACE_BEGIN

UTEST(TarantoolSettings, ParseSecdistBasic) {
    const auto json = formats::json::FromString(R"({
      "tarantool_settings": {
        "mydb": {
          "hosts": ["127.0.0.1"],
          "port": 3301,
          "user": "guest",
          "password": ""
        }
      }
    })");
    storages::tarantool::impl::TarantoolSettingsMulti multi{json};
    const auto& s = multi.Get("mydb");
    ASSERT_EQ(s.endpoints.size(), 1u);
    EXPECT_EQ(s.endpoints[0].host, "127.0.0.1");
    EXPECT_EQ(s.endpoints[0].port, 3301);
    EXPECT_EQ(s.auth.user, "guest");
    EXPECT_EQ(s.auth.password, "");
}

UTEST(TarantoolSettings, ParseSecdistMultipleHosts) {
    const auto json = formats::json::FromString(R"({
      "tarantool_settings": {
        "mydb": {
          "hosts": ["host1", "host2", "host3"],
          "port": 3302,
          "user": "admin",
          "password": "secret"
        }
      }
    })");
    storages::tarantool::impl::TarantoolSettingsMulti multi{json};
    const auto& s = multi.Get("mydb");
    ASSERT_EQ(s.endpoints.size(), 3u);
    EXPECT_EQ(s.endpoints[0].host, "host1");
    EXPECT_EQ(s.endpoints[1].host, "host2");
    EXPECT_EQ(s.endpoints[2].host, "host3");
    EXPECT_EQ(s.endpoints[0].port, 3302);
    EXPECT_EQ(s.auth.user, "admin");
    EXPECT_EQ(s.auth.password, "secret");
}

UTEST(TarantoolSettings, MissingAliasThrows) {
    const auto json = formats::json::FromString(R"({
      "tarantool_settings": {}
    })");
    storages::tarantool::impl::TarantoolSettingsMulti multi{json};
    EXPECT_THROW(multi.Get("nonexistent"), std::runtime_error);
}

USERVER_NAMESPACE_END
