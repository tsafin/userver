# Roadmap: путь к 1M ops/sec

**Дата:** 2026-10-23 · **От:** `c1c53c7a1` (все код-эксперименты завершены,
решение в `docs/cpu-experiments.md`)

Текущее состояние: proxy 151.8k ops/s (лучший замер, 8 мастеров,
wal_mode=none, pipelined-клиент), +34% к Lua-роутеру. Все
внутрифреймворковые CPU-рычаги выжаты и задокументированы.

## P1. Гигиена измерений и репозиториев (час)
- [x] Зафиксировать план (этот документ)
- [ ] `run_benchmarks.sh`: secdist-шаблон на 8 RS (+флаги --extra-masters),
      чтобы sweep-протокол совпадал с текущим стендом
- [ ] Закоммитить правки кластера в `~/src/vshard` (storage_3..8, localcfg,
      router, Makefile, wal_mode=none)
- [ ] Убрать worktree'ы экспериментов (ветки `exp/e1..e3` остаются)

## P2. Доверие к коду (параллельно с P3)
- [ ] Функциональные pytest-тесты (`ctest -R testsuite-userver-tarantool-vshard`)
      против текущего прокси на 8-RS стенде
- [ ] ASAN-сборка + прогон loadgen (race в конкурентной обработке Phase 1c/2)
- [ ] TSAN-сборка + короткий прогон (если переживает TSAN-замедление)

## P3. Прототип thread-per-core + io_uring (главный софтверный путь к 1M)
Шаг 0 (завершён 2026-10-23): **замер потолка подхода заблокирован
платформой** — io_uring в WSL2-ядре (6.18 microsoft-standard) отдаёт
completions лениво/сломанно: poll-completion через ~15M итераций горячего
цикла, io_uring_submit возвращает 0 при заполненном SQ. Код probe сохранён
в ветке `exp/p3-iouring-probe` (bench/EXPERIMENTS-io_uring.md). Бонус-факт:
direct storage через лёгкий клиент = **243k ops/s** на одном мастере
(wal none) — Lua-CALL-путь Tarantool стоит ~2.4x.
- [ ] ПОВТОРИТЬ на bare metal: собрать `probe` (gcc -O2 probe.c -luring),
      замерить релей против userver-прокси тем же loadgen
      (--storage-direct); если CPU/op ≥ 2x лучше — порт протокольного слоя
      (`ParseIprotoRequest`, `CallRouteInfo`, `ParseCallBody` уже изолированы
      в iproto_frames.hpp / iproto_server.cpp)
Шаги 1+ (отдельный проект, вне userver): полномасштабный standalone-прокси
(роутинг бакетов, MOVED-логика, discovery) — переиспользует алгоритмы
vshard_proxy.cpp.

## P4. Железо (внешняя зависимость)
- 10–12 мастеров на bare metal ≈ 80–130k каждый ⇒ ~1M текущим кодом
- тюнинг: busy_poll, IRQ/worker affinity, isolcpus (10–20%)

## Правила измерений (из docs/cpu-experiments.md)
Только same-context interleaved min-of-N A/B; один прокси на порту
(проверять `ss -tlnp`); loadgen `userver-tarantool-vshard-loadgen`;
исторические sweep — только на простаивающем боксе.
