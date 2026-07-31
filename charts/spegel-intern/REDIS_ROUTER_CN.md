# Spegel Redis Router 中文文档

英文版本：[REDIS_ROUTER.md](REDIS_ROUTER.md)

## 概述

Spegel 支持使用 Redis 作为默认 P2P（libp2p）路由器之外的另一种路由后端。Redis router 通过 Redis 服务集中完成节点发现和内容路由。

## 功能特性

- **集中式发现**：所有节点通过 Redis 注册并发现其他节点
- **基于 TTL 的过期**：节点广告会在可配置的 TTL 后自动过期
- **有界查询**：当只需要固定数量的 peer 时，查询只读取有限数量的 Redis sorted-set 成员
- **批量广告**：大量广告 key 会拆分为固定大小的 Redis pipeline 批次
- **带抖动的刷新**：Redis 重新广告的时间间隔可以加入随机抖动，减少同步写入尖峰
- **连接池控制**：可以配置 Redis 连接池大小和命令超时，适用于大规模集群
- **Peer 随机化**：镜像请求前随机化查询候选 peer，减少热点 peer 偏载
- **Redis 拓扑选项**：支持单实例、独立实例分片、Redis Cluster client mode 和 Sentinel 故障转移
- **ACL 和 TLS 支持**：支持 Redis 用户名、Sentinel 凭据、TLS、CA bundle 和客户端证书
- **Redis Router 指标**：导出 Redis 命令数、清理数量、查询候选数和连接池统计
- **部署简单**：不需要 P2P bootstrap 或 NAT 穿透
- **Debug Web 支持**：与 Spegel debug web 界面完全兼容

## 配置

### 命令行选项

```bash
spegel registry \
  --router-kind=redis \
  --redis-addr=redis.example.com:6379 \
  --redis-username=default \
  --redis-password=yourpassword \
  --redis-key-prefix=spegel \
  --redis-advertise-ttl=15m \
  --redis-advertise-ip=192.168.1.10 \
  --redis-advertise-batch-size=1000 \
  --redis-expired-cleanup-interval=100 \
  --redis-readvertise-jitter=45s \
  --redis-tls-enabled=true \
  --redis-tls-ca-file=/etc/redis-tls/ca.crt \
  --redis-pool-size=8 \
  --redis-min-idle-conns=1 \
  --redis-dial-timeout=1s \
  --redis-read-timeout=20ms \
  --redis-write-timeout=20ms
```

选择一种 Redis 拓扑：

```bash
# 多个非 Cluster Redis 实例之间进行独立分片。
spegel registry --router-kind=redis \
  --redis-addrs=redis-a.example.com:6379 \
  --redis-addrs=redis-b.example.com:6379

# Redis Cluster client mode。
spegel registry --router-kind=redis \
  --redis-addrs=redis-cluster-a.example.com:6379 \
  --redis-addrs=redis-cluster-b.example.com:6379 \
  --redis-cluster-enabled=true

# Redis Sentinel 故障转移。
spegel registry --router-kind=redis \
  --redis-sentinel-addrs=sentinel-a.example.com:26379 \
  --redis-sentinel-addrs=sentinel-b.example.com:26379 \
  --redis-sentinel-master-name=mymaster
```

### 环境变量

```bash
export ROUTER_KIND=redis
export REDIS_ADDR=redis.example.com:6379
export REDIS_ADDRS=redis-a.example.com:6379,redis-b.example.com:6379
export REDIS_USERNAME=default
export REDIS_PASSWORD=yourpassword
export REDIS_KEY_PREFIX=spegel
export REDIS_ADVERTISE_TTL=15m
export REDIS_ADVERTISE_IP=192.168.1.10
export REDIS_ADVERTISE_BATCH_SIZE=1000
export REDIS_EXPIRED_CLEANUP_INTERVAL=100
export REDIS_READVERTISE_JITTER=45s
export REDIS_SENTINEL_ADDRS=sentinel-a.example.com:26379,sentinel-b.example.com:26379
export REDIS_SENTINEL_MASTER_NAME=mymaster
export REDIS_SENTINEL_USERNAME=sentinel-user
export REDIS_SENTINEL_PASSWORD=sentinel-password
export REDIS_TLS_ENABLED=true
export REDIS_TLS_CA_FILE=/etc/redis-tls/ca.crt
export REDIS_POOL_SIZE=8
export REDIS_MIN_IDLE_CONNS=1
export REDIS_DIAL_TIMEOUT=1s
export REDIS_READ_TIMEOUT=20ms
export REDIS_WRITE_TIMEOUT=20ms
```

### 参数说明

- `--router-kind`：路由器类型（`p2p` 或 `redis`），默认值为 `p2p`
- `--redis-addr`：Redis 服务地址。Redis 模式必须配置 `--redis-addr`、`--redis-addrs` 或 `--redis-sentinel-addrs` 之一
- `--redis-addrs`：Redis 服务地址列表，优先级高于 `--redis-addr`。多个独立实例会按内容 key 分片，除非设置 `--redis-cluster-enabled=true`
- `--redis-username`：Redis ACL 用户名，可选
- `--redis-password`：Redis 认证密码，可选
- `--redis-key-prefix`：Redis key 命名空间前缀，默认值为 `spegel`
- `--redis-advertise-ttl`：peer 广告 TTL，默认值为 `15m`
- `--redis-advertise-ip`：向 Redis 广告、供 registry peer 通信使用的 IP 地址
- `--redis-advertise-batch-size`：每个 Redis 广告 pipeline 批次的最大 route key 数，默认值为 `1000`
- `--redis-expired-cleanup-interval`：每 N 次清理机会清理一次过期 sorted-set 成员，默认值为 `100`；设置为 `0` 禁用机会式清理
- `--redis-readvertise-jitter`：Redis 重新广告间隔的随机抖动，默认值为 `0`，表示使用 `redis-advertise-ttl / 2` 的 10%
- `--redis-cluster-enabled`：对 `--redis-addrs` 使用 go-redis Cluster client mode，默认值为 `false`
- `--redis-sentinel-addrs`：Redis Sentinel 地址列表；设置后必须配置 `--redis-sentinel-master-name`
- `--redis-sentinel-master-name`：Redis Sentinel master 名称
- `--redis-sentinel-username`：Redis Sentinel ACL 用户名，可选
- `--redis-sentinel-password`：Redis Sentinel 密码，可选
- `--redis-tls-enabled`：启用 Redis TLS
- `--redis-tls-ca-file`：Redis TLS 校验使用的 CA bundle 文件
- `--redis-tls-cert-file`：Redis mTLS 客户端证书文件
- `--redis-tls-key-file`：Redis mTLS 客户端私钥文件
- `--redis-tls-insecure-skip-verify`：跳过 Redis TLS 证书校验
- `--redis-pool-size`：每个 Spegel 进程的最大 Redis 连接数，默认值为 `0`，使用 go-redis 默认值
- `--redis-min-idle-conns`：每个 Spegel 进程的最小 Redis 空闲连接数，默认值为 `0`
- `--redis-dial-timeout`：Redis 建连超时，默认值为 `0`，使用 go-redis 默认值
- `--redis-read-timeout`：Redis 读超时，默认值为 `0`，使用 go-redis 默认值
- `--redis-write-timeout`：Redis 写超时，默认值为 `0`，使用 go-redis 默认值

### 私有 Helm Chart

Redis router 配置已经接入私有 chart `charts/spegel-intern`。

```yaml
spegel:
  routerRedis:
    enabled: true
    secretAutoCreate: true
    secretName: spegel-redis
    address: redis-service:6379
    addresses: []
    username: ""
    password: ""
    clusterEnabled: false
    sentinel:
      addresses: []
      masterName: ""
      username: ""
      password: ""
    tls:
      enabled: false
      caFile: ""
      certFile: ""
      keyFile: ""
      insecureSkipVerify: false
      extraVolumeMounts: []
      extraVolumes: []
    advertiseBatchSize: 1000
    expiredCleanupInterval: 100
    readvertiseJitter: 45s
    poolSize: 8
    minIdleConns: 1
    dialTimeout: 1s
    readTimeout: 20ms
    writeTimeout: 20ms
```

公共 chart `charts/spegel` 不包含私有 Redis 调优配置，私有配置应使用 `charts/spegel-intern`。

使用 TLS 时，通过 `spegel.routerRedis.tls.extraVolumes` 和 `extraVolumeMounts` 挂载 CA 或客户端证书文件，再将 `caFile`、`certFile` 和 `keyFile` 指向挂载路径。

### Valkey Helm Chart

`charts/valkey` 提供一个兼容 Redis 协议的单实例 Valkey 部署，适合本地测试或规模较小的私有 Spegel Redis router 部署。

```bash
helm install valkey charts/valkey
```

默认配置：

- Service：`redis-service:6379`
- Secret：`redis-service`
- 密码 key：`password`
- 密码值：`spegel`
- 镜像：`valkey:8.1-alpine`
- 默认关闭持久化；如果重启后需要保留 route 恢复信息，可通过 `persistence.enabled=true` 启用

## 架构

### Peer 标识

Redis router 广告的 IP 来自 `--redis-advertise-ip` / `REDIS_ADVERTISE_IP`。
在私有 Helm chart 中，该值设置为 `$(NODE_IP)`，而 `NODE_IP` 来自 `status.hostIP`。

广告 IP 同时作为 Redis router 的 host ID。registry 端口取自 `--registry-addr`。

### 数据结构

Peer 以 Redis Sorted Set 的形式保存：

```
Key: {prefix}:{escaped-content-key}
Member: {advertise-ip}|{registry-port}
Score: Unix 毫秒时间戳形式的过期时间
```

示例：

```text
Key: spegel:registry.example.com%3A30500/t-mke/dev/mke-front-priv%3Av1.0.2
Member: 192.168.1.10|5000
Score: 1760000000000
```

内容 key 保持可读文本形式，只转义 Redis key 中的分隔符：
`%` 转换为 `%25`，`:` 转换为 `%3A`。这样既保持 key 可读，又避免 Redis key 中的冒号产生歧义。

`Advertise` 会为每个内容 key 执行一次 `ZADD`。大量 key 会按照 `REDIS_ADVERTISE_BATCH_SIZE` 拆分，避免单次刷新创建无界 pipeline。

过期成员清理采用机会式执行。Lookup 和 Advertise 路径每经过 `REDIS_EXPIRED_CLEANUP_INTERVAL` 次清理机会后，会删除对应 sorted-set 中的过期成员。设置为 `0` 时禁用清理；即使不清理，Lookup 仍会根据 score 忽略过期成员。

Redis 模式下，Spegel 每隔 `REDIS_ADVERTISE_TTL / 2` 对本地所有 key 重新广告，并应用 `REDIS_READVERTISE_JITTER` 配置的抖动。如果该配置为 `0`，使用重新广告间隔的 10% 作为抖动范围。

对于本进程已经广告过的 key，重复的 Create 事件会跳过。周期性重新广告仍会刷新所有本地 key，以续租 TTL。

`Lookup` 使用 `ZRANGEBYSCORE` 查询未过期成员。当调用方请求固定数量的 peer 时，router 使用 Redis `LIMIT` 分批读取有限候选，而不是读取热点 key 的全部成员。无界查询（`count == 0`）仍会返回该 key 当前所有 peer。

Lookup 候选在构造 round-robin balancer 前会进行随机化。固定数量查询会读取不超过上限的候选、随机化可用 peer，再返回请求数量，从而减少热点内容反复选择相同低 score peer 的问题。

### Redis 拓扑

- 单地址：`REDIS_ADDR` 创建一个 Redis client
- 多个独立实例：`REDIS_ADDRS` 为每个地址创建一个 client，并按内容 key hash 到对应分片
- Redis Cluster：`REDIS_CLUSTER_ENABLED=true` 使用 go-redis Cluster client mode，`REDIS_ADDRS` 作为 seed 地址
- Redis Sentinel：使用 `REDIS_SENTINEL_ADDRS` 和 `REDIS_SENTINEL_MASTER_NAME` 创建故障转移 client

独立实例分片采用简单的 key hash。增加或删除分片地址会改变 key 的分布，route entry 会在下一次正常周期性重新广告时恢复。

## 生产容量评估：单实例 Redis

### 范围和结论

Redis router 没有固定的 Spegel 节点数量上限。实际限制由以下因素共同决定：

1. 活跃 route membership 数量，即 `节点数 x 每节点广告的内容 key 数`。
2. 重新广告写入速率和镜像拉取查询速率。
3. Redis 客户端连接数以及可用 CPU、内存。
4. 故障恢复要求。单实例 Redis 始终是单点故障。

生产容量规划可以采用以下保守控制线：

| 部署规模 | 单实例建议 | Redis 资源基线 |
|----------|------------|----------------|
| 不超过 200 节点 | 活跃 route membership 不超过 200 万，且拉取流量中等时可以使用 | 4 vCPU、4 GiB 内存 |
| 200～500 节点 | 有条件使用；活跃 route membership 控制在 500 万以内，并完成代表性压测 | 4～8 vCPU、8 GiB 内存 |
| 超过 500 节点、超过 500 万 membership，或 Redis 持续流量超过 15～20k ops/s | 不建议使用单实例生产部署，应使用 Sentinel、Cluster 或独立实例分片 | 根据实测负载为每个 Redis 成员单独规划 |

以上是运维容量规划控制线，不是 Redis 协议限制，也不是性能承诺。镜像目录较小的集群可以支持更多节点；镜像目录较大或并发拉取较高时，更少的节点也可能超过控制线。即使单实例容量足够，生产环境仍建议使用 Sentinel 或 Cluster，因为容量足够不能消除单实例故障风险。

### 内存估算

定义：

```text
N = Spegel 节点数
K = 单节点广告的活跃内容 key 数
E = route membership 数量 = N x K
```

每个 membership 对应一个 sorted-set member 和 score。容量规划可按每个活跃 membership 约 `150～300 bytes` 估算，其中包括 Redis object 和 sorted-set 开销。Redis key 和内存分配器开销取决于实际数据；当大部分内容 key 在不同节点上都不相同时，应按高位估算，或使用代表性 staging 数据实测。

```text
route metadata = E x 150～300 bytes
规划内存 = route metadata x 2～3 + Redis 基础开销/持久化开销
```

乘数用于预留内存分配器碎片、临时命令 buffer、复制/AOF buffer 和运行余量。route 记录会自然过期，并可由 Spegel 下一次重新广告恢复，因此这些内存主要是路由元数据，不是镜像 blob 存储。

| 节点数 | 每节点 key 数 | Membership 数量 | 路由元数据估算 | 建议内存 |
|-------:|--------------:|----------------:|---------------:|---------:|
| 200 | 5,000 | 100 万 | 150～300 MiB | 最低 1 GiB，建议 2 GiB |
| 200 | 10,000 | 200 万 | 300～600 MiB | 最低 2 GiB，建议 4 GiB |
| 200 | 50,000 | 1,000 万 | 1.5～3 GiB | 4～8 GiB；不再建议单实例 |
| 500 | 10,000 | 500 万 | 0.75～1.5 GiB | 最低 4 GiB，建议 8 GiB |

表格仅估算路由元数据。应使用代表性数据集，通过 `INFO memory` 和 `MEMORY USAGE` 确认实际占用。将 `maxmemory` 设置为低于容器内存限制的值并预留余量；不要依赖 eviction 保存 route data，因为 eviction 可能降低 peer 可用性并增加上游 registry 流量。

### CPU 和命令速率估算

Redis router 默认 TTL 为 15 分钟，Spegel 每隔 TTL 的一半重新广告，因此平均刷新写入速率为：

```text
平均刷新 ZADD/s = E / 450
```

示例：

| Membership 数量 | 平均刷新写入 |
|----------------:|-------------:|
| 100 万 | 约 2,200 ZADD/s |
| 200 万 | 约 4,400 ZADD/s |
| 500 万 | 约 11,100 ZADD/s |
| 1,000 万 | 约 22,200 ZADD/s |

`REDIS_ADVERTISE_BATCH_SIZE` 通过合并命令减少网络 round trip，但不会减少 Redis 命令总数。重新广告抖动可以把刷新分散到时间窗口中，但不会改变平均速率。还需要额外计入事件驱动的 advertise/withdraw 操作，以及镜像拉取产生的 `ZRANGEBYSCORE` 查询。当一个热点 key 存在大量 peer 时，一次查询可能需要多个有界查询命令。

容量验收应使用真实的节点数、每节点 key 数、镜像拉取并发数和 registry 延迟进行压测。峰值时建议 Redis CPU 保持在约 70% 以下，命令 p99 延迟低于 5～10 ms，`connected_clients` 低于 `maxclients` 的 50%，并确保 `rejected_connections`、`evicted_keys` 和 blocked clients 均为 0。应监控 `used_memory / maxmemory`、`instantaneous_ops_per_sec`、命令延迟以及 Spegel 暴露的 Redis router 连接池指标。

### 连接数估算

Redis 客户端连接数可以近似计算为：

```text
客户端连接数 = Spegel 节点数 x REDIS_POOL_SIZE
```

连接池按需创建连接，但 `REDIS_POOL_SIZE` 是每个 Spegel 进程、每个 Redis client 的连接上限。单 Redis 实例且 `REDIS_POOL_SIZE=8` 时，200 节点最多约使用 1,600 个连接；500 节点最多约使用 4,000 个连接。还要为 Sentinel、监控、备份、管理操作和故障转移预留连接。生产环境应显式配置连接池大小，不要依赖 go-redis 默认值，并在镜像拉取高峰期间同时检查 `PoolStats` 和 Redis `connected_clients`。

### 200 节点生产评估

对于 200 节点 Kubernetes 集群，当每个节点广告约 5,000～10,000 个活跃 key 且镜像拉取流量中等时，一个 Redis/Valkey 实例通常可以满足路由元数据需求。建议起始资源为 4 vCPU、4 GiB 内存，配置 `REDIS_POOL_SIZE=4-8`，根据恢复要求决定是否启用持久化，并启用 Redis 监控和告警。

这并不意味着架构具备高可用性。Redis 重启、节点故障、网络分区或连接耗尽会同时影响所有 Spegel 节点的路由。由于 route metadata 使用租约机制，Redis 数据丢失后可以在重新广告后恢复，但故障期间的镜像拉取可能失败、回源 registry，或者根据 containerd mirror 配置等待客户端超时。如果镜像拉取属于关键业务，应在达到单实例控制线之前使用 Sentinel 或 Cluster。

## 优化状态

### 已实现

- 使用 Redis `LIMIT` 实现固定 peer 数量查询的有界 Lookup
- 在 Lookup 路径上机会式清理过期成员
- 支持配置 Redis client 连接池大小以及 dial/read/write 超时
- 通过 `REDIS_ADVERTISE_BATCH_SIZE` 对 Advertise pipeline 分批
- 通过 `REDIS_READVERTISE_JITTER` 为重新广告增加抖动
- 跳过本进程已经记录过的 key 的重复 Create 事件广告
- 通过 `REDIS_EXPIRED_CLEANUP_INTERVAL` 配置机会式清理间隔
- 在 Redis 查询结果返回给 mirror resolver 前随机化 peer
- 导出 Redis router 命令、清理、查询和连接池指标
- 支持 Redis ACL 用户名和 Sentinel 凭据
- 支持 Redis TLS、CA bundle 和 mTLS 客户端证书
- 支持 Redis Sentinel 故障转移 client
- 支持 Redis Cluster client mode
- 支持独立多 Redis 实例按内容 key 分片

### 尚未实现

以下优化项已知但尚未实现：

| 领域 | 优化项 | 预期收益 |
|------|--------|----------|
| 过期清理 | 增加独立的后台清理循环 | 将清理工作从用户请求的 Lookup 和 Advertise 操作中解耦 |
| 高可用 | 增加 Sentinel 和 Cluster 故障转移 e2e 测试 | 验证 master 故障转移以及 slot/node 移动期间的行为 |
| 分片 | 为独立实例分片增加一致性 hash 或分片迁移工具 | 减少增加或删除分片时的 route churn |
| 运维 | 增加 Redis router 指标的生产 runbook 和 dashboard | 更容易发现 Redis 饱和并执行故障转移 |

## Redis Router 与 P2P Router 对比

| 特性 | Redis Router | P2P Router |
|------|--------------|------------|
| 发现方式 | 集中式 | 分布式（DHT） |
| Bootstrap | 不需要 | 需要 |
| NAT 穿透 | 不需要 | 需要 |
| 可扩展性 | 受 Redis 限制 | 点对点扩展 |
| 延迟 | 直接查询，较低 | DHT 跳数导致延迟不固定 |
| 单点故障 | 有（Redis） | 无 |
| Debug Web | 支持 | 支持 |

## 使用场景

### 适合使用 Redis Router 的场景

- **部署简单**：不需要 P2P bootstrap
- **网络受控**：所有节点都可以访问 Redis
- **低延迟要求**：直接 Redis 查询比 DHT 更快
- **中小规模集群**：Redis 能够承载对应负载

### 适合使用 P2P Router 的场景

- **大规模部署**：没有中心化瓶颈
- **高可用要求**：没有单点故障
- **复杂网络拓扑**：内置 NAT 穿透
- **去中心化架构**：不依赖外部基础设施

## Kubernetes 部署示例

```yaml
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: spegel
spec:
  template:
    spec:
      containers:
      - name: spegel
        image: ghcr.io/spegel-org/spegel:latest
        env:
        - name: ROUTER_KIND
          value: "redis"
        - name: REDIS_ADDR
          value: "redis-service:6379"
        - name: REDIS_PASSWORD
          valueFrom:
            secretKeyRef:
              name: redis-secret
              key: password
        - name: REDIS_POOL_SIZE
          value: "8"
        - name: REDIS_ADVERTISE_BATCH_SIZE
          value: "1000"
        - name: REDIS_EXPIRED_CLEANUP_INTERVAL
          value: "100"
        - name: REDIS_READVERTISE_JITTER
          value: "45s"
        - name: REDIS_READ_TIMEOUT
          value: "20ms"
        - name: REDIS_WRITE_TIMEOUT
          value: "20ms"
        # 用于 registry 通信的主 IP 地址
        - name: NODE_IP
          valueFrom:
            fieldRef:
              fieldPath: status.hostIP
        - name: REDIS_ADVERTISE_IP
          valueFrom:
            fieldRef:
              fieldPath: status.hostIP
```

## 监控

Redis router 除了通用 router 指标外，还暴露以下 Redis 专用指标：

- `spegel_resolve_duration_seconds{router="redis"}`：解析 peer 的耗时
- `spegel_redis_router_commands_total{command,result}`：router 发出的 Redis 命令总数
- `spegel_redis_router_cleanup_removed_total`：清理掉的过期 Redis peer member 总数
- `spegel_redis_router_lookup_candidates`：每次 Redis router 查询读取的 sorted-set 成员数
- `spegel_redis_router_lookup_peers`：每次查询返回的可用 peer 数
- `spegel_redis_router_pool_stats{client,stat}`：每个 Redis client 的 go-redis 连接池统计

## 故障排查

### 连接问题

```bash
# 测试 Redis 连通性
redis-cli -h redis.example.com -p 6379 -a yourpassword PING

# 查看 peer 注册信息
redis-cli -h redis.example.com -p 6379 -a yourpassword KEYS "spegel:*"

# 查看一个 route key
redis-cli -h redis.example.com -p 6379 -a yourpassword ZRANGE "spegel:registry.example.com%3A30500/t-mke/dev/mke-front-priv%3Av1.0.2" 0 -1 WITHSCORES
```

### Pod 拉取镜像卡住

如果 Kubernetes Pod 拉取镜像卡住，但手动执行 `nerdctl pull` 正常，首先确认 Pod 拉取是否使用了 Spegel 写入的 containerd mirror 配置：

```bash
# 检查 registry 是否被重定向到本地 Spegel mirror。
cat /etc/containerd/certs.d/<registry>/hosts.toml

# 测试 kubelet 使用的 CRI/containerd 路径。
crictl pull <image>

# 检查请求 digest 对应的 Redis route。sha256:... 中的 ':' 已被转义。
redis-cli -h redis.example.com -p 6379 -a yourpassword ZRANGE "spegel:sha256%3A<digest>" 0 -1 WITHSCORES
```

Spegel 对主要 mirror 阶段分别提供超时限制：

- `MIRROR_RESOLVE_TIMEOUT` / `--mirror-resolve-timeout`：路由查询超时
- `MIRROR_MANIFEST_TIMEOUT` / `--mirror-manifest-timeout`：从 mirror peer 获取 manifest 或执行 HEAD 请求的超时
- `MIRROR_BLOB_TIMEOUT` / `--mirror-blob-timeout`：从 mirror peer 获取 blob 的超时
- `CONTAINERD_MIRROR_DIAL_TIMEOUT` / `--containerd-mirror-dial-timeout`：生成的 containerd `hosts.toml` 中 mirror host 的建连超时

紧急绕过时，可以关闭 `containerdMirrorAdd`，为受影响的 registry 增加 registry filter，或者删除受影响的 `/etc/containerd/certs.d/<registry>/hosts.toml` 后重启该节点上的 containerd。

### Debug Web 界面

访问 `http://localhost:9090/debug/web/` 可以查看：

- 本地地址
- 可用镜像
- Peer 列表（Redis router 返回空列表，peer 会按需发现）
- Mirror 性能指标

## 实现细节

### Router 接口

Redis 和 P2P router 都实现相同的 `Router` 接口：

```go
type Router interface {
    Ready(ctx context.Context) (bool, error)
    Lookup(ctx context.Context, key string, count int) (Balancer, error)
    Advertise(ctx context.Context, keys []string) error
    Withdraw(ctx context.Context, keys []string) error
    LocalAddresses() ([]netip.Addr, error)
    ListPeers() ([]Peer, error)
    HostID() string
}
```

### 关键差异

- **`ListPeers()`**：Redis router 返回空列表，peer 会按需发现
- **`HostID()`**：Redis 使用广告 IP，P2P 使用 libp2p peer ID
- **`LocalAddresses()`**：Redis 返回广告 IP，P2P 返回 libp2p host 地址
