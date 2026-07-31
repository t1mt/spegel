# Valkey Chart

This chart deploys a single Redis-compatible Valkey instance for Spegel Redis router testing or small private deployments.

The default Service name is `redis-service`, matching `charts/spegel-intern` default `spegel.routerRedis.address`.

```bash
helm install valkey charts/valkey
```

Default credentials:

- Secret: `redis-service`
- Key: `password`
- Password: `spegel`

For production, set explicit resources, enable persistence if route recovery after restart matters, and consider Sentinel or a managed Redis/Valkey service for high availability.
