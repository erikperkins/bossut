## Bossut
This is a simple containerized Python application.

### Local testing
To connect to MinIO and Kafka in the cluster from a local machine, export the MinIO connection
credentials, port-forward each service, and add the following to `/etc/hosts`
```agsl
127.0.0.1   minio.minio.svc.cluster.local
127.0.0.1   kafka-service.kafka.svc.cluster.local
```
