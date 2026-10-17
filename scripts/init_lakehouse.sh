#!/usr/bin/env bash
set -euo pipefail
LK=http://localhost:8181

echo "== ждём Lakekeeper"
until curl -sf $LK/health >/dev/null; do sleep 2; done

echo "== бакеты MinIO"
docker exec lakehouse-minio sh -c "
    mc alias set m http://localhost:9000 minioadmin minioadmin >/dev/null &&
    for b in bronze silver gold; do mc mb --ignore-existing m/\$b; done"

echo "== bootstrap"
curl -s -o /dev/null -w "bootstrap: %{http_code}\n" -X POST $LK/management/v1/bootstrap \
  -H "Content-Type: application/json" -d '{"accept-terms-of-use": true}'

for w in bronze silver gold; do
  curl -s -w "\nwarehouse $w: %{http_code}\n" -X POST $LK/management/v1/warehouse \
    -H "Content-Type: application/json" -d @- <<EOF
{
  "warehouse-name": "$w",
  "project-id": "00000000-0000-0000-0000-000000000000",
  "storage-profile": {
    "type": "s3", "bucket": "$w", "key-prefix": "",
    "endpoint": "http://minio:9000", "region": "us-east-1",
    "path-style-access": true, "flavor": "s3-compat", "sts-enabled": false
  },
  "storage-credential": {
    "type": "s3", "credential-type": "access-key",
    "aws-access-key-id": "minioadmin", "aws-secret-access-key": "minioadmin"
  },
  "delete-profile": { "type": "hard" }
}
EOF
done

echo "== схемы"
for w in bronze silver gold; do
  docker exec trino trino --execute "CREATE SCHEMA IF NOT EXISTS $w.lakehouse"
done
docker exec trino trino --execute "CREATE SCHEMA IF NOT EXISTS bronze.bench"
docker exec trino trino --execute "SHOW CATALOGS"
