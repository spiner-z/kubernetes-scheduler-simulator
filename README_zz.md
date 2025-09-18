# Kubernetes Scheduler Simulator Lab

## Quickstart

Run command for go:

```bash
go run cmd/main.go apply --extended-resources "gpu" \
                  -f example/test-cluster-config.yaml \
                  -s example/test-scheduler-config.yaml
```

Compile and execute:

```bash
make
bin/simon apply --extended-resources "gpu" \
                  -f example/test-cluster-config.yaml \
                  -s example/test-scheduler-config.yaml
```

