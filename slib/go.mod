module cs.utexas.edu/zjia/faas/slib

go 1.18

require (
	cs.utexas.edu/zjia/faas v0.0.0
	github.com/Jeffail/gabs/v2 v2.6.0
	github.com/go-redis/redis/v8 v8.8.2
	github.com/golang/snappy v0.0.4
	github.com/pkg/errors v0.9.1
)

require (
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	go.opentelemetry.io/otel v0.19.0 // indirect
	go.opentelemetry.io/otel/metric v0.19.0 // indirect
	go.opentelemetry.io/otel/trace v0.19.0 // indirect
)

replace cs.utexas.edu/zjia/faas => ../worker/golang
