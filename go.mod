module github.com/bugnet-sh/bugnet-router

go 1.27

require (
	connectrpc.com/connect v1.21.1
	github.com/google/uuid v1.6.0
	github.com/jackc/pgx/v5 v5.12.0
	github.com/oapi-codegen/runtime v1.8.0
	github.com/bugnet-sh/bugnet-proto v0.1.0
	github.com/bugnet-sh/bugnet-run v0.1.2
	github.com/pelletier/go-toml/v2 v2.2.4
	github.com/pressly/goose/v3 v3.29.0
	github.com/riverqueue/river v0.51.0
	github.com/riverqueue/river/riverdriver/riverpgxv5 v0.51.0
	github.com/riverqueue/river/rivertype v0.51.0
	github.com/valkey-io/valkey-go v1.0.79
	golang.org/x/net v0.61.0
	golang.org/x/sync v0.24.0
	golang.org/x/sys v0.49.0
	google.golang.org/genproto/googleapis/rpc v0.0.0-20261005182115-fad411399dd8
	google.golang.org/grpc v1.85.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/apapsch/go-jsonmerge/v2 v2.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.3 // indirect
	github.com/mfridman/interpolate v0.0.2 // indirect
	github.com/riverqueue/river/riverdriver v0.51.0 // indirect
	github.com/riverqueue/river/rivershared v0.51.0 // indirect
	github.com/sethvargo/go-retry v0.5.0 // indirect
	github.com/tidwall/gjson v1.20.0 // indirect
	github.com/tidwall/match v1.2.0 // indirect
	github.com/tidwall/pretty v1.2.1 // indirect
	github.com/tidwall/sjson v1.2.5 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/text v0.43.0 // indirect
)

replace github.com/bugnet-sh/bugnet-run => ../bugnet-run

replace github.com/bugnet-sh/bugnet-proto => ../bugnet-proto
