package store

import "testing"

func TestMigrateDropsPoolSettings(t *testing.T) {
	cfg, err := connConfig("postgres://bugnet_router@/bugnet?host=/var/run/postgresql" +
		"&pool_max_conns=48&application_name=router")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cfg.RuntimeParams["pool_max_conns"]; ok {
		t.Fatal("pool_max_conns would reach Postgres as a runtime parameter")
	}
	if cfg.RuntimeParams["application_name"] != "router" || cfg.User != "bugnet_router" {
		t.Fatalf("got %+v", cfg.RuntimeParams)
	}
}
