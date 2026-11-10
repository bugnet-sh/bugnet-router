package config

import (
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestTelemetryDefaults(t *testing.T) {
	c, err := LoadFile(writeFile(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, c.Telemetry, Telemetry{SampleRatio: 1, Environment: "main",
		ExportTimeout: 10 * time.Second, BatchTimeout: 5 * time.Second, BatchSize: 512,
		QueueSize: 2048})
	eq(t, loadExample(t).Telemetry, c.Telemetry)
	doc, err := outline([]byte(committed(t, "example.toml")))
	if err != nil {
		t.Fatal(err)
	}
	var written []string
	for _, s := range doc {
		if len(s.path) == 2 && s.path[0] == "telemetry" && !s.header {
			written = append(written, s.path[1])
		}
	}
	var keys []string
	schema := reflect.TypeFor[telemetryFile]()
	for i := range schema.NumField() {
		keys = append(keys, schema.Field(i).Tag.Get("toml"))
	}
	slices.Sort(written)
	slices.Sort(keys)
	eq(t, written, keys)
}

func TestTelemetryDevTomlKeepsTracesOff(t *testing.T) {
	c, err := LoadFile("../../config/dev.toml")
	if err != nil {
		t.Fatal(err)
	}
	eq(t, c.Telemetry.Traces, false)
	eq(t, c.Telemetry.Environment, "dev")
}

func TestTelemetryValidation(t *testing.T) {
	cases := []struct{ body, want string }{
		{"traces = true\n", "telemetry.otlp_endpoint is required while telemetry.traces is true"},
		{"traces = true\notlp_endpoint = \"http://traces.example.com/v1/traces\"\n",
			"telemetry.otlp_endpoint must be an https URL"},
		{"otlp_endpoint = \"https://u:p@traces.example.com/v1/traces\"\n",
			"telemetry.otlp_endpoint must be an https URL"},
		{"otlp_endpoint = \"ftp://traces.example.com\"\n",
			"telemetry.otlp_endpoint must be an https URL"},
		{"sample_ratio = 1.5\n", "telemetry.sample_ratio must be 0 to 1"},
		{"sample_ratio = -0.1\n", "telemetry.sample_ratio must be 0 to 1"},
		{"environment = \"Main Env\"\n", "telemetry.environment must be"},
		{"export_timeout = \"0s\"\n", "telemetry.export_timeout must be a Go duration"},
		{"batch_timeout = \"2h\"\n", "telemetry.batch_timeout must be a Go duration"},
		{"queue_size = 0\n", "telemetry.queue_size must be 1 to"},
		{"batch_size = 4096\n", "telemetry.batch_size must be 1 to telemetry.queue_size"},
	}
	for _, c := range cases {
		msg := problem(t, "[telemetry]\n"+c.body)
		if !strings.Contains(msg, c.want) {
			t.Errorf("%q gave %q, want %q", c.body, msg, c.want)
		}
	}
	for _, ok := range []string{
		"traces = true\notlp_endpoint = \"http://127.0.0.1:4318/v1/traces\"\n",
		"traces = true\notlp_endpoint = \"https://otlp.example.com/v1/traces\"\n",
		"sample_ratio = 0\n",
	} {
		if _, err := LoadFile(writeFile(t, "[telemetry]\n"+ok)); err != nil {
			t.Errorf("%q refused: %v", ok, err)
		}
	}
}
