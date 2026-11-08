package canon

import (
	"encoding/json"
	"reflect"
	"testing"
)

// eq fails the test when got and want differ
func eq[T any](t *testing.T, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func mustMarshal(t *testing.T, v any) string {
	t.Helper()
	b, err := Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(b)
}

func TestSortsKeysByUTF16(t *testing.T) {
	// The sorting example of RFC 8785 section 3.2.3
	in := json.RawMessage(`{"\u20ac":"Euro Sign","\r":"Carriage Return","\ufb33":"Hebrew Letter ` +
		`Dalet With Dagesh","1":"One","\ud83d\ude00":"Emoji: Grinning Face","\u0080":"Control",` +
		`"\u00f6":"Latin Small Letter O With Diaeresis"}`)
	want := "{\"\\r\":\"Carriage Return\",\"1\":\"One\",\"\u0080\":\"Control\"," +
		"\"\u00f6\":\"Latin Small Letter O With Diaeresis\",\"\u20ac\":\"Euro Sign\"," +
		"\"\U0001F600\":\"Emoji: Grinning Face\",\"\ufb33\":\"Hebrew Letter Dalet With Dagesh\"}"
	eq(t, mustMarshal(t, in), want)
}

func TestNumbersInECMAScriptForm(t *testing.T) {
	for in, want := range map[string]string{
		"0": "0", "-0": "0", "1": "1", "-1": "-1", "1.0": "1", "1e21": "1e+21",
		"1e20": "100000000000000000000", "0.000001": "0.000001", "1e-7": "1e-7",
		"333333333.33333329": "333333333.3333333", "4.50": "4.5", "2e-3": "0.002",
		"9007199254740991": "9007199254740991", "1.5e300": "1.5e+300", "-1e-7": "-1e-7",
		"1.2345678901234568e20": "123456789012345680000",
	} {
		eq(t, mustMarshal(t, json.RawMessage(in)), want)
	}
}

func TestRefusesIntegersPastExactDoubles(t *testing.T) {
	_, err := Marshal(json.RawMessage(`9007199254740993`))
	eq(t, err != nil, true)
	_, err = Marshal(uint64(1)<<60 + 1)
	eq(t, err != nil, true)
}

func TestStringEscapes(t *testing.T) {
	eq(t, mustMarshal(t, "a\"b\\c\n\t\b\f\r\x01<>&\u2028é"),
		"\"a\\\"b\\\\c\\n\\t\\b\\f\\r\\u0001<>&\u2028é\"")
}

func TestStructsAndNesting(t *testing.T) {
	type inner struct {
		Z int    `json:"z"`
		A string `json:"a,omitempty"`
	}
	v := struct {
		List []inner          `json:"list"`
		M    map[string]int64 `json:"m"`
	}{List: []inner{{Z: 2}, {Z: 1, A: "x"}}, M: map[string]int64{"b": 2, "a": 1}}
	eq(t, mustMarshal(t, v), `{"list":[{"z":2},{"a":"x","z":1}],"m":{"a":1,"b":2}}`)
	eq(t, mustMarshal(t, []any{nil, true, false, map[string]any{}}), `[null,true,false,{}]`)
}

func TestSumIsHexSHA256(t *testing.T) {
	eq(t, Sum([]byte("abc")),
		"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
}
