package config

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/pelletier/go-toml/v2"
	"github.com/pelletier/go-toml/v2/unstable"
)

/* Outline */

// setting is one key or table header the document writes
type setting struct {
	path   []string
	id     string // the quoted path with each array element's index, the same for a key set twice
	line   int
	column int
	header bool          // a [table] or [[array]] header
	array  bool          // an [[array]] header
	value  unstable.Kind // the kind of a key's value
}

type outliner struct {
	p      unstable.Parser
	out    []setting
	arrays map[string]int // elements of each array of tables so far, by id
}

// outline lists the keys and table headers of a document in the order they are written
func outline(raw []byte) ([]setting, error) {
	o := &outliner{arrays: map[string]int{}}
	o.p.Reset(raw)
	var table []string
	scope := ""
	for o.p.NextExpression() {
		e := o.p.Expression()
		switch e.Kind {
		case unstable.Table, unstable.ArrayTable:
			s := o.header(e)
			table, scope = s.path, s.id
			o.out = append(o.out, s)
		case unstable.KeyValue:
			o.keyValue(e, table, scope)
		}
	}
	if err := o.p.Error(); err != nil {
		return nil, o.syntaxError(err, table)
	}
	return o.out, nil
}

// key reads a dotted key and the line and column it starts at
func (o *outliner) key(it unstable.Iterator) (parts []string, line, column int) {
	for it.Next() {
		n := it.Node()
		if len(parts) == 0 {
			start := o.p.Shape(n.Raw).Start
			line, column = start.Line, start.Column
		}
		parts = append(parts, string(n.Data))
	}
	return parts, line, column
}

func (o *outliner) header(e *unstable.Node) setting {
	path, line, column := o.key(e.Key())
	array := e.Kind == unstable.ArrayTable
	id := ""
	for i, part := range path {
		id += "." + strconv.Quote(part)
		if array && i == len(path)-1 {
			o.arrays[id]++
		}
		// A key below an array of tables belongs to its latest element
		if n, ok := o.arrays[id]; ok {
			id += "#" + strconv.Itoa(n)
		}
	}
	return setting{path: path, id: id, line: line, column: column, header: true, array: array}
}

func (o *outliner) keyValue(kv *unstable.Node, table []string, scope string) {
	parts, line, column := o.key(kv.Key())
	id := scope
	for _, part := range parts {
		id += "." + strconv.Quote(part)
	}
	path := slices.Concat(table, parts)
	v := kv.Value()
	o.out = append(o.out, setting{path: path, id: id, line: line, column: column, value: v.Kind})
	o.inner(v, path, id)
}

// inner adds the keys of the inline tables in a value, those inside arrays included
func (o *outliner) inner(v *unstable.Node, path []string, id string) {
	it := v.Children()
	switch v.Kind {
	case unstable.InlineTable:
		for it.Next() {
			o.keyValue(it.Node(), path, id)
		}
	case unstable.Array:
		for i := 0; it.Next(); i++ {
			o.inner(it.Node(), path, id+"#"+strconv.Itoa(i))
		}
	}
}

// bareKey is the bare or dotted key a line starts with
var bareKey = regexp.MustCompile(`^\s*([A-Za-z0-9_-]+(?:\s*\.\s*[A-Za-z0-9_-]+)*)\s*=`)

// syntaxError names the line TOML breaks on and its key, never what the parser saw there
func (o *outliner) syntaxError(err error, table []string) error {
	data := o.p.Data()
	// An empty highlight is the end of the document
	line := bytes.Count(data, []byte{'\n'}) + 1
	var pe *unstable.ParserError
	if errors.As(err, &pe) && len(pe.Highlight) > 0 {
		line = o.p.Shape(o.p.Range(pe.Highlight)).Start.Line
	}
	text := strings.Split(string(data), "\n")[line-1]
	m := bareKey.FindStringSubmatch(text)
	if m == nil {
		return fmt.Errorf("line %d: not valid TOML", line)
	}
	path := slices.Clone(table)
	for part := range strings.SplitSeq(m[1], ".") {
		path = append(path, strings.TrimSpace(part))
	}
	if env, ok := secretFor(path); ok {
		return fmt.Errorf("line %d: %s", line, secretKey(path, env))
	}
	return fmt.Errorf("line %d: %s is not valid TOML", line, strings.Join(path, "."))
}

/* Keys */

// sizeName is what a key of boat.types or koyeb.instance_types must look like
var sizeName = regexp.MustCompile(`^[a-z0-9-]{1,32}$`)

// check finds keys the schema lacks or spells in another case, map keys that are not size names,
// tables of the wrong kind and keys set twice
func check(doc []setting) error {
	var problems []string
	var unknown [][]string
	seen := map[string]bool{}
	for _, s := range doc {
		// Keys below an unknown key are not reported again
		if slices.ContainsFunc(unknown, func(u []string) bool {
			return len(u) <= len(s.path) && slices.Equal(u, s.path[:len(u)])
		}) {
			continue
		}
		problem, isUnknown := fit(s)
		if seen[s.id] {
			problem = strings.Join(s.path, ".") + " is set twice"
		}
		seen[s.id] = true
		if isUnknown {
			unknown = append(unknown, s.path)
		}
		if problem != "" {
			problems = append(problems, fmt.Sprintf("line %d: %s", s.line, problem))
		}
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, ", "))
	}
	return nil
}

// fit says what is wrong with a setting the schema does not take, and whether its key is unknown
func fit(s setting) (problem string, unknown bool) {
	t := reflect.TypeFor[file]()
	for i, part := range s.path {
		t = element(t)
		switch t.Kind() {
		case reflect.Map:
			key := strings.Join(s.path[:i+1], ".")
			switch {
			case !sizeName.MatchString(part):
				return key + " is not a size name, size names are lowercase letters, digits " +
					"and hyphens", false
			case i < len(s.path)-1 || s.header:
				return key + " must be " + valueKind(t.Elem()), false
			}
			return "", false
		case reflect.Struct:
			f, near, ok := field(t, part)
			if !ok {
				if near != "" {
					near = strings.Join(slices.Concat(s.path[:i], []string{near}, s.path[i+1:]),
						".")
				}
				return unknownKey(s.path, near), true
			}
			t = f.Type
		default:
			return strings.Join(s.path[:i], ".") + " must be " + valueKind(t), false
		}
	}
	if s.header && !takesHeader(t, s.array) {
		return strings.Join(s.path, ".") + " must be " + valueKind(t), false
	}
	return "", false
}

// field finds the field of struct t whose TOML key is name, or the key name spells in another case
func field(t reflect.Type, name string) (f reflect.StructField, near string, ok bool) {
	for i := range t.NumField() {
		f := t.Field(i)
		switch key := f.Tag.Get("toml"); {
		case key == name:
			return f, "", true
		case strings.EqualFold(key, name):
			near = key
		}
	}
	return reflect.StructField{}, near, false
}

// element is the type that keys below a value of type t describe
func element(t reflect.Type) reflect.Type {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Struct {
		t = t.Elem()
	}
	return t
}

func takesHeader(t reflect.Type, array bool) bool {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if array {
		return t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Struct
	}
	return t.Kind() == reflect.Struct || t.Kind() == reflect.Map
}

// schemaType is the type a known path decodes into
func schemaType(path []string) (reflect.Type, bool) {
	t := reflect.TypeFor[file]()
	for _, part := range path {
		t = element(t)
		switch t.Kind() {
		case reflect.Map:
			t = t.Elem()
		case reflect.Struct:
			f, _, ok := field(t, part)
			if !ok {
				return nil, false
			}
			t = f.Type
		default:
			return nil, false
		}
	}
	return t, true
}

func valueKind(t reflect.Type) string {
	switch t.Kind() {
	case reflect.Pointer:
		return valueKind(t.Elem())
	case reflect.Bool:
		return "true or false"
	case reflect.Int, reflect.Int64:
		return "a whole number"
	case reflect.Float64:
		return "a number"
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Struct {
			return "an array of tables"
		}
		return "an array of strings"
	case reflect.Map:
		return "a table of strings"
	case reflect.Struct:
		return "a table"
	}
	return "a string"
}

/* Decode errors */

// secretNames are the environment variables that hold secrets
var secretNames = []string{
	databaseURLEnv, "BUGNET_INTERNAL_TOKEN", "BUGNET_HANDLE_KEY", "BUGNET_EVENT_SIGNING_KEY",
	"BUGNET_ORDER_PRIVATE_KEY", "BOXD_API_KEY", "E2B_API_KEY", "KOYEB_API_TOKEN", "BOAT_API_KEY",
}

// describe names the key and line of a decode error and never quotes go-toml or the document
func describe(err error, doc []setting) string {
	var missing *toml.StrictMissingError
	if errors.As(err, &missing) {
		var out []string
		for _, e := range missing.Errors {
			row, _ := e.Position()
			out = append(out, fmt.Sprintf("line %d: %s", row, unknownKey(e.Key(), "")))
		}
		return strings.Join(out, ", ")
	}
	var de *toml.DecodeError
	if !errors.As(err, &de) {
		return "a key or table is set twice or as two kinds of value"
	}
	row, column := de.Position()
	// The error is about the last key written before its position
	var at *setting
	for i := range doc {
		if doc[i].line > row || doc[i].line == row && doc[i].column > column {
			break
		}
		at = &doc[i]
	}
	if at == nil {
		return fmt.Sprintf("line %d: a value does not fit its setting", row)
	}
	key := strings.Join(at.path, ".")
	t, ok := schemaType(at.path)
	if !ok {
		return fmt.Sprintf("line %d: %s does not fit its setting", row, key)
	}
	want := valueKind(t)
	if want == "a whole number" && at.value == unstable.Integer {
		want = "a whole number that fits in 64 bits"
	}
	return fmt.Sprintf("line %d: %s must be %s", row, key, want)
}

// unknownKey says what is wrong with a key the schema lacks, pointing secrets to the environment
func unknownKey(key []string, near string) string {
	if env, ok := secretFor(key); ok {
		return secretKey(key, env)
	}
	name := strings.Join(key, ".")
	if near != "" {
		return name + " is not a setting, did you mean " + near
	}
	return name + " is not a setting"
}

// secretFor finds the secret a key names, with or without BUGNET_ in front
func secretFor(key []string) (string, bool) {
	env := strings.ToUpper(strings.Join(key, "_"))
	for _, s := range secretNames {
		if env == s || "BUGNET_"+env == s {
			return s, true
		}
	}
	return "", false
}

func secretKey(key []string, env string) string {
	return fmt.Sprintf("%s is a secret, set %s in the environment instead",
		strings.Join(key, "."), env)
}
