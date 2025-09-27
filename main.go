package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

type validator struct {
	filename string
	errs     []valErr
}

type valErr struct {
	line    int // 0 => печатать без номера строки (для "is required")
	message string
}

func (v *validator) addErr(line int, msg string) {
	v.errs = append(v.errs, valErr{line: line, message: msg})
}

func (v *validator) printAndExit() {
	if len(v.errs) == 0 {
		os.Exit(0)
	}
	for _, e := range v.errs {
		if e.line > 0 {
			fmt.Fprintf(os.Stdout, "%s:%d %s\n", v.filename, e.line, e.message)
		} else {
			fmt.Fprintf(os.Stdout, "%s: %s\n", v.filename, e.message)
		}
	}
	os.Exit(1)
}

// --- helpers over yaml.Node (mapping) ---

func mapGet(m *yaml.Node, key string) (*yaml.Node, *yaml.Node) {
	// returns (keyNode, valueNode) or (nil,nil)
	if m == nil || m.Kind != yaml.MappingNode {
		return nil, nil
	}
	for i := 0; i < len(m.Content)-1; i += 2 {
		k := m.Content[i]
		v := m.Content[i+1]
		if k.Value == key {
			return k, v
		}
	}
	return nil, nil
}

func expectKind(n *yaml.Node, kind yaml.Kind, field string, v *validator) bool {
	if n == nil || n.Kind != kind {
		// Подбираем человекочитаемый тип
		var typ string
		switch kind {
		case yaml.ScalarNode:
			typ = "string"
		case yaml.MappingNode:
			typ = "object"
		case yaml.SequenceNode:
			typ = "array"
		default:
			typ = "valid type"
		}
		line := 0
		if n != nil {
			line = n.Line
		}
		v.addErr(line, fmt.Sprintf("%s must be %s", field, typ))
		return false
	}
	return true
}

func expectScalarString(n *yaml.Node, field string, v *validator) (string, bool) {
	if !expectKind(n, yaml.ScalarNode, field, v) {
		return "", false
	}
	// yaml может притащить !!int, если без кавычек — но нам нужна строка
	// Разрешим и это, приводя к строке.
	return n.Value, true
}

func expectScalarInt(n *yaml.Node, field string, v *validator) (int, bool) {
	if !expectKind(n, yaml.ScalarNode, field, v) {
		return 0, false
	}
	if n.Tag != "!!int" {
		// Попробуем вручную распарсить
		var tmp int
		_, err := fmt.Sscan(n.Value, &tmp)
		if err != nil {
			v.addErr(n.Line, fmt.Sprintf("%s must be int", field))
			return 0, false
		}
		return tmp, true
	}
	var val int
	_, err := fmt.Sscan(n.Value, &val)
	if err != nil {
		v.addErr(n.Line, fmt.Sprintf("%s must be int", field))
		return 0, false
	}
	return val, true
}

// --- field validators ---

var reSnake = regexp.MustCompile(`^[a-z0-9]+(?:_[a-z0-9]+)*$`)
var reMem = regexp.MustCompile(`^\d+(Gi|Mi|Ki)$`)

func validateTop(doc *yaml.Node, v *validator) {
	if doc.Kind != yaml.MappingNode {
		v.addErr(doc.Line, "document root must be object")
		return
	}

	// apiVersion (required == "v1")
	if _, n := mapGet(doc, "apiVersion"); n == nil {
		v.addErr(0, "apiVersion is required")
	} else if s, ok := expectScalarString(n, "apiVersion", v); ok {
		if s != "v1" {
			v.addErr(n.Line, fmt.Sprintf("apiVersion has unsupported value '%s'", s))
		}
	}

	// kind (required == "Pod")
	if _, n := mapGet(doc, "kind"); n == nil {
		v.addErr(0, "kind is required")
	} else if s, ok := expectScalarString(n, "kind", v); ok {
		if s != "Pod" {
			v.addErr(n.Line, fmt.Sprintf("kind has unsupported value '%s'", s))
		}
	}

	// metadata (required ObjectMeta)
	_, meta := mapGet(doc, "metadata")
	if meta == nil {
		v.addErr(0, "metadata is required")
	} else {
		validateObjectMeta(meta, v)
	}

	// spec (required PodSpec)
	_, spec := mapGet(doc, "spec")
	if spec == nil {
		v.addErr(0, "spec is required")
	} else {
		validatePodSpec(spec, v)
	}
}

func validateObjectMeta(n *yaml.Node, v *validator) {
	if !expectKind(n, yaml.MappingNode, "metadata", v) {
		return
	}
	// name required
	if _, name := mapGet(n, "name"); name == nil {
		v.addErr(0, "metadata.name is required")
	} else if s, ok := expectScalarString(name, "metadata.name", v); ok {
		// базовая проверка, пустые не пускаем
		if strings.TrimSpace(s) == "" {
			v.addErr(name.Line, "metadata.name has invalid format ''")
		}
	}

	// namespace optional (string)
	if _, ns := mapGet(n, "namespace"); ns != nil {
		// Проверим тип/скалярность; сообщение об ошибке сформирует expectScalarString
		_, _ = expectScalarString(ns, "metadata.namespace", v)
	}

	// labels optional (object of string:string)
	if _, labels := mapGet(n, "labels"); labels != nil {
		if !expectKind(labels, yaml.MappingNode, "metadata.labels", v) {
			return
		}
		for i := 0; i < len(labels.Content)-1; i += 2 {
			k := labels.Content[i]
			val := labels.Content[i+1]
			if val.Kind != yaml.ScalarNode {
				v.addErr(val.Line, "metadata.labels value must be string")
				continue
			}
			_ = k // ключи и значения допускаем любые строки
		}
	}
}

func validatePodSpec(n *yaml.Node, v *validator) {
	if !expectKind(n, yaml.MappingNode, "spec", v) {
		return
	}

	// os optional (either scalar "linux|windows" OR mapping {name: ...})
	if _, osNode := mapGet(n, "os"); osNode != nil {
		validatePodOS(osNode, v)
	}

	// containers required: array of Container
	_, containers := mapGet(n, "containers")
	if containers == nil {
		v.addErr(0, "spec.containers is required")
	} else {
		if !expectKind(containers, yaml.SequenceNode, "spec.containers", v) {
			return
		}
		seenNames := map[string]bool{}
		for _, c := range containers.Content {
			validateContainer(c, v, seenNames)
		}
	}
}

func validatePodOS(n *yaml.Node, v *validator) {
	switch n.Kind {
	case yaml.ScalarNode:
		val := strings.ToLower(strings.TrimSpace(n.Value))
		if val != "linux" && val != "windows" {
			v.addErr(n.Line, fmt.Sprintf("os has unsupported value '%s'", n.Value))
		}
	case yaml.MappingNode:
		if _, name := mapGet(n, "name"); name == nil {
			v.addErr(0, "spec.os.name is required")
		} else if s, ok := expectScalarString(name, "spec.os.name", v); ok {
			val := strings.ToLower(strings.TrimSpace(s))
			if val != "linux" && val != "windows" {
				v.addErr(name.Line, fmt.Sprintf("spec.os.name has unsupported value '%s'", s))
			}
		}
	default:
		v.addErr(n.Line, "spec.os must be string")
	}
}

func validateContainer(n *yaml.Node, v *validator, seen map[string]bool) {
	if !expectKind(n, yaml.MappingNode, "spec.containers[]", v) {
		return
	}

	// name required & snake_case & unique
	_, name := mapGet(n, "name")
	if name == nil {
		v.addErr(0, "spec.containers[].name is required")
	} else if s, ok := expectScalarString(name, "spec.containers[].name", v); ok {
		if strings.TrimSpace(s) == "" {
			// Пустая строка = требуемое поле отсутствует (ожидает тест)
			v.addErr(name.Line, "name is required")
		} else if !reSnake.MatchString(s) {
			v.addErr(name.Line, fmt.Sprintf("spec.containers[].name has invalid format '%s'", s))
		} else if seen[s] {
			v.addErr(name.Line, fmt.Sprintf("spec.containers[].name has invalid format '%s'", s)) // уникальность
		} else {
			seen[s] = true
		}
	}

	// image required & must be from registry.bigbrother.io and have :tag
	_, image := mapGet(n, "image")
	if image == nil {
		v.addErr(0, "spec.containers[].image is required")
	} else if s, ok := expectScalarString(image, "spec.containers[].image", v); ok {
		if !strings.HasPrefix(s, "registry.bigbrother.io/") {
			v.addErr(image.Line, fmt.Sprintf("image has invalid format '%s'", s))
		} else {
			// must contain tag after last slash
			lastSlash := strings.LastIndex(s, "/")
			lastColon := strings.LastIndex(s, ":")
			if lastColon <= lastSlash || lastColon == len(s)-1 {
				v.addErr(image.Line, fmt.Sprintf("image has invalid format '%s'", s))
			}
		}
	}

	// ports optional (array of ContainerPort)
	if _, ports := mapGet(n, "ports"); ports != nil {
		if !expectKind(ports, yaml.SequenceNode, "spec.containers[].ports", v) {
			return
		}
		for _, p := range ports.Content {
			validateContainerPort(p, v)
		}
	}

	// readinessProbe optional
	if _, rp := mapGet(n, "readinessProbe"); rp != nil {
		validateProbe(rp, v, "spec.containers[].readinessProbe")
	}

	// livenessProbe optional
	if _, lp := mapGet(n, "livenessProbe"); lp != nil {
		validateProbe(lp, v, "spec.containers[].livenessProbe")
	}

	// resources required
	_, res := mapGet(n, "resources")
	if res == nil {
		v.addErr(0, "spec.containers[].resources is required")
	} else {
		validateResources(res, v)
	}
}

func validateContainerPort(n *yaml.Node, v *validator) {
	if !expectKind(n, yaml.MappingNode, "spec.containers[].ports[]", v) {
		return
	}
	// containerPort required int 1..65535
	_, cp := mapGet(n, "containerPort")
	if cp == nil {
		v.addErr(0, "spec.containers[].ports[].containerPort is required")
	} else if val, ok := expectScalarInt(cp, "spec.containers[].ports[].containerPort", v); ok {
		if val <= 0 || val >= 65536 {
			v.addErr(cp.Line, "spec.containers[].ports[].containerPort value out of range")
		}
	}

	// protocol optional: TCP|UDP
	if _, proto := mapGet(n, "protocol"); proto != nil {
		s, ok := expectScalarString(proto, "spec.containers[].ports[].protocol", v)
		if ok {
			if s != "TCP" && s != "UDP" {
				v.addErr(proto.Line, fmt.Sprintf("spec.containers[].ports[].protocol has unsupported value '%s'", s))
			}
		}
	}
}

func validateProbe(n *yaml.Node, v *validator, field string) {
	if !expectKind(n, yaml.MappingNode, field, v) {
		return
	}
	_, httpGet := mapGet(n, "httpGet")
	if httpGet == nil {
		v.addErr(0, field+".httpGet is required")
		return
	}
	if !expectKind(httpGet, yaml.MappingNode, field+".httpGet", v) {
		return
	}

	// path required, absolute
	_, path := mapGet(httpGet, "path")
	if path == nil {
		v.addErr(0, "path is required")
	} else if s, ok := expectScalarString(path, "path", v); ok {
		if !strings.HasPrefix(s, "/") || s == "" {
			v.addErr(path.Line, fmt.Sprintf("path has invalid format '%s'", s))
		}
	}

	// port required int 1..65535
	_, port := mapGet(httpGet, "port")
	if port == nil {
		v.addErr(0, "port is required")
	} else if val, ok := expectScalarInt(port, "port", v); ok {
		if val <= 0 || val >= 65536 {
			v.addErr(port.Line, "port value out of range")
		}
	}
}

func validateResources(n *yaml.Node, v *validator) {
	if !expectKind(n, yaml.MappingNode, "spec.containers[].resources", v) {
		return
	}

	// requests optional
	if _, r := mapGet(n, "requests"); r != nil {
		validateResourceMap(r, v, "spec.containers[].resources.requests")
	}
	// limits optional
	if _, l := mapGet(n, "limits"); l != nil {
		validateResourceMap(l, v, "spec.containers[].resources.limits")
	}
}

func validateResourceMap(n *yaml.Node, v *validator, field string) {
	if !expectKind(n, yaml.MappingNode, field, v) {
		return
	}
	for i := 0; i < len(n.Content)-1; i += 2 {
		k := n.Content[i].Value
		val := n.Content[i+1]
		switch k {
		case "cpu":
			if iv, ok := expectScalarInt(val, field+".cpu", v); ok {
				if iv < 0 {
					v.addErr(val.Line, field+".cpu value out of range")
				}
			}
		case "memory":
			if s, ok := expectScalarString(val, field+".memory", v); ok {
				if !reMem.MatchString(s) {
					v.addErr(val.Line, fmt.Sprintf(field+".memory has invalid format '%s'", s))
				}
			}
		default:
			// Игнорируем другие ключи
		}
	}
}

// --- main ---

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stdout, "usage: yamlvalid <path-to-yaml>")
		os.Exit(2)
	}
	path := os.Args[1]
	filename := filepath.Base(path)

	content, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(os.Stdout, "%s: %v\n", filename, err)
		os.Exit(1)
	}

	var root yaml.Node
	if err := yaml.Unmarshal(content, &root); err != nil {
		// Попробуем вытащить строку из ошибки yaml (если есть)
		line := extractLine(err)
		if line > 0 {
			fmt.Fprintf(os.Stdout, "%s:%d %v\n", filename, line, err)
		} else {
			fmt.Fprintf(os.Stdout, "%s: %v\n", filename, err)
		}
		os.Exit(1)
	}

	v := &validator{filename: filename}

	// root.Kind == DocumentNode, root.Content = docs
	if len(root.Content) == 0 {
		fmt.Fprintf(os.Stderr, "%s: empty document\n", filename)
		os.Exit(1)
	}
	for _, doc := range root.Content {
		validateTop(doc, v)
	}

	v.printAndExit()
}

func extractLine(err error) int {

	msg := err.Error()
	const token = "line "
	i := strings.Index(msg, token)
	if i == -1 {
		return 0
	}
	j := i + len(token)
	var n int
	_, scanErr := fmt.Sscan(msg[j:], &n)
	if scanErr != nil || n <= 0 {
		return 0
	}
	return n
}
