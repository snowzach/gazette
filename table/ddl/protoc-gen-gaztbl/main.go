package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"text/template"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/protoc-gen-gogo/descriptor"
	"github.com/gogo/protobuf/protoc-gen-gogo/generator"
	plugin_go "github.com/gogo/protobuf/protoc-gen-gogo/plugin"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/table/ddl"
)

var tpl = template.Must(template.New("file").Funcs(template.FuncMap{
	"base": path.Base,
}).Parse(`
package {{ base .Options.GoPackage }};

import (
	"strconv"

	"go.gazette.dev/core/table/ddl"
	"go.gazette.dev/core/broker/protocol"
)

{{ range $tbl := .Tables }}
func (m *{{ $tbl.Name }}) TableSpec() ddl.TableSpec {
	return ddl.TableSpec{
		Namespace: "{{ $tbl.Namespace }}",
		Name: "{{ $tbl.Name }}",
		Template: {{ printf "%#v" $tbl.Template }},
		PartitionLimit: {{ $tbl.PartitionLimit }},
	}
}

func (m *{{ $tbl.Name }}) VisitPartitionFields(cb func(field, value string)) {
{{ range $fld := $tbl.Fields }}
{{- if not $fld.Partitioned }}
{{- else if $fld.Desc.IsBool }}
	if m.{{ $fld.Name }} { cb("{{ $fld.Desc.Name }}", "1") } else { cb("{{ $fld.Desc.Name }}", "0") }
{{- else if $fld.Desc.IsString }}
	cb("{{ $fld.Desc.Name }}", m.{{ $fld.Name }})
{{- else if $fld.Desc.IsEnum }}
	cb("{{ $fld.Desc.Name }}", m.{{ $fld.Name }}.String())
{{- else }}
	cb("{{ $fld.Desc.Name }}", strconv.FormatInt(int64(m.{{ $fld.Name }}), 10))
{{- end -}}
{{ end }}
}
{{ end }}
`))

type FileContext struct {
	*descriptor.FileDescriptorProto
}

func (c FileContext) Tables() ([]TableContext, error) {
	var out []TableContext

	for _, msg := range c.MessageType {
		if msg.Options == nil {
			continue
		} else if v, err := proto.GetExtension(msg.Options, ddl.E_Spec); err != nil {
			continue
		} else {
			var spec = *v.(*ddl.TableSpec)

			spec.Namespace = strings.ReplaceAll(*c.Package, ".", "/")
			spec.Name = *msg.Name
			spec.Template.Name = pb.Journal(path.Join(spec.Namespace, spec.Name))

			if err = spec.Template.Validate(); err != nil {
				return nil, fmt.Errorf("validating JournalSpec %s template: %w",
					spec.Template.Name, err)
			}

			out = append(out, TableContext{
				Desc:      msg,
				TableSpec: spec,
			})
		}
	}
	return out, nil
}

type TableContext struct {
	ddl.TableSpec
	Desc *descriptor.DescriptorProto
}

func (c TableContext) Fields() ([]FieldContext, error) {
	var out []FieldContext

	for _, fld := range c.Desc.Field {
		var spec ddl.TableFieldSpec

		if fld.Options != nil {
			if v, err := proto.GetExtension(fld.Options, ddl.E_Field); err == nil {
				spec = *v.(*ddl.TableFieldSpec)
			}
		}
		spec.Name = generator.CamelCase(*fld.Name)

		if spec.Partitioned {
			if fld.IsRepeated() {
				return nil, fmt.Errorf("cannot partition repeated field: %s", fld)
			}
			if fld.IsScalar() || fld.IsString() || fld.IsEnum() || fld.IsBool() {
				// Okay to partition.
			} else {
				return nil, fmt.Errorf("cannot partition field type: %s", fld)
			}
		}

		out = append(out, FieldContext{
			TableFieldSpec: spec,
			Desc:           fld,
		})
	}
	return out, nil
}

type FieldContext struct {
	ddl.TableFieldSpec
	Desc *descriptor.FieldDescriptorProto
}

type codeGen struct {
	req  plugin_go.CodeGeneratorRequest
	resp plugin_go.CodeGeneratorResponse
}

/*
func (g *generator) generateTable(file *descriptor.FileDescriptorProto, desc *descriptor.DescriptorProto, spec ddl.TableSpec) {
	log.Println("validate: ", spec.JournalSpec.Validate())

		for _, field := range desc.Field {
			log.Println("field", *field.Name)

			if field.Options != nil {
				log.Println("Options", field.Options)

				var v, err = proto.GetExtension(field.Options, ddl.E_Field)
				log.Println("ext ", v.(*ddl.TableFieldSpec), err)
			}
		}
}
*/

func main() {
	var g codeGen

	if in, err := ioutil.ReadAll(os.Stdin); err != nil {
		log.Fatalf("reading stdin: %v\n", err)
	} else if err = proto.Unmarshal(in, &g.req); err != nil {
		log.Fatalf("unmarshalling CodeGeneratorRequest: %v\n", err)
	}

	for _, file := range g.req.ProtoFile {
		if !stringIn(*file.Name, g.req.FileToGenerate) {
			continue
		}

		var bw bytes.Buffer
		if err := tpl.Execute(&bw, FileContext{file}); err != nil {
			g.resp.Error = proto.String(err.Error())
			break
		}

		g.resp.File = append(g.resp.File, &plugin_go.CodeGeneratorResponse_File{
			Name:    proto.String(path.Join(path.Dir(*file.Name), path.Base(*file.Name)+"_table.go")),
			Content: proto.String(bw.String()),
		})
	}

	if out, err := proto.Marshal(&g.resp); err != nil {
		log.Fatalf("marshalling CodeGeneratorResponse: %v\n", err)
	} else if _, err = os.Stdout.Write(out); err != nil {
		log.Fatalf("writing stdout: %v\n", err)
	}
}

func stringIn(str string, strs []string) bool {
	for _, s := range strs {
		if s == str {
			return true
		}
	}
	return false
}
