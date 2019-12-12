
package main;

import (
	"strconv"

	"go.gazette.dev/core/table/ddl"
	"go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/message"
)

var _ = strconv.Atoi


func main() {}



func (m *GithubEvent) TableSpec() ddl.TableSpec {
	return ddl.TableSpec{
		Namespace: "foo/bar",
		Name: "GithubEvent",
		Template: &protocol.JournalSpec{Name:"foo/bar/GithubEvent", Replication:1, LabelSet:protocol.LabelSet{Labels:[]protocol.Label{protocol.Label{Name:"content-type", Value:"application/x-ndjson"}}}, Fragment:protocol.JournalSpec_Fragment{Length:268435456, CompressionCodec:2, Stores:[]protocol.FragmentStore{"file:///"}, RefreshInterval:300000000000, Retention:0, FlushInterval:0, PathPostfixTemplate:"date={{ .Spool.FirstAppendTime.Format \"2006-01-02\" }}"}, Flags:0, MaxAppendRate:0},
		PartitionLimit: 50,
	}
}

// TODO(johnny): use envoy validator
func (m *GithubEvent) Validate() error { return nil }

func (m *GithubEvent) NewAcknowledgement(protocol.Journal) message.Message {
	return new(GithubEvent)
}

func (m *GithubEvent) GetUUID() (uuid message.UUID) {
	copy(uuid[:], m.Record.GetUuid())
	return
}
func (m *GithubEvent) SetUUID(uuid message.UUID) {
	if m.Record == nil {
		m.Record = new(ddl.Record)
	}
	m.Record.Uuid = uuid[:]
}

func (m *GithubEvent) VisitPartitionFields(cb func(field, value string)) {

	cb("type", m.Type)
}

