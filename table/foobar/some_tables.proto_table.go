
package foobar;

import (
	"strconv"

	"go.gazette.dev/core/table/ddl"
	"go.gazette.dev/core/broker/protocol"
)


func (m *AccessLog) TableSpec() ddl.TableSpec {
	return ddl.TableSpec{
		Namespace: "foo/bar",
		Name: "AccessLog",
		Template: &protocol.JournalSpec{Name:"foo/bar/AccessLog", Replication:1, LabelSet:protocol.LabelSet{Labels:[]protocol.Label{protocol.Label{Name:"a-name", Value:"a-value"}, protocol.Label{Name:"other-name", Value:"other-value"}}}, Fragment:protocol.JournalSpec_Fragment{Length:12345, CompressionCodec:2, Stores:[]protocol.FragmentStore{"s3://my-bucket/and-path/"}, RefreshInterval:300000000000, Retention:0, FlushInterval:0, PathPostfixTemplate:"date={{ .Spool.FirstAppendTime.Format \"2006-01-02\" }}"}, Flags:0, MaxAppendRate:2345},
		PartitionLimit: 50,
	}
}

func (m *AccessLog) VisitPartitionFields(cb func(field, value string)) {

	cb("host", m.Host)
	cb("status", strconv.FormatInt(int64(m.Status), 10))
}

