package table

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/message"
)

// Ways of writing to tables:
// * stdout / stream => gazctl
//   - AppendService -like batching of rows
//   - Bad rows abort stream OR written to dead row queue.

type TableSpec struct {
	Namespace             string
	Name                  string
	MaxPartitionsToCreate int
	JournalSpecModel      *pb.JournalSpec
}

type Row interface {
	message.Message
	proto.Message

	TableSpec() *TableSpec
	Validate() error
	VisitPartitionFields(func(field, value string))
}

type rowMapper struct {
	ctx          context.Context
	client       pb.RoutedJournalClient
	pollInterval time.Duration

	lists map[string]*client.PolledList
	mu    sync.Mutex
}

func (m *rowMapper) Map(mappable message.Mappable) (_ pb.Journal, contentType string, _ error) {
	var row = mappable.(Row)
	var tbl = row.TableSpec()

	// Build the namespaced table prefix.
	// Eg, "a/namespace/AndTable".
	var b = new(bytes.Buffer)
	b.WriteString(tbl.Namespace)
	b.WriteByte('/')
	b.WriteString(tbl.Name)

	// Fetch or create a PolledList for this table.
	m.mu.Lock()
	var list, ok = m.lists[string(b.Bytes())]
	if !ok {
		list = client.NewPolledList(m.ctx, m.client, m.pollInterval, pb.ListRequest{
			Selector: pb.LabelSelector{Include: pb.MustLabelSet("prefix", b.String()+"/")},
		})
		m.lists[string(b.Bytes())] = list
	}
	m.mu.Unlock()

	// Extend table name with partitioned fields to obtain the full journal prefix.
	// Eg, "a/namespace/AndTable/foo=bar/baz=bing".
	row.VisitPartitionFields(func(field, value string) {
		b.WriteByte('/')
		b.WriteString(field)
		b.WriteByte('=')
		b.WriteString(value)
	})

	// Find partitions matching the prefix. If none exists, we may create one.
	var readThroughRevision int64 = 1
	var parts = list.List()
	var begin, end = prefixedRange(b.String(), parts.Journals)

	for begin == end {
		// Do we need to refresh our cached partition list?
		if parts.Header.Etcd.Revision < readThroughRevision {
			if err := list.Refresh(readThroughRevision); err != nil {
				err = fmt.Errorf("refresh of %s partitions @%d: %w",
					b.String(), readThroughRevision, err)
				return "", "", err
			}
			parts = list.List()
			continue
		}

		if len(parts.Journals) >= tbl.MaxPartitionsToCreate {
			return "", "", MissingPartition{Prefix: b.String()}
		}

		// We must create a new partition.
		var labels pb.LabelSet
		// TODO: name label? Namespace?

		row.VisitPartitionFields(func(field, value string) {
			labels.AddValue("table.gazette.dev/field/"+field, value)
		})

		var spec = pb.UnionJournalSpecs(*tbl.JournalSpecModel, pb.JournalSpec{
			Name:     pb.Journal(b.String() + "/part=000"),
			LabelSet: labels,
		})

		resp, err := client.ApplyJournals(m.ctx, m.client, &pb.ApplyRequest{
			Changes: []pb.ApplyRequest_Change{
				{
					ExpectModRevision: 0,
					Upsert:            &spec,
				},
			}})

		switch err {
		case nil, client.ErrEtcdTransactionFailed:
			readThroughRevision = resp.Header.Etcd.Revision
		default:
			err = fmt.Errorf("creating table partition %s: %w", spec.Name, err)
			return "", "", err
		}
	}

	// Select a partition among [begin, end).

	// Publish row as uncommitted message to its partition.

}

func Insert(ctx context.Context, row Row, mapping message.MappingFunc, pub *message.Publisher) error {
	var _, err = pub.PublishUncommitted(mapping, row)
	return err
}

/*
type foo struct {
	pub *message.Publisher
}

func (foo) Transaction(ctx context.Context, req *pt.InsertRequest) (*pt.InsertResponse, error) {
	var err error
	var da ptypes.DynamicAny

	for i := range req.Rows {
		// Do we need to allocate a message of a different type?
		// (If not, re-use a previous allocated instance).
		if i == 0 || req.Rows[i].TypeUrl != req.Rows[i-1].TypeUrl {
			if da.Message, err = ptypes.EmptyAny(&req.Rows[i]); err != nil {
				return nil, err
			}
		}
		if err = ptypes.UnmarshalAny(&req.Rows[i], &da); err != nil {
			return nil, err
		}

		// Validate the row.
		var row, ok = da.Message.(Row)
		if !ok {
			return nil, fmt.Errorf("message %#v does not implement Row", da)
		} else if err = row.Validate(); err != nil {
			return nil, fmt.Errorf("validating row %d: %w", i, err)
		}

	}
}
*/

func prefixedRange(prefix string, parts []pb.ListResponse_Journal) (begin, end int) {
	begin = sort.Search(len(parts), func(i int) bool {
		return parts[i].Spec.Name.String() >= prefix
	})
	for end = begin; end != len(parts) &&
		strings.HasPrefix(parts[end].Spec.Name.String(), prefix); end++ {
	}
	return
}

type MissingPartition struct {
	Prefix string
}

func (mp MissingPartition) Error() string {
	return fmt.Sprintf("table partition not found: %s", mp.Prefix)
}
