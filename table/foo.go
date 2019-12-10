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

type PartitionField struct {
	Field, Value string
}

type FieldWriter interface {
	io.Writer
	io.StringWriter
	io.ByteWriter
}

type TableSpec struct {
	Namespace             string
	Name                  string
	MaxPartitionsToCreate int
}

type Row interface {
	message.Message
	proto.Message

	TableSpec() *TableSpec

	Validate() error
	WritePartitionFields(FieldWriter) error
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

	// Extract table name component.
	var b bytes.Buffer
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

		// Defer initial load until we're out of this critical section.
	}
	m.mu.Unlock()

	// Extend table name with partitions.
	if err := row.WritePartitionFields(&b); err != nil {
		return "", "", err
	}

	var readThroughRevision int64 = 1
	var parts = list.List()

	for {
		if parts.Header.Etcd.Revision < readThroughRevision {
			if err := list.Refresh(1); err != nil {
				return "", "",
					fmt.Errorf("refresh of table %s partitions: %w", b.String(), err)
			}
			parts = list.List()
			continue
		}

		// Find a matched partition.
		var begin, end = prefixedRange(b.String(), parts.Journals)

		if begin == end && len(parts.Journals) >= tbl.MaxPartitionsToCreate {
			return "", "", MissingPartition{
				Prefix: b.String(),
			}
		}

	}

	// Do we need to perform an initial load?

	// Map journal prefix to a partition.
	// - (Always select the same partition for a given prefix)
	// Create new partition if requested, and prefix doesn't exist.

	// Publish row as uncommitted message to its partition.

	// Build the fully-qualified table name.
	/*
		b.WriteString(tbl.Spec.Name.String())
		b.WriteByte('/')

		if err := r.WritePartitionFields(&b); err != nil {
			return fmt.Errorf("WritePartitionFields: %w", err)
		}

		// Find the set of partitions prefixed by |b|.
		var parts = partsFn()
		var begin, end = prefixedRange(b.String(), parts.Journals)

		if begin == end && len(parts.Journals) >= int(tbl.Spec.MaxPartitionsToAutoCreate) {
			return MissingPartition{
				Prefix: b.String(),
			}
		}

		// Task: we must identify a journal to which Row is directed.
		// - Ground out all partitioned fields, on tag order.
	*/
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
