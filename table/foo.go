package table

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"

	ptypes "github.com/gogo/protobuf/types"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/message"
	pt "go.gazette.dev/core/table/protocol"
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

type Row interface {
	message.Message

	Validate() error
	WritePartitionFields(FieldWriter) error
}

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

		// Map row to a journal prefix.
		// Map journal prefix to a partition.
		// - (Always select the same partition for a given prefix)
		// Create new partition if requested, and prefix doesn't exist.

		// Publish row as uncommitted message to its partition.
	}


}

func insertRow(tbl *Table, r Row, partsFn message.PartitionsFunc) error {
	var b strings.Builder

	// Build the fully-qualified table name.
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
}

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
