package table

import (
	"bufio"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/message"
	pt "go.gazette.dev/core/table/protocol"
)

// Path hierarchy.
// Cluster <- This is a tenant / seat / account. Users are namespaced at the cluster level.
// Database <- Maps to bigger units within the org (ie platoons). Some common definitions can live here? Also a unit of permission.
// Schema <- 1:1 with larger applications / service; one team could own many schemas. Common definitions also live here, as do permissions.
// Make this dynamic? why force a fixed hierarchy... allow arbitrary nesting!

const (
	schemasPrefix = "/schemas/"
	tablesPrefix  = "/tables/"
)

type Table struct {
	Spec   pt.TableSpec
	NewRow func() Row
}

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

	WritePartitionFields(FieldWriter) error
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
