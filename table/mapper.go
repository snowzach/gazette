package table

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"go.gazette.dev/core/broker/client"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/labels"
	"go.gazette.dev/core/message"
)

type Mapper struct {
	ctx          context.Context
	client       pb.RoutedJournalClient
	pollInterval time.Duration
	lists        map[string]*client.PolledList
	mu           sync.Mutex
}

func (m *Mapper) Map(mappable message.Mappable) (_ pb.Journal, contentType string, _ error) {
	if parts, err := m.mapRowToJournals(mappable.(Row)); err != nil {
		return "", "", err
	} else {
		var ind = 0 // TODO(johnny).

		return parts[ind].Spec.Name,
			parts[ind].Spec.LabelSet.ValueOf(labels.ContentType), nil
	}
}

func (m *Mapper) mapRowToJournals(row Row) ([]pb.ListResponse_Journal, error) {
	var tbl = row.TableSpec()

	// Build the qualified table name, i.e. "a/namespace/AndTable".
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

	// Revision that |list| must have read through in order to continue.
	var revision int64 = 1

	for {
		var parts = list.List()

		if begin, end := prefixedRange(b.String(), parts.Journals); begin != end {
			return parts.Journals[begin:end], nil // All done.
		}

		// Do we need to refresh our cached partition list?
		if parts.Header.Etcd.Revision >= revision {
			// Pass.
		} else if err := list.Refresh(revision); err != nil {
			err = fmt.Errorf("refresh of %s partitions @%d: %w", b.String(), revision, err)
			return nil, err
		} else {
			continue
		}

		if len(parts.Journals) >= int(tbl.PartitionLimit) {
			return nil, MissingPartition{Prefix: b.String()}
		}

		// We attempt to create new journal(s) for this partition.
		// It's expected that this Apply RPC may race with other processors.
		var labels pb.LabelSet
		// TODO: name label? Namespace?

		row.VisitPartitionFields(func(field, value string) {
			labels.AddValue("table.gazette.dev/field/"+field, value)
		})
		var spec = pb.UnionJournalSpecs(*tbl.Template, pb.JournalSpec{
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
			revision = resp.Header.Etcd.Revision
		default:
			err = fmt.Errorf("creating table partition %s: %w", spec.Name, err)
			return nil, err
		}
	}
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
