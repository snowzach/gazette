package main

import (
	"context"

	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	"go.gazette.dev/core/broker/fragment"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type cmdShardsPrune struct {
	pruneConfig
}

func init() {
	_ = mustAddCmd(cmdShards, "prune", "Removes fragments of a hinted recovery log which are no longer needed", `
Recovery logs capture every write which has ever occurred in a Shard DB.
This includes all prior writes of client keys & values, and also RocksDB
compactions, which can significantly inflate the total volume of writes
relative to the data currently represented in a RocksDB.

Prune log examines the provided hints to identify Fragments of the log
which have no intersection with any live files of the DB, and can thus
be safely deleted.
`, &cmdShardsPrune{})
}

func (cmd *cmdShardsPrune) Execute([]string) error {
	startup()

	var ctx = context.Background()
	var m = shardsPruneMetrics{}
	var logSegmentSets = make(map[pb.Journal]recoverylog.SegmentSet)

	for _, shard := range listShards(cmd.Selector).Shards {
		m.shardsTotal++
		var lastHints = fetchLastHints(ctx, shard.Spec.Id)

		// We require that we see hints for _all_ shards before we may make _any_ deletions.
		// This is because shards could technically include segments from any log,
		// and without comprehensive hints which are proof-positive that _no_ shard
		// references a given journal fragment, we cannot be sure it's safe to remove.
		if lastHints == nil {
			log.Panicf("shard %s is missing backup hints", shard.Spec.Id)
		}

		var _, segments, err = lastHints.LiveLogSegments()
		if err != nil {
			mbp.Must(err, "unable to fetch hint segments")
		}

		// Zero the LastOffset of the final hinted Segment. This has the effect of implicitly
		// intersecting with all fragments having offsets greater than its FirstOffset.
		// We want this behavior because playback will continue to read offsets & Fragments
		// after reading past the final hinted Segment.
		if len(segments) != 0 {
			segments[len(segments)-1].LastOffset = 0
		}

		for _, segment := range segments {
			var set = logSegmentSets[segment.Log]

			// set.Add() will return an error if we attempt to add a segment having a
			// greater SeqNo and LastOffset != 0, to a set already having a lesser
			// SeqNo and LastOffset == 0. Or, if FirstSeqNo is equal, it will replace
			// a zero LastOffset with a non-zero one (which is not what we want
			// in this case).
			//
			// So, zero LastOffset here if |segment| isn't strictly less than
			// and non-overlapping with the last set element.
			//
			// Conceptually, we've defined a "tail" of the log where we won't delete
			// anything, and are letting the /oldest/ hints bound how early that tail
			// portion begins.
			if l := len(set); l != 0 && set[l].FirstSeqNo <= segment.LastSeqNo {
				segment.LastOffset = 0
			}

			mbp.Must(set.Add(segment), "failed to add segment", "log", segment.Log)
			logSegmentSets[segment.Log] = set
		}
	}

	for journal, segments := range logSegmentSets {
		for _, f := range fetchFragments(ctx, journal) {
			var spec = f.Spec

			m.fragmentsTotal++
			m.bytesTotal += spec.ContentLength()

			if len(segments.Intersect(journal, spec.Begin, spec.End)) == 0 {
				log.WithFields(log.Fields{
					"log":  spec.Journal,
					"name": spec.ContentName(),
					"size": spec.ContentLength(),
					"mod":  spec.ModTime,
				}).Info("pruning fragment")

				m.fragmentsPruned++
				m.bytesPruned += spec.ContentLength()

				if !cmd.DryRun {
					mbp.Must(fragment.Remove(ctx, spec), "error removing fragment", "path", spec.ContentPath())
				}
			}
		}
		logShardsPruneMetrics(m, journal.String(), "finished pruning log")
	}
	logShardsPruneMetrics(m, "", "finished pruning logs for all shards")
	return nil
}

func fetchLastHints(ctx context.Context, id pc.ShardID) *recoverylog.FSMHints {
	var req = &pc.GetHintsRequest{
		Shard: id,
	}

	var resp, err = consumer.FetchHints(ctx, shardsCfg.Consumer.MustShardClient(ctx), req)
	mbp.Must(err, "failed to fetch hints")
	if resp.Status != pc.Status_OK {
		log.Panic("failed to fetch hints ", resp.Status.String())
	}

	for i := len(resp.BackupHints) - 1; i >= 0; i-- {
		if resp.BackupHints[i].Hints != nil {
			return resp.BackupHints[i].Hints
		}
	}

	return nil
}

func fetchFragments(ctx context.Context, journal pb.Journal) []pb.FragmentsResponse__Fragment {
	var err error
	var req = pb.FragmentsRequest{
		Journal: journal,
	}
	var brokerClient = journalsCfg.Broker.MustRoutedJournalClient(ctx)

	resp, err := client.ListAllFragments(ctx, brokerClient, req)
	mbp.Must(err, "failed to fetch fragments")

	return resp.Fragments
}

type shardsPruneMetrics struct {
	shardsTotal     int64
	fragmentsTotal  int64
	fragmentsPruned int64
	bytesTotal      int64
	bytesPruned     int64
}

func logShardsPruneMetrics(m shardsPruneMetrics, journal, message string) {
	var fields = log.Fields{
		"shardsTotal":     m.shardsTotal,
		"fragmentsTotal":  m.fragmentsTotal,
		"fragmentsPruned": m.fragmentsPruned,
		"fragmentsKept":   m.fragmentsTotal - m.fragmentsPruned,
		"bytesTotal":      m.bytesTotal,
		"bytesPruned":     m.bytesPruned,
		"bytesKept":       m.bytesTotal - m.bytesPruned,
	}
	if journal != "" {
		fields["journal"] = journal
	}
	log.WithFields(fields).Info(message)
}
