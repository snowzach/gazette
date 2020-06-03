package main

import (
	"context"
	"encoding/json"
	"os"

	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	pc "go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/consumer/recoverylog"
	mbp "go.gazette.dev/core/mainboilerplate"
)

type cmdShardsRecover struct {
	Selector string `long:"selector" short:"l" required:"false" description:"Label Selector query to filter on"`
	Hints    string `long:"fsm-hints" required:"false" description:"Path to JSON-encoded FSM hints to recover"`

	hints recoverylog.FSMHints
}

func init() {
	_ = mustAddCmd(cmdShards, "recover", "Recover shard stores to local disk", `
Recover shard databases to the local disk, from their recovery logs.

Use --selector to supply a LabelSelector which determines the set of shards
to recover. See gazctl shards list for further details on usage.

As an alternative, use --fsm-hints to provide specific JSON-encoded FSM hints to recover.
`, &cmdShardsRecover{})
}

func (cmd *cmdShardsRecover) Execute([]string) error {
	startup()

	if cmd.Hints != "" {
		cmd.recoverFromHints()
	} else {
		cmd.recoverFromSelector()
	}
	return nil
}

func (cmd *cmdShardsRecover) recoverFromSelector() {
	var ctx = context.Background()
	var resp = listShards(cmd.Selector)
	var sc = shardsCfg.Consumer.MustRoutedShardClient(ctx)

	for _, shard := range resp.Shards {
		var hints, err = sc.GetHints(ctx, &pc.GetHintsRequest{Shard: shard.Spec.Id})
		mbp.Must(err, "failed to fetch FSM hints")

		if hints.PrimaryHints.Hints == nil {
			log.WithField("shard", shard.Spec.Id).Warn("shard has no primary FSM hints; skipping...")
			continue
		}
		cmd.recover(shard.Spec.Id, *hints.PrimaryHints.Hints)
	}
}

func (cmd *cmdShardsRecover) recoverFromHints() {
	var f, err = os.Open(cmd.Hints)
	mbp.Must(err, "failed to open FSM hints")

	var hints recoverylog.FSMHints
	mbp.Must(json.NewDecoder(f).Decode(&hints), "failed to parse FSM hints")

	cmd.recover("recovered-shard-store", hints)
}

func (cmd *cmdShardsRecover) recover(id pc.ShardID, hints recoverylog.FSMHints) {
	var ctx = context.Background()
	var rjc = shardsCfg.Broker.MustRoutedJournalClient(ctx)
	var ajc = client.NewAppendService(ctx, rjc)
	var player = recoverylog.NewPlayer()
	go func() { player.FinishAtWriteHead() }()

	mbp.Must(player.Play(ctx, hints, id.String(), ajc), "failed to playback recovery log")
	log.WithField("shard", id).Info("completed playback")
}
