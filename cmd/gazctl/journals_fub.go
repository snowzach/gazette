package main

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"
	"plugin"
	"reflect"
	"time"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/broker/client"
	mbp "go.gazette.dev/core/mainboilerplate"
	"go.gazette.dev/core/message"
	"go.gazette.dev/core/table"
)

type cmdJournalFub struct {
	Input  string `long:"input" short:"i" default:"-" description:"Input file path. Use '-' for stdin"`
	Plugin string `long:"plugin" description:"Plugin to load containing table schemas"`
	Table  string `long:"table" description:"Table name and namespace to append to"`

	mapping   message.MappingFunc
	appendSvc *client.AppendService
}

func init() {
	_ = mustAddCmd(cmdJournals, "fub", "Fub fubber", `
Foo bar baz	
	`, &cmdJournalFub{})
}

func (cmd *cmdJournalFub) Execute([]string) error {
	startup()

	if cmd.Plugin != "" {
		var _, err = plugin.Open(cmd.Plugin)
		mbp.Must(err, "failed to load schema plugin", cmd.Plugin)
	}

	var msgType = proto.MessageType(cmd.Table)
	if msgType == nil {
		log.Fatalf("schema %q is not a registered protobuf message type", cmd.Table)
	}
	var msg = reflect.New(msgType.Elem()).Interface().(table.Row)

	var err error
	var ctx = context.Background()
	var rjc = journalsCfg.Broker.MustRoutedJournalClient(ctx)
	var ajc = client.NewAppendService(ctx, rjc)
	var pub = message.NewPublisher(ajc, nil)
	var mapping = table.NewMapper(ctx, rjc, time.Minute).Map

	var fin = os.Stdin
	if cmd.Input != "-" {
		fin, err = os.Open(cmd.Input)
		mbp.Must(err, "failed to open input file")
	}
	var input = bufio.NewReaderSize(fin, 32*1024)
	var unmarshaler = jsonpb.Unmarshaler{AllowUnknownFields: true}
	var dec = json.NewDecoder(input)

	for {
		if err := unmarshaler.UnmarshalNext(dec, msg); err == io.EOF {
			break
		} else {
			mbp.Must(err, "failed to unmarshal message")
		}

		aa, err := pub.PublishCommitted(mapping, msg)
		mbp.Must(err, "failed to publish message")

		if log.GetLevel() >= log.DebugLevel {
			log.WithFields(log.Fields{
				"journal": aa.Request().Journal,
			}).Debug("mapped message to journal")
		}
	}

	for op := range ajc.PendingExcept("") {
		<-op.Done()
	}
	return nil
}
