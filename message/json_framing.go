package message

import (
	"bufio"
	"bytes"
	"encoding/json"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"go.gazette.dev/core/labels"
)

type jsonFraming struct{}

// ContentType returns labels.ContentType_JSONLines.
func (*jsonFraming) ContentType() string { return labels.ContentType_JSONLines }

// Marshal implements Framing.
func (*jsonFraming) Marshal(msg Frameable, bw *bufio.Writer) error {
	if protoMsg, ok := msg.(proto.Message); ok {

		// Preserve the naming convention used in the protobuf definition.
		// This is explicitly at odds with the official PB <-> JSON mapping,
		// which defines that "my_pb_field" => "myPBField".

		// The rationale is that the end-user may be reading and writing JSON
		// messages which only happen to be transiting through a parsed protobuf
		// representation. By keeping original naming, under_score and camelCase
		// conventions can each be supported by using the corresponding convention
		// in the protobuf definition itself.
		var marshaler = jsonpb.Marshaler{OrigName: true}

		if err := marshaler.Marshal(bw, protoMsg); err != nil {
			return err
		}
		return bw.WriteByte('\n')
	} else {
		return json.NewEncoder(bw).Encode(msg)
	}
}

// NewUnmarshalFunc returns an UnmarshalFunc which decodes JSON messages from the Reader.
func (*jsonFraming) NewUnmarshalFunc(r *bufio.Reader) UnmarshalFunc {
	// We cannot use json.NewDecoder, as it buffers internally beyond the
	// precise boundary of a JSON message.
	return func(f Frameable) error {
		if l, err := UnpackLine(r); err != nil {
			return err
		} else if protoMsg, ok := f.(proto.Message); ok {
			// Allow unknown fields, in order to permit limited schema evolution
			// over time (fields may be added and removed, but never renamed or
			// reused).
			var unmarshaler = jsonpb.Unmarshaler{AllowUnknownFields: true}

			return unmarshaler.Unmarshal(bytes.NewReader(l), protoMsg)
		} else {
			return json.Unmarshal(l, f)
		}
	}
}

func init() { RegisterFraming(new(jsonFraming)) }
