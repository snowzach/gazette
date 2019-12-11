package table

import (
	"github.com/gogo/protobuf/proto"
	"go.gazette.dev/core/message"
	"go.gazette.dev/core/table/ddl"
)

// Ways of writing to tables:
// * stdout / stream => gazctl
//   - AppendService -like batching of rows
//   - Bad rows abort stream OR written to dead row queue.

type Row interface {
	message.Message
	proto.Message

	TableSpec() ddl.TableSpec
	Validate() error
	VisitPartitionFields(func(field, value string))
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
