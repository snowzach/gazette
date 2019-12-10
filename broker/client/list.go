package client

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	pb "go.gazette.dev/core/broker/protocol"
	"google.golang.org/grpc"
)

// PolledList periodically polls the List RPC with a given ListRequest, making
// its most recent result available via List. It's a building block for
// applications which interact with dynamic journal sets and wish to react
// to changes in their set membership over time.
//
//      var partitions, _ = protocol.ParseLabelSelector("logs=clicks, source=mobile")
//      var pl, err = NewPolledList(ctx, client, time.Minute, protocol.ListRequest{
//          Selector: partitions,
//      })
//
type PolledList struct {
	ctx       context.Context
	client    pb.JournalClient
	req       pb.ListRequest
	resp      atomic.Value  // Holds *pb.ListResponse.
	updateCh  atomic.Value  // Holds chan struct{}.
	refreshCh chan struct{} // Semaphore over calls to Refresh.
}

// NewPolledList returns a PolledList of the ListRequest which is initialized and
// ready for immediate use, and which will regularly refresh with the given Duration.
// An error encountered in the first List RPC is returned. Subsequent RPC errors
// will be logged as warnings and retried as part of regular refreshes.
func NewPolledList(ctx context.Context, client pb.JournalClient, dur time.Duration, req pb.ListRequest) *PolledList {
	var pl = &PolledList{
		ctx:       ctx,
		client:    client,
		req:       req,
		refreshCh: make(chan struct{}, 1),
	}
	pl.resp.Store(new(pb.ListResponse))
	pl.updateCh.Store(make(chan struct{}))
	pl.refreshCh <- struct{}{}

	go pl.periodicRefresh(dur)
	return pl
}

// List returns the most recent polled & merged ListResponse (see ListAllJournals).
// If a Refresh of this PolledList has yet to complete, a non-nil but zero valued
// ListResponse is returned.
func (pl *PolledList) List() *pb.ListResponse { return pl.resp.Load().(*pb.ListResponse) }

// UpdateCh returns a channel which will be closed on the next refresh of the
// PolledList.
func (pl *PolledList) UpdateCh() <-chan struct{} { return pl.updateCh.Load().(chan struct{}) }

// Refresh the PolledList. Only one call to Refresh will proceed at a time, and
// others will block. If the given revision is non-zero, it's attached to the
// List RPC as the minimum revision which must be read through before replying.
// If the cached response is current as-of the revision, Refresh returns immediately
// without invoking the List RPC. A revision of zero always invokes an RPC.
//
// The combination of these behaviors let PolledList safely handle a
// "thundering herd" of refreshes: if many clients concurrently request a
// Refresh at future revision R, only one will proceed and others will block.
// The proceeding Refresh will cache a response at revision N >= R on completion.
// Other calls will then unblock, observe revision N, and return immediately.
func (pl *PolledList) Refresh(revision int64) error {
	<-pl.refreshCh
	defer func() {
		pl.refreshCh <- struct{}{}
	}()

	if revision != 0 && pl.List().Header.Etcd.Revision >= revision {
		return nil // List is already current.
	}

	var req = pl.req
	if revision > req.MinRevision {
		req.MinRevision = revision
	}

	var resp, err = ListAllJournals(pl.ctx, pl.client, req)
	if err != nil {
		return err
	}
	pl.resp.Store(resp)

	// Signal that List has been updated.
	var ch = pl.updateCh.Load().(chan struct{})
	pl.updateCh.Store(make(chan struct{}))
	close(ch)

	return nil
}

func (pl *PolledList) periodicRefresh(dur time.Duration) {
	var ticker = time.NewTicker(dur)
	for {
		select {
		case <-ticker.C:
			if err := pl.Refresh(0); err != nil {
				log.WithFields(log.Fields{"err": err, "req": pl.req.String()}).
					Warn("periodic PolledList refresh failed (will retry)")
			}
		case <-pl.ctx.Done():
			ticker.Stop()
			return
		}
	}
}

// ListAllJournals performs multiple List RPCs, as required to join across multiple
// ListResponse pages, and returns the complete ListResponse of the ListRequest.
// Any encountered error is returned.
func ListAllJournals(ctx context.Context, client pb.JournalClient, req pb.ListRequest) (*pb.ListResponse, error) {
	var resp *pb.ListResponse

	for {
		// List RPCs may be dispatched to any broker.
		if r, err := client.List(pb.WithDispatchDefault(ctx), &req, grpc.FailFast(false)); err != nil {
			return resp, mapGRPCCtxErr(ctx, err)
		} else if err = r.Validate(); err != nil {
			return resp, err
		} else if r.Status != pb.Status_OK {
			return resp, errors.New(r.Status.String())
		} else {
			req.PageToken, r.NextPageToken = r.NextPageToken, ""

			if resp == nil {
				resp = r
			} else {
				resp.Journals = append(resp.Journals, r.Journals...)
			}
		}
		if req.PageToken == "" {
			break // All done.
		}
	}

	if dr, ok := client.(pb.DispatchRouter); ok {
		for _, j := range resp.Journals {
			dr.UpdateRoute(j.Spec.Name.String(), &j.Route)
		}
	}
	return resp, nil
}

// GetJournal retrieves the JournalSpec of the named Journal, or returns an error.
func GetJournal(ctx context.Context, jc pb.JournalClient, journal pb.Journal) (*pb.JournalSpec, error) {
	var lr, err = ListAllJournals(ctx, jc, pb.ListRequest{
		Selector: pb.LabelSelector{
			Include: pb.LabelSet{Labels: []pb.Label{{Name: "name", Value: journal.String()}}},
		},
	})
	if err == nil && len(lr.Journals) == 0 {
		err = errors.Errorf("named journal does not exist (%s)", journal)
	}
	if err != nil {
		return nil, err
	}
	return &lr.Journals[0].Spec, nil
}

// ApplyJournals applies journal changes detailed in the ApplyRequest via the broker Apply RPC.
// Changes are applied as a single Etcd transaction. If the change list is larger than an
// Etcd transaction can accommodate, ApplyJournalsInBatches should be used instead.
// ApplyResponse statuses other than OK are mapped to an error.
func ApplyJournals(ctx context.Context, jc pb.JournalClient, req *pb.ApplyRequest) (*pb.ApplyResponse, error) {
	return ApplyJournalsInBatches(ctx, jc, req, 0)
}

// ApplyJournalsInBatches is like ApplyJournals, but chunks the ApplyRequest
// into batches of the given size, which should be less than Etcd's maximum
// configured transaction size (usually 128). If size is 0 all changes will
// be attempted in a single transaction. Be aware that ApplyJournalsInBatches
// may only partially succeed, with some batches having applied and others not.
// The final ApplyResponse is returned, unless an error occurs.
// ApplyResponse statuses other than OK are mapped to an error.
func ApplyJournalsInBatches(ctx context.Context, jc pb.JournalClient, req *pb.ApplyRequest, size int) (*pb.ApplyResponse, error) {
	if size == 0 {
		size = len(req.Changes)
	}
	var offset = 0

	for {
		var r *pb.ApplyRequest
		if len(req.Changes[offset:]) > size {
			r = &pb.ApplyRequest{Changes: req.Changes[offset : offset+size]}
		} else {
			r = &pb.ApplyRequest{Changes: req.Changes[offset:]}
		}

		var resp, err = jc.Apply(pb.WithDispatchDefault(ctx), r, grpc.WaitForReady(true))
		if err != nil {
			return resp, err
		} else if err = resp.Validate(); err != nil {
			return resp, err
		} else if resp.Status != pb.Status_OK {
			return resp, errors.New(resp.Status.String())
		}

		if offset += len(r.Changes); offset == len(req.Changes) {
			return resp, nil
		}
	}
}

// ListAllFragments performs multiple Fragments RPCs, as required to join across multiple
// FragmentsResponse pages, and returns the completed FragmentResponse.
// Any encountered error is returned.
func ListAllFragments(ctx context.Context, client pb.RoutedJournalClient, req pb.FragmentsRequest) (*pb.FragmentsResponse, error) {
	var resp *pb.FragmentsResponse
	var routedCtx = pb.WithDispatchItemRoute(ctx, client, req.Journal.String(), false)

	for {
		if r, err := client.ListFragments(routedCtx, &req); err != nil {
			return resp, mapGRPCCtxErr(ctx, err)
		} else if err = r.Validate(); err != nil {
			return resp, err
		} else if r.Status != pb.Status_OK {
			return resp, errors.New(r.Status.String())
		} else {
			req.NextPageToken, r.NextPageToken = r.NextPageToken, 0

			if resp == nil {
				resp = r
			} else {
				resp.Fragments = append(resp.Fragments, r.Fragments...)
			}
			if req.NextPageToken == 0 {
				break // All done.
			}
		}
	}
	return resp, nil
}
