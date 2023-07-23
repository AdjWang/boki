package objectview

import (
	"context"
	"fmt"

	"cs.utexas.edu/zjia/faas/protocol"
	"cs.utexas.edu/zjia/faas/types"
	"github.com/pkg/errors"
)

type Synchronizer interface {
	// to user
	RegisterObject(obj ObjectView) error
	UnregisterObject(obj ObjectView) error
}

// implement Synchronizer
type SyncImpl struct {
	ctx context.Context
	env types.Environment

	streamHandlers map[uint64]streamHandler
}

type streamHandler struct {
	cancel     context.CancelFunc
	objectView ObjectView
}

func NewSynchronizer(ctx context.Context, env types.Environment) Synchronizer {
	return &SyncImpl{
		ctx:            ctx,
		env:            env,
		streamHandlers: make(map[uint64]streamHandler),
	}
}

func (s *SyncImpl) RegisterObject(obj ObjectView) error {
	if _, ok := s.streamHandlers[obj.GetTag()]; ok {
		return errors.Errorf("[ERROR] streamId=%d already registered", obj.GetTag())
	}
	handlerCtx, handlerCancel := context.WithCancel(context.Background())
	s.streamHandlers[obj.GetTag()] = streamHandler{cancel: handlerCancel, objectView: obj}
	logStream, err := s.env.AsyncSharedLogReadNextUntil(s.ctx, obj.GetTag(), protocol.MaxLogSeqnum)
	doneCh := make(chan struct{})
	errCh := make(chan error)
	go func(ctx context.Context) {
		var view interface{}
		for {
			var logEntry *types.LogEntryWithMeta
			select {
			case <-ctx.Done():
				return
			default:
				logStreamEntry, err := logStream.DequeueOrWaitForNextElement()
				if err != nil {
					output += fmt.Sprintf("[FAIL] log stream deqeque failed: %v\n", err)
					return []byte(output), nil
				}
				logEntry = logStreamEntry.(types.LogStreamEntry[types.LogEntryWithMeta]).LogEntry
				err = logStreamEntry.(types.LogStreamEntry[types.LogEntryWithMeta]).Err
			}
			if logEntry.AuxData != nil {
				decoded, err := obj.DecodeView(logEntry.AuxData)
				if err != nil {
					panic(err)
				}
				view = decoded
			} else {
				var auxTags []uint64
				auxTags, view = obj.UpdateView(view, logEntry)
				// some times we only need to grab log entries with out view
				// so output view can be nil
				if view != nil {
					encoded, err := obj.EncodeView(view)
					if err != nil {
						panic(err)
					}
					if err := s.env.SharedLogSetAuxDataWithShards(s.ctx, auxTags, logEntry.SeqNum, encoded); err != nil {
						panic(err)
					}
				}
			}
		}
	}(handlerCtx)
	select {
	case <-doneCh:
		return nil
	case err := <-errCh:
		return err
	}
}

func (s *SyncImpl) UnregisterObject(obj ObjectView) error {
	if streamHandler, ok := s.streamHandlers[obj.GetTag()]; !ok {
		return errors.Errorf("[ERROR] streamId=%d not registered", obj.GetTag())
	} else {
		streamHandler.cancel()
		delete(s.streamHandlers, obj.GetTag())
		return nil
	}
}
