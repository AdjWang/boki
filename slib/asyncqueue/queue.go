package asyncqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cs.utexas.edu/zjia/faas/slib/common"

	"cs.utexas.edu/zjia/faas/protocol"
	"cs.utexas.edu/zjia/faas/types"
	"github.com/pkg/errors"
)

type Queue struct {
	ctx context.Context
	env types.Environment

	name     string
	nameHash uint64

	view *QueueAuxData
}

type QueueAuxData struct {
	Consumed uint64 `json:"h"`
	Tail     uint64 `json:"t"`
}

type QueueLogEntry struct {
	seqNum  uint64
	auxData *QueueAuxData

	QueueName string `json:"n"`
	IsPush    bool   `json:"t"`
	Payload   string `json:"p,omitempty"`
}

func queueLogTag(nameHash uint64) uint64 {
	return (nameHash << common.LogTagReserveBits) + common.QueueLogTagLowBits
}

func queuePushLogTag(nameHash uint64) uint64 {
	return (nameHash << common.LogTagReserveBits) + common.QueuePushLogTagLowBits
}

func decodeQueueLogEntry(logEntry *types.LogEntryWithMeta) *QueueLogEntry {
	queueLog := &QueueLogEntry{}
	err := json.Unmarshal(logEntry.Data, queueLog)
	if err != nil {
		panic(errors.Wrapf(err, "decodeQueueLogEntry json unmarshal error: %+v", logEntry))
	}
	if len(logEntry.AuxData) > 0 {
		auxData := &QueueAuxData{}
		err := json.Unmarshal(logEntry.AuxData, auxData)
		if err != nil {
			panic(errors.Wrapf(err, "auxdata json unmarshal error: %v:%+v",
				string(logEntry.AuxData), logEntry))
		}
		queueLog.auxData = auxData
	}
	queueLog.seqNum = logEntry.SeqNum
	return queueLog
}

func NewQueue(ctx context.Context, env types.Environment, name string) (*Queue, error) {
	q := &Queue{
		ctx:      ctx,
		env:      env,
		name:     name,
		nameHash: common.NameHash(name),
		view:     nil,
	}
	if err := q.syncTo(types.LogEntryIndex{
		LocalId: protocol.InvalidLogLocalId,
		SeqNum:  protocol.MaxLogSeqnum,
	}); err != nil {
		return nil, errors.Wrap(err, "initial syncTo")
	}
	return q, nil
}

func (q *Queue) GetTag() uint64 {
	return queueLogTag(q.nameHash)
}

func (q *Queue) UpdateView(view interface{}, logEntry *types.LogEntryWithMeta) ([]uint64, interface{}) {
	if logEntry == nil {
		panic("unreachable")
	}
	queueLog := decodeQueueLogEntry(logEntry)
	if queueLog.auxData != nil {
		panic("unreachable")
	}
	queueAuxData := &QueueAuxData{Consumed: 0, Tail: 0}
	if view != nil {
		queueAuxData = view.(*QueueAuxData)
	}
	// apply log to view
	if queueLog.QueueName != q.name {
		panic("unreachable")
	}
	if queueLog.IsPush {
		queueAuxData.Tail = queueLog.seqNum + 1
	} else {
		nextLog, err := q.findNext(queueAuxData.Consumed, queueAuxData.Tail)
		if err != nil {
			panic(errors.Wrapf(err, "view: %+v", queueAuxData))
		}
		if nextLog != nil {
			queueAuxData.Consumed = nextLog.seqNum + 1
		} else {
			queueAuxData.Consumed = queueLog.seqNum
		}
	}
	return []uint64{q.GetTag()}, queueAuxData
}

func (q *Queue) EncodeView(view interface{}) ([]byte, error) {
	encoded, err := json.Marshal(view.(*QueueAuxData))
	if err != nil {
		return nil, errors.Wrapf(err, "auxdata json marshal error: %v", view)
	}
	return encoded, nil
}

func (q *Queue) DecodeView(rawViewData []byte) (interface{}, error) {
	view := QueueAuxData{Consumed: 0, Tail: 0}
	err := json.Unmarshal(rawViewData, &view)
	if err != nil {
		return nil, errors.Wrapf(err, "auxdata json unmarshal error: %v:%+v", string(rawViewData), rawViewData)
	}
	return &view, nil
}

func (q *Queue) appendPopLogAndSync() error {
	logEntry := &QueueLogEntry{
		QueueName: q.name,
		IsPush:    false,
	}
	encoded, err := json.Marshal(logEntry)
	if err != nil {
		panic(err)
	}
	tags := []types.Tag{
		{StreamType: common.FsmType_QueueLog, StreamId: queueLogTag(q.nameHash)},
	}
	if future, err := q.env.AsyncSharedLogAppend(q.ctx, tags, encoded); err != nil {
		return errors.Wrap(err, "AsyncSharedLogAppend")
	} else {
		localId := future.GetLocalId()
		return q.syncTo(types.LogEntryIndex{
			LocalId: localId,
			SeqNum:  protocol.InvalidLogSeqNum,
		})
	}
}

func (q *Queue) syncTo(logIndex types.LogEntryIndex) error {
	logStream := q.env.AsyncSharedLogReadNextUntil(q.ctx, q.GetTag(), logIndex)
	doneCh := make(chan struct{})
	errCh := make(chan error)
	go func(ctx context.Context) {
		var view interface{}
		for {
			var logEntry *types.LogEntryWithMeta
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			default:
				logStreamEntry, err := logStream.DequeueOrWaitForNextElement()
				if err != nil {
					errCh <- ctx.Err()
					return
				}
				logEntry = logStreamEntry.(types.LogStreamEntry[types.LogEntryWithMeta]).LogEntry
				err = logStreamEntry.(types.LogStreamEntry[types.LogEntryWithMeta]).Err
				if err != nil {
					errCh <- ctx.Err()
					return
				}
			}
			if logEntry == nil {
				if view != nil {
					q.view = view.(*QueueAuxData)
				}
				doneCh <- struct{}{}
				break
			}
			// log.Printf("[DEBUG] got logEntry=%+v", logEntry)
			if len(logEntry.AuxData) != 0 {
				decoded, err := q.DecodeView(logEntry.AuxData)
				if err != nil {
					errCh <- ctx.Err()
					return
				}
				view = decoded
			} else {
				var auxTags []uint64
				auxTags, view = q.UpdateView(view, logEntry)
				// log.Printf("[DEBUG] applying logEntry=%+v, got view=%+v", logEntry, view)
				// some times we only need to grab log entries with out view
				// so output view can be nil
				if view != nil {
					encoded, err := q.EncodeView(view)
					if err != nil {
						errCh <- ctx.Err()
						return
					}
					if err := q.env.SharedLogSetAuxDataWithShards(q.ctx, auxTags, logEntry.SeqNum, encoded); err != nil {
						errCh <- ctx.Err()
						return
					}
				}
			}
		}
	}(q.ctx)
	select {
	case <-doneCh:
		return nil
	case err := <-errCh:
		return err
	}
}

// -----------------------------------------------------------------------------

func (q *Queue) Clone() *Queue {
	return &Queue{
		ctx:      q.ctx,
		env:      q.env,
		name:     q.name,
		nameHash: q.nameHash,
	}
}

func (q *Queue) BatchPush(payloads []string) error {
	futures := make([]types.Future[uint64], 0, len(payloads))
	for _, payload := range payloads {
		future, err := q.doPush(payload)
		if err != nil {
			return err
		}
		futures = append(futures, future)
	}
	for _, future := range futures {
		if err := future.Await(60 * time.Second); err != nil {
			return err
		}
	}
	return nil
}

func (q *Queue) Push(payload string) error {
	future, err := q.doPush(payload)
	if err != nil {
		return errors.Wrap(err, "doPush")
	}
	return future.Await(60 * time.Second)
}

func (q *Queue) doPush(payload string) (types.Future[uint64], error) {
	if len(payload) == 0 {
		return nil, fmt.Errorf("Payload cannot be empty")
	}
	logEntry := &QueueLogEntry{
		QueueName: q.name,
		IsPush:    true,
		Payload:   payload,
	}
	encoded, err := json.Marshal(logEntry)
	if err != nil {
		panic(err)
	}
	tags := []types.Tag{
		{StreamType: common.FsmType_QueueLog, StreamId: queueLogTag(q.nameHash)},
		{StreamType: common.FsmType_QueuePushLog, StreamId: queuePushLogTag(q.nameHash)},
	}
	return q.env.AsyncSharedLogAppend(q.ctx, tags, encoded)
}

func (q *Queue) isEmpty() bool {
	return q.view == nil || q.view.Consumed >= q.view.Tail
}

func (q *Queue) findNext(minSeqNum, maxSeqNum uint64) (*QueueLogEntry, error) {
	tag := queuePushLogTag(q.nameHash)
	seqNum := minSeqNum
	for seqNum < maxSeqNum {
		condLogEntry, err := q.env.AsyncSharedLogReadNext(q.ctx, tag, seqNum)
		if err != nil {
			return nil, err
		}
		if condLogEntry == nil || condLogEntry.SeqNum >= maxSeqNum {
			return nil, nil
		}
		queueLog := decodeQueueLogEntry(condLogEntry)
		if queueLog.IsPush && queueLog.QueueName == q.name {
			return queueLog, nil
		}
		seqNum = condLogEntry.SeqNum + 1
	}
	return nil, nil
}

// func (q *Queue) applyLog(queueLog *QueueLogEntry) error {
// 	if queueLog.seqNum < q.nextSeqNum {
// 		// DEBUG
// 		debug.PrintStack()
// 		log.Fatalf("[FATAL] LogSeqNum=%#016x, NextSeqNum=%#016x", queueLog.seqNum, q.nextSeqNum)
// 	}
// 	if queueLog.IsPush {
// 		q.tail = queueLog.seqNum + 1
// 	} else {
// 		nextLog, err := q.findNext(q.consumed, q.tail)
// 		if err != nil {
// 			return err
// 		}
// 		if nextLog != nil {
// 			q.consumed = nextLog.seqNum + 1
// 		} else {
// 			q.consumed = queueLog.seqNum
// 		}
// 	}
// 	q.nextSeqNum = queueLog.seqNum + 1
// 	return nil
// }

// func (q *Queue) setAuxData(seqNum uint64, auxData *QueueAuxData) error {
// 	tag := queueLogTag(q.nameHash)
// 	encoded, err := json.Marshal(auxData)
// 	if err != nil {
// 		panic(err)
// 	}
// 	// DEBUG
// 	// log.Printf("[DEBUG] setting auxdata %v:%v", string(encoded), encoded)
// 	return q.env.SharedLogSetAuxDataWithShards(q.ctx, []uint64{tag}, seqNum, encoded)
// }

var kQueueEmptyError = errors.New("Queue empty")
var kQueueTimeoutError = errors.New("Blocking pop timeout")

func IsQueueEmptyError(err error) bool {
	return err == kQueueEmptyError
}

func IsQueueTimeoutError(err error) bool {
	return err == kQueueTimeoutError
}

func (q *Queue) Pop() (string /* payload */, error) {
	if q.isEmpty() {
		if err := q.syncTo(types.LogEntryIndex{
			LocalId: protocol.InvalidLogLocalId,
			SeqNum:  protocol.MaxLogSeqnum,
		}); err != nil {
			return "", errors.Wrap(err, "syncTo")
		}
		if q.isEmpty() {
			return "", kQueueEmptyError
		}
	}
	if err := q.appendPopLogAndSync(); err != nil {
		return "", errors.Wrap(err, "appendPopLogAndSync")
	}
	if q.isEmpty() {
		return "", kQueueEmptyError
	}
	if nextLog, err := q.findNext(q.view.Consumed, q.view.Tail); err != nil {
		return "", errors.Wrap(err, "findNext")
	} else if nextLog != nil {
		return nextLog.Payload, nil
	} else {
		return "", kQueueEmptyError
	}
}

func (q *Queue) asyncAppendPopLog() (types.Future[uint64], error) {
	logEntry := &QueueLogEntry{
		QueueName: q.name,
		IsPush:    false,
	}
	encoded, err := json.Marshal(logEntry)
	if err != nil {
		panic(err)
	}
	tags := []types.Tag{
		{StreamType: common.FsmType_QueueLog, StreamId: queueLogTag(q.nameHash)},
	}
	return q.env.AsyncSharedLogAppend(q.ctx, tags, encoded)
}

func (q *Queue) BatchPop(n int) ([]string /* payloads */, error) {
	panic("not implemented")
	// if q.isEmpty() {
	// 	if err := q.syncTo(protocol.MaxLogSeqnum); err != nil {
	// 		return nil, err
	// 	}
	// 	if q.isEmpty() {
	// 		return nil, kQueueEmptyError
	// 	}
	// }
	// // append pop log and sync
	// future, err := q.asyncAppendPopLog()
	// if err != nil {
	// 	return nil, err
	// }

	// synced := false
	// payloads := make([]string, 0, n)
	// for i := 0; i < n; i++ {
	// continueWithoutIter:
	// 	if nextLog, err := q.findNext(q.consumed, q.tail); err != nil {
	// 		return nil, err
	// 	} else if nextLog != nil {
	// 		payloads = append(payloads, nextLog.Payload)
	// 	} else {
	// 		if !synced {
	// 			if seqNum, err := future.GetResult(60 * time.Second); err != nil {
	// 				return nil, err
	// 			} else if err := q.syncTo(seqNum); err != nil {
	// 				return nil, err
	// 			}
	// 			synced = true
	// 			goto continueWithoutIter
	// 		} else {
	// 			return payloads, kQueueEmptyError
	// 		}
	// 	}
	// }
	// return payloads, nil
}

const kBlockingPopTimeout = 1 * time.Second

func (q *Queue) PopBlocking() (string /* payload */, error) {
	panic("not implemented")
	// tag := queuePushLogTag(q.nameHash)
	// startTime := time.Now()
	// for time.Since(startTime) < kBlockingPopTimeout {
	// 	if q.isEmpty() {
	// 		if err := q.syncTo(protocol.MaxLogSeqnum); err != nil {
	// 			return "", err
	// 		}
	// 	}
	// 	if q.isEmpty() {
	// 		seqNum := q.nextSeqNum
	// 		for {
	// 			// log.Printf("[DEBUG] BlockingRead: NextSeqNum=%#016x", seqNum)
	// 			newCtx, _ := context.WithTimeout(q.ctx, kBlockingPopTimeout)
	// 			condLogEntry, err := q.env.AsyncSharedLogReadNextBlock(newCtx, tag, seqNum)
	// 			if err != nil {
	// 				return "", err
	// 			}
	// 			if condLogEntry != nil {
	// 				queueLog := decodeQueueLogEntry(condLogEntry)
	// 				if queueLog.IsPush && queueLog.QueueName == q.name {
	// 					break
	// 				}
	// 				seqNum = condLogEntry.SeqNum + 1
	// 			} else if time.Since(startTime) >= kBlockingPopTimeout {
	// 				return "", kQueueTimeoutError
	// 			}
	// 		}
	// 	}
	// 	if err := q.appendPopLogAndSync(); err != nil {
	// 		return "", err
	// 	}
	// 	if nextLog, err := q.findNext(q.consumed, q.tail); err != nil {
	// 		return "", err
	// 	} else if nextLog != nil {
	// 		return nextLog.Payload, nil
	// 	}
	// }
	// return "", kQueueTimeoutError
}
