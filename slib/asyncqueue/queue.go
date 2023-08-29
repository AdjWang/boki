package asyncqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"cs.utexas.edu/zjia/faas/slib/common"

	"cs.utexas.edu/zjia/faas/protocol"
	"cs.utexas.edu/zjia/faas/types"
	"github.com/pkg/errors"
)

const TimestampStrLen = 20
const SeqNumStrLen = 16

func ParseSeqNum(payload string) string {
	seqNumStr := payload[TimestampStrLen : TimestampStrLen+SeqNumStrLen]
	if s, err := strconv.ParseUint(seqNumStr, 16, 64); err == nil {
		return strconv.FormatUint(s, 10)
	} else {
		panic(err)
	}
}

// AuxData format:
type AuxData map[ /*tag*/ uint64] /*value*/ string

func NewAuxData() AuxData {
	return make(AuxData)
}

func DeserializeAuxData(rawData []byte) AuxData {
	if len(rawData) == 0 {
		return nil
	}
	result := NewAuxData()
	if err := json.Unmarshal(rawData, &result); err != nil {
		rawDataStr := "["
		for _, i := range rawData {
			rawDataStr += fmt.Sprintf("%02X ", i)
		}
		rawDataStr += "]"
		panic(errors.Wrap(err, rawDataStr))
	}
	return result
}

type Queue struct {
	ctx context.Context
	env types.Environment

	name     string
	nameHash uint64
	iShard   int

	consumed   uint64
	tail       uint64
	nextSeqNum uint64
}

type QueueAuxData struct {
	Consumed uint64 `json:"h"`
	Tail     uint64 `json:"t"`
}

// DEBUG
func (q *QueueAuxData) String() string {
	return fmt.Sprintf("Consumed=%016X Tail=%016X", q.Consumed, q.Tail)
}

type QueueLogEntry struct {
	seqNum uint64

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
	queueLog.seqNum = logEntry.SeqNum
	return queueLog
}

func NewQueue(ctx context.Context, env types.Environment, name string, iShard int) (*Queue, error) {
	q := &Queue{
		ctx:        ctx,
		env:        env,
		name:       name,
		nameHash:   common.NameHash(name),
		iShard:     iShard,
		consumed:   0,
		tail:       0,
		nextSeqNum: 0,
	}
	if err := q.syncTo(types.LogEntryIndex{
		LocalId: protocol.InvalidLogLocalId,
		SeqNum:  protocol.MaxLogSeqnum,
	}); err != nil {
		return nil, errors.Wrap(err, "initial syncTo")
	}
	return q, nil
}

func (q *Queue) applyLog(queueLog *QueueLogEntry) error {
	if queueLog.seqNum < q.nextSeqNum {
		log.Fatalf("[FATAL] LogSeqNum=%#016x, NextSeqNum=%#016x", queueLog.seqNum, q.nextSeqNum)
	}
	if queueLog.IsPush {
		q.tail = queueLog.seqNum + 1
	} else {
		nextLog, err := q.findNext(q.consumed, q.tail)
		if err != nil {
			return err
		}
		if nextLog != nil {
			q.consumed = nextLog.seqNum + 1
		} else {
			q.consumed = queueLog.seqNum
		}
	}
	q.nextSeqNum = queueLog.seqNum + 1
	return nil
}

// DEBUG: PROF
// func (q *Queue) appendPopLogAndSync() (int64, int64, error) {
// 	logEntry := &QueueLogEntry{
// 		QueueName: q.name,
// 		IsPush:    false,
// 	}
// 	encoded, err := json.Marshal(logEntry)
// 	if err != nil {
// 		panic(err)
// 	}
// 	tags := []types.Tag{
// 		{StreamType: common.FsmType_QueueLog, StreamId: queueLogTag(q.nameHash)},
// 	}
// 	appendStart := time.Now()
// 	future, err := q.env.AsyncSharedLogAppend(q.ctx, tags, encoded)
// 	localId := future.GetLocalId()
// 	appendElapsed := time.Since(appendStart).Microseconds()
// 	if err != nil {
// 		return 0, 0, errors.Wrap(err, "AsyncSharedLogAppend")
// 	} else {
// 		syncToStart := time.Now()
// 		err = q.syncTo(types.LogEntryIndex{
// 			LocalId: localId,
// 			SeqNum:  protocol.InvalidLogSeqNum,
// 		})
// 		syncToElapsed := time.Since(syncToStart).Microseconds()
// 		return appendElapsed, syncToElapsed, err
// 	}
// }

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
	future, err := q.env.AsyncSharedLogAppend(q.ctx, tags, encoded)
	localId := future.GetLocalId()
	if err != nil {
		return errors.Wrap(err, "AsyncSharedLogAppend")
	} else {
		return q.syncTo(types.LogEntryIndex{
			LocalId: localId,
			SeqNum:  protocol.InvalidLogSeqNum,
		})
	}
}

type AuxView struct {
	seqNum  uint64
	auxTags []uint64
	view    interface{}
}

type syncToInspector struct {
	seqNum  uint64
	withAux bool
}

// DEBUG
func (s syncToInspector) String() string {
	return fmt.Sprintf("seqNum=%016X withAux=%v", s.seqNum, s.withAux)
}

func (q *Queue) syncTo(logIndex types.LogEntryIndex) error {
	tag := queueLogTag(q.nameHash)
	logStream := q.env.AsyncSharedLogReadNextUntil(q.ctx, tag, q.nextSeqNum, logIndex,
		types.ReadOptions{FromCached: true, AuxTags: []uint64{tag}})
	for {
		logStreamEntry := logStream.BlockingDequeue()
		logEntry := logStreamEntry.LogEntry
		err := logStreamEntry.Err
		if err != nil {
			return err
		}
		if logEntry == nil {
			break
		}
		queueLog := decodeQueueLogEntry(logEntry)
		if queueLog.QueueName != q.name {
			continue
		}
		if len(logEntry.AuxData) > 0 {
			auxData := DeserializeAuxData(logEntry.AuxData)
			if viewData, found := auxData[tag]; found {
				view := QueueAuxData{Consumed: 0, Tail: 0}
				err := json.Unmarshal([]byte(viewData), &view)
				if err != nil {
					panic(errors.Wrapf(err, "auxdata json unmarshal error: %v", viewData))
				}
				q.nextSeqNum = logEntry.SeqNum + 1
				q.consumed = view.Consumed
				q.tail = view.Tail
				continue
			}
		}
		q.applyLog(queueLog)
		auxData := &QueueAuxData{
			Consumed: q.consumed,
			Tail:     q.tail,
		}
		encoded, err := json.Marshal(auxData)
		if err != nil {
			panic(err)
		}
		if err := q.env.SharedLogSetAuxDataWithShards(q.ctx, queueLog.seqNum, tag, encoded); err != nil {
			panic(err)
		}
	}
	return nil
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
	_, err = future.GetResult(60 * time.Second)
	return err
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
	return q.consumed >= q.tail
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

var kQueueEmptyError = errors.New("Queue empty")
var kQueueTimeoutError = errors.New("Blocking pop timeout")

func IsQueueEmptyError(err error) bool {
	return err == kQueueEmptyError
}

func IsQueueTimeoutError(err error) bool {
	return err == kQueueTimeoutError
}

func (q *Queue) Pop() (string /* payload */, error) {
	// popStart := time.Now()
	if q.isEmpty() {
		if err := q.syncTo(types.LogEntryIndex{
			LocalId: protocol.InvalidLogLocalId,
			SeqNum:  protocol.MaxLogSeqnum,
		}); err != nil {
			return "", errors.Wrap(err, "syncTo")
		}
		if q.isEmpty() {
			// log.Printf("[PROF] pop empty=%v", time.Since(popStart).Microseconds())
			return "", kQueueEmptyError
		}
	}
	// appendElapsed, syncToElapsed, err := q.appendPopLogAndSync()
	// if err != nil {
	// 	return "", errors.Wrap(err, "appendPopLogAndSync")
	// }
	if err := q.appendPopLogAndSync(); err != nil {
		return "", errors.Wrap(err, "appendPopLogAndSync")
	}
	// defer log.Printf("[PROF] pop=%v append=%v read=%v", time.Since(popStart).Microseconds(), appendElapsed, syncToElapsed)
	if q.isEmpty() {
		return "", kQueueEmptyError
	}
	if nextLog, err := q.findNext(q.consumed, q.tail); err != nil {
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
