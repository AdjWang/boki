package sync

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

type Queue struct {
	ctx context.Context
	env types.Environment

	name     string
	nameHash uint64

	consumed   uint64
	tail       uint64
	nextSeqNum uint64
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

func decodeQueueLogEntry(condLogEntry *types.CondLogEntry) *QueueLogEntry {
	queueLog := &QueueLogEntry{}
	err := json.Unmarshal(condLogEntry.Data, queueLog)
	if err != nil {
		panic(errors.Wrapf(err, "decodeQueueLogEntry json unmarshal error: %+v", condLogEntry))
	}
	if len(condLogEntry.AuxData) > 0 {
		auxData := &QueueAuxData{}
		err := json.Unmarshal(condLogEntry.AuxData, auxData)
		if err != nil {
			panic(err)
		}
		queueLog.auxData = auxData
	}
	queueLog.seqNum = condLogEntry.SeqNum
	return queueLog
}

func NewQueue(ctx context.Context, env types.Environment, name string) (*Queue, error) {
	q := &Queue{
		ctx:        ctx,
		env:        env,
		name:       name,
		nameHash:   common.NameHash(name),
		consumed:   0,
		tail:       0,
		nextSeqNum: 0,
	}
	if err := q.syncToBackward(protocol.MaxLogSeqnum); err != nil {
		return nil, err
	}
	return q, nil
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
		return err
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
	tags := []uint64{queueLogTag(q.nameHash), queuePushLogTag(q.nameHash)}
	tagMetas := []types.TagMeta{
		{FsmType: common.FsmType_QueueLog, TagKeys: []string{strconv.FormatUint(q.nameHash, common.TagKeyBase)}},
		{FsmType: common.FsmType_QueuePushLog, TagKeys: []string{strconv.FormatUint(q.nameHash, common.TagKeyBase)}},
	}
	return q.env.AsyncSharedLogAppend(q.ctx, tags, tagMetas, encoded)
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

func (q *Queue) setAuxData(seqNum uint64, auxData *QueueAuxData) error {
	encoded, err := json.Marshal(auxData)
	if err != nil {
		panic(err)
	}
	return q.env.SharedLogSetAuxData(q.ctx, seqNum, encoded)
}

func (q *Queue) syncToBackward(tailSeqNum uint64) error {
	if tailSeqNum < q.nextSeqNum {
		log.Fatalf("[FATAL] Current seqNum=%#016x, cannot sync to %#016x", q.nextSeqNum, tailSeqNum)
	}
	if tailSeqNum == q.nextSeqNum {
		return nil
	}

	tag := queueLogTag(q.nameHash)
	queueLogs := make([]*QueueLogEntry, 0, 4)

	seqNum := tailSeqNum
	currentSeqNum := q.nextSeqNum

	var err error
	var currentLogEntryFuture types.Future[*types.CondLogEntry] = nil
	var nextLogEntryFuture types.Future[*types.CondLogEntry] = nil
	first := true
	for seqNum > currentSeqNum {
		if seqNum != protocol.MaxLogSeqnum {
			seqNum -= 1
		}
		if first {
			first = false
			// 1. first read
			currentLogEntryFuture, err = q.env.AsyncSharedLogReadPrev2(q.ctx, tag, seqNum)
			if err != nil {
				return err
			}
		}
		// seqNum is stored as the LocalId
		if currentLogEntryFuture == nil || currentLogEntryFuture.GetLocalId() < currentSeqNum {
			break
		}
		// 3. aggressively do next read
		seqNum = currentLogEntryFuture.GetLocalId()
		if seqNum > currentSeqNum {
			if seqNum != protocol.MaxLogSeqnum {
				seqNum -= 1
			}
			nextLogEntryFuture, err = q.env.AsyncSharedLogReadPrev2(q.ctx, tag, seqNum)
			if err != nil {
				return err
			}
		}
		// 2. sync the read
		logEntry, err := currentLogEntryFuture.GetResult()
		if err != nil {
			return err
		}
		if logEntry == nil || logEntry.SeqNum < currentSeqNum {
			// unreachable since the log's seqnum exists and had been asserted
			// by the future object above
			panic(fmt.Errorf("unreachable: %+v, %v", logEntry, currentSeqNum))
		}
		seqNum = logEntry.SeqNum

		currentLogEntryFuture = nextLogEntryFuture
		nextLogEntryFuture = nil

		// for seqNum > q.nextSeqNum {
		// 	if seqNum != protocol.MaxLogSeqnum {
		// 		seqNum -= 1
		// 	}
		// 	logEntry, err := q.env.AsyncSharedLogReadPrev(q.ctx, tag, seqNum)
		// 	if err != nil {
		// 		return err
		// 	}
		// 	if logEntry == nil || logEntry.SeqNum < q.nextSeqNum {
		// 		break
		// 	}
		// 	seqNum = logEntry.SeqNum
		queueLog := decodeQueueLogEntry(logEntry)
		if queueLog.QueueName != q.name {
			continue
		}
		if queueLog.auxData != nil {
			q.nextSeqNum = queueLog.seqNum + 1
			q.consumed = queueLog.auxData.Consumed
			q.tail = queueLog.auxData.Tail
			break
		} else {
			queueLogs = append(queueLogs, queueLog)
		}
	}

	for i := len(queueLogs) - 1; i >= 0; i-- {
		queueLog := queueLogs[i]
		q.applyLog(queueLog)
		auxData := &QueueAuxData{
			Consumed: q.consumed,
			Tail:     q.tail,
		}
		if err := q.setAuxData(queueLog.seqNum, auxData); err != nil {
			return err
		}
	}
	return nil
}

func (q *Queue) syncToForward(tailSeqNum uint64) error {
	if tailSeqNum < q.nextSeqNum {
		log.Fatalf("[FATAL] Current seqNum=%#016x, cannot sync to %#016x", q.nextSeqNum, tailSeqNum)
	}
	tag := queueLogTag(q.nameHash)
	seqNum := q.nextSeqNum
	for seqNum < tailSeqNum {
		condLogEntry, err := q.env.AsyncSharedLogReadNext(q.ctx, tag, seqNum)
		if err != nil {
			return err
		}
		if condLogEntry == nil || condLogEntry.SeqNum >= tailSeqNum {
			break
		}
		seqNum = condLogEntry.SeqNum + 1
		queueLog := decodeQueueLogEntry(condLogEntry)
		if queueLog.QueueName == q.name {
			q.applyLog(queueLog)
			if queueLog.auxData == nil {
				auxData := &QueueAuxData{
					Consumed: q.consumed,
					Tail:     q.tail,
				}
				if err := q.setAuxData(queueLog.seqNum, auxData); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (q *Queue) syncTo(tailSeqNum uint64) error {
	return q.syncToBackward(tailSeqNum)
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
	tags := []uint64{queueLogTag(q.nameHash)}
	tagMetas := []types.TagMeta{
		{FsmType: common.FsmType_QueueLog, TagKeys: []string{strconv.FormatUint(q.nameHash, common.TagKeyBase)}},
	}
	if future, err := q.env.AsyncSharedLogAppend(q.ctx, tags, tagMetas, encoded); err != nil {
		return err
	} else {
		if seqNum, err := future.GetResult(); err != nil {
			return err
		} else {
			return q.syncTo(seqNum)
		}
	}
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
	if q.isEmpty() {
		if err := q.syncTo(protocol.MaxLogSeqnum); err != nil {
			return "", err
		}
		if q.isEmpty() {
			return "", kQueueEmptyError
		}
	}
	if err := q.appendPopLogAndSync(); err != nil {
		return "", err
	}
	if nextLog, err := q.findNext(q.consumed, q.tail); err != nil {
		return "", err
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
	tags := []uint64{queueLogTag(q.nameHash)}
	tagMetas := []types.TagMeta{
		{FsmType: common.FsmType_QueueLog, TagKeys: []string{strconv.FormatUint(q.nameHash, common.TagKeyBase)}},
	}
	return q.env.AsyncSharedLogAppend(q.ctx, tags, tagMetas, encoded)
}

func (q *Queue) BatchPop(n int) ([]string /* payloads */, error) {
	if q.isEmpty() {
		if err := q.syncTo(protocol.MaxLogSeqnum); err != nil {
			return nil, err
		}
		if q.isEmpty() {
			return nil, kQueueEmptyError
		}
	}
	// append pop log and sync
	future, err := q.asyncAppendPopLog()
	if err != nil {
		return nil, err
	}

	synced := false
	payloads := make([]string, 0, n)
	for i := 0; i < n; i++ {
	continueWithoutIter:
		if nextLog, err := q.findNext(q.consumed, q.tail); err != nil {
			return nil, err
		} else if nextLog != nil {
			payloads = append(payloads, nextLog.Payload)
		} else {
			if !synced {
				if seqNum, err := future.GetResult(); err != nil {
					return nil, err
				} else if err := q.syncTo(seqNum); err != nil {
					return nil, err
				}
				synced = true
				goto continueWithoutIter
			} else {
				return payloads, kQueueEmptyError
			}
		}
	}
	return payloads, nil
}

const kBlockingPopTimeout = 1 * time.Second

func (q *Queue) PopBlocking() (string /* payload */, error) {
	tag := queuePushLogTag(q.nameHash)
	startTime := time.Now()
	for time.Since(startTime) < kBlockingPopTimeout {
		if q.isEmpty() {
			if err := q.syncTo(protocol.MaxLogSeqnum); err != nil {
				return "", err
			}
		}
		if q.isEmpty() {
			seqNum := q.nextSeqNum
			for {
				// log.Printf("[DEBUG] BlockingRead: NextSeqNum=%#016x", seqNum)
				newCtx, _ := context.WithTimeout(q.ctx, kBlockingPopTimeout)
				condLogEntry, err := q.env.AsyncSharedLogReadNextBlock(newCtx, tag, seqNum)
				if err != nil {
					return "", err
				}
				if condLogEntry != nil {
					queueLog := decodeQueueLogEntry(condLogEntry)
					if queueLog.IsPush && queueLog.QueueName == q.name {
						break
					}
					seqNum = condLogEntry.SeqNum + 1
				} else if time.Since(startTime) >= kBlockingPopTimeout {
					return "", kQueueTimeoutError
				}
			}
		}
		if err := q.appendPopLogAndSync(); err != nil {
			return "", err
		}
		if nextLog, err := q.findNext(q.consumed, q.tail); err != nil {
			return "", err
		} else if nextLog != nil {
			return nextLog.Payload, nil
		}
	}
	return "", kQueueTimeoutError
}
