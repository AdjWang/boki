package asyncstatestore

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"cs.utexas.edu/zjia/faas/protocol"
	"cs.utexas.edu/zjia/faas/slib/common"

	"cs.utexas.edu/zjia/faas/types"

	gabs "github.com/Jeffail/gabs/v2"
	redis "github.com/go-redis/redis/v8"
	"github.com/pkg/errors"
)

var FLAGS_DisableAuxData bool = false
var FLAGS_RedisForAuxData bool = false

var redisClient *redis.Client

func init() {
	if val, exists := os.LookupEnv("DISABLE_AUXDATA"); exists && val == "1" {
		FLAGS_DisableAuxData = true
		log.Printf("[INFO] AuxData disabled")
	}
	if val, exists := os.LookupEnv("AUXDATA_REDIS_URL"); exists {
		FLAGS_RedisForAuxData = true
		log.Printf("[INFO] Use Redis for AuxData")
		opt, err := redis.ParseURL(val)
		if err != nil {
			log.Fatalf("[FATAL] Failed to parse Redis URL %s: %v", val, err)
		}
		redisClient = redis.NewClient(opt)
	}
}

const (
	LOG_NormalOp = iota
	LOG_TxnBegin
	LOG_TxnAbort
	LOG_TxnCommit
	LOG_TxnHistory
)

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

// func (ab AuxData) Serialize() []byte {
// 	res, err := json.Marshal(ab)
// 	if err != nil {
// 		panic(err)
// 	}
// 	return res
// }

type ObjectLogEntry struct {
	localId  uint64
	seqNum   uint64
	auxData  AuxData
	writeSet map[string]bool

	LogType int        `json:"t"`
	Ops     []*WriteOp `json:"o,omitempty"`
	TxnId   uint64     `json:"x"`
}

func objectLogTag(objNameHash uint64) uint64 {
	return (objNameHash << common.LogTagReserveBits) + common.ObjectLogTagLowBits
}

func txnHistoryLogTag(txnId uint64) uint64 {
	return (txnId << common.LogTagReserveBits) + common.TxnHistoryLogTagLowBits
}

func (l *ObjectLogEntry) fillWriteSet() {
	if l.LogType == LOG_NormalOp || l.LogType == LOG_TxnCommit {
		l.writeSet = make(map[string]bool)
		for _, op := range l.Ops {
			l.writeSet[op.ObjName] = true
		}
	}
}

func decodeLogEntry(logEntry *types.LogEntryWithMeta) *ObjectLogEntry {
	rawObjectLog, err := common.DecompressData(logEntry.Data)
	if err != nil {
		panic(err)
	}
	objectLog := &ObjectLogEntry{}
	err = json.Unmarshal(rawObjectLog, objectLog)
	if err != nil {
		panic(err)
	}
	var auxData []byte
	if FLAGS_RedisForAuxData {
		key := fmt.Sprintf("%#016x", logEntry.SeqNum)
		val, err := redisClient.Get(context.Background(), key).Bytes()
		if err != nil {
			if err != redis.Nil {
				log.Fatalf("[FATAL] Failed to get AuxData from Redis: %v", err)
			}
		} else {
			auxData = val
		}
	} else {
		auxData = logEntry.AuxData
	}
	if len(auxData) > 0 {
		var contents AuxData
		err = json.Unmarshal(auxData, &contents)
		if err != nil {
			panic(err)
		}
		objectLog.auxData = contents
	}
	objectLog.localId = logEntry.LocalId
	objectLog.seqNum = logEntry.SeqNum
	objectLog.fillWriteSet()
	return objectLog
}

func (l *ObjectLogEntry) writeSetOverlapped(other *ObjectLogEntry) bool {
	if l.writeSet == nil || other.writeSet == nil {
		return false
	}
	for key, _ := range other.writeSet {
		if _, exists := l.writeSet[key]; exists {
			return true
		}
	}
	return false
}

func (l *ObjectLogEntry) withinWriteSet(objName string) bool {
	if l.writeSet == nil {
		return false
	}
	_, exists := l.writeSet[objName]
	return exists
}

// func (txnCommitLog *ObjectLogEntry) checkTxnCommitResult(env *envImpl) (bool, error) {
// 	if txnCommitLog.LogType != LOG_TxnCommit {
// 		panic("Wrong log type")
// 	}
// 	if txnCommitLog.auxData != nil {
// 		if v, exists := txnCommitLog.auxData[common.KeyCommitResult]; exists {
// 			// use json ["t"]
// 			return v == "\"t\"", nil
// 		}
// 	} else {
// 		txnCommitLog.auxData = NewAuxData()
// 	}
// 	// log.Printf("[DEBUG] Failed to load txn status: seqNum=%#016x", txnCommitLog.seqNum)
// 	tags := make([]uint64, 0, len(txnCommitLog.Ops))
// 	commitResult := true
// 	checkedTag := make(map[uint64]bool)
// 	for _, op := range txnCommitLog.Ops {
// 		tag := objectLogTag(common.NameHash(op.ObjName))
// 		tags = append(tags, objectLogTag(common.NameHash("commit"+op.ObjName)))
// 		if _, exists := checkedTag[tag]; exists {
// 			continue
// 		}
// 		seqNum := txnCommitLog.seqNum
// 		currentSeqNum := txnCommitLog.TxnId

// 		var err error
// 		var currentLogEntryFuture types.Future[*types.LogEntryWithMeta] = nil
// 		var nextLogEntryFuture types.Future[*types.LogEntryWithMeta] = nil
// 		first := true
// 		for seqNum > currentSeqNum {
// 			if seqNum != protocol.MaxLogSeqnum {
// 				seqNum -= 1
// 			}
// 			if first {
// 				first = false
// 				// 1. first read
// 				currentLogEntryFuture, err = env.faasEnv.AsyncSharedLogReadPrev2(env.faasCtx, tag, seqNum)
// 				if err != nil {
// 					return false, newRuntimeError(err.Error())
// 				}
// 			}
// 			if currentLogEntryFuture == nil || currentLogEntryFuture.GetSeqNum() <= currentSeqNum {
// 				break
// 			}
// 			if currentLogEntryFuture.IsResolved() {
// 				// HACK: disable async read if logs are cached
// 				first = true
// 			} else {
// 				// 3. aggressively do next read
// 				seqNum = currentLogEntryFuture.GetSeqNum()
// 				if seqNum > currentSeqNum {
// 					if seqNum != protocol.MaxLogSeqnum {
// 						seqNum -= 1
// 					}
// 					nextLogEntryFuture, err = env.faasEnv.AsyncSharedLogReadPrev2(env.faasCtx, tag, seqNum)
// 					if err != nil {
// 						return false, newRuntimeError(err.Error())
// 					}
// 				}
// 			}
// 			// 2. sync the current read
// 			logEntry, err := currentLogEntryFuture.GetResult(60 * time.Second)
// 			if err != nil {
// 				return false, newRuntimeError(err.Error())
// 			}
// 			if logEntry == nil || logEntry.SeqNum < currentSeqNum {
// 				// unreachable since the log's seqnum exists and had been asserted
// 				// by the future object above
// 				panic(fmt.Errorf("unreachable: %+v, %v", logEntry, currentSeqNum))
// 			}
// 			seqNum = logEntry.SeqNum

// 			currentLogEntryFuture = nextLogEntryFuture
// 			nextLogEntryFuture = nil

// 			// log.Printf("[DEBUG] Read log with seqnum %#016x", seqNum)

// 			objectLog := decodeLogEntry(logEntry)
// 			if !txnCommitLog.writeSetOverlapped(objectLog) {
// 				continue
// 			}
// 			if objectLog.LogType == LOG_NormalOp {
// 				commitResult = false
// 				break
// 			} else if objectLog.LogType == LOG_TxnCommit {
// 				if committed, err := objectLog.checkTxnCommitResult(env); err != nil {
// 					return false, err
// 				} else if committed {
// 					commitResult = false
// 					break
// 				}
// 			}
// 		}
// 		if !commitResult {
// 			break
// 		}
// 		checkedTag[tag] = true
// 	}
// 	commitResultStr := "f"
// 	if commitResult {
// 		commitResultStr = "t"
// 	}
// 	txnCommitLog.auxData[common.KeyCommitResult] = commitResultStr
// 	if !FLAGS_DisableAuxData {
// 		log.Println("[DEBUG] SharedLogSetAuxData of commit")
// 		env.setLogAuxData(txnCommitLog.seqNum, common.KeyCommitResult, commitResultStr)
// 	}
// 	return commitResult, nil
// }

func (txnCommitLog *ObjectLogEntry) checkTxnCommitResult(env *envImpl) (bool, error) {
	if txnCommitLog.LogType != LOG_TxnCommit {
		panic("Wrong log type")
	}
	if txnCommitLog.auxData != nil {
		if v, exists := txnCommitLog.auxData[common.KeyCommitResult]; exists {
			// use json ["t"]
			return v == "\"t\"", nil
		}
	} else {
		txnCommitLog.auxData = NewAuxData()
	}
	// log.Printf("[DEBUG] Failed to load txn status: seqNum=%#016x", txnCommitLog.seqNum)
	commitResult := true
	checkedTag := make(map[uint64]bool)
	doneCh := make(chan bool)
	errCh := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())
	for _, op := range txnCommitLog.Ops {
		tag := objectLogTag(common.NameHash(op.ObjName))
		if _, exists := checkedTag[tag]; exists {
			continue
		}

		logStream := env.faasEnv.AsyncSharedLogReadNextUntil(env.faasCtx, tag, txnCommitLog.TxnId, types.LogEntryIndex{
			LocalId: txnCommitLog.localId,
			SeqNum:  txnCommitLog.seqNum,
		}, false /*fromCached*/)
		go func(ctx context.Context) {
			opCommitResult := true
			for {
				var logEntry *types.LogEntryWithMeta
				select {
				case <-ctx.Done():
					errCh <- ctx.Err()
					return
				default:
					logStreamEntry := logStream.BlockingDequeue()
					logEntry = logStreamEntry.LogEntry
					err := logStreamEntry.Err
					if err != nil {
						errCh <- ctx.Err()
						return
					}
				}
				if logEntry == nil {
					break
				}
				objectLog := decodeLogEntry(logEntry)
				if !txnCommitLog.writeSetOverlapped(objectLog) {
					continue
				}
				if objectLog.LogType == LOG_NormalOp {
					opCommitResult = false
					break
				} else if objectLog.LogType == LOG_TxnCommit {
					if committed, err := objectLog.checkTxnCommitResult(env); err != nil {
						errCh <- err
						return
					} else if committed {
						opCommitResult = false
						break
					}
				}
			}
			doneCh <- opCommitResult
		}(ctx)
		checkedTag[tag] = true
		// DEBUG
		// select {
		// case committed := <-doneCh:
		// 	if !committed {
		// 		commitResult = false
		// 		break
		// 	}
		// case err := <-errCh:
		// 	cancel()
		// 	close(doneCh)
		// 	close(errCh)
		// 	return false, err
		// }
	}
	// DEBUG
	for range checkedTag {
		select {
		case committed := <-doneCh:
			if !committed {
				commitResult = false
				break
			}
		case err := <-errCh:
			cancel()
			close(doneCh)
			close(errCh)
			return false, err
		}
	}
	cancel()
	close(doneCh)
	close(errCh)

	commitResultStr := "f"
	if commitResult {
		commitResultStr = "t"
	}
	txnCommitLog.auxData[common.KeyCommitResult] = commitResultStr
	if !FLAGS_DisableAuxData {
		// log.Println("[DEBUG] SharedLogSetAuxData of commit")
		env.setLogAuxData(txnCommitLog.seqNum, common.KeyCommitResult, commitResultStr)
	}
	return commitResult, nil
}

func (l *ObjectLogEntry) listCachedObjectView() string {
	if l.auxData == nil {
		return ""
	}
	if l.LogType == LOG_NormalOp {
		return "NormalOp"
	} else if l.LogType == LOG_TxnCommit {
		objNames := make([]uint64, 0, len(l.auxData))
		for key, _ := range l.auxData {
			objNames = append(objNames, key)
		}
		return fmt.Sprint(objNames)
	}
	return ""
}

func (l *ObjectLogEntry) hasCachedObjectView(objName string) bool {
	if l.auxData == nil {
		return false
	}
	if l.LogType == LOG_NormalOp {
		return true
	} else if l.LogType == LOG_TxnCommit {
		_, exists := l.auxData[objectLogTag(common.NameHash(objName))]
		return exists
	}
	return false
}

func (l *ObjectLogEntry) loadCachedObjectView(objName string) *ObjectView {
	if l.auxData == nil {
		return nil
	}
	key := objectLogTag(common.NameHash(objName))
	if l.LogType == LOG_NormalOp {
		var gabsData map[string]interface{}
		if data, exists := l.auxData[key]; exists {
			if err := json.Unmarshal([]byte(data), &gabsData); err != nil {
				rawDataStr := fmt.Sprintf("NormalOp %v %+v [", key, l.auxData)
				for _, i := range []byte(l.auxData[key]) {
					rawDataStr += fmt.Sprintf("%02X ", i)
				}
				rawDataStr += "]"
				panic(errors.Wrap(err, rawDataStr))
			}
			return &ObjectView{
				name:       objName,
				nextSeqNum: l.seqNum + 1,
				contents:   gabs.Wrap(gabsData),
			}
		}
	} else if l.LogType == LOG_TxnCommit {
		if data, exists := l.auxData[key]; exists {
			var gabsData map[string]interface{}
			if err := json.Unmarshal([]byte(data), &gabsData); err != nil {
				rawDataStr := fmt.Sprintf("TxnCommit %v %+v [", key, l.auxData)
				for _, i := range []byte(l.auxData[key]) {
					rawDataStr += fmt.Sprintf("%02X ", i)
				}
				rawDataStr += "]"
			}
			return &ObjectView{
				name:       objName,
				nextSeqNum: l.seqNum + 1,
				contents:   gabs.Wrap(gabsData),
			}
		}
	}
	return nil
}

func (l *ObjectLogEntry) cacheObjectView(env *envImpl, view *ObjectView) []string {
	if FLAGS_DisableAuxData {
		return nil
	}
	key := objectLogTag(common.NameHash(view.name))
	// DEBUG
	keySet := make([]string, 0, 10)
	if l.LogType == LOG_NormalOp {
		if l.auxData == nil {
			// log.Println("[DEBUG] SharedLogSetAuxData of syncTo NormalOp")
			env.setLogAuxData(l.seqNum, key, view.contents.Data())
			keySet = append(keySet, view.name)
		}
	} else if l.LogType == LOG_TxnCommit {
		if l.auxData == nil {
			l.auxData = NewAuxData()
		}
		if _, exists := l.auxData[key]; !exists {
			// log.Println("[DEBUG] SharedLogSetAuxData of syncTo TxnCommit")
			env.setLogAuxData(l.seqNum, key, view.contents.Data())
		}
	} else {
		panic("Wrong log type")
	}
	return keySet
}

// func (obj *ObjectRef) syncTo(tailSeqNum uint64) error {
// 	return obj.syncToBackward(tailSeqNum)
// }

func (obj *ObjectRef) syncTo(logIndex types.LogEntryIndex) error {
	// var refView *ObjectView
	// if err := obj.syncToBackward(tailSeqNum, &refView); err != nil {
	// 	panic(err)
	// }

	if logIndex.SeqNum != protocol.InvalidLogSeqNum {
		tailSeqNum := logIndex.SeqNum
		currentSeqNum := uint64(0)
		if obj.view != nil {
			currentSeqNum = obj.view.nextSeqNum
			if tailSeqNum < currentSeqNum {
				log.Fatalf("[FATAL] Current seqNum=%#016x, cannot sync to %#016x", currentSeqNum, tailSeqNum)
			}
		}
		if tailSeqNum == currentSeqNum {
			return nil
		}
	}

	tag := objectLogTag(obj.nameHash)
	env := obj.env

	// DEBUG
	// prefix := fmt.Sprintf("%v:%v", obj.name, tag)
	// log.Printf("[DEBUG] %v syncToFuture start until seqnum=%016X", prefix, logIndex.SeqNum)
	// defer log.Println("")
	// defer log.Printf("[DEBUG] %v syncToFuture end", prefix)

	var view *ObjectView
	if obj.view != nil {
		view = obj.view
	} else {
		view = &ObjectView{
			name:       obj.name,
			nextSeqNum: 0,
			contents:   gabs.New(),
		}
	}
	// count := 0
	// seqNums := make([]uint64, 0)
	// auxCount := 0
	// auxSeqNums := make([]uint64, 0)
	// startSeqNum := view.nextSeqNum
	logStream := env.faasEnv.AsyncSharedLogReadNextUntil(obj.env.faasCtx, tag, view.nextSeqNum, logIndex, true /*fromCached*/)
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
		// DEBUG
		// count++
		// seqNums = append(seqNums, logEntry.SeqNum)

		objectLog := decodeLogEntry(logEntry)
		// log.Printf("[DEBUG] %v syncToFuture got seqnum=%016X objName=%v opSet=%v writeSet=%+v",
		// 	prefix, logEntry.SeqNum, obj.name, objectLog.listCachedObjectView(), objectLog.writeSet)
		if !objectLog.withinWriteSet(obj.name) {
			continue
		}
		if cachedView := objectLog.loadCachedObjectView(obj.name); cachedView != nil {
			// DEBUG
			// auxCount++
			// auxSeqNums = append(auxSeqNums, logEntry.SeqNum)

			view = cachedView
			// log.Printf("[DEBUG] %v syncToFuture Load cached view: seqNum=%016X obj=%s", prefix, objectLog.seqNum, obj.name)
			continue
		}
		if objectLog.LogType == LOG_TxnCommit {
			if committed, err := objectLog.checkTxnCommitResult(env); err != nil {
				return err
			} else if !committed {
				// log.Printf("[DEBUG] %v syncToFuture skip seqnum=%016X due to not committed", prefix, objectLog.seqNum)
				continue
			}
		}
		// apply view
		// log.Printf("[DEBUG] %v syncToFuture apply seqnum=%016X", prefix, objectLog.seqNum)
		if objectLog.seqNum < view.nextSeqNum {
			log.Fatalf("[FATAL] LogSeqNum=%#016x, ViewNextSeqNum=%#016x", objectLog.seqNum, view.nextSeqNum)
		}
		view.nextSeqNum = objectLog.seqNum + 1
		for _, op := range objectLog.Ops {
			if op.ObjName == obj.name {
				// log.Printf("[DEBUG] %v syncToFuture apply op=%+v", prefix, op)
				view.applyWriteOp(op)
			}
		}
		objectLog.cacheObjectView(env, view)
		// opSet := objectLog.cacheObjectView(env, view)
		// log.Printf("[DEBUG] %v syncToFuture cache view for seqnum=%016X obj=%s opSet=%v", prefix, objectLog.seqNum, view.name, opSet)
	}
	obj.view = view
	// log.Printf("[DEBUG] syncTo obj=%v from=%016X tag=%v nextFrom=%016X count=%v auxCount=%v",
	// 	obj.name, startSeqNum, tag, view.nextSeqNum, count, auxCount)
	// log.Printf("[DEBUG] syncTo from=%016X tag=%v count=%v:%v auxCount=%v:%v", startSeqNum, tag, count, seqNums, auxCount, auxSeqNums)
	return nil
}

// func (obj *ObjectRef) syncToForward(tailSeqNum uint64) error {
// 	tag := objectLogTag(obj.nameHash)
// 	env := obj.env
// 	if obj.view == nil {
// 		log.Fatalf("[FATAL] Empty object view: %s", obj.name)
// 	}
// 	seqNum := obj.view.nextSeqNum
// 	if tailSeqNum < seqNum {
// 		log.Fatalf("[FATAL] Current seqNum=%#016x, cannot sync to %#016x", seqNum, tailSeqNum)
// 	}
// 	for seqNum < tailSeqNum {
// 		logEntry, err := env.faasEnv.AsyncSharedLogReadNext(env.faasCtx, tag, seqNum)
// 		if err != nil {
// 			return newRuntimeError(err.Error())
// 		}
// 		if logEntry == nil || logEntry.SeqNum >= tailSeqNum {
// 			break
// 		}
// 		seqNum = logEntry.SeqNum + 1
// 		objectLog := decodeLogEntry(logEntry)
// 		if !objectLog.withinWriteSet(obj.name) {
// 			continue
// 		}
// 		if objectLog.LogType == LOG_TxnCommit {
// 			if committed, err := objectLog.checkTxnCommitResult(env); err != nil {
// 				return err
// 			} else if !committed {
// 				continue
// 			}
// 		}
// 		obj.view.nextSeqNum = objectLog.seqNum + 1
// 		for _, op := range objectLog.Ops {
// 			if op.ObjName == obj.name {
// 				obj.view.applyWriteOp(op)
// 			}
// 		}
// 		if !objectLog.hasCachedObjectView(obj.name) {
// 			objectLog.cacheObjectView(env, obj.view)
// 		}
// 	}
// 	return nil
// }

// func (obj *ObjectRef) syncToBackward(tailSeqNum uint64, tempView **ObjectView) error {
// 	tag := objectLogTag(obj.nameHash)
// 	env := obj.env
// 	objectLogs := make([]*ObjectLogEntry, 0, 4)
// 	var view *ObjectView
// 	seqNum := tailSeqNum
// 	currentSeqNum := uint64(0)
// 	if obj.view != nil {
// 		currentSeqNum = obj.view.nextSeqNum
// 		if tailSeqNum < currentSeqNum {
// 			log.Fatalf("[FATAL] Current seqNum=%#016x, cannot sync to %#016x", currentSeqNum, tailSeqNum)
// 		}
// 	}
// 	if tailSeqNum == currentSeqNum {
// 		return nil
// 	}

// 	// DEBUG
// 	// prefix := fmt.Sprintf("%v:%v", obj.name, tag)
// 	// log.Printf("[DEBUG] %v syncToBackward start until seqnum=%016X", prefix, tailSeqNum)
// 	// defer log.Printf("[DEBUG] %v syncToBackward end", prefix)

// 	var err error
// 	var currentLogEntryFuture types.Future[*types.LogEntryWithMeta] = nil
// 	var nextLogEntryFuture types.Future[*types.LogEntryWithMeta] = nil
// 	first := true
// 	for seqNum > currentSeqNum {
// 		if seqNum != protocol.MaxLogSeqnum {
// 			seqNum -= 1
// 		}
// 		if first {
// 			first = false
// 			// 1. first read
// 			currentLogEntryFuture, err = env.faasEnv.AsyncSharedLogReadPrev2(env.faasCtx, tag, seqNum)
// 			if err != nil {
// 				return newRuntimeError(err.Error())
// 			}
// 		}
// 		if currentLogEntryFuture == nil || currentLogEntryFuture.GetSeqNum() < currentSeqNum {
// 			break
// 		}
// 		if currentLogEntryFuture.IsResolved() {
// 			// HACK: disable async read if logs are cached
// 			first = true
// 		} else {
// 			// 3. aggressively do next read
// 			seqNum = currentLogEntryFuture.GetSeqNum()
// 			if seqNum > currentSeqNum {
// 				if seqNum != protocol.MaxLogSeqnum {
// 					seqNum -= 1
// 				}
// 				nextLogEntryFuture, err = env.faasEnv.AsyncSharedLogReadPrev2(env.faasCtx, tag, seqNum)
// 				if err != nil {
// 					return newRuntimeError(err.Error())
// 				}
// 			}
// 		}
// 		// 2. sync the current read
// 		logEntry, err := currentLogEntryFuture.GetResult(common.AsyncWaitTimeout)
// 		if err != nil {
// 			return newRuntimeError(err.Error())
// 		}
// 		if logEntry == nil || logEntry.SeqNum < currentSeqNum {
// 			// unreachable since the log's seqnum exists and had been asserted
// 			// by the future object above
// 			panic(fmt.Errorf("unreachable: %+v, %v", logEntry, currentSeqNum))
// 		}
// 		seqNum = logEntry.SeqNum

// 		currentLogEntryFuture = nextLogEntryFuture
// 		nextLogEntryFuture = nil

// 		// log.Printf("[DEBUG] %v syncToBackward got seqnum=%016X", prefix, logEntry.SeqNum)
// 		objectLog := decodeLogEntry(logEntry)
// 		if !objectLog.withinWriteSet(obj.name) {
// 			continue
// 		}
// 		if objectLog.LogType == LOG_TxnCommit {
// 			if committed, err := objectLog.checkTxnCommitResult(env); err != nil {
// 				return err
// 			} else if !committed {
// 				continue
// 			}
// 		}
// 		view = objectLog.loadCachedObjectView(obj.name)
// 		if view == nil {
// 			objectLogs = append(objectLogs, objectLog)
// 		} else {
// 			// log.Printf("[DEBUG] %v syncToBackward Load cached view: seqNum=%016X obj=%s", prefix, seqNum, obj.name)
// 			break
// 		}
// 	}

// 	if view == nil {
// 		if obj.view != nil {
// 			view = obj.view
// 		} else {
// 			view = &ObjectView{
// 				name:       obj.name,
// 				nextSeqNum: 0,
// 				contents:   gabs.New(),
// 			}
// 		}
// 	}
// 	for i := len(objectLogs) - 1; i >= 0; i-- {
// 		objectLog := objectLogs[i]
// 		// log.Printf("[DEBUG] %v syncToBackward apply seqnum=%016X", prefix, objectLog.seqNum)
// 		if objectLog.seqNum < view.nextSeqNum {
// 			log.Fatalf("[FATAL] LogSeqNum=%#016x, ViewNextSeqNum=%#016x", objectLog.seqNum, view.nextSeqNum)
// 		}
// 		view.nextSeqNum = objectLog.seqNum + 1
// 		for _, op := range objectLog.Ops {
// 			if op.ObjName == obj.name {
// 				view.applyWriteOp(op)
// 			}
// 		}
// 		// DEBUG
// 		objectLog.cacheObjectView(env, view)
// 	}
// 	// DEBUG
// 	obj.view = view
// 	// log.Printf("[DEBUG] %v syncToBackward final view=%+v", prefix, view.contents.Data())
// 	if tempView != nil {
// 		*tempView = view
// 	}
// 	return nil
// }

func (obj *ObjectRef) appendNormalOpLog(ops []*WriteOp) (types.Future[uint64], error) {
	if len(ops) == 0 {
		panic("Empty Ops for NormalOp log")
	}
	logEntry := &ObjectLogEntry{
		LogType: LOG_NormalOp,
		Ops:     ops,
	}
	encoded, err := json.Marshal(logEntry)
	if err != nil {
		panic(err)
	}
	tags := []types.Tag{
		{StreamType: common.FsmType_ObjectLog, StreamId: objectLogTag(obj.nameHash)},
	}
	future, err := obj.env.faasEnv.AsyncSharedLogAppend(obj.env.faasCtx, tags, common.CompressData(encoded))
	if err != nil {
		return nil, newRuntimeError(err.Error())
	} else {
		return future, nil
	}
}

func (obj *ObjectRef) appendWriteLog(op *WriteOp) (types.Future[uint64], error) {
	return obj.appendNormalOpLog([]*WriteOp{op})
}

func (env *envImpl) appendTxnBeginLog() (types.Future[uint64], error) {
	logEntry := &ObjectLogEntry{LogType: LOG_TxnBegin}
	encoded, err := json.Marshal(logEntry)
	if err != nil {
		panic(err)
	}
	tags := []types.Tag{
		{StreamType: common.FsmType_TxnMetaLog, StreamId: common.TxnMetaLogTag},
	}
	future, err := env.faasEnv.AsyncSharedLogAppend(env.faasCtx, tags, common.CompressData(encoded))
	if err != nil {
		return nil, newRuntimeError(err.Error())
	} else {
		// log.Printf("[DEBUG] Append TxnBegin log: seqNum=%#016x", seqNum)
		return future, nil
	}
}

func (env *envImpl) setLogAuxData(seqNum uint64, key uint64, data interface{}) error {
	encoded, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}
	compressed := common.CompressData(encoded)
	if FLAGS_RedisForAuxData {
		panic("not implemented")
		// key := fmt.Sprintf("%#016x", seqNum)
		// result := redisClient.Set(context.Background(), key, compressed, 0)
		// if result.Err() != nil {
		// 	log.Fatalf("[FATAL] Failed to set AuxData in Redis: %v", result.Err())
		// }
		// return nil
	}
	// log.Println("[DEBUG] SharedLogSetAuxData overall")
	err = env.faasEnv.SharedLogSetAuxDataWithShards(env.faasCtx, seqNum, key, compressed)
	if err != nil {
		return newRuntimeError(err.Error())
	} else {
		// log.Printf("[DEBUG] Set AuxData for log (seqNum=%#016x): contents=%s", seqNum, string(encoded))
		return nil
	}
}
