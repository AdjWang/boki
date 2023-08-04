package asyncstatestore

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"cs.utexas.edu/zjia/faas/protocol"
	"cs.utexas.edu/zjia/faas/slib/common"

	"cs.utexas.edu/zjia/faas/types"

	gabs "github.com/Jeffail/gabs/v2"
	redis "github.com/go-redis/redis/v8"
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

type ObjectLogEntry struct {
	seqNum   uint64
	auxData  map[string]interface{}
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
	reader, err := common.DecompressReader(logEntry.Data)
	if err != nil {
		panic(err)
	}
	objectLog := &ObjectLogEntry{}
	err = json.NewDecoder(reader).Decode(objectLog)
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
		reader, err := common.DecompressReader(auxData)
		if err != nil {
			panic(err)
		}
		var contents map[string]interface{}
		err = json.NewDecoder(reader).Decode(&contents)
		if err != nil {
			panic(err)
		}
		objectLog.auxData = contents
	}
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

func (txnCommitLog *ObjectLogEntry) checkTxnCommitResult(env *envImpl) (bool, error) {
	if txnCommitLog.LogType != LOG_TxnCommit {
		panic("Wrong log type")
	}
	if txnCommitLog.auxData != nil {
		if v, exists := txnCommitLog.auxData["r"]; exists {
			return v.(bool), nil
		}
	} else {
		txnCommitLog.auxData = make(map[string]interface{})
	}
	// log.Printf("[DEBUG] Failed to load txn status: seqNum=%#016x", txnCommitLog.seqNum)
	tags := make([]uint64, 0, len(txnCommitLog.Ops))
	commitResult := true
	checkedTag := make(map[uint64]bool)
	for _, op := range txnCommitLog.Ops {
		tag := objectLogTag(common.NameHash(op.ObjName))
		tags = append(tags, objectLogTag(common.NameHash("commit"+op.ObjName)))
		if _, exists := checkedTag[tag]; exists {
			continue
		}
		seqNum := txnCommitLog.seqNum
		currentSeqNum := txnCommitLog.TxnId

		var err error
		var currentLogEntryFuture types.Future[*types.LogEntryWithMeta] = nil
		var nextLogEntryFuture types.Future[*types.LogEntryWithMeta] = nil
		first := true
		for seqNum > currentSeqNum {
			if seqNum != protocol.MaxLogSeqnum {
				seqNum -= 1
			}
			if first {
				first = false
				// 1. first read
				currentLogEntryFuture, err = env.faasEnv.AsyncSharedLogReadPrev2(env.faasCtx, tag, seqNum)
				if err != nil {
					return false, newRuntimeError(err.Error())
				}
			}
			if currentLogEntryFuture == nil || currentLogEntryFuture.GetSeqNum() <= currentSeqNum {
				break
			}
			if currentLogEntryFuture.IsResolved() {
				// HACK: disable async read if logs are cached
				first = true
			} else {
				// 3. aggressively do next read
				seqNum = currentLogEntryFuture.GetSeqNum()
				if seqNum > currentSeqNum {
					if seqNum != protocol.MaxLogSeqnum {
						seqNum -= 1
					}
					nextLogEntryFuture, err = env.faasEnv.AsyncSharedLogReadPrev2(env.faasCtx, tag, seqNum)
					if err != nil {
						return false, newRuntimeError(err.Error())
					}
				}
			}
			// 2. sync the current read
			logEntry, err := currentLogEntryFuture.GetResult(60 * time.Second)
			if err != nil {
				return false, newRuntimeError(err.Error())
			}
			if logEntry == nil || logEntry.SeqNum < currentSeqNum {
				// unreachable since the log's seqnum exists and had been asserted
				// by the future object above
				panic(fmt.Errorf("unreachable: %+v, %v", logEntry, currentSeqNum))
			}
			seqNum = logEntry.SeqNum

			currentLogEntryFuture = nextLogEntryFuture
			nextLogEntryFuture = nil

			// log.Printf("[DEBUG] Read log with seqnum %#016x", seqNum)

			objectLog := decodeLogEntry(logEntry)
			if !txnCommitLog.writeSetOverlapped(objectLog) {
				continue
			}
			if objectLog.LogType == LOG_NormalOp {
				commitResult = false
				break
			} else if objectLog.LogType == LOG_TxnCommit {
				if committed, err := objectLog.checkTxnCommitResult(env); err != nil {
					return false, err
				} else if committed {
					commitResult = false
					break
				}
			}
		}
		if !commitResult {
			break
		}
		checkedTag[tag] = true
	}
	txnCommitLog.auxData["r"] = commitResult
	if !FLAGS_DisableAuxData {
		env.setLogAuxData(tags, txnCommitLog.seqNum, txnCommitLog.auxData)
	}
	return commitResult, nil
}

// func (txnCommitLog *ObjectLogEntry) checkTxnCommitResult(env *envImpl) (bool, error) {
// 	if txnCommitLog.LogType != LOG_TxnCommit {
// 		panic("Wrong log type")
// 	}
// 	if txnCommitLog.auxData != nil {
// 		if v, exists := txnCommitLog.auxData["r"]; exists {
// 			return v.(bool), nil
// 		}
// 	} else {
// 		txnCommitLog.auxData = make(map[string]interface{})
// 	}
// 	// log.Printf("[DEBUG] Failed to load txn status: seqNum=%#016x", txnCommitLog.seqNum)
// 	commitResult := true
// 	tags := make([]uint64, 0, len(txnCommitLog.Ops))
// 	checkedTag := make(map[uint64]bool)
// 	for _, op := range txnCommitLog.Ops {
// 		tag := objectLogTag(common.NameHash(op.ObjName))
// 		tags = append(tags, tag)
// 		if _, exists := checkedTag[tag]; exists {
// 			continue
// 		}
// 		logStream := env.faasEnv.AsyncSharedLogReadNextUntil(env.faasCtx, tag, types.LogEntryIndex{
// 			LocalId: protocol.InvalidLogLocalId,
// 			SeqNum:  txnCommitLog.seqNum,
// 		})
// 		doneCh := make(chan bool)
// 		errCh := make(chan error)
// 		go func(ctx context.Context) {
// 			opCommitResult := true
// 			for {
// 				var logEntry *types.LogEntryWithMeta
// 				select {
// 				case <-ctx.Done():
// 					errCh <- ctx.Err()
// 					return
// 				default:
// 					logStreamEntry := logStream.BlockingDequeue()
// 					logEntry = logStreamEntry.LogEntry
// 					err := logStreamEntry.Err
// 					if err != nil {
// 						errCh <- ctx.Err()
// 						return
// 					}
// 				}
// 				if logEntry == nil {
// 					break
// 				}
// 				objectLog := decodeLogEntry(logEntry)
// 				if !txnCommitLog.writeSetOverlapped(objectLog) {
// 					continue
// 				}
// 				if objectLog.LogType == LOG_NormalOp {
// 					opCommitResult = false
// 					break
// 				} else if objectLog.LogType == LOG_TxnCommit {
// 					if committed, err := objectLog.checkTxnCommitResult(env); err != nil {
// 						errCh <- err
// 						return
// 					} else if committed {
// 						opCommitResult = false
// 						break
// 					}
// 				}
// 			}
// 			doneCh <- opCommitResult
// 		}(env.faasCtx)
// 		checkedTag[tag] = true

// 		select {
// 		case commitResult = <-doneCh:
// 			if !commitResult {
// 				break
// 			}
// 		case err := <-errCh:
// 			return false, err
// 		}
// 	}
// 	txnCommitLog.auxData["r"] = true
// 	if !FLAGS_DisableAuxData {
// 		env.setLogAuxData(tags, txnCommitLog.seqNum, txnCommitLog.auxData)
// 	}
// 	return commitResult, nil
// }

func (l *ObjectLogEntry) listCachedObjectView() string {
	if l.auxData == nil {
		return ""
	}
	if l.LogType == LOG_NormalOp {
		return "NormalOp"
	} else if l.LogType == LOG_TxnCommit {
		objNames := make([]string, 0, len(l.auxData))
		for key, _ := range l.auxData {
			objNames = append(objNames, key[1:])
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
		key := "v" + objName
		_, exists := l.auxData[key]
		return exists
	}
	return false
}

func (l *ObjectLogEntry) loadCachedObjectView(objName string) *ObjectView {
	if l.auxData == nil {
		return nil
	}
	if l.LogType == LOG_NormalOp {
		return &ObjectView{
			name:       objName,
			nextSeqNum: l.seqNum + 1,
			contents:   gabs.Wrap(l.auxData),
		}
	} else if l.LogType == LOG_TxnCommit {
		key := "v" + objName
		if data, exists := l.auxData[key]; exists {
			return &ObjectView{
				name:       objName,
				nextSeqNum: l.seqNum + 1,
				contents:   gabs.Wrap(data),
			}
		}
	}
	return nil
}

func (l *ObjectLogEntry) cacheObjectView(env *envImpl, view *ObjectView) []string {
	if FLAGS_DisableAuxData {
		return nil
	}
	// DEBUG
	keySet := make([]string, 0, 10)
	if l.LogType == LOG_NormalOp {
		if l.auxData == nil {
			tag := objectLogTag(common.NameHash(view.name))
			env.setLogAuxData([]uint64{tag}, l.seqNum, view.contents.Data())
			keySet = append(keySet, "n-"+view.name)
		}
	} else if l.LogType == LOG_TxnCommit {
		if l.auxData == nil {
			l.auxData = make(map[string]interface{})
		}
		key := "v" + view.name
		if _, exists := l.auxData[key]; !exists {
			l.auxData[key] = view.contents.Data()
			tags := make([]uint64, 0, len(l.auxData))
			for key := range l.auxData {
				tag := objectLogTag(common.NameHash(key[1:]))
				tags = append(tags, tag)
				keySet = append(keySet, key[1:])
			}
			env.setLogAuxData(tags, l.seqNum, l.auxData)
			delete(l.auxData, key)
		}
	} else {
		panic("Wrong log type")
	}
	return keySet
}

// func (obj *ObjectRef) syncTo(tailSeqNum uint64) error {
// 	return obj.syncToBackward(tailSeqNum)
// }

func (obj *ObjectRef) syncTo(tailSeqNum uint64) error {
	var refView *ObjectView
	if err := obj.syncToBackward(tailSeqNum, &refView); err != nil {
		panic(err)
	}

	tag := objectLogTag(obj.nameHash)
	env := obj.env
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

	// DEBUG
	prefix := fmt.Sprintf("%v:%v", obj.name, tag)
	log.Printf("[DEBUG] %v syncToFuture start until seqnum=%016X", prefix, tailSeqNum)
	defer log.Println("")
	defer log.Printf("[DEBUG] %v syncToFuture end", prefix)

	logStream := env.faasEnv.AsyncSharedLogReadNextUntil(obj.env.faasCtx, tag, types.LogEntryIndex{
		LocalId: protocol.InvalidLogLocalId,
		SeqNum:  tailSeqNum,
	})

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
	doneCh := make(chan struct{})
	errCh := make(chan error)
	go func(ctx context.Context) {
		// var view interface{}
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
				doneCh <- struct{}{}
				break
			}
			objectLog := decodeLogEntry(logEntry)
			log.Printf("[DEBUG] %v syncToFuture got seqnum=%016X opSet=%v", prefix, logEntry.SeqNum, objectLog.listCachedObjectView())
			if !objectLog.withinWriteSet(obj.name) {
				continue
			}
			if cachedView := objectLog.loadCachedObjectView(obj.name); cachedView != nil {
				view = cachedView
				log.Printf("[DEBUG] %v syncToFuture Load cached view: seqNum=%016X obj=%s", prefix, objectLog.seqNum, obj.name)
				continue
			}
			if objectLog.LogType == LOG_TxnCommit {
				if committed, err := objectLog.checkTxnCommitResult(env); err != nil {
					errCh <- ctx.Err()
					return
				} else if !committed {
					continue
				}
			}
			// apply view
			log.Printf("[DEBUG] %v syncToFuture apply seqnum=%016X", prefix, objectLog.seqNum)
			if objectLog.seqNum < view.nextSeqNum {
				log.Fatalf("[FATAL] LogSeqNum=%#016x, ViewNextSeqNum=%#016x", objectLog.seqNum, view.nextSeqNum)
			}
			view.nextSeqNum = objectLog.seqNum + 1
			for _, op := range objectLog.Ops {
				if op.ObjName == obj.name {
					view.applyWriteOp(op)
				}
			}
			opSet := objectLog.cacheObjectView(env, view)
			log.Printf("[DEBUG] %v syncToFuture cache view for seqnum=%016X obj=%s opSet=%v", prefix, objectLog.seqNum, view.name, opSet)
		}
	}(obj.env.faasCtx)
	select {
	case <-doneCh:
		log.Printf("[DEBUG] %v syncToFuture final view=%+v", prefix, view.contents.Data())
		if !refView.Equal(*view) {
			log.Fatalf("different view: ref=%v current=%v", refView, view)
		}
		obj.view = view
		return nil
	case err := <-errCh:
		return err
	}
}

func (obj *ObjectRef) syncToForward(tailSeqNum uint64) error {
	tag := objectLogTag(obj.nameHash)
	env := obj.env
	if obj.view == nil {
		log.Fatalf("[FATAL] Empty object view: %s", obj.name)
	}
	seqNum := obj.view.nextSeqNum
	if tailSeqNum < seqNum {
		log.Fatalf("[FATAL] Current seqNum=%#016x, cannot sync to %#016x", seqNum, tailSeqNum)
	}
	for seqNum < tailSeqNum {
		logEntry, err := env.faasEnv.AsyncSharedLogReadNext(env.faasCtx, tag, seqNum)
		if err != nil {
			return newRuntimeError(err.Error())
		}
		if logEntry == nil || logEntry.SeqNum >= tailSeqNum {
			break
		}
		seqNum = logEntry.SeqNum + 1
		objectLog := decodeLogEntry(logEntry)
		if !objectLog.withinWriteSet(obj.name) {
			continue
		}
		if objectLog.LogType == LOG_TxnCommit {
			if committed, err := objectLog.checkTxnCommitResult(env); err != nil {
				return err
			} else if !committed {
				continue
			}
		}
		obj.view.nextSeqNum = objectLog.seqNum + 1
		for _, op := range objectLog.Ops {
			if op.ObjName == obj.name {
				obj.view.applyWriteOp(op)
			}
		}
		if !objectLog.hasCachedObjectView(obj.name) {
			objectLog.cacheObjectView(env, obj.view)
		}
	}
	return nil
}

func (obj *ObjectRef) syncToBackward(tailSeqNum uint64, tempView **ObjectView) error {
	tag := objectLogTag(obj.nameHash)
	env := obj.env
	objectLogs := make([]*ObjectLogEntry, 0, 4)
	var view *ObjectView
	seqNum := tailSeqNum
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

	// DEBUG
	prefix := fmt.Sprintf("%v:%v", obj.name, tag)
	log.Printf("[DEBUG] %v syncToBackward start until seqnum=%016X", prefix, tailSeqNum)
	defer log.Printf("[DEBUG] %v syncToBackward end", prefix)

	var err error
	var currentLogEntryFuture types.Future[*types.LogEntryWithMeta] = nil
	var nextLogEntryFuture types.Future[*types.LogEntryWithMeta] = nil
	first := true
	for seqNum > currentSeqNum {
		if seqNum != protocol.MaxLogSeqnum {
			seqNum -= 1
		}
		if first {
			first = false
			// 1. first read
			currentLogEntryFuture, err = env.faasEnv.AsyncSharedLogReadPrev2(env.faasCtx, tag, seqNum)
			if err != nil {
				return newRuntimeError(err.Error())
			}
		}
		if currentLogEntryFuture == nil || currentLogEntryFuture.GetSeqNum() < currentSeqNum {
			break
		}
		if currentLogEntryFuture.IsResolved() {
			// HACK: disable async read if logs are cached
			first = true
		} else {
			// 3. aggressively do next read
			seqNum = currentLogEntryFuture.GetSeqNum()
			if seqNum > currentSeqNum {
				if seqNum != protocol.MaxLogSeqnum {
					seqNum -= 1
				}
				nextLogEntryFuture, err = env.faasEnv.AsyncSharedLogReadPrev2(env.faasCtx, tag, seqNum)
				if err != nil {
					return newRuntimeError(err.Error())
				}
			}
		}
		// 2. sync the current read
		logEntry, err := currentLogEntryFuture.GetResult(60 * time.Second)
		if err != nil {
			return newRuntimeError(err.Error())
		}
		if logEntry == nil || logEntry.SeqNum < currentSeqNum {
			// unreachable since the log's seqnum exists and had been asserted
			// by the future object above
			panic(fmt.Errorf("unreachable: %+v, %v", logEntry, currentSeqNum))
		}
		seqNum = logEntry.SeqNum

		currentLogEntryFuture = nextLogEntryFuture
		nextLogEntryFuture = nil

		log.Printf("[DEBUG] %v syncToBackward got seqnum=%016X", prefix, logEntry.SeqNum)
		objectLog := decodeLogEntry(logEntry)
		if !objectLog.withinWriteSet(obj.name) {
			continue
		}
		if objectLog.LogType == LOG_TxnCommit {
			if committed, err := objectLog.checkTxnCommitResult(env); err != nil {
				return err
			} else if !committed {
				continue
			}
		}
		view = objectLog.loadCachedObjectView(obj.name)
		if view == nil {
			objectLogs = append(objectLogs, objectLog)
		} else {
			log.Printf("[DEBUG] %v syncToBackward Load cached view: seqNum=%016X obj=%s", prefix, seqNum, obj.name)
			break
		}
	}

	if view == nil {
		if obj.view != nil {
			view = obj.view
		} else {
			view = &ObjectView{
				name:       obj.name,
				nextSeqNum: 0,
				contents:   gabs.New(),
			}
		}
	}
	for i := len(objectLogs) - 1; i >= 0; i-- {
		objectLog := objectLogs[i]
		log.Printf("[DEBUG] %v syncToBackward apply seqnum=%016X", prefix, objectLog.seqNum)
		if objectLog.seqNum < view.nextSeqNum {
			log.Fatalf("[FATAL] LogSeqNum=%#016x, ViewNextSeqNum=%#016x", objectLog.seqNum, view.nextSeqNum)
		}
		view.nextSeqNum = objectLog.seqNum + 1
		for _, op := range objectLog.Ops {
			if op.ObjName == obj.name {
				view.applyWriteOp(op)
			}
		}
		// DEBUG
		// objectLog.cacheObjectView(env, view)
	}
	// DEBUG
	// obj.view = view
	log.Printf("[DEBUG] %v syncToBackward final view=%+v", prefix, view.contents.Data())
	*tempView = view
	return nil
}

func (obj *ObjectRef) appendNormalOpLog(ops []*WriteOp) (uint64 /* seqNum */, error) {
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
		return 0, newRuntimeError(err.Error())
	} else {
		// TODO: optimize
		seqNum, err := future.GetResult(60 * time.Second)
		if err != nil {
			return 0, newRuntimeError(err.Error())
		}
		return seqNum, nil
	}
}

func (obj *ObjectRef) appendWriteLog(op *WriteOp) (uint64 /* seqNum */, error) {
	return obj.appendNormalOpLog([]*WriteOp{op})
}

func (env *envImpl) appendTxnBeginLog() (uint64 /* seqNum */, error) {
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
		return 0, newRuntimeError(err.Error())
	} else {
		// log.Printf("[DEBUG] Append TxnBegin log: seqNum=%#016x", seqNum)
		// TODO: optimize
		seqNum, err := future.GetResult(60 * time.Second)
		if err != nil {
			return 0, newRuntimeError(err.Error())
		}
		return seqNum, nil
	}
}

func (env *envImpl) setLogAuxData(tags []uint64, seqNum uint64, data interface{}) error {
	encoded, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}
	compressed := common.CompressData(encoded)
	if FLAGS_RedisForAuxData {
		key := fmt.Sprintf("%#016x", seqNum)
		result := redisClient.Set(context.Background(), key, compressed, 0)
		if result.Err() != nil {
			log.Fatalf("[FATAL] Failed to set AuxData in Redis: %v", result.Err())
		}
		return nil
	}
	err = env.faasEnv.SharedLogSetAuxDataWithShards(env.faasCtx, tags, seqNum, compressed)
	if err != nil {
		return newRuntimeError(err.Error())
	} else {
		// log.Printf("[DEBUG] Set AuxData for log (seqNum=%#016x): contents=%s", seqNum, string(encoded))
		return nil
	}
}
