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
	seqNum     uint64
	viewsCache map[string]interface{}
	writeSet   map[string]bool

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
		if view, err := DeserializeViewsCache(auxData); err != nil {
			panic(err)
		} else {
			objectLog.viewsCache = view
		}
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

// func (txnCommitLog *ObjectLogEntry) checkTxnCommitResult(env *envImpl) (bool, error) {
// 	if txnCommitLog.LogType != LOG_TxnCommit {
// 		panic("Wrong log type")
// 	}
// 	if txnCommitLog.viewsCache != nil {
// 		if v, exists := txnCommitLog.viewsCache["r"]; exists {
// 			return v.(bool), nil
// 		}
// 	} else {
// 		txnCommitLog.viewsCache = make(map[string]interface{})
// 	}
// 	// log.Printf("[DEBUG] Failed to load txn status: seqNum=%#016x", txnCommitLog.seqNum)
// 	commitResult := true
// 	checkedTag := make(map[uint64]bool)
// 	for _, op := range txnCommitLog.Ops {
// 		tag := objectLogTag(common.NameHash(op.ObjName))
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
// 			// seqNum is stored as the LocalId
// 			if currentLogEntryFuture == nil || currentLogEntryFuture.GetLocalId() <= currentSeqNum {
// 				break
// 			}
// 			if currentLogEntryFuture.IsResolved() {
// 				// HACK: disable async read if logs are cached
// 				first = true
// 			} else {
// 				// 3. aggressively do next read
// 				seqNum = currentLogEntryFuture.GetLocalId()
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
// 			logEntry, err := currentLogEntryFuture.GetResult(common.AsyncWaitTimeout)
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

//				objectLog := decodeLogEntry(logEntry)
//				if !txnCommitLog.writeSetOverlapped(objectLog) {
//					continue
//				}
//				if objectLog.LogType == LOG_NormalOp {
//					commitResult = false
//					break
//				} else if objectLog.LogType == LOG_TxnCommit {
//					if committed, err := objectLog.checkTxnCommitResult(env); err != nil {
//						return false, err
//					} else if committed {
//						commitResult = false
//						break
//					}
//				}
//			}
//			if !commitResult {
//				break
//			}
//			checkedTag[tag] = true
//		}
//		txnCommitLog.viewsCache["r"] = commitResult
//		if !FLAGS_DisableAuxData {
//			env.setLogViewCache(common.TxnMetaLogTag, txnCommitLog.seqNum, txnCommitLog.viewsCache)
//		}
//		return commitResult, nil
//	}
func (txnCommitLog *ObjectLogEntry) checkTxnCommitResult(env *envImpl) (bool, error) {
	if txnCommitLog.LogType != LOG_TxnCommit {
		panic("Wrong log type")
	}
	if txnCommitLog.viewsCache != nil {
		if v, exists := txnCommitLog.viewsCache["r"]; exists {
			return v.(bool), nil
		}
	} else {
		txnCommitLog.viewsCache = make(map[string]interface{})
	}
	// log.Printf("[DEBUG] Failed to load txn status: seqNum=%#016x", txnCommitLog.seqNum)
	commitResult := true
	checkedTag := make(map[uint64]bool)
	for _, op := range txnCommitLog.Ops {
		tag := objectLogTag(common.NameHash(op.ObjName))
		if _, exists := checkedTag[tag]; exists {
			continue
		}
		seqNum := txnCommitLog.TxnId

		// get the last view position
		logEntry, err := env.faasEnv.AsyncSharedLogReadPrevWithAux(env.faasCtx, tag, seqNum)
		if err != nil {
			return false, newRuntimeError(err.Error())
		}
		if logEntry != nil && logEntry.SeqNum > seqNum {
			// update view
			objectLog := decodeLogEntry(logEntry)
			if view := objectLog.loadCachedObjectView(op.ObjName); view != nil {
				// update syncTo start position
				seqNum = view.nextSeqNum
			} else {
				// because aux data is promised and read by shard: tag=objectLogTag(obj.name)
				panic("unreachable")
			}
		}

		futures := make([]types.Future[*types.LogEntryWithMeta], 0, 100)
		for seqNum < txnCommitLog.seqNum {
			logEntryFuture, err := env.faasEnv.AsyncSharedLogReadNext2(env.faasCtx, tag, seqNum)
			if err != nil {
				return false, newRuntimeError(err.Error())
			}
			// logEntryFuture.GetLocalId() => logEntrySeqNum
			if logEntryFuture == nil || logEntryFuture.GetLocalId() >= txnCommitLog.seqNum {
				break
			}
			seqNum = logEntryFuture.GetLocalId() + 1
			futures = append(futures, logEntryFuture)
		}

		for _, logEntryFuture := range futures {
			logEntry, err := logEntryFuture.GetResult(common.AsyncWaitTimeout)
			if err != nil {
				return false, newRuntimeError(err.Error())
			}
			// bussiness logics
			objectLog := decodeLogEntry(logEntry)
			// log.Printf("[DEBUG] Read log with seqnum %#016x", seqNum)

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
	txnCommitLog.viewsCache["r"] = commitResult
	if !FLAGS_DisableAuxData {
		env.setLogViewCache(txnCommitLog.seqNum, txnCommitLog.viewsCache)
	}
	return commitResult, nil
}

func (l *ObjectLogEntry) hasCachedObjectView(objName string) bool {
	if l.viewsCache == nil {
		return false
	}
	if l.LogType == LOG_NormalOp {
		return true
	} else if l.LogType == LOG_TxnCommit {
		key := "v" + objName
		_, exists := l.viewsCache[key]
		return exists
	}
	return false
}

func (l *ObjectLogEntry) loadCachedObjectView(objName string) *ObjectView {
	if l.viewsCache == nil {
		return nil
	}
	if l.LogType == LOG_NormalOp {
		return &ObjectView{
			name:       objName,
			nextSeqNum: l.seqNum + 1,
			contents:   gabs.Wrap(l.viewsCache),
		}
	} else if l.LogType == LOG_TxnCommit {
		key := "v" + objName
		if data, exists := l.viewsCache[key]; exists {
			return &ObjectView{
				name:       objName,
				nextSeqNum: l.seqNum + 1,
				contents:   gabs.Wrap(data),
			}
		}
	}
	return nil
}

func (l *ObjectLogEntry) cacheObjectView(env *envImpl, view *ObjectView) {
	if FLAGS_DisableAuxData {
		return
	}
	if l.LogType == LOG_NormalOp {
		if l.viewsCache == nil {
			env.setLogViewCache(l.seqNum, view.contents.Data())
		}
	} else if l.LogType == LOG_TxnCommit {
		if l.viewsCache == nil {
			l.viewsCache = make(map[string]interface{})
		}
		key := "v" + view.name
		if _, exists := l.viewsCache[key]; !exists {
			l.viewsCache[key] = view.contents.Data()
			env.setLogViewCache(l.seqNum, l.viewsCache)
			delete(l.viewsCache, key)
		}
	} else {
		panic("Wrong log type")
	}
}

func (obj *ObjectRef) syncTo(tailSeqNum uint64) error {
	return obj.syncToForward(tailSeqNum)
	// return obj.syncToBackward(tailSeqNum)
}

func (obj *ObjectRef) syncToForward(tailSeqNum uint64) error {
	log.SetPrefix("[" + obj.name + "] ")
	defer log.SetPrefix("")

	tag := objectLogTag(obj.nameHash)
	env := obj.env
	seqNum := uint64(0)
	if obj.view != nil {
		seqNum = obj.view.nextSeqNum
	}
	if tailSeqNum < seqNum {
		log.Fatalf("[FATAL] Current seqNum=%#016x, cannot sync to %#016x", seqNum, tailSeqNum)
	}

	lastViewSeqNum := tailSeqNum
	for lastViewSeqNum > seqNum {
		// get the last view position
		logEntry, err := env.faasEnv.AsyncSharedLogReadPrevWithAux(env.faasCtx, tag, lastViewSeqNum)
		if err != nil {
			return err
		}
		if logEntry != nil && logEntry.SeqNum >= seqNum {
			// update view
			objectLog := decodeLogEntry(logEntry)
			if view := objectLog.loadCachedObjectView(obj.name); view != nil {
				obj.view = view
				// update syncTo start position
				seqNum = view.nextSeqNum
				break
			} else {
				lastViewSeqNum = logEntry.SeqNum - 1
				continue
			}
		} else {
			break
		}
	}
	// DEBUG
	// log.Printf("[DEBUG] syncToForward retrive view tag=%v, logEntry=%+v, view=%+v", tag, logEntry, obj.view)

	// DEBUG
	log.Printf("[DEBUG] syncToForward start of obj=%+v seqnumRange=[%d,%d]", obj, seqNum, tailSeqNum)

	futures := make([]types.Future[*types.LogEntryWithMeta], 0, 100)
	for seqNum <= tailSeqNum {
		logEntryFuture, err := env.faasEnv.AsyncSharedLogReadNext2(env.faasCtx, tag, seqNum)
		if err != nil {
			return err
		}
		// logEntryFuture.GetLocalId() => logEntrySeqNum
		if logEntryFuture == nil || logEntryFuture.GetLocalId() > tailSeqNum {
			break
		} else if logEntryFuture.GetLocalId() == tailSeqNum {
			// mitigate next useless read
			futures = append(futures, logEntryFuture)
			break
		}
		seqNum = logEntryFuture.GetLocalId() + 1
		futures = append(futures, logEntryFuture)
	}
	// DEBUG
	seqNums := make([]uint64, 0)
	for _, future := range futures {
		seqNums = append(seqNums, future.GetLocalId())
	}
	log.Printf("[DEBUG] syncToForward got seqnumRange=%v", seqNums)

	var view *ObjectView
	if obj.view != nil {
		// DEBUG
		log.Println("[DEBUG] syncToForward use objectView")
		view = obj.view
	} else {
		// DEBUG
		log.Println("[DEBUG] syncToForward use new empty objectView")
		view = NewEmptyObjectView(obj.name)
	}

	for _, logEntryFuture := range futures {
		logEntry, err := logEntryFuture.GetResult(common.AsyncWaitTimeout)
		if err != nil {
			return err
		}
		// bussiness logics
		objectLog := decodeLogEntry(logEntry)
		if !objectLog.withinWriteSet(obj.name) {
			continue
		} else if cachedView := objectLog.loadCachedObjectView(obj.name); cachedView != nil {
			// resolved by others (same object)
			view = cachedView
			continue
		}
		// DEBUG
		log.Printf("[DEBUG] logEntry=%+v", objectLog)
		if objectLog.LogType == LOG_TxnCommit {
			if objectLog.viewsCache != nil {
				// resolved by others (different object)
				if v, exists := objectLog.viewsCache["r"]; exists && v.(bool) {
					view.ApplyLogEntry(objectLog)
				}
			} else {
				txnSnapshot := objectLog.TxnId
				if txnSnapshot < view.nextSeqNum {
					// abort due to interval writes on self obj
					objectLog.viewsCache["r"] = false
				} else {
					// check if other objects are able to commit
					if committed, err := objectLog.checkTxnCommitResult(env); err != nil {
						return err
					} else if committed {
						view.ApplyLogEntry(objectLog)
					}
				}
			}
		} else if objectLog.LogType == LOG_NormalOp {
			view.ApplyLogEntry(objectLog)
		} else {
			panic(fmt.Sprintf("unreachable LogType=%d", objectLog.LogType))
		}
		// cache view
		if !objectLog.hasCachedObjectView(obj.name) {
			// DEBUG
			log.Printf("[DEBUG] syncToForward cache view at id=%d, tag=%d", objectLog.seqNum, tag)
			objectLog.cacheObjectView(env, view)
		}
	}
	obj.view = view
	// DEBUG
	log.Printf("[DEBUG] syncToForward end of obj=%+v", obj)
	return nil
}

func (obj *ObjectRef) syncToBackward(tailSeqNum uint64) error {
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
		// seqNum is stored as the LocalId
		if currentLogEntryFuture == nil || currentLogEntryFuture.GetLocalId() < currentSeqNum {
			break
		}
		if currentLogEntryFuture.IsResolved() {
			// HACK: disable async read if logs are cached
			first = true
		} else {
			// 3. aggressively do next read
			seqNum = currentLogEntryFuture.GetLocalId()
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
		logEntry, err := currentLogEntryFuture.GetResult(common.AsyncWaitTimeout)
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

		// log.Printf("[DEBUG] Read log with seqnum %#016x", seqNum)
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
			// log.Printf("[DEBUG] Load cached view: seqNum=%#016x, obj=%s", seqNum, obj.name)
			break
		}
	}

	if view == nil {
		if obj.view != nil {
			view = obj.view
		} else {
			view = NewEmptyObjectView(obj.name)
		}
	}
	for i := len(objectLogs) - 1; i >= 0; i-- {
		objectLog := objectLogs[i]
		if objectLog.seqNum < view.nextSeqNum {
			log.Fatalf("[FATAL] LogSeqNum=%#016x, ViewNextSeqNum=%#016x", objectLog.seqNum, view.nextSeqNum)
		}
		view.nextSeqNum = objectLog.seqNum + 1
		for _, op := range objectLog.Ops {
			if op.ObjName == obj.name {
				view.applyWriteOp(op)
			}
		}
		objectLog.cacheObjectView(env, view)
	}
	obj.view = view
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
		seqNum, err := future.GetResult(common.AsyncWaitTimeout)
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
		seqNum, err := future.GetResult(common.AsyncWaitTimeout)
		if err != nil {
			return 0, newRuntimeError(err.Error())
		}
		return seqNum, nil
	}
}

// maybe one view by NormalOp or multiple views by TxnOps
func (env *envImpl) setLogViewCache(seqNum uint64, viewsCache interface{}) error {
	data, err := SerializeViewsCache(viewsCache)
	if err != nil {
		return newRuntimeError(err.Error())
	}
	if FLAGS_RedisForAuxData {
		key := fmt.Sprintf("%#016x", seqNum)
		result := redisClient.Set(context.Background(), key, data, 0)
		if result.Err() != nil {
			log.Fatalf("[FATAL] Failed to set AuxData in Redis: %v", result.Err())
		}
		return nil
	}
	err = env.faasEnv.AsyncSharedLogSetAuxData(env.faasCtx, seqNum, data)
	if err != nil {
		return newRuntimeError(err.Error())
	} else {
		// log.Printf("[DEBUG] Set AuxData for log (seqNum=%#016x): contents=%s", seqNum, string(encoded))
		return nil
	}
}
