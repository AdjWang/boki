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
	LOG_NormalOpSync
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
	var result AuxData
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
		objectLog.auxData = DeserializeAuxData(auxData)
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

func (txnCommitLog *ObjectLogEntry) checkTxnCommitResult(env *envImpl, awaitSeqNum func() (uint64, error)) (bool, error) {
	if txnCommitLog.LogType != LOG_TxnCommit {
		panic("Wrong log type")
	}
	if txnCommitLog.auxData != nil {
		if v, exists := txnCommitLog.auxData[common.KeyCommitResult]; exists {
			commitResult, err := common.DecompressData([]byte(v))
			if err != nil {
				panic(err)
			}
			// use json ["t"]
			return string(commitResult) == "\"t\"", nil
		}
	} else {
		txnCommitLog.auxData = NewAuxData()
	}
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
		}, types.ReadOptions{FromCached: false, AuxTags: []uint64{common.KeyCommitResult}})
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
					if committed, err := objectLog.checkTxnCommitResult(env, nil /*getSeqNum*/); err != nil {
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
	}
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
		// txnCommitLog.seqNum maybe invalid here from txn.go:objectLog.seqNum = protocol.InvalidLogSeqNum
		if txnCommitLog.seqNum == protocol.InvalidLogSeqNum {
			if awaitSeqNum == nil {
				panic("unreachable")
			}
			seqNum, err := awaitSeqNum()
			if err != nil {
				return false, err
			}
			txnCommitLog.seqNum = seqNum
		}
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
	if l.LogType == LOG_NormalOp || l.LogType == LOG_TxnCommit {
		if data, exists := l.auxData[key]; exists {
			decompressedData, err := common.DecompressData([]byte(data))
			if err != nil {
				panic(err)
			}
			var gabsData map[string]interface{}
			if err := json.Unmarshal(decompressedData, &gabsData); err != nil {
				rawDataStr := fmt.Sprintf("%v %v %+v [", l.LogType, key, l.auxData)
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
	}
	return nil
}

func (l *ObjectLogEntry) cacheObjectView(env *envImpl, view *ObjectView) {
	if FLAGS_DisableAuxData {
		return
	}
	key := objectLogTag(common.NameHash(view.name))
	if l.LogType == LOG_NormalOp {
		if l.auxData == nil {
			env.setLogAuxData(l.seqNum, key, view.contents.Data())
		}
	} else if l.LogType == LOG_TxnCommit {
		if l.auxData == nil {
			l.auxData = NewAuxData()
		}
		if _, exists := l.auxData[key]; !exists {
			env.setLogAuxData(l.seqNum, key, view.contents.Data())
		}
	} else {
		panic("Wrong log type")
	}
}

func (obj *ObjectRef) syncTo(logIndex types.LogEntryIndex) error {
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
	logStream := env.faasEnv.AsyncSharedLogReadNextUntil(obj.env.faasCtx, tag, view.nextSeqNum, logIndex,
		types.ReadOptions{FromCached: true, AuxTags: []uint64{tag, common.KeyCommitResult}})
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
		objectLog := decodeLogEntry(logEntry)
		if !objectLog.withinWriteSet(obj.name) {
			continue
		}
		if cachedView := objectLog.loadCachedObjectView(obj.name); cachedView != nil {
			view = cachedView
			continue
		}
		if objectLog.LogType == LOG_TxnCommit {
			if committed, err := objectLog.checkTxnCommitResult(env, nil /*getSeqNum*/); err != nil {
				return err
			} else if !committed {
				continue
			}
		}
		// apply view
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

func (obj *ObjectRef) appendNormalOpSyncLog() (types.Future[uint64], error) {
	logEntry := &ObjectLogEntry{
		LogType: LOG_NormalOpSync,
	}
	encoded, err := json.Marshal(logEntry)
	if err != nil {
		panic(err)
	}
	tags := []types.Tag{
		{StreamType: common.FsmType_ObjectIdLog, StreamId: common.ObjectIdLogTag},
	}
	future, err := obj.env.faasEnv.AsyncSharedLogAppend(obj.env.faasCtx, tags, common.CompressData(encoded))
	if err != nil {
		return nil, newRuntimeError(err.Error())
	} else {
		return future, nil
	}
}

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
		{StreamType: common.FsmType_TxnIdLog, StreamId: common.TxnIdLogTag},
	}
	future, err := env.faasEnv.AsyncSharedLogAppend(env.faasCtx, tags, common.CompressData(encoded))
	if err != nil {
		return nil, newRuntimeError(err.Error())
	} else {
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
	err = env.faasEnv.SharedLogSetAuxDataWithShards(env.faasCtx, seqNum, key, compressed)
	if err != nil {
		return newRuntimeError(err.Error())
	} else {
		return nil
	}
}
