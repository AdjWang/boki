package protocol

import (
	"encoding/binary"
	"fmt"
	"math"
)

type FuncCall struct {
	FuncId   uint16
	MethodId uint16
	ClientId uint16
	CallId   uint32
}

const FuncCallByteSize = 8

const FuncIdBits = 8
const MethodIdBits = 6
const ClientIdBits = 14

func (funcCall *FuncCall) FullCallId() uint64 {
	return uint64(funcCall.FuncId) +
		(uint64(funcCall.MethodId) << FuncIdBits) +
		(uint64(funcCall.ClientId) << (FuncIdBits + MethodIdBits)) +
		(uint64(funcCall.CallId) << (FuncIdBits + MethodIdBits + ClientIdBits))
}

func FuncCallFromFullCallId(fullCallId uint64) FuncCall {
	return FuncCall{
		FuncId:   uint16(fullCallId & ((1 << FuncIdBits) - 1)),
		MethodId: uint16((fullCallId >> FuncIdBits) & ((1 << MethodIdBits) - 1)),
		ClientId: uint16((fullCallId >> (FuncIdBits + MethodIdBits)) & ((1 << ClientIdBits) - 1)),
		CallId:   uint32(fullCallId >> (FuncIdBits + MethodIdBits + ClientIdBits)),
	}
}

// MessageType enum
const (
	MessageType_INVALID               uint16 = 0
	MessageType_FUNC_WORKER_HANDSHAKE uint16 = 3
	MessageType_HANDSHAKE_RESPONSE    uint16 = 4
	MessageType_CREATE_FUNC_WORKER    uint16 = 5
	MessageType_INVOKE_FUNC           uint16 = 6
	MessageType_DISPATCH_FUNC_CALL    uint16 = 7
	MessageType_FUNC_CALL_COMPLETE    uint16 = 8
	MessageType_FUNC_CALL_FAILED      uint16 = 9
	MessageType_SHARED_LOG_OP         uint16 = 10
)

// SharedLogOpType enum
const (
	SharedLogOpType_INVALID     uint16 = 0x00
	SharedLogOpType_APPEND      uint16 = 0x01
	SharedLogOpType_READ_NEXT   uint16 = 0x02
	SharedLogOpType_READ_PREV   uint16 = 0x03
	SharedLogOpType_TRIM        uint16 = 0x04
	SharedLogOpType_SET_AUXDATA uint16 = 0x05
	SharedLogOpType_READ_NEXT_B uint16 = 0x06
	SharedLogOpType_READ_SYNCTO uint16 = 0x07

	SharedLogOpType_ASYNC_APPEND        uint16 = 0x20
	SharedLogOpType_ASYNC_READ_NEXT     uint16 = 0x21
	SharedLogOpType_ASYNC_READ_NEXT_B   uint16 = 0x22
	SharedLogOpType_ASYNC_READ_PREV     uint16 = 0x23
	SharedLogOpType_ASYNC_READ_PREV_AUX uint16 = 0x24
	SharedLogOpType_ASYNC_READ_LOCALID  uint16 = 0x25
)

// SharedLogResultType enum
const (
	SharedLogResultType_INVALID uint16 = 0x00
	// Successful results
	SharedLogResultType_APPEND_OK  uint16 = 0x20
	SharedLogResultType_READ_OK    uint16 = 0x21
	SharedLogResultType_TRIM_OK    uint16 = 0x22
	SharedLogResultType_LOCALID    uint16 = 0x23
	SharedLogResultType_AUXDATA_OK uint16 = 0x24
	// Async successful results
	SharedLogResultType_ASYNC_APPEND_OK uint16 = 0x30
	SharedLogResultType_ASYNC_READ_OK   uint16 = 0x31
	SharedLogResultType_ASYNC_DISCARDED uint16 = 0x32
	SharedLogResultType_ASYNC_EMPTY     uint16 = 0x33
	// NO ASYNC_DATA_LOST because all async reads are local index reads
	// Error results
	SharedLogResultType_BAD_ARGS    uint16 = 0x40
	SharedLogResultType_DISCARDED   uint16 = 0x41
	SharedLogResultType_EMPTY       uint16 = 0x42
	SharedLogResultType_DATA_LOST   uint16 = 0x43
	SharedLogResultType_TRIM_FAILED uint16 = 0x44
)

const MaxLogSeqnum = uint64(0xffff000000000000)
const InvalidLogLocalId = math.MaxUint64
const InvalidLogSeqNum = math.MaxUint64

const MessageTypeBits = 4

// Matches __FAAS_CACHE_LINE_SIZE in base/macro.h
const MessageHeaderByteSize = 64

// Matches __FAAS_MESSAGE_SIZE in base/macro.h
const MessageFullByteSize = 2816
const MessageInlineDataSize = MessageFullByteSize - MessageHeaderByteSize

const SharedLogTagByteSize = 8

const (
	FLAG_FuncWorkerUseEngineSocket uint32 = (1 << 0)
	FLAG_UseFifoForNestedCall      uint32 = (1 << 1)
	FLAG_kAsyncInvokeFuncFlag      uint32 = (1 << 2)
	FLAG_kLogResponseContinueFlag  uint32 = (1 << 3)
	FLAG_kLogResponseEOFDataFlag   uint32 = (1 << 4)
	FLAG_kLogResponseEOFFlag       uint32 = (1 << 5)
	FLAG_kLogQueryLocalIdFlag      uint32 = (1 << 6)
)

// DEBUG
func InspectMessage(buffer []byte) string {
	localId := GetLogLocalIdFromMessage(buffer)
	seqNum := GetLogSeqNumFromMessage(buffer)
	result := GetSharedLogResultTypeFromMessage(buffer)
	flags := GetFlagsFromMessage(buffer)
	respId := GetResponseIdFromMessage(buffer)
	return fmt.Sprintf("localId=%016X, seqNum=%016X, resultType=%v, flags=%v, respId=%v",
		localId, seqNum, result, flags, respId)
}

func GetResponseIdFromMessage(buffer []byte) uint64 {
	return binary.LittleEndian.Uint64(buffer[40:48])
}

func GetFlagsFromMessage(buffer []byte) uint32 {
	return binary.LittleEndian.Uint32(buffer[28:32])
}
func SetFlagsToMessage(buffer []byte, flag uint32) {
	flags := binary.LittleEndian.Uint32(buffer[28:32])
	flags |= flag
	binary.LittleEndian.PutUint32(buffer[28:32], flags)
}

func GetSharedLogOpFlagsFromMessage(buffer []byte) uint32 {
	flags := GetFlagsFromMessage(buffer)

	// DEBUG: can only set one log response hint flag
	logRespFlagCount := 0
	if (flags & FLAG_kLogResponseContinueFlag) != 0 {
		logRespFlagCount++
	}
	if (flags & FLAG_kLogResponseEOFDataFlag) != 0 {
		logRespFlagCount++
	}
	if (flags & FLAG_kLogResponseEOFFlag) != 0 {
		logRespFlagCount++
	}
	if logRespFlagCount != 1 {
		panic(fmt.Sprintf("invalid flags: %b", flags))
	}

	return flags
}

func GetFuncCallFromMessage(buffer []byte) FuncCall {
	tmp := binary.LittleEndian.Uint64(buffer[0:8])
	return FuncCallFromFullCallId(tmp >> MessageTypeBits)
}

func GetSharedLogOpTypeFromMessage(buffer []byte) uint16 {
	return binary.LittleEndian.Uint16(buffer[32:34])
}

func GetSharedLogResultTypeFromMessage(buffer []byte) uint16 {
	return binary.LittleEndian.Uint16(buffer[34:36])
}

// only from response
func GetLogLocalIdFromMessage(buffer []byte) uint64 {
	return binary.LittleEndian.Uint64(buffer[56:64])
}

func GetLogSeqNumFromMessage(buffer []byte) uint64 {
	return binary.LittleEndian.Uint64(buffer[8:16])
}

func GetLogNumTagsFromMessage(buffer []byte) int {
	return int(binary.LittleEndian.Uint16(buffer[36:38]))
}

func GetLogTagFromMessage(buffer []byte, tagIndex int) uint64 {
	bufIndex := MessageHeaderByteSize + tagIndex*SharedLogTagByteSize
	return binary.LittleEndian.Uint64(buffer[bufIndex : bufIndex+SharedLogTagByteSize])
}

func GetLogAuxDataSizeFromMessage(buffer []byte) int {
	return int(binary.LittleEndian.Uint16(buffer[38:40]))
}

func GetLogClientDataFromMessage(buffer []byte) uint64 {
	return binary.LittleEndian.Uint64(buffer[48:56])
}

func getMessageType(buffer []byte) uint16 {
	firstByte := buffer[0]
	return uint16(firstByte & ((1 << MessageTypeBits) - 1))
}

func IsHandshakeResponseMessage(buffer []byte) bool {
	return getMessageType(buffer) == MessageType_HANDSHAKE_RESPONSE
}

func IsCreateFuncWorkerMessage(buffer []byte) bool {
	return getMessageType(buffer) == MessageType_CREATE_FUNC_WORKER
}

func IsDispatchFuncCallMessage(buffer []byte) bool {
	return getMessageType(buffer) == MessageType_DISPATCH_FUNC_CALL
}

func IsFuncCallCompleteMessage(buffer []byte) bool {
	return getMessageType(buffer) == MessageType_FUNC_CALL_COMPLETE
}

func IsFuncCallFailedMessage(buffer []byte) bool {
	return getMessageType(buffer) == MessageType_FUNC_CALL_FAILED
}

func IsSharedLogOpMessage(buffer []byte) bool {
	return getMessageType(buffer) == MessageType_SHARED_LOG_OP
}

func IsSharedLogAsyncResult(buffer []byte) bool {
	resultType := GetSharedLogResultTypeFromMessage(buffer)
	return (resultType >= SharedLogResultType_ASYNC_APPEND_OK &&
		resultType < SharedLogResultType_BAD_ARGS)
}

func NewEmptyMessage() []byte {
	buffer := make([]byte, MessageFullByteSize)
	// set default localid and seqnum
	binary.LittleEndian.PutUint64(buffer[8:16], InvalidLogSeqNum)  // target
	binary.LittleEndian.PutUint64(buffer[56:64], InvalidLogSeqNum) // from
	return buffer
}

func NewFuncWorkerHandshakeMessage(funcId uint16, clientId uint16) []byte {
	buffer := NewEmptyMessage()
	tmp := uint64(funcId) << MessageTypeBits
	tmp += uint64(clientId) << (MessageTypeBits + FuncIdBits + MethodIdBits)
	tmp += uint64(MessageType_FUNC_WORKER_HANDSHAKE)
	binary.LittleEndian.PutUint64(buffer[0:8], tmp)
	return buffer
}

func NewInvokeFuncCallMessage(funcCall FuncCall, parentCallId uint64, async bool) []byte {
	buffer := NewEmptyMessage()
	tmp := (funcCall.FullCallId() << MessageTypeBits) + uint64(MessageType_INVOKE_FUNC)
	binary.LittleEndian.PutUint64(buffer[0:8], tmp)
	binary.LittleEndian.PutUint64(buffer[8:16], parentCallId)
	if async {
		binary.LittleEndian.PutUint32(buffer[28:32], FLAG_kAsyncInvokeFuncFlag)
	}
	return buffer
}

func NewFuncCallCompleteMessage(funcCall FuncCall, processingTime int32) []byte {
	buffer := NewEmptyMessage()
	tmp := (funcCall.FullCallId() << MessageTypeBits) + uint64(MessageType_FUNC_CALL_COMPLETE)
	binary.LittleEndian.PutUint64(buffer[0:8], tmp)
	binary.LittleEndian.PutUint32(buffer[12:16], uint32(processingTime))
	return buffer
}

func NewFuncCallFailedMessage(funcCall FuncCall) []byte {
	buffer := NewEmptyMessage()
	tmp := (funcCall.FullCallId() << MessageTypeBits) + uint64(MessageType_FUNC_CALL_FAILED)
	binary.LittleEndian.PutUint64(buffer[0:8], tmp)
	return buffer
}

func NewSharedLogAppendMessage(currentCallId uint64, myClientId uint16, numTags uint16, clientData uint64) []byte {
	buffer := NewEmptyMessage()
	tmp := (currentCallId << MessageTypeBits) + uint64(MessageType_SHARED_LOG_OP)
	binary.LittleEndian.PutUint64(buffer[0:8], tmp)
	binary.LittleEndian.PutUint16(buffer[32:34], SharedLogOpType_APPEND)
	binary.LittleEndian.PutUint16(buffer[34:36], myClientId)
	binary.LittleEndian.PutUint16(buffer[36:38], numTags)
	binary.LittleEndian.PutUint64(buffer[48:56], clientData)
	return buffer
}

func NewAsyncSharedLogAppendMessage(currentCallId uint64, myClientId uint16, numTags uint16, clientData uint64) []byte {
	buffer := NewEmptyMessage()
	tmp := (currentCallId << MessageTypeBits) + uint64(MessageType_SHARED_LOG_OP)
	binary.LittleEndian.PutUint64(buffer[0:8], tmp)
	binary.LittleEndian.PutUint16(buffer[32:34], SharedLogOpType_ASYNC_APPEND)
	binary.LittleEndian.PutUint16(buffer[34:36], myClientId)
	binary.LittleEndian.PutUint16(buffer[36:38], numTags)
	binary.LittleEndian.PutUint64(buffer[48:56], clientData)
	return buffer
}

func NewSharedLogSyncToMessage(currentCallId uint64, myClientId uint16, tag uint64, fromSeqNum uint64, logIndex uint64, useLocalId bool, clientData uint64) []byte {
	buffer := NewEmptyMessage()
	tmp := (currentCallId << MessageTypeBits) + uint64(MessageType_SHARED_LOG_OP)
	binary.LittleEndian.PutUint64(buffer[0:8], tmp)
	binary.LittleEndian.PutUint16(buffer[32:34], SharedLogOpType_READ_SYNCTO)
	binary.LittleEndian.PutUint16(buffer[34:36], myClientId)
	binary.LittleEndian.PutUint64(buffer[40:48], tag)
	binary.LittleEndian.PutUint64(buffer[48:56], clientData)
	binary.LittleEndian.PutUint64(buffer[56:64], fromSeqNum)
	binary.LittleEndian.PutUint64(buffer[8:16], logIndex)
	if useLocalId {
		SetFlagsToMessage(buffer, FLAG_kLogQueryLocalIdFlag)
	}
	return buffer
}

func NewSharedLogReadMessage(currentCallId uint64, myClientId uint16, tag uint64, seqNum uint64, direction int, block bool, clientData uint64) []byte {
	buffer := NewEmptyMessage()
	tmp := (currentCallId << MessageTypeBits) + uint64(MessageType_SHARED_LOG_OP)
	binary.LittleEndian.PutUint64(buffer[0:8], tmp)
	if direction > 0 {
		if block {
			binary.LittleEndian.PutUint16(buffer[32:34], SharedLogOpType_READ_NEXT_B)
		} else {
			binary.LittleEndian.PutUint16(buffer[32:34], SharedLogOpType_READ_NEXT)
		}
	} else {
		binary.LittleEndian.PutUint16(buffer[32:34], SharedLogOpType_READ_PREV)
	}
	binary.LittleEndian.PutUint16(buffer[34:36], myClientId)
	binary.LittleEndian.PutUint64(buffer[40:48], tag)
	binary.LittleEndian.PutUint64(buffer[48:56], clientData)
	binary.LittleEndian.PutUint64(buffer[8:16], seqNum)
	return buffer
}

func NewAsyncSharedLogReadMessage(currentCallId uint64, myClientId uint16, tag uint64, seqNum uint64,
	direction int, block bool, promiseAux bool, clientData uint64) []byte {

	buffer := NewEmptyMessage()
	tmp := (currentCallId << MessageTypeBits) + uint64(MessageType_SHARED_LOG_OP)
	binary.LittleEndian.PutUint64(buffer[0:8], tmp)
	if direction > 0 {
		if block {
			binary.LittleEndian.PutUint16(buffer[32:34], SharedLogOpType_ASYNC_READ_NEXT_B)
		} else {
			binary.LittleEndian.PutUint16(buffer[32:34], SharedLogOpType_ASYNC_READ_NEXT)
		}
	} else {
		if promiseAux {
			binary.LittleEndian.PutUint16(buffer[32:34], SharedLogOpType_ASYNC_READ_PREV_AUX)
		} else {
			binary.LittleEndian.PutUint16(buffer[32:34], SharedLogOpType_ASYNC_READ_PREV)
		}
	}
	binary.LittleEndian.PutUint16(buffer[34:36], myClientId)
	binary.LittleEndian.PutUint64(buffer[40:48], tag)
	binary.LittleEndian.PutUint64(buffer[48:56], clientData)
	binary.LittleEndian.PutUint64(buffer[8:16], seqNum)
	return buffer
}

func NewAsyncSharedLogReadIndexMessage(currentCallId uint64, myClientId uint16, localId uint64, clientData uint64) []byte {
	buffer := NewEmptyMessage()
	tmp := (currentCallId << MessageTypeBits) + uint64(MessageType_SHARED_LOG_OP)
	binary.LittleEndian.PutUint64(buffer[0:8], tmp)
	binary.LittleEndian.PutUint16(buffer[32:34], SharedLogOpType_ASYNC_READ_LOCALID)
	binary.LittleEndian.PutUint16(buffer[34:36], myClientId)
	binary.LittleEndian.PutUint64(buffer[48:56], clientData)
	binary.LittleEndian.PutUint64(buffer[8:16], localId)
	SetFlagsToMessage(buffer, FLAG_kLogQueryLocalIdFlag)
	return buffer
}

func NewSharedLogSetAuxDataMessage(currentCallId uint64, myClientId uint16, seqNum uint64, clientData uint64) []byte {
	return NewSharedLogSetAuxDataMessageWithShards(currentCallId, myClientId, 0 /*numTags*/, seqNum, clientData)
}

func NewSharedLogSetAuxDataMessageWithShards(currentCallId uint64, myClientId uint16, numTags uint16, seqNum uint64, clientData uint64) []byte {
	buffer := NewEmptyMessage()
	tmp := (currentCallId << MessageTypeBits) + uint64(MessageType_SHARED_LOG_OP)
	binary.LittleEndian.PutUint64(buffer[0:8], tmp)
	binary.LittleEndian.PutUint16(buffer[32:34], SharedLogOpType_SET_AUXDATA)
	binary.LittleEndian.PutUint16(buffer[34:36], myClientId)
	binary.LittleEndian.PutUint16(buffer[36:38], numTags)
	binary.LittleEndian.PutUint64(buffer[48:56], clientData)
	binary.LittleEndian.PutUint64(buffer[8:16], seqNum)
	return buffer
}

func GetClientIdFromMessage(buffer []byte) uint16 {
	return GetFuncCallFromMessage(buffer).ClientId
}

func GetSendTimestampFromMessage(buffer []byte) int64 {
	return int64(binary.LittleEndian.Uint64(buffer[16:24]))
}

func SetSendTimestampInMessage(buffer []byte, sendTimestamp int64) {
	binary.LittleEndian.PutUint64(buffer[16:24], uint64(sendTimestamp))
}

func GetPayloadSizeFromMessage(buffer []byte) int32 {
	return int32(binary.LittleEndian.Uint32(buffer[24:28]))
}

func SetPayloadSizeInMessage(buffer []byte, payloadSize int32) {
	binary.LittleEndian.PutUint32(buffer[24:28], uint32(payloadSize))
}

func FillInlineDataInMessage(buffer []byte, data []byte) {
	n := copy(buffer[MessageHeaderByteSize:], data)
	SetPayloadSizeInMessage(buffer, int32(n))
}

func GetInlineDataFromMessage(buffer []byte) []byte {
	payloadSize := GetPayloadSizeFromMessage(buffer)
	if payloadSize > 0 {
		return buffer[MessageHeaderByteSize : MessageHeaderByteSize+payloadSize]
	} else {
		return nil
	}
}

func SetDispatchDelayInMessage(buffer []byte, dispatchDelay int32) {
	binary.LittleEndian.PutUint32(buffer[8:12], uint32(dispatchDelay))
}

func BuildLogTagsBuffer(tags []uint64) []byte {
	buffer := make([]byte, len(tags)*SharedLogTagByteSize)
	for i := 0; i < len(tags); i++ {
		bufIndex := i * SharedLogTagByteSize
		binary.LittleEndian.PutUint64(buffer[bufIndex:bufIndex+SharedLogTagByteSize], tags[i])
	}
	return buffer
}
