package worker

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	common "cs.utexas.edu/zjia/faas/common"
	config "cs.utexas.edu/zjia/faas/config"
	ipc "cs.utexas.edu/zjia/faas/ipc"
	protocol "cs.utexas.edu/zjia/faas/protocol"
	types "cs.utexas.edu/zjia/faas/types"
)

// region debug pipe
type dbgPipe struct {
	fp *os.File
}

func newDebugPipe(fp *os.File) *dbgPipe {
	return &dbgPipe{
		fp: fp,
	}
}

func (p *dbgPipe) Write(b []byte) (n int, err error) {
	dbgPrintMessage(b)
	return p.fp.Write(b)
}

func dbgPrintMessage(rawMsg []byte) {
	funcCall := protocol.GetFuncCallFromMessage(rawMsg)
	if funcCall.FullCallId() == 0 {
		buf := make([]byte, 10000)
		n := runtime.Stack(buf, false)
		dbgPrintFuncCall(rawMsg)
		log.Printf("[DEBUG] Stack trace : %s ", string(buf[:n]))
	}
}

func dbgPrintFuncCall(rawMsg []byte) {
	funcCall := protocol.GetFuncCallFromMessage(rawMsg)
	log.Printf("[DEBUG] funcCall: %+v", funcCall)
}

func hexBytes2String(data []byte) string {
	output := "["
	for i := range data {
		output += fmt.Sprintf("%02X ", data[i])
	}
	return strings.TrimSpace(output) + "]"
}

// region end

const PIPE_BUF = 4096

type FuncWorker struct {
	funcId               uint16
	clientId             uint16
	factory              types.FuncHandlerFactory
	configEntry          *config.FuncConfigEntry
	isGrpcSrv            bool
	useFifoForNestedCall bool
	engineConn           net.Conn
	newFuncCallChan      chan []byte
	inputPipe            *os.File
	// outputPipe           *os.File                 // protected by mux
	// DEBUG
	outputPipe        *dbgPipe                 // protected by mux
	outgoingFuncCalls map[uint64](chan []byte) // protected by mux
	// an async request returns twice, first to asyncOutgoing, second to outgoing
	asyncOutgoingLogOps map[uint64](chan []byte) // protected by mux
	outgoingLogOps      map[uint64](chan []byte) // protected by mux
	handler             types.FuncHandler
	grpcHandler         types.GrpcFuncHandler
	nextCallId          uint32
	nextLogOpId         uint64
	currentCall         uint64
	uidHighHalf         uint32
	nextUidLowHalf      uint32
	sharedLogReadCount  int32
	mux                 sync.Mutex

	asyncLogCtx types.AsyncLogContext
}

func NewFuncWorker(funcId uint16, clientId uint16, factory types.FuncHandlerFactory) (*FuncWorker, error) {
	engineId := uint32(0)
	if parsed, err := strconv.Atoi(os.Getenv("FAAS_ENGINE_ID")); err == nil {
		log.Printf("[INFO] Parse FAAS_ENGINE_ID: %d", parsed)
		engineId = uint32(parsed)
	}
	uidHighHalf := (engineId << protocol.ClientIdBits) + uint32(clientId)
	w := &FuncWorker{
		funcId:               funcId,
		clientId:             clientId,
		factory:              factory,
		isGrpcSrv:            false,
		useFifoForNestedCall: false,
		newFuncCallChan:      make(chan []byte, 4),
		outgoingFuncCalls:    make(map[uint64](chan []byte)),
		asyncOutgoingLogOps:  make(map[uint64](chan []byte)),
		outgoingLogOps:       make(map[uint64](chan []byte)),
		nextCallId:           0,
		nextLogOpId:          0,
		currentCall:          0,
		uidHighHalf:          uidHighHalf,
		nextUidLowHalf:       0,

		asyncLogCtx: nil,
	}
	w.asyncLogCtx = types.NewAsyncLogContext(w)
	return w, nil
}

// TODO: better place
func (w *FuncWorker) AsyncLogCtx() types.AsyncLogContext {
	return w.asyncLogCtx
}

func (w *FuncWorker) NewAsyncLogCtx(data []byte) error {
	newCtx, err := types.DeserializeAsyncLogContext(w, data)
	if err != nil {
		return err
	} else {
		w.asyncLogCtx = newCtx
		return nil
	}
}

func (w *FuncWorker) Run() {
	log.Printf("[INFO] Start new FuncWorker with client id %d", w.clientId)
	err := w.doHandshake()
	if err != nil {
		log.Fatalf("[FATAL] Handshake failed: %v", err)
	}
	log.Printf("[INFO] Handshake with engine done")

	go w.servingLoop()
	for {
		message := protocol.NewEmptyMessage()
		if n, err := w.inputPipe.Read(message); err != nil {
			log.Fatalf("[FATAL] Failed to read engine message: %v", err)
		} else if n != protocol.MessageFullByteSize {
			log.Fatalf("[FATAL] Failed to read one complete engine message: nread=%d", n)
		}
		if protocol.IsDispatchFuncCallMessage(message) {
			w.newFuncCallChan <- message
		} else if protocol.IsFuncCallCompleteMessage(message) || protocol.IsFuncCallFailedMessage(message) {
			funcCall := protocol.GetFuncCallFromMessage(message)
			w.mux.Lock()
			if ch, exists := w.outgoingFuncCalls[funcCall.FullCallId()]; exists {
				ch <- message
				delete(w.outgoingFuncCalls, funcCall.FullCallId())
			}
			w.mux.Unlock()
		} else if protocol.IsSharedLogOpMessage(message) {
			id := protocol.GetLogClientDataFromMessage(message)
			w.mux.Lock()
			if protocol.IsSharedLogAsyncResult(message) {
				if ch, exists := w.asyncOutgoingLogOps[id]; exists {
					ch <- message
					delete(w.asyncOutgoingLogOps, id)
				} else {
					log.Printf("[WARN] Unexpected log message id for async ops: %d", id)
				}
			} else {
				if ch, exists := w.outgoingLogOps[id]; exists {
					ch <- message
					delete(w.outgoingLogOps, id)
				} else {
					log.Printf("[WARN] Unexpected log message id for sync ops: %d", id)
				}
			}
			w.mux.Unlock()
		} else {
			log.Fatal("[FATAL] Unknown message type")
		}
	}
}

func (w *FuncWorker) doHandshake() error {
	c, err := net.Dial("unix", ipc.GetEngineUnixSocketPath())
	if err != nil {
		return err
	}
	w.engineConn = c

	ip, err := ipc.FifoOpenForRead(ipc.GetFuncWorkerInputFifoName(w.clientId), true)
	if err != nil {
		return err
	}
	w.inputPipe = ip

	message := protocol.NewFuncWorkerHandshakeMessage(w.funcId, w.clientId)
	_, err = w.engineConn.Write(message)
	if err != nil {
		return err
	}
	response := protocol.NewEmptyMessage()
	n, err := w.engineConn.Read(response)
	if err != nil {
		return err
	} else if n != protocol.MessageFullByteSize {
		return fmt.Errorf("Unexpcted size for handshake response")
	} else if !protocol.IsHandshakeResponseMessage(response) {
		return fmt.Errorf("Unexpcted type of response")
	}

	flags := protocol.GetFlagsFromMessage(response)
	if (flags & protocol.FLAG_UseFifoForNestedCall) != 0 {
		log.Printf("[INFO] Use FIFO for nested calls")
		w.useFifoForNestedCall = true
	}

	w.configEntry = config.FindByFuncId(w.funcId)
	if w.configEntry == nil {
		return fmt.Errorf("Invalid funcId: %d", w.funcId)
	}
	w.isGrpcSrv = strings.HasPrefix(w.configEntry.FuncName, "grpc:")

	if w.isGrpcSrv {
		handler, err := w.factory.GrpcNew(w, strings.TrimPrefix(w.configEntry.FuncName, "grpc:"))
		if err != nil {
			return err
		}
		w.grpcHandler = handler
	} else {
		handler, err := w.factory.New(w, w.configEntry.FuncName)
		if err != nil {
			return err
		}
		w.handler = handler
	}

	op, err := ipc.FifoOpenForWrite(ipc.GetFuncWorkerOutputFifoName(w.clientId), false)
	if err != nil {
		return err
	}
	// DEBUG
	w.outputPipe = newDebugPipe(op)

	return nil
}

func (w *FuncWorker) servingLoop() {
	for {
		message := <-w.newFuncCallChan
		w.executeFunc(message)
	}
}

func (w *FuncWorker) executeFunc(dispatchFuncMessage []byte) {
	dispatchDelay := common.GetMonotonicMicroTimestamp() - protocol.GetSendTimestampFromMessage(dispatchFuncMessage)
	funcCall := protocol.GetFuncCallFromMessage(dispatchFuncMessage)

	var input []byte
	var inputRegion *ipc.ShmRegion
	var err error

	if protocol.GetPayloadSizeFromMessage(dispatchFuncMessage) < 0 {
		shmName := ipc.GetFuncCallInputShmName(funcCall.FullCallId())
		inputRegion, err = ipc.ShmOpen(shmName, true)
		if err != nil {
			log.Printf("[ERROR] ShmOpen %s failed: %v", shmName, err)
			response := protocol.NewFuncCallFailedMessage(funcCall)
			protocol.SetSendTimestampInMessage(response, common.GetMonotonicMicroTimestamp())
			w.mux.Lock()
			_, err = w.outputPipe.Write(response)
			w.mux.Unlock()
			if err != nil {
				log.Fatal("[FATAL] Failed to write engine message!")
			}
			return
		}
		defer inputRegion.Close()
		input = inputRegion.Data
	} else {
		input = protocol.GetInlineDataFromMessage(dispatchFuncMessage)
	}

	methodName := ""
	if w.isGrpcSrv {
		methodId := int(funcCall.MethodId)
		if methodId < len(w.configEntry.GrpcMethods) {
			methodName = w.configEntry.GrpcMethods[methodId]
		} else {
			log.Fatalf("[FATAL] Invalid methodId: %s", funcCall.MethodId)
		}
	}

	var output []byte
	atomic.StoreInt32(&w.sharedLogReadCount, int32(0))
	atomic.StoreUint64(&w.currentCall, funcCall.FullCallId())
	startTimestamp := common.GetMonotonicMicroTimestamp()
	if w.isGrpcSrv {
		output, err = w.grpcHandler.Call(context.Background(), methodName, input)
	} else {
		output, err = w.handler.Call(context.Background(), input)
	}
	processingTime := common.GetMonotonicMicroTimestamp() - startTimestamp
	atomic.StoreUint64(&w.currentCall, 0)
	if err != nil {
		log.Printf("[ERROR] FuncCall failed with error: %v", err)
	}

	var response []byte
	if w.useFifoForNestedCall {
		response = w.fifoFuncCallFinished(funcCall, err == nil, output, int32(processingTime))
	} else {
		response = w.funcCallFinished(funcCall, err == nil, output, int32(processingTime))
	}
	protocol.SetDispatchDelayInMessage(response, int32(dispatchDelay))
	protocol.SetSendTimestampInMessage(response, common.GetMonotonicMicroTimestamp())
	w.mux.Lock()
	_, err = w.outputPipe.Write(response)
	w.mux.Unlock()
	if err != nil {
		log.Fatal("[FATAL] Failed to write engine message!")
	}
}

func (w *FuncWorker) funcCallFinished(funcCall protocol.FuncCall, success bool, output []byte, processingTime int32) []byte {
	var response []byte
	if success {
		response = protocol.NewFuncCallCompleteMessage(funcCall, processingTime)
		if len(output) > protocol.MessageInlineDataSize {
			err := w.writeOutputToShm(funcCall, output)
			if err != nil {
				log.Printf("[ERROR] writeOutputToShm failed: %v", err)
				response = protocol.NewFuncCallFailedMessage(funcCall)
			} else {
				protocol.SetPayloadSizeInMessage(response, int32(-len(output)))
			}
		} else if len(output) > 0 {
			protocol.FillInlineDataInMessage(response, output)
		}
	} else {
		response = protocol.NewFuncCallFailedMessage(funcCall)
	}
	return response
}

func (w *FuncWorker) fifoFuncCallFinished(funcCall protocol.FuncCall, success bool, output []byte, processingTime int32) []byte {
	var response []byte
	if success {
		response = protocol.NewFuncCallCompleteMessage(funcCall, processingTime)
	} else {
		response = protocol.NewFuncCallFailedMessage(funcCall)
	}

	if funcCall.ClientId == 0 {
		// FuncCall from engine directly
		if success {
			if len(output) > protocol.MessageInlineDataSize {
				err := w.writeOutputToShm(funcCall, output)
				if err != nil {
					log.Printf("[ERROR] writeOutputToShm failed: %v", err)
					response = protocol.NewFuncCallFailedMessage(funcCall)
				} else {
					protocol.SetPayloadSizeInMessage(response, int32(-len(output)))
				}
			} else if len(output) > 0 {
				protocol.FillInlineDataInMessage(response, output)
			}
		}
	} else {
		// FuncCall from another FuncWorker
		err := w.writeOutputToFifo(funcCall, success, output)
		if err != nil {
			log.Printf("[ERROR] writeOutputToFifo failed: %v", err)
			response = protocol.NewFuncCallFailedMessage(funcCall)
		} else if success {
			protocol.SetPayloadSizeInMessage(response, int32(len(output)))
		}
	}

	return response
}

func (w *FuncWorker) writeOutputToShm(funcCall protocol.FuncCall, output []byte) error {
	shmName := ipc.GetFuncCallOutputShmName(funcCall.FullCallId())
	outputRegion, err := ipc.ShmCreate(shmName, len(output))
	if err != nil {
		return err
	}
	defer outputRegion.Close()
	copy(outputRegion.Data, output)
	return nil
}

func (w *FuncWorker) writeOutputToFifo(funcCall protocol.FuncCall, success bool, output []byte) error {
	fifo, err := ipc.FifoOpenForWrite(ipc.GetFuncCallOutputFifoName(funcCall.FullCallId()), true)
	if err != nil {
		return err
	}
	defer fifo.Close()
	var buffer []byte
	if success {
		if len(output)+4 > PIPE_BUF {
			err := w.writeOutputToShm(funcCall, output)
			if err != nil {
				return err
			}
			buffer = make([]byte, 4)
			binary.LittleEndian.PutUint32(buffer, uint32(len(output)))
		} else {
			buffer = make([]byte, len(output)+4)
			binary.LittleEndian.PutUint32(buffer[0:4], uint32(len(output)))
			copy(buffer[4:], output)
		}
	} else {
		buffer = make([]byte, 4)
		header := int32(-1)
		binary.LittleEndian.PutUint32(buffer, uint32(header))
	}
	_, err = fifo.Write(buffer)
	return err
}

func (w *FuncWorker) newFuncCallCommon(funcCall protocol.FuncCall, input []byte, async bool) ([]byte, error) {
	if async && w.useFifoForNestedCall {
		log.Fatalf("[FATAL] Unsupported")
	}

	message := protocol.NewInvokeFuncCallMessage(funcCall, atomic.LoadUint64(&w.currentCall), async)

	var inputRegion *ipc.ShmRegion
	var outputFifo *os.File
	var outputChan chan []byte
	var output []byte
	var err error

	if len(input) > protocol.MessageInlineDataSize {
		inputRegion, err = ipc.ShmCreate(ipc.GetFuncCallInputShmName(funcCall.FullCallId()), len(input))
		if err != nil {
			return nil, fmt.Errorf("ShmCreate failed: %v", err)
		}
		defer func() {
			inputRegion.Close()
			if !async {
				inputRegion.Remove()
			}
		}()
		copy(inputRegion.Data, input)
		protocol.SetPayloadSizeInMessage(message, int32(-len(input)))
	} else {
		protocol.FillInlineDataInMessage(message, input)
	}

	if w.useFifoForNestedCall {
		outputFifoName := ipc.GetFuncCallOutputFifoName(funcCall.FullCallId())
		err = ipc.FifoCreate(outputFifoName)
		if err != nil {
			return nil, fmt.Errorf("FifoCreate failed: %v", err)
		}
		defer ipc.FifoRemove(outputFifoName)
		outputFifo, err = ipc.FifoOpenForReadWrite(outputFifoName, true)
		if err != nil {
			return nil, fmt.Errorf("FifoOpenForReadWrite failed: %v", err)
		}
		defer outputFifo.Close()
	}

	w.mux.Lock()
	if !w.useFifoForNestedCall {
		outputChan = make(chan []byte, 1)
		w.outgoingFuncCalls[funcCall.FullCallId()] = outputChan
	}
	_, err = w.outputPipe.Write(message)
	w.mux.Unlock()

	if w.useFifoForNestedCall {
		headerBuf := make([]byte, 4)
		nread, err := outputFifo.Read(headerBuf)
		if err != nil {
			return nil, fmt.Errorf("Failed to read from fifo: %v", err)
		} else if nread < len(headerBuf) {
			return nil, fmt.Errorf("Failed to read header from output fifo")
		}

		header := int32(binary.LittleEndian.Uint32(headerBuf))
		if header < 0 {
			return nil, fmt.Errorf("FuncCall failed due to invalid header: %v", headerBuf)
		}

		outputSize := int(header)
		output = make([]byte, outputSize)
		if outputSize+4 > PIPE_BUF {
			outputRegion, err := ipc.ShmOpen(ipc.GetFuncCallOutputShmName(funcCall.FullCallId()), true)
			if err != nil {
				return nil, fmt.Errorf("ShmOpen failed: %v", err)
			}
			defer func() {
				outputRegion.Close()
				outputRegion.Remove()
			}()
			if outputRegion.Size != outputSize {
				return nil, fmt.Errorf("Shm size mismatch with header read from output fifo")
			}
			copy(output, outputRegion.Data)
		} else {
			nread, err = outputFifo.Read(output)
			if err != nil {
				return nil, fmt.Errorf("Failed to read from fifo: %v", err)
			} else if nread < outputSize {
				return nil, fmt.Errorf("Failed to read output from fifo")
			}
		}
	} else {
		message := <-outputChan
		if async {
			return nil, nil
		}
		if protocol.IsFuncCallFailedMessage(message) {
			dbgPrintFuncCall(message)
			return nil, fmt.Errorf("FuncCall failed due to failed message: %v", hexBytes2String(message))
		}
		payloadSize := protocol.GetPayloadSizeFromMessage(message)
		if payloadSize < 0 {
			outputSize := int(-payloadSize)
			output = make([]byte, outputSize)
			outputRegion, err := ipc.ShmOpen(ipc.GetFuncCallOutputShmName(funcCall.FullCallId()), true)
			if err != nil {
				return nil, fmt.Errorf("ShmOpen failed: %v", err)
			}
			defer func() {
				outputRegion.Close()
				outputRegion.Remove()
			}()
			if outputRegion.Size != outputSize {
				return nil, fmt.Errorf("Shm size mismatch with header read from output fifo")
			}
			copy(output, outputRegion.Data)
		} else {
			output = protocol.GetInlineDataFromMessage(message)
		}
	}

	return output, nil
}

// Implement types.Environment
func (w *FuncWorker) InvokeFunc(ctx context.Context, funcName string, input []byte) ([]byte, error) {
	entry := config.FindByFuncName(funcName)
	if entry == nil {
		return nil, fmt.Errorf("Invalid function name: %s", funcName)
	}
	funcCall := protocol.FuncCall{
		FuncId:   entry.FuncId,
		MethodId: 0,
		ClientId: w.clientId,
		CallId:   atomic.AddUint32(&w.nextCallId, 1) - 1,
	}
	return w.newFuncCallCommon(funcCall, input, false /* async */)
}

// Implement types.Environment
func (w *FuncWorker) InvokeFuncAsync(ctx context.Context, funcName string, input []byte) error {
	entry := config.FindByFuncName(funcName)
	if entry == nil {
		return fmt.Errorf("Invalid function name: %s", funcName)
	}
	funcCall := protocol.FuncCall{
		FuncId:   entry.FuncId,
		MethodId: 0,
		ClientId: w.clientId,
		CallId:   atomic.AddUint32(&w.nextCallId, 1) - 1,
	}
	_, err := w.newFuncCallCommon(funcCall, input, true /* async */)
	return err
}

// Implement types.Environment
func (w *FuncWorker) GrpcCall(ctx context.Context, service string, method string, request []byte) ([]byte, error) {
	entry := config.FindByFuncName("grpc:" + service)
	if entry == nil {
		return nil, fmt.Errorf("Invalid gRPC service: %s", service)
	}
	methodId := entry.FindGrpcMethod(method)
	if methodId < 0 {
		return nil, fmt.Errorf("Invalid gRPC method: %s", method)
	}
	funcCall := protocol.FuncCall{
		FuncId:   entry.FuncId,
		MethodId: uint16(methodId),
		ClientId: w.clientId,
		CallId:   atomic.AddUint32(&w.nextCallId, 1) - 1,
	}
	return w.newFuncCallCommon(funcCall, request, false /* async */)
}

func checkAndDuplicateTags(tags []uint64) ([]uint64, error) {
	if len(tags) == 0 {
		return nil, nil
	}
	tagSet := make(map[uint64]bool)
	for _, tag := range tags {
		if tag == 0 || ^tag == 0 {
			return nil, fmt.Errorf("Invalid tag: %v", tag)
		}
		tagSet[tag] = true
	}
	results := make([]uint64, 0, len(tags))
	for tag, _ := range tagSet {
		results = append(results, tag)
	}
	return results, nil
}

// Implement types.Environment
func (w *FuncWorker) SharedLogAppend(ctx context.Context, tags []uint64, data []byte) (uint64, error) {
	if len(data) == 0 {
		return 0, fmt.Errorf("data cannot be empty")
	}
	tags, err := checkAndDuplicateTags(tags)
	if err != nil {
		return 0, err
	}
	if len(data)+len(tags)*protocol.SharedLogTagByteSize > protocol.MessageInlineDataSize {
		return 0, fmt.Errorf("data too larger (size=%d, num_tags=%d), expect no more than %d bytes", len(data), len(tags), protocol.MessageInlineDataSize)
	}

	sleepDuration := 5 * time.Millisecond
	remainingRetries := 4

	for {
		id := atomic.AddUint64(&w.nextLogOpId, 1)
		currentCallId := atomic.LoadUint64(&w.currentCall)
		message := protocol.NewSharedLogAppendMessage(currentCallId, w.clientId, uint16(len(tags)), id)
		if len(tags) == 0 {
			protocol.FillInlineDataInMessage(message, data)
		} else {
			tagBuffer := protocol.BuildLogTagsBuffer(tags)
			protocol.FillInlineDataInMessage(message, bytes.Join([][]byte{tagBuffer, data}, nil /* sep */))
		}

		w.mux.Lock()
		outputChan := make(chan []byte, 1)
		w.outgoingLogOps[id] = outputChan
		_, err = w.outputPipe.Write(message)
		w.mux.Unlock()
		if err != nil {
			return 0, err
		}

		response := <-outputChan
		result := protocol.GetSharedLogResultTypeFromMessage(response)
		if result == protocol.SharedLogResultType_APPEND_OK {
			return protocol.GetLogSeqNumFromMessage(response), nil
		} else if result == protocol.SharedLogResultType_DISCARDED {
			log.Printf("[ERROR] Append discarded, will retry")
			if remainingRetries > 0 {
				time.Sleep(sleepDuration)
				sleepDuration *= 2
				remainingRetries--
				continue
			} else {
				return 0, fmt.Errorf("failed to append log, exceeds maximum number of retries")
			}
		} else {
			return 0, fmt.Errorf("failed to append log, unacceptable result type: %d", result)
		}
	}
}

// Implement types.Environment
func (w *FuncWorker) AsyncSharedLogAppend(ctx context.Context, tags []uint64, tagBuildMeta []types.TagMeta, data []byte) (types.Future[uint64], error) {
	return w.AsyncSharedLogCondAppend(ctx, tags, tagBuildMeta, data, func(condHandle types.CondHandle) {})
}

// Implement types.Environment
func (w *FuncWorker) AsyncSharedLogCondAppend(ctx context.Context, tags []uint64, tagBuildMeta []types.TagMeta, data []byte, cond func(types.CondHandle)) (types.Future[uint64], error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("data cannot be empty")
	}
	tags, err := checkAndDuplicateTags(tags)
	if err != nil {
		return nil, err
	}

	condHandle, condWrapper := types.NewCond()
	// apply conditions
	cond(condHandle)
	// wrap conditions to original data
	data = condWrapper.WrapData(tagBuildMeta, data)

	if len(data)+len(tags)*protocol.SharedLogTagByteSize > protocol.MessageInlineDataSize {
		return nil, fmt.Errorf("data too larger (size=%d, num_tags=%d), expect no more than %d bytes",
			len(data), len(tags), protocol.MessageInlineDataSize)
	}

	sleepDuration := 5 * time.Millisecond
	remainingRetries := 4

	for {
		id := atomic.AddUint64(&w.nextLogOpId, 1)
		currentCallId := atomic.LoadUint64(&w.currentCall)
		message := protocol.NewAsyncSharedLogAppendMessage(currentCallId, w.clientId, uint16(len(tags)), id)
		if len(tags) == 0 {
			protocol.FillInlineDataInMessage(message, data)
		} else {
			tagBuffer := protocol.BuildLogTagsBuffer(tags)
			protocol.FillInlineDataInMessage(message, bytes.Join([][]byte{tagBuffer, data}, nil /* sep */))
		}

		w.mux.Lock()
		asyncOutputChan := make(chan []byte, 1)
		w.asyncOutgoingLogOps[id] = asyncOutputChan
		outputChan := make(chan []byte, 1)
		w.outgoingLogOps[id] = outputChan
		_, err = w.outputPipe.Write(message)
		w.mux.Unlock()
		if err != nil {
			return nil, err
		}

		response := <-asyncOutputChan // should return immediately
		result := protocol.GetSharedLogResultTypeFromMessage(response)
		if result == protocol.SharedLogResultType_ASYNC_APPEND_OK {
			localId := protocol.GetLogSeqNumFromMessage(response)
			resolve := func() (uint64, error) {
				sleepDuration := 5 * time.Millisecond
				remainingRetries := 4
				for {
					response := <-outputChan
					result := protocol.GetSharedLogResultTypeFromMessage(response)
					if result == protocol.SharedLogResultType_APPEND_OK {
						return protocol.GetLogSeqNumFromMessage(response), nil
					} else if result == protocol.SharedLogResultType_DISCARDED {
						log.Printf("[ERROR] Append discarded, will retry")
						if remainingRetries > 0 {
							time.Sleep(sleepDuration)
							sleepDuration *= 2
							remainingRetries--
							continue
						} else {
							return 0, fmt.Errorf("failed to append log, exceeds maximum number of retries")
						}
					} else {
						return 0, fmt.Errorf("failed to append log, unacceptable result type: %d", result)
					}
				}
			}
			return types.NewFuture(localId, resolve), nil
		} else if result == protocol.SharedLogResultType_DISCARDED {
			log.Printf("[ERROR] Append discarded, will retry")
			if remainingRetries > 0 {
				time.Sleep(sleepDuration)
				sleepDuration *= 2
				remainingRetries--
				continue
			} else {
				return nil, fmt.Errorf("failed to append log, exceeds maximum number of retries")
			}
		} else {
			return nil, fmt.Errorf("failed to append log, unacceptable result type: %d", result)
		}
	}
}

func buildLogEntryFromReadResponse(response []byte) *types.LogEntry {
	seqNum := protocol.GetLogSeqNumFromMessage(response)
	numTags := protocol.GetLogNumTagsFromMessage(response)
	auxDataSize := protocol.GetLogAuxDataSizeFromMessage(response)
	responseData := protocol.GetInlineDataFromMessage(response)
	logDataSize := len(responseData) - numTags*protocol.SharedLogTagByteSize - auxDataSize
	if logDataSize <= 0 {
		log.Fatalf("[FATAL] Size of inline data too smaler: size=%d, num_tags=%d, aux_data=%d", len(responseData), numTags, auxDataSize)
	}
	tags := make([]uint64, numTags)
	for i := 0; i < numTags; i++ {
		tags[i] = protocol.GetLogTagFromMessage(response, i)
	}
	logDataStart := numTags * protocol.SharedLogTagByteSize
	return &types.LogEntry{
		SeqNum:  seqNum,
		Tags:    tags,
		Data:    responseData[logDataStart : logDataStart+logDataSize],
		AuxData: responseData[logDataStart+logDataSize:],
	}
}

func (w *FuncWorker) sharedLogReadCommon(ctx context.Context, message []byte, opId uint64) (*types.LogEntry, error) {
	// count := atomic.AddInt32(&w.sharedLogReadCount, int32(1))
	// if count > 16 {
	// 	log.Printf("[WARN] Make %d-th shared log read request", count)
	// }

	w.mux.Lock()
	outputChan := make(chan []byte, 1)
	w.outgoingLogOps[opId] = outputChan
	_, err := w.outputPipe.Write(message)
	w.mux.Unlock()
	if err != nil {
		return nil, err
	}

	var response []byte
	select {
	case <-ctx.Done():
		return nil, nil
	case response = <-outputChan:
	}
	result := protocol.GetSharedLogResultTypeFromMessage(response)
	if result == protocol.SharedLogResultType_READ_OK {
		return buildLogEntryFromReadResponse(response), nil
	} else if result == protocol.SharedLogResultType_EMPTY {
		return nil, nil
	} else {
		return nil, fmt.Errorf("Failed to read log")
	}
}

// async appends are wrapped with cond, remember to unwrap it before return to user
func (w *FuncWorker) asyncSharedLogReadCommon(ctx context.Context, message []byte, opId uint64) (*types.CondLogEntry, error) {
	// count := atomic.AddInt32(&w.sharedLogReadCount, int32(1))
	// if count > 16 {
	// 	log.Printf("[WARN] Make %d-th shared log read request", count)
	// }

	w.mux.Lock()
	outputChan := make(chan []byte, 1)
	w.outgoingLogOps[opId] = outputChan
	_, err := w.outputPipe.Write(message)
	w.mux.Unlock()
	if err != nil {
		return nil, err
	}
	var response []byte
	select {
	case <-ctx.Done():
		return nil, nil
	case response = <-outputChan:
	}
	result := protocol.GetSharedLogResultTypeFromMessage(response)
	if result == protocol.SharedLogResultType_READ_OK {
		logEntry := buildLogEntryFromReadResponse(response)
		cond, originalData, err := types.UnwrapData(logEntry.Data)
		if err != nil {
			return nil, err
		}
		logEntry.Data = originalData
		return &types.CondLogEntry{
			LogEntry:     *logEntry,
			Deps:         cond.Deps,
			Cond:         cond.Ops,
			TagBuildMeta: cond.TagBuildMeta,
		}, nil
	} else if result == protocol.SharedLogResultType_EMPTY {
		return nil, nil
	} else {
		return nil, fmt.Errorf("failed to read log")
	}
}

func (w *FuncWorker) asyncSharedLogReadIndex(ctx context.Context, message []byte, opId uint64) (types.Future[uint64], error) {
	w.mux.Lock()
	outputChan := make(chan []byte, 1)
	w.outgoingLogOps[opId] = outputChan
	_, err := w.outputPipe.Write(message)
	w.mux.Unlock()
	if err != nil {
		return nil, err
	}
	resolve := func() (uint64, error) {
		var response []byte
		select {
		case <-ctx.Done():
			return 0, nil
		case response = <-outputChan:
		}
		result := protocol.GetSharedLogResultTypeFromMessage(response)
		if result == protocol.SharedLogResultType_READ_OK {
			seqnum := protocol.GetLogSeqNumFromMessage(response)
			return seqnum, nil
		} else if result == protocol.SharedLogResultType_EMPTY {
			return 0, nil
		} else {
			return 0, fmt.Errorf("failed to read log index")
		}
	}
	return types.NewFuture(opId, resolve), nil
}

// Implement types.Environment
func (w *FuncWorker) GenerateUniqueID() uint64 {
	uidLowHalf := atomic.AddUint32(&w.nextUidLowHalf, 1)
	return (uint64(w.uidHighHalf) << 32) + uint64(uidLowHalf)
}

// Implement types.Environment
func (w *FuncWorker) SharedLogReadNext(ctx context.Context, tag uint64, seqNum uint64) (*types.LogEntry, error) {
	id := atomic.AddUint64(&w.nextLogOpId, 1)
	currentCallId := atomic.LoadUint64(&w.currentCall)
	message := protocol.NewSharedLogReadMessage(currentCallId, w.clientId, tag, seqNum, 1 /* direction */, false /* block */, id)
	return w.sharedLogReadCommon(ctx, message, id)
}

// Implement types.Environment
func (w *FuncWorker) SharedLogReadNextBlock(ctx context.Context, tag uint64, seqNum uint64) (*types.LogEntry, error) {
	id := atomic.AddUint64(&w.nextLogOpId, 1)
	currentCallId := atomic.LoadUint64(&w.currentCall)
	message := protocol.NewSharedLogReadMessage(currentCallId, w.clientId, tag, seqNum, 1 /* direction */, true /* block */, id)
	return w.sharedLogReadCommon(ctx, message, id)
}

// Implement types.Environment
func (w *FuncWorker) SharedLogReadPrev(ctx context.Context, tag uint64, seqNum uint64) (*types.LogEntry, error) {
	id := atomic.AddUint64(&w.nextLogOpId, 1)
	currentCallId := atomic.LoadUint64(&w.currentCall)
	message := protocol.NewSharedLogReadMessage(currentCallId, w.clientId, tag, seqNum, -1 /* direction */, false /* block */, id)
	return w.sharedLogReadCommon(ctx, message, id)
}

// Implement types.Environment
func (w *FuncWorker) AsyncSharedLogReadNext(ctx context.Context, tag uint64, seqNum uint64) (*types.CondLogEntry, error) {
	id := atomic.AddUint64(&w.nextLogOpId, 1)
	currentCallId := atomic.LoadUint64(&w.currentCall)
	message := protocol.NewSharedLogReadMessage(currentCallId, w.clientId, tag, seqNum, 1 /* direction */, false /* block */, id)
	return w.asyncSharedLogReadCommon(ctx, message, id)
}

// Implement types.Environment
func (w *FuncWorker) AsyncSharedLogRead(ctx context.Context, futureMeta types.FutureMeta) (*types.CondLogEntry, error) {
	id := atomic.AddUint64(&w.nextLogOpId, 1)
	currentCallId := atomic.LoadUint64(&w.currentCall)
	message := protocol.NewAsyncSharedLogReadMessage(currentCallId, w.clientId, futureMeta, id)
	return w.asyncSharedLogReadCommon(ctx, message, id)
}

// Implement types.Environment
func (w *FuncWorker) AsyncSharedLogReadIndex(ctx context.Context, futureMeta types.FutureMeta) (types.Future[uint64], error) {
	id := atomic.AddUint64(&w.nextLogOpId, 1)
	currentCallId := atomic.LoadUint64(&w.currentCall)
	message := protocol.NewAsyncSharedLogReadIndexMessage(currentCallId, w.clientId, futureMeta, id)
	return w.asyncSharedLogReadIndex(ctx, message, id)
}

// Implement types.Environment
func (w *FuncWorker) SharedLogCheckTail(ctx context.Context, tag uint64) (*types.LogEntry, error) {
	return w.SharedLogReadPrev(ctx, tag, protocol.MaxLogSeqnum)
}

// Implement types.Environment
func (w *FuncWorker) SharedLogSetAuxData(ctx context.Context, seqNum uint64, auxData []byte) error {
	if len(auxData) == 0 {
		return fmt.Errorf("Auxiliary data cannot be empty")
	}
	if len(auxData) > protocol.MessageInlineDataSize {
		return fmt.Errorf("Auxiliary data too larger (size=%d), expect no more than %d bytes", len(auxData), protocol.MessageInlineDataSize)
	}

	id := atomic.AddUint64(&w.nextLogOpId, 1)
	currentCallId := atomic.LoadUint64(&w.currentCall)
	message := protocol.NewSharedLogSetAuxDataMessage(currentCallId, w.clientId, seqNum, id)
	protocol.FillInlineDataInMessage(message, auxData)

	w.mux.Lock()
	outputChan := make(chan []byte, 1)
	w.outgoingLogOps[id] = outputChan
	_, err := w.outputPipe.Write(message)
	w.mux.Unlock()
	if err != nil {
		return err
	}

	response := <-outputChan
	result := protocol.GetSharedLogResultTypeFromMessage(response)
	if result == protocol.SharedLogResultType_AUXDATA_OK {
		return nil
	} else {
		return fmt.Errorf("Failed to set auxiliary data for log (seqnum %#016x)", seqNum)
	}
}
