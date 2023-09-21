package ipc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"cs.utexas.edu/zjia/faas/common"
	"cs.utexas.edu/zjia/faas/utils"
)

func getSendTimestampFromMessage(buffer []byte) int64 {
	return int64(binary.LittleEndian.Uint64(buffer[0:8]))
}

func setSendTimestampInMessage(buffer []byte, sendTimestamp int64) {
	binary.LittleEndian.PutUint64(buffer[0:8], uint64(sendTimestamp))
}

type ipcChannel interface {
	getReader() io.Reader
	getWriter() io.Writer
	close()
}

type fifo struct {
	inputFifo  *os.File
	outputFifo *os.File
}

func newFifo(id int) (ipcChannel, error) {
	fifoName := GetFuncWorkerInputFifoName(1)
	err := FifoCreate(fifoName)
	if err != nil {
		return nil, fmt.Errorf("FifoCreate failed: %v %v", err, fifoName)
	}
	defer FifoRemove(fifoName)

	inputFifo, err := FifoOpenForRead(fifoName, true)
	if err != nil {
		return nil, fmt.Errorf("FifoOpenForRead failed: %v", err)
	}

	outputFifo, err := FifoOpenForWrite(fifoName, false)
	if err != nil {
		return nil, fmt.Errorf("FifoOpenForWrite failed: %v", err)
	}

	return &fifo{
		inputFifo:  inputFifo,
		outputFifo: outputFifo,
	}, nil
}

func (f *fifo) getReader() io.Reader {
	return f.inputFifo
}

func (f *fifo) getWriter() io.Writer {
	return f.outputFifo
}

func (f *fifo) close() {
	f.inputFifo.Close()
	f.outputFifo.Close()
}

type unixsocket struct {
	inputEp  net.Conn
	outputEp net.Conn
}

func newUnixSocket(id int) (ipcChannel, error) {
	var spDomain, spType int
	network := "unix"
	switch network {
	case "unix":
		spDomain, spType = syscall.AF_LOCAL, syscall.SOCK_STREAM
	case "unixgram":
		spDomain, spType = syscall.AF_LOCAL, syscall.SOCK_DGRAM
	default:
		return nil, errors.New("unknown network " + network)
	}

	fds, err := syscall.Socketpair(spDomain, spType, 0)
	if err != nil {
		return nil, err
	}

	fd1 := os.NewFile(uintptr(fds[0]), "fd1")
	defer fd1.Close()

	fd2 := os.NewFile(uintptr(fds[1]), "fd2")
	defer fd2.Close()

	sock1, err := net.FileConn(fd1)
	if err != nil {
		return nil, err
	}

	sock2, err := net.FileConn(fd2)
	if err != nil {
		sock1.Close()
		return nil, err
	}

	return &unixsocket{
		inputEp:  sock1,
		outputEp: sock2,
	}, nil
}

func (u *unixsocket) getReader() io.Reader {
	return u.inputEp
}

func (u *unixsocket) getWriter() io.Writer {
	return u.outputEp
}

func (u *unixsocket) close() {
	u.inputEp.Close()
	u.outputEp.Close()
}

func BenchmarkFifo(b *testing.B) {
	SetRootPathForIpc("/tmp/ipctest")

	ipcgroup := 4
	ipcs := make([]ipcChannel, ipcgroup)
	for i := 0; i < ipcgroup; i++ {
		// ipc, err := newFifo(1)
		ipc, err := newUnixSocket(i)
		if err != nil {
			b.Fatal(err)
		}
		defer ipc.close()
		ipcs[i] = ipc
	}

	wg := sync.WaitGroup{}
	buflen := 2816 // MUST be >= 8 to carry timestamp
	concurrency := 4
	duration := 10 * time.Second

	startTs := time.Now()
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func /*sender*/ () {
			iIPC := 0
			buf := make([]byte, buflen)
			for time.Since(startTs) < duration {
				setSendTimestampInMessage(buf, common.GetMonotonicMicroTimestamp())
				ipc := ipcs[iIPC]
				iIPC = (iIPC + 1) % ipcgroup
				_, err := ipc.getWriter().Write(buf)
				if err != nil {
					log.Panicf("fifo write failed: %v", err)
				}
			}
			wg.Done()
		}()
	}

	msgCount := 0
	go func /*receiver*/ () {
		bufCh := make(chan []byte, 1000)
		for i := 0; i < ipcgroup; i++ {
			go func(iIPC int) {
				buf := make([]byte, buflen)
				ipc := ipcs[iIPC]
				for {
					n, err := ipc.getReader().Read(buf)
					if err != nil {
						log.Printf("[WARN] fifo read err: %v", err)
						break
					}
					if n != buflen {
						log.Panicf("inconsistent buflen recv_n=%v expected=%v", n, buflen)
					}
					bufCh <- buf
				}
			}(i)
		}
		sc := utils.NewStatisticsCollector("IPCBench (us)", 200 /*reportSamples*/, 1*time.Second)
		// buflenSC := utils.NewStatisticsCollector("Buffer (n)", 200 /*reportSamples*/, 1*time.Second)
		for buf := range bufCh {
			// buflenSC.AddSample(float64(len(bufCh)))
			dispatchDelay := common.GetMonotonicMicroTimestamp() - getSendTimestampFromMessage(buf)
			sc.AddSample(float64(dispatchDelay))
			msgCount++
		}
	}()

	wg.Wait()
	log.Printf("[STAT] throughput=%.1f ops", float64(msgCount)/duration.Seconds())
}
