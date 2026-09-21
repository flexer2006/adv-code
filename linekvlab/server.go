package linekvlab

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	preallckv = 4 << 8
	preallcbf = 2 << 8
)

type KVServer struct {
	pool       sync.Pool
	mtx        sync.RWMutex
	lstr       net.Listener
	ctx        context.Context
	wgp        sync.WaitGroup
	onc        sync.Once
	objs       map[string]string
	cncl       context.CancelFunc
	ops, conns atomic.Uint64
}

type connBufs struct {
	br *bufio.Reader
	bw *bufio.Writer
}

func NewKVServer() *KVServer {
	ctx, cancel := context.WithCancel(context.Background())
	return new(KVServer{
		ctx:  ctx,
		cncl: cancel,
		objs: make(map[string]string, preallckv),
		lstr: nil,
		pool: sync.Pool{
			New: func() any {
				return new(connBufs{
					br: bufio.NewReaderSize(nil, preallcbf),
					bw: bufio.NewWriterSize(nil, preallcbf),
				})
			},
		},
		mtx:   sync.RWMutex{},
		wgp:   sync.WaitGroup{},
		onc:   sync.Once{},
		ops:   atomic.Uint64{},
		conns: atomic.Uint64{},
	})
}

func (s *KVServer) Start(addr string) (lnAddr string, err error) {
	lstr, errL := net.Listen("tcp", addr)
	if errL != nil {
		return "", errL
	}

	s.lstr = lstr

	go s.serve()

	return s.lstr.Addr().String(), nil
}

func (s *KVServer) Stop(ctx context.Context) error {
	s.onc.Do(func() {
		s.cncl()

		if s.lstr != nil {
			_ = s.lstr.Close()
		}
	})

	done := make(chan struct{})

	go func() {
		s.wgp.Wait()

		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()

	case <-done:
		return nil
	}
}

func (s *KVServer) handleConn(c net.Conn) {
	s.conns.Add(1)
	defer s.conns.Add(^uint64(0))

	defer func() { _ = c.Close() }()

	buf := s.pool.Get().(*connBufs)
	buf.br.Reset(c)
	buf.bw.Reset(c)

	defer func() {
		buf.br.Reset(nil)
		buf.bw.Reset(nil)

		s.pool.Put(buf)
	}()

	for {
		line, err := buf.br.ReadString('\n')
		if err != nil {
			break
		}

		line = strings.TrimSpace(line)
		ans := s.handleLine(line)

		err = s.reply(buf.bw, ans)
		if err != nil {
			break
		}

		if ans == "BYE" {
			break
		}
	}
}

func (s *KVServer) handleLine(line string) string {
	line = strings.TrimSpace(line)
	if line == "" {
		return "ERR empty"
	}

	cmd, data, _ := strings.Cut(line, " ")
	data = strings.TrimSpace(data)

	switch cmd {
	case "QUIT":
		s.ops.Add(1)

		return "BYE"

	case "STATS":
		s.ops.Add(1)

		return s.statsLine()

	case "SET":
		key, val, ok := strings.Cut(data, " ")
		if !ok || key == "" {
			return "ERR bad_request"
		}

		s.mtx.Lock()
		s.objs[key] = strings.TrimSpace(val)
		s.mtx.Unlock()

		s.ops.Add(1)

		return "OK"

	case "GET":
		if data == "" || strings.ContainsRune(data, ' ') {
			return "ERR bad_request"
		}

		s.mtx.RLock()
		v, ok := s.objs[data]
		s.mtx.RUnlock()
		if !ok {

			return "ERR not_found"
		}

		s.ops.Add(1)

		return "VALUE " + v

	case "INCR":
		if data == "" || strings.ContainsRune(data, ' ') {
			return "ERR bad_request"
		}

		s.mtx.Lock()
		raw, ok := s.objs[data]
		if !ok {
			raw = "0"
		}

		n, err := strconv.Atoi(raw)
		if err != nil {
			s.mtx.Unlock()

			return "ERR not_int"
		}

		n++
		s.objs[data] = string(strconv.AppendInt(make([]byte, 0, 20), int64(n), 10))
		s.mtx.Unlock()

		s.ops.Add(1)
		var buf [26]byte

		return string(strconv.AppendInt(append(buf[:0], "VALUE "...), int64(n), 10))

	default:
		return "ERR unknown"
	}
}

func (s *KVServer) serve() {
	for {
		c, err := s.lstr.Accept()
		if err != nil {
			return
		}

		s.wgp.Go(func() {
			s.handleConn(c)
		})
	}
}

func (s *KVServer) statsLine() string {
	s.mtx.RLock()
	n := len(s.objs)
	s.mtx.RUnlock()

	return fmt.Sprintf("STATS keys=%d ops=%d conns=%d", n, s.ops.Load(), s.conns.Load())
}

func (s *KVServer) reply(bw *bufio.Writer, str string) error {
	_, errW := bw.WriteString(str + "\n")
	if errW != nil {
		return fmt.Errorf("kv: reply write: %w", errW)
	}

	errF := bw.Flush()
	if errF != nil {
		return fmt.Errorf("kv: reply flush: %w", errF)
	}

	return nil
}
