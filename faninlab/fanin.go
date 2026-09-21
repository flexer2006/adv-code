package faninlab

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

const maxFrame = 64 << 10

var errInvalidFrame = errors.New("invalid frame")

func FanInFrames(conns []net.Conn, limit int) (out <-chan []byte, wait func() (totalBytes int64, err error)) {
	var (
		mu       sync.Mutex
		wg       sync.WaitGroup
		total    int64
		firstErr error
	)

	ch := make(chan []byte)
	sem := make(chan struct{}, limit)

	for _, c := range conns {
		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()

			br := bufio.NewReader(c)

			for {
				var hdr [4]byte

				_, errR := io.ReadFull(br, hdr[:])
				if errR != nil {
					if errR == io.EOF {
						return
					}

					mu.Lock()
					if firstErr == nil {
						firstErr = errR
					}
					mu.Unlock()

					return
				}

				n := binary.BigEndian.Uint32(hdr[:])
				if n == 0 || n > maxFrame {
					mu.Lock()
					if firstErr == nil {
						firstErr = errInvalidFrame
					}
					mu.Unlock()

					return
				}

				payload := make([]byte, n)

				_, errR = io.ReadFull(br, payload)
				if errR != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = errR
					}
					mu.Unlock()

					return
				}

				ch <- payload

				mu.Lock()
				total += int64(n)
				mu.Unlock()
			}
		})
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()

		close(ch)
		close(done)
	}()

	wait = func() (int64, error) {
		<-done
		mu.Lock()
		defer mu.Unlock()

		return total, firstErr
	}

	return ch, wait
}

// --- helpers ---

func writeFrame(w io.Writer, payload []byte) error {
	if len(payload) == 0 || len(payload) > maxFrame {
		return fmt.Errorf("bad payload size %d", len(payload))
	}

	var hdr [4]byte
	n := uint32(len(payload))
	hdr[0] = byte(n >> 24)
	hdr[1] = byte(n >> 16)
	hdr[2] = byte(n >> 8)
	hdr[3] = byte(n)

	bw := bufio.NewWriter(w)
	if _, err := bw.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := bw.Write(payload); err != nil {
		return err
	}

	return bw.Flush()
}

func DialKV(addr string) (net.Conn, *bufio.Reader, *bufio.Writer, error) {
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return nil, nil, nil, err
	}

	return c, bufio.NewReader(c), bufio.NewWriter(c), nil
}

func kvLine(bw *bufio.Writer, br *bufio.Reader, send string) (string, error) {
	if _, err := bw.WriteString(send + "\n"); err != nil {
		return "", err
	}
	if err := bw.Flush(); err != nil {
		return "", err
	}

	line, err := br.ReadString('\n')
	if err != nil {
		return "", err
	}

	return strings.TrimRight(line, "\r\n"), nil
}
