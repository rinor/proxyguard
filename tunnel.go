package proxyguard

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"time"
)

// bufSize is the total length that we receive at once
// 2^16
const bufSize = 2 << 15

// hdrLength is the length of our own crafted header
// This header contains the length of a UDP packet
const hdrLength = 2

// writeTCP writes a buffer to the connection
// This buffer is prefixed with a 2 byte length specified with n
func writeTCP(w *bufio.Writer, buf []byte, n int) error {
	// Put the header length at the front
	binary.BigEndian.PutUint16(buf[:hdrLength], uint16(n))
	// store the length and packet itself
	_, err := w.Write(buf)
	if err != nil {
		return err
	}
	return w.Flush()
}

// tcpToUDP reads from the TCP reader r and writes packets to the udpc connection
// The incoming TCP packets are encapsulated UDP packets with a 2 byte length prefix
func tcpToUDP(r *bufio.Reader, udpc *net.UDPConn, wgaddr *net.UDPAddr) error {
	var hdr [hdrLength]byte
	var b [bufSize]byte

	writefn := udpc.Write
	if wgaddr != nil {
		writefn = func(b []byte) (int, error) {
			return udpc.WriteTo(b, wgaddr)
		}
	}
	for {
		_, rerr := io.ReadFull(r, hdr[:])
		// we do not check the size here
		// as if we can't even read the full header
		// there is nothing left to do
		if rerr != nil {
			return rerr
		}
		glen := binary.BigEndian.Uint16(hdr[:])
		n, rerr := io.ReadFull(r, b[:glen])
		// we need a complete packet
		if rerr != nil {
			return rerr
		}
		if n > 0 {
			_, werr := writefn(b[:n])
			if werr != nil {
				return werr
			}
		}
	}
}

// udpToTCP reads from the UDP connection udpc and writes packets to the tcp buffer
// The incoming UDP packets are encapsulated inside TCP with a 2 byte length prefix
func udpToTCP(udpc *net.UDPConn, w *bufio.Writer) error {
	var bufs [bufSize]byte
	for {
		n, _, rerr := udpc.ReadFromUDP(bufs[2:])
		if n > 0 {
			werr := writeTCP(w, bufs[:n+2], n)
			if werr != nil {
				return werr
			}
		}
		if rerr != nil {
			return rerr
		}
	}
}

func tunnel(ctx context.Context, udpc *net.UDPConn, brw *bufio.ReadWriter, wgaddr *net.UDPAddr) error {
	cancel := make(chan struct{})
	go func() {
		for {
			select {
			case <-ctx.Done():
				_ = udpc.SetDeadline(time.Now())
			case <-cancel:
				return
			}
		}
	}()
	defer close(cancel)

	errChan := make(chan error, 1)
	wg := sync.WaitGroup{}
	wg.Add(1)
	// read from udp and write to tcp buffer
	go func() {
		defer wg.Done()
		errChan <- udpToTCP(udpc, brw.Writer)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		errChan <- tcpToUDP(brw.Reader, udpc, wgaddr)
	}()
	go func() {
		wg.Wait()
		close(errChan)
	}()

	return <-errChan
}
