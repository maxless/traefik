package udp

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"time"

	"github.com/rs/zerolog/log"
)

// Proxy is a reverse-proxy implementation of the Handler interface.
type Proxy struct {
	// TODO: maybe optimize by pre-resolving it at proxy creation time
	target string
}

// NewProxy creates a new Proxy.
func NewProxy(address string) (*Proxy, error) {
	return &Proxy{target: address}, nil
}

// ServeUDP implements the Handler interface.
// mmm: separate goroutine for every connection from pkg/server/server_entrypoint_udp.go
func (p *Proxy) ServeUDP(conn *Conn) {
	log.Debug().Msgf("Handling UDP stream from %s to %s", conn.rAddr, p.target)

	// needed because of e.g. server.trackedConnection
	defer conn.Close()

	connBackend, err := net.Dial("udp", p.target)
	if err != nil {
		log.Error().Err(err).Msg("Error while dialing backend")
		return
	}

	// maybe not needed, but just in case
	defer connBackend.Close()
/*
	defer func (){
		fmt.Println("exit1")
	}()*/

	errChan := make(chan error)
	header := makeProxyV2Header(conn, connBackend)
//	go connCopy(conn, connBackend, errChan, "from")
	go connCopyWithProxy(connBackend, conn.timeout, conn, connBackend, header, errChan, false, "from backend")
//	go connCopy(connBackend, conn, errChan, "to")
	go connCopyWithProxy(connBackend, conn.timeout, connBackend, conn, header, errChan, true, "to backend")

	err = <-errChan
//	fmt.Println("5 stop")
	if err != nil {
		log.Error().Err(err).Msg("Error while handling UDP stream")
	}

	<-errChan
}

// mmm: write proxy v2 header
func makeProxyV2Header(conn *Conn, connBackend net.Conn) []byte {
	ipv6 := false
	srcAddr := conn.rAddr.(*net.UDPAddr)
	dstAddr := conn.listener.pConn.LocalAddr().(*net.UDPAddr)
//	fmt.Println(srcAddr)
//	fmt.Println(dstAddr)
	if srcAddr.IP.To4() == nil {
		ipv6 = true
	}
	size := 28 // 16 + 12
	if ipv6 {
		size = 52 // 16 + 36
	}
	header := make([]byte, size)
	// signature
	copy(header, []byte{0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A})
	// version 2, PROXY command
	header[12] = 0x21
	// address family and protocol
	// 0x1 : AF_INET
	// 0x2 : AF_INET6
	// 0x3 : AF_UNIX
	// 0x1 : STREAM
	// 0x2 : DGRAM
	if ipv6 {
		header[13] = 0x22
	} else {
		header[13] = 0x12
	}
	// address block length
	if ipv6 {
		// 2*16 + 2*2
		binary.BigEndian.PutUint16(header[14:16], 36)
	} else {
		// 2*4 bytes for IPv4 addresses + 2*2 bytes for ports
		binary.BigEndian.PutUint16(header[14:16], 12)
	}

	// source and destination
	if ipv6 {
		// addresses
		srcIP := srcAddr.IP.To16()
		copy(header[16:32], srcIP)
		dstIP := dstAddr.IP.To16()
		copy(header[32:48], dstIP)
		// ports
		binary.BigEndian.PutUint16(header[48:50],
			uint16(srcAddr.Port))
		binary.BigEndian.PutUint16(header[50:52],
			uint16(dstAddr.Port))
	} else {
		// addresses
		srcIP := srcAddr.IP.To4()
		copy(header[16:20], srcIP)
		dstIP := dstAddr.IP.To4()
		copy(header[20:24], dstIP)
		// ports
		binary.BigEndian.PutUint16(header[24:26],
			uint16(srcAddr.Port))
		binary.BigEndian.PutUint16(header[26:28],
			uint16(dstAddr.Port))
	}

	return header
}

// mmm: write proxy v2 header and copy packet
func connCopyWithProxy(connBackend net.Conn, timeout time.Duration, dst io.WriteCloser, src io.Reader, header []byte, errCh chan error, withProxy bool, tag string) {
/*
	defer func (){
		fmt.Println("exit " + tag)
	}()*/
	// The buffer is initialized to the maximum UDP datagram size,
	// to make sure that the whole UDP datagram is read or written atomically (no data is discarded).
	buffer := make([]byte, maxDatagramSize)
//	fmt.Println(tag, "start")

	// TODO: datagram size limit
	var er, err error
	written := 0
	for {
		nr := 0
		connBackend.SetReadDeadline(time.Now().Add(timeout))
		nr, er = src.Read(buffer)
//		fmt.Println(tag, "read", nr, "er", er)
		if nr > 0 {
//			fmt.Print(header)
			var msg []byte
			if withProxy {
				msg = append(header, buffer[0:nr]...)
				// mmm: otherwise error is generated on write
				nr += len(header)
			} else {
				msg = buffer[0:nr]
			}
			connBackend.SetWriteDeadline(time.Now().Add(timeout))
			nw, ew := dst.Write(msg)
//			fmt.Println(tag, "write", nw, "ew", ew)
			if nw < 0 || nr < nw {
				nw = 0
				if ew == nil {
					ew = errors.New("invalid write result")
				}
			}
			written += nw
			if ew != nil {
//				fmt.Println(tag, "ew != nil, break")
				er = ew
				break
			}
			if nr != nw {
				er = io.ErrShortWrite
				// fmt.Println(tag, "short, break", er)
				break
			}
		}
		if er != nil {
			// fmt.Println(tag, "short, er != nil, break", er)
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	errCh <- err
/*
	// return quietly on timeout
	if os.IsTimeout(err) {
		return
	}*/

	// fmt.Println(tag, "close")
	if err := dst.Close(); err != nil {
//		log.Debug().Err(err).Msg("Error while terminating UDP stream")
	}
}

func connCopy(dst io.WriteCloser, src io.Reader, errCh chan error, tag string) {
	// The buffer is initialized to the maximum UDP datagram size,
	// to make sure that the whole UDP datagram is read or written atomically (no data is discarded).
	buffer := make([]byte, maxDatagramSize)

	_, err := io.CopyBuffer(dst, src, buffer)
	errCh <- err

//	fmt.Println("close ", tag)
	if err := dst.Close(); err != nil {
		log.Debug().Err(err).Msg("Error while terminating UDP stream")
	}
}
