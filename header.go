package yamux

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// HeaderSize is the serialised frame header size in bytes.
const HeaderSize = 12

// FrameType is the frame type tag (header byte 1).
type FrameType uint8

const (
	TypeData         FrameType = 0
	TypeWindowUpdate FrameType = 1
	TypePing         FrameType = 2
	TypeGoAway       FrameType = 3
)

func (t FrameType) String() string {
	switch t {
	case TypeData:
		return "Data"
	case TypeWindowUpdate:
		return "WindowUpdate"
	case TypePing:
		return "Ping"
	case TypeGoAway:
		return "GoAway"
	}
	return fmt.Sprintf("FrameType(%d)", uint8(t))
}

// Flags is the 16-bit flag set of a frame header.
type Flags uint16

const (
	FlagSYN Flags = 1
	FlagACK Flags = 2
	FlagFIN Flags = 4
	FlagRST Flags = 8
)

// Contains reports whether all bits of other are set in f.
func (f Flags) Contains(other Flags) bool { return f&other == other }

var (
	ErrInvalidVersion = errors.New("yamux: invalid protocol version")
	ErrInvalidMsgType = errors.New("yamux: invalid message type")
	ErrShortHeader    = errors.New("yamux: short frame header")
	ErrUnexpectedFlag = errors.New("yamux: unexpected flag")
)

// Header is a decoded frame header. Length is the payload size for Data, the
// credit for WindowUpdate, the opaque id for Ping, and the error code for GoAway.
type Header struct {
	Version  uint8
	Type     FrameType
	Flags    Flags
	StreamID uint32
	Length   uint32
}

// Encode serialises h in big-endian order.
func (h Header) Encode() [HeaderSize]byte {
	var b [HeaderSize]byte
	b[0] = h.Version
	b[1] = uint8(h.Type)
	binary.BigEndian.PutUint16(b[2:4], uint16(h.Flags))
	binary.BigEndian.PutUint32(b[4:8], h.StreamID)
	binary.BigEndian.PutUint32(b[8:12], h.Length)
	return b
}

// DecodeHeader parses the first HeaderSize bytes of b. It rejects unknown
// versions and types but does not validate flags; see ValidateFlags.
func DecodeHeader(b []byte) (Header, error) {
	if len(b) < HeaderSize {
		return Header{}, ErrShortHeader
	}
	if b[0] != 0 {
		return Header{}, fmt.Errorf("%w: %d", ErrInvalidVersion, b[0])
	}
	if b[1] > uint8(TypeGoAway) {
		return Header{}, fmt.Errorf("%w: %d", ErrInvalidMsgType, b[1])
	}
	return Header{
		Version:  b[0],
		Type:     FrameType(b[1]),
		Flags:    Flags(binary.BigEndian.Uint16(b[2:4])),
		StreamID: binary.BigEndian.Uint32(b[4:8]),
		Length:   binary.BigEndian.Uint32(b[8:12]),
	}, nil
}

// ValidateFlags checks that h's flags are legal for its type: Data and
// WindowUpdate allow SYN/ACK/FIN/RST, Ping allows SYN or ACK, GoAway allows none.
func (h Header) ValidateFlags() error {
	var allowed Flags
	switch h.Type {
	case TypeData, TypeWindowUpdate:
		allowed = FlagSYN | FlagACK | FlagFIN | FlagRST
	case TypePing:
		allowed = FlagSYN | FlagACK
	}
	if h.Flags&^allowed != 0 {
		return fmt.Errorf("%w: %#x on %v frame", ErrUnexpectedFlag, uint16(h.Flags), h.Type)
	}
	return nil
}
