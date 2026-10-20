package yamux

import (
	"errors"
	"testing"
)

func TestFrameHeaderRoundTrip(t *testing.T) {
	for _, typ := range []FrameType{TypeData, TypeWindowUpdate, TypePing, TypeGoAway} {
		for _, flags := range []Flags{0, FlagSYN, FlagACK, FlagFIN, FlagRST, FlagSYN | FlagACK, 0xffff} {
			h := Header{Version: 0, Type: typ, Flags: flags, StreamID: 0xdeadbeef, Length: 0x01020304}
			buf := h.Encode()
			got, err := DecodeHeader(buf[:])
			if err != nil {
				t.Fatalf("type %v flags %#x: %v", typ, flags, err)
			}
			if got != h {
				t.Fatalf("round trip: got %+v want %+v", got, h)
			}
		}
	}
}

func TestFrameHeaderWireLayout(t *testing.T) {
	h := Header{Type: TypeWindowUpdate, Flags: FlagSYN | FlagFIN, StreamID: 0x01020304, Length: 0x0a0b0c0d}
	want := [HeaderSize]byte{0, 1, 0, 5, 1, 2, 3, 4, 0x0a, 0x0b, 0x0c, 0x0d}
	if got := h.Encode(); got != want {
		t.Fatalf("got % x want % x", got, want)
	}
}

func TestFrameHeaderDecodeErrors(t *testing.T) {
	_, err := DecodeHeader([]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	if !errors.Is(err, ErrInvalidVersion) {
		t.Fatalf("version: got %v", err)
	}
	_, err = DecodeHeader([]byte{0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	if !errors.Is(err, ErrInvalidMsgType) {
		t.Fatalf("type: got %v", err)
	}
	if errors.Is(ErrInvalidVersion, ErrInvalidMsgType) {
		t.Fatal("errors must be distinct")
	}
	if _, err = DecodeHeader(make([]byte, 11)); !errors.Is(err, ErrShortHeader) {
		t.Fatalf("short: got %v", err)
	}
}

func TestFrameFlagValidity(t *testing.T) {
	cases := []struct {
		typ   FrameType
		flags Flags
		ok    bool
	}{
		{TypeData, FlagSYN | FlagACK | FlagFIN | FlagRST, true},
		{TypeWindowUpdate, FlagACK, true},
		{TypePing, FlagSYN, true},
		{TypePing, FlagACK, true},
		{TypePing, FlagFIN, false},
		{TypePing, FlagRST, false},
		{TypeGoAway, 0, true},
		{TypeGoAway, FlagSYN, false},
		{TypeData, 0x10, false},
	}
	for _, c := range cases {
		err := Header{Type: c.typ, Flags: c.flags}.ValidateFlags()
		if (err == nil) != c.ok || (err != nil && !errors.Is(err, ErrUnexpectedFlag)) {
			t.Errorf("%v %#x: err=%v want ok=%v", c.typ, c.flags, err, c.ok)
		}
	}
}

func FuzzDecodeHeader(f *testing.F) {
	f.Add([]byte{0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 5})
	f.Add([]byte{1, 9, 0xff, 0xff})
	f.Fuzz(func(t *testing.T, b []byte) {
		h, err := DecodeHeader(b)
		if err != nil {
			return
		}
		enc := h.Encode()
		if string(enc[:]) != string(b[:HeaderSize]) {
			t.Fatalf("re-encode mismatch: % x vs % x", enc, b[:HeaderSize])
		}
		_ = h.ValidateFlags()
	})
}
