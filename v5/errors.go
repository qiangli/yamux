package yamux

import (
	"errors"
	"fmt"
	"os"

	core "github.com/qiangli/yamux"
)

// The three protocol-defined go-away codes. Larger values are application
// defined and travel unchanged in both directions.
const (
	goAwayNormal        = core.GoAwayNormal
	goAwayProtocolError = core.GoAwayProtocolError
	goAwayInternalError = core.GoAwayInternalError
)

// Error is a yamux protocol or session error. It implements net.Error so
// callers can distinguish deadlines from permanent failures.
type Error struct {
	msg       string
	timeout   bool
	temporary bool
}

func (ye *Error) Error() string   { return ye.msg }
func (ye *Error) Temporary() bool { return ye.temporary }
func (ye *Error) Timeout() bool   { return ye.timeout }

// StreamError is returned by stream reads and writes after the stream has been
// reset, and reports the error code the resetting side chose. Remote is true
// when the peer initiated the reset.
type StreamError struct {
	ErrorCode uint32
	Remote    bool
}

func (s *StreamError) Error() string {
	if s.Remote {
		return fmt.Sprintf("stream reset by remote, error code: %d", s.ErrorCode)
	}
	return fmt.Sprintf("stream reset, error code: %d", s.ErrorCode)
}

// Is reports a match against ErrStreamReset and against any StreamError from
// the same side, so errors.Is(err, ErrStreamReset) holds for every reset
// regardless of code.
func (s *StreamError) Is(target error) bool {
	if target == ErrStreamReset {
		return true
	}
	other, ok := target.(*StreamError)
	return ok && other.Remote == s.Remote
}

// GoAwayError reports that a session was shut down, and the code the shutting
// side chose. Remote is true when the peer sent the go-away.
type GoAwayError struct {
	ErrorCode uint32
	Remote    bool
}

func (e *GoAwayError) Error() string {
	if e.Remote {
		return fmt.Sprintf("remote sent go away, code: %d", e.ErrorCode)
	}
	return fmt.Sprintf("sent go away, code: %d", e.ErrorCode)
}

// Is matches any go-away from the same side, so errors.Is(err, ErrRemoteGoAway)
// and errors.Is(err, ErrSessionShutdown) hold regardless of code.
func (e *GoAwayError) Is(target error) bool {
	other, ok := target.(*GoAwayError)
	return ok && other.Remote == e.Remote
}

func (e *GoAwayError) Temporary() bool { return false }
func (e *GoAwayError) Timeout() bool   { return false }

// The exported sentinels of the v5 surface. ErrSessionShutdown and
// ErrRemoteGoAway are go-away errors; ErrStreamReset matches every StreamError.
var (
	ErrInvalidVersion         = &Error{msg: "invalid protocol version"}
	ErrInvalidMsgType         = &Error{msg: "invalid msg type"}
	ErrSessionShutdown        = &GoAwayError{ErrorCode: goAwayNormal, Remote: false}
	ErrStreamsExhausted       = &Error{msg: "streams exhausted"}
	ErrDuplicateStream        = &Error{msg: "duplicate stream initiated"}
	ErrRecvWindowExceeded     = &Error{msg: "recv window exceeded"}
	ErrTimeout                = &Error{msg: "i/o deadline reached", timeout: true, temporary: true}
	ErrStreamClosed           = &Error{msg: "stream closed"}
	ErrUnexpectedFlag         = &Error{msg: "unexpected flag"}
	ErrRemoteGoAway           = &GoAwayError{Remote: true, ErrorCode: goAwayNormal}
	ErrStreamReset            = &Error{msg: "stream reset"}
	ErrConnectionWriteTimeout = &Error{msg: "connection write timeout", timeout: true}
	ErrKeepAliveTimeout       = &Error{msg: "keepalive timeout", timeout: true}
)

// mapSentinel rewrites the core sentinels that need no session context. The
// core wraps most of them with %w, so matching goes through errors.Is.
func mapSentinel(err error) (error, bool) {
	switch {
	case errors.Is(err, core.ErrInvalidVersion):
		return ErrInvalidVersion, true
	case errors.Is(err, core.ErrInvalidMsgType):
		return ErrInvalidMsgType, true
	case errors.Is(err, core.ErrStreamsExhausted):
		return ErrStreamsExhausted, true
	case errors.Is(err, core.ErrDuplicateStream):
		return ErrDuplicateStream, true
	case errors.Is(err, core.ErrRecvWindowExceeded):
		return ErrRecvWindowExceeded, true
	case errors.Is(err, core.ErrUnexpectedFlag):
		return ErrUnexpectedFlag, true
	case errors.Is(err, core.ErrStreamClosed):
		return ErrStreamClosed, true
	case errors.Is(err, core.ErrConnectionWriteTimeout):
		return ErrConnectionWriteTimeout, true
	case errors.Is(err, core.ErrKeepAliveTimeout):
		return ErrKeepAliveTimeout, true
	case errors.Is(err, core.ErrTimeout), errors.Is(err, os.ErrDeadlineExceeded):
		return ErrTimeout, true
	}
	return err, false
}
