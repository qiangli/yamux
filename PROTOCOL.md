# Yamux Protocol — Design Record

Source basis: [HashiCorp yamux `spec.md`](https://github.com/hashicorp/yamux/blob/master/spec.md) (read for facts, not vendored) and [libp2p/rust-yamux](https://github.com/libp2p/rust-yamux) pinned at `6610cff7b3fe81aeb4fda9fa96d7ac4b3b2dbaf8`. All prose below is original; it restates the wire facts in our own words.

## 1. Overview

Yamux multiplexes many independent byte streams on top of a single ordered reliable transport (typically TCP). Every exchange is a sequence of frames. There is no out-of-band handshake for the session itself; the first data-bearing or window frame that carries a fresh identifier implicitly opens a stream. One side is designated client, the other server, to partition the identifier space and avoid collisions.

## 2. Frame header layout

Each frame is a 12-byte header followed by an optional payload. Fields are transmitted in big-endian order.

| Offset | Size | Field   | Meaning |
|--------|------|---------|---------|
| 0      | 1    | Version | Protocol version. Currently always `0`. Any other value is a protocol error. |
| 1      | 1    | Type    | Discriminates the frame kind (see §3). |
| 2      | 2    | Flags   | Bitset modifying the frame (see §4). |
| 4      | 4    | StreamID| Unsigned 32-bit stream identifier (see §5). |
| 8      | 4    | Length  | Unsigned 32-bit length whose meaning depends on Type (see §3). |

The header size is fixed at 12 bytes. A receiver first reads exactly 12 bytes, validates version and type, then reads `Length` bytes of body if `Type == Data`. Other types carry no body beyond the header.

## 3. Frame types

| Value | Name            | Length semantics | StreamID rule | Body |
|-------|-----------------|----------------|---------------|------|
| `0x00`| Data            | Number of payload bytes that follow the header. | Stream-specific (>0) or accepted for SYN/FIN/RST with data | `Length` bytes |
| `0x01`| Window Update   | Credit (see §6) granted to the peer; delta added to the peer's send window for that identifier. | Stream-specific, or `0` for a session-level update in some implementations | none |
| `0x02`| Ping            | Opaque 32-bit identifier chosen by the sender; echoed by the responder. | Must be `0` (session) | none |
| `0x03`| Go Away         | Error code (see §8) | Must be `0` (session) | none |

Any unknown Type is a protocol error and terminates the session.

## 4. Flags

Flags occupy the 16-bit field; only the low four bits are assigned in version 0. Bits may be combined where the Type permits.

| Bit    | Name | Value  | Applies to        | Effect |
|--------|------|--------|-------------------|--------|
| `0x01` | SYN  | 1      | Data, WindowUpdate, Ping | Marks the first frame for a stream (outbound open), or the outbound direction of a ping. |
| `0x02` | ACK  | 2      | Data, WindowUpdate, Ping | Acknowledges a SYN, or marks a ping reply. For streams, indicates the peer accepts the stream. |
| `0x04` | FIN  | 4      | Data, WindowUpdate | Half-close: the sender will emit no further payload on this stream. |
| `0x08` | RST  | 8      | Data, WindowUpdate | Immediate reset: tears the stream down without graceful drain. |

Notable combinations:

* `SYN` alone — initiator opens a stream and may already carry payload.
* `SYN|ACK` — responder accepts a stream; may coincide with data.
* `ACK` alone on a WindowUpdate — common accept path in the Rust implementation.
* `FIN` alone or `FIN|ACK` — graceful shutdown of the send side.
* `RST` — abort; the peer must discard buffered bytes for that stream.

FIN and RST are mutually exclusive in well-formed usage. Ping uses SYN for the request and ACK for the response.

## 5. Stream identifiers

* `0` is reserved for the session. Only Ping and GoAway may use it.
* Client-originated streams use odd identifiers (1, 3, 5, ...).
* Server-originated streams use even identifiers (2, 4, 6, ...).
* Identifiers within each parity increase monotonically; exhaustion (wrap past `2^32-1`) is a session error.
* A newly seen odd (client) or even (server) identifier with SYN signals an inbound open.

## 6. Window / flow-control semantics

Yamux provides per-stream credit-based flow control; there is no separate session-wide data window.

* Initial credit for each new stream is 256 KiB (`256 * 1024` bytes). Both sides assume this value for a peer until a WindowUpdate says otherwise.
* Only Data payload bytes count against the peer's send window. Headers, WindowUpdate, Ping and GoAway do not.
* The receiver advertises additional credit with WindowUpdate frames. The Length field carries the delta to add to the sender's available window.
* A sender must not transmit Data that would exceed its current window for that stream. If it does, the peer treats it as a window overflow error.
* The receiver should send updates before the window collapses. A common policy is to issue an update when roughly half the window has been consumed, coalescing small increments.
* The Rust implementation auto-tunes the receive window toward the bandwidth-delay product (BDP). When the receiver drains data fast enough that half the window is consumed within roughly two round trips, it doubles the maximum window, bounded by a configurable per-connection aggregate limit (default 1 GiB across all streams, or user-set). The tuning never shrinks the maximum.
* Session-level WindowUpdate with StreamID 0, if present, is an aggregate hint; Go implementations track per-stream limits and an optional connection-level cap (`MaxStreamWindowSize`, `MaxConnectionReceiveWindow` in Rust).
* Overflow checks are mandatory: any WindowUpdate that would make the window wrap or exceed the configured maximum is rejected and terminates the session with a protocol error.
* Payload splitting: implementations typically cap a single Data frame's payload (e.g., 16 KiB) and split larger writes into multiple frames to limit head-of-line delay and interleaving latency.

## 7. Keepalive / ping

Ping measures round-trip time and acts as a session keepalive.

* The sender emits a Ping with a monotonically increasing opaque value in Length and the SYN flag set, using StreamID 0.
* The receiver must echo the same opaque value in a Ping with the ACK flag set.
* Pings may also carry WindowUpdate semantics when piggybacked on SYN/ACK flows in some stacks, but the plain Ping Type is the dedicated RTT mechanism.
* Configuration options: enabling keepalives, interval between pings, and an ack backlog limit.
* RTT measurement in rust-yamux: a single outstanding ping at a time, scheduled at most every 10 s (or at the configured interval). If an unexpected pong arrives, or the wrong identifier is echoed, the connection is treated as a protocol error.
* A peer that fails to answer within a timeout may cause the session to be closed and an error surfaced to streams (`ErrKeepAliveTimeout`).

## 8. Go Away

GoAway terminates the session.

* Always sent with StreamID 0.
* Length holds an error code:

| Code | Meaning |
|------|---------|
| `0`  | Normal termination — the sender is no longer accepting new streams but may finish existing ones. |
| `1`  | Protocol error — the sender detected a violation. |
| `2`  | Internal error — the sender encountered a local failure. |

* Upon receiving GoAway, a peer must stop creating new streams, fail pending opens with a go-away error, and may continue draining already-open streams. Sending a second GoAway with a more specific code is permitted before the underlying transport closes.
* Implementations map these codes to distinct error values surfaced on `Accept`/`Open` and on in-flight streams.

## 9. Error codes and exported errors

Wire error codes appear only in GoAway Length. Library-level errors translate those and other conditions:

* `ErrInvalidVersion` — version field was not zero.
* `ErrInvalidMsgType` — type field outside 0–3.
* `ErrSessionShutdown` / `ErrRemoteGoAway` — session is going away.
* `ErrStreamsExhausted` — identifier space exhausted.
* `ErrDuplicateStream` — peer reused an active stream identifier.
* `ErrRecvWindowExceeded` — peer sent more data than the window allowed.
* `ErrUnexpectedFlag` — flag combination illegal for the type.
* `ErrConnectionReset` / `ErrTimeout` / `ErrConnectionWriteTimeout` / `ErrKeepAliveTimeout` — transport or liveness failures.
* `ErrStreamClosed` — operation on a stream already fully closed.
* `ErrStreamReset` / `StreamError` / `GoAwayError` — reset and go-away details in the libp2p v5 surface (see API-INVENTORY).

Decoding errors (`HeaderDecodeError`, `FrameDecodeError`, `FrameTooLarge`) map I/O and framing faults, and usually cause a session-level termination with GoAway code 1 or 2.

## 10. Half-close and reset

* Half-close: either side may send `FIN` on a Data or WindowUpdate frame. After sending FIN, that side's write path is closed; it may still read until the peer also sends FIN. When both directions have sent FIN, the stream reaches the closed state and its resources may be released. FIN may accompany the last payload bytes.
* Late data after FIN (or after the stream is closed) is discarded and may trigger a reset.
* Reset (`RST`): an immediate tear-down. The sender discards receive buffers for that stream, replies with RST if it sees further frames for it, and surfaces a reset error to the application (`ErrConnectionReset` / `StreamError{Remote:true}` depending on the surface). RST can be sent to reject an inbound SYN before any ACK, to abort a half-closed stream that exceeded a close timeout, or when the accept backlog is full. Unlike FIN, RST does not wait for the peer's FIN.

## 11. References

* Upstream specification: <https://github.com/hashicorp/yamux/blob/master/spec.md>
* Design source port: rust-yamux `6610cff7b3fe81aeb4fda9fa96d7ac4b3b2dbaf8`

