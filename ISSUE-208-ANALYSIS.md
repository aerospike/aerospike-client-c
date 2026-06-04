# Issue #208 — `aerospike_key_put_async` TLS timeout on libev/libevent

`aerospike_key_put_async` over TLS returns `AEROSPIKE_ERR_TIMEOUT` for payloads
larger than the kernel TCP send buffer on the **libev** and **libevent**
backends, but not on **libuv**, and only when client and server are on
different hosts with ≥13 ms latency.

This document records what was verified empirically, the root cause, and the
recommended fix.

---

## 1. What was ruled out (verified empirically)

A standalone OpenSSL harness was built that drives `SSL_write()` on a
non-blocking socket exactly the way `as_ev_write()` / `as_event_write()` do
(arm a single‑direction, level‑triggered watcher chosen from
`SSL_get_error()`, then wait for that readiness and retry). It was run under a
network namespace with `tc netem delay 13ms` on loopback, with the send buffer
pinned small to force the "above kernel send buffer" condition.

Results:

* **The `WANT_WRITE` retry loop is correct.** A 512 KB write over a 16 KB send
  buffer with 13 ms latency completed after 11 `WANT_WRITE` cycles. The
  `as_ev_write()` logic of "park on writability, retry `SSL_write()` with the
  same buffer" works as designed.
* **`SSL_write()` never makes a partial positive return** (no
  `SSL_MODE_ENABLE_PARTIAL_WRITE`), so the retry always passes the *same*
  buffer pointer and length — there is **no moving‑write‑buffer hazard**.
* **`SSL_write()` returned `WANT_READ` zero times** during bulk upload on
  OpenSSL 3.0.2, so a naive "switch to read watch" deadlock did not reproduce
  there.
* **The read‑drain loop (`while (as_tls_read_pending() > 0)`) is sound for this
  client.** With OpenSSL read‑ahead off (the default), a second TLS record is
  left in the *kernel* buffer rather than inside OpenSSL, so the socket stays
  readable and a new event fires. `SSL_pending()` vs `SSL_has_pending()`
  divergence did **not** occur.

So the failure is **not** in the high‑level retry logic, nor in the read path.

## 2. Root cause

The libev/libevent backends bind OpenSSL **directly to the socket fd**:

```c
// as_tls.c : as_tls_wrap()
SSL_set_fd(sock->ssl, (int)sock->fd);
```

`SSL_write()` therefore writes encrypted records straight into the kernel TCP
send buffer. When the request is larger than that buffer — which only happens
once there is enough in‑flight data, hence the ≥13 ms latency requirement —
the write cannot complete in one call and the command must be driven across
many readiness events while parked on a **single‑direction, level‑triggered**
fd watcher (`conn->watching` is exactly one of `EV_READ`/`EV_WRITE` at a time,
chosen from the last `SSL_get_error()`).

This design is correct only while OpenSSL's `WANT_READ`/`WANT_WRITE` exactly
matches what kernel‑fd readiness will unblock. It is fragile precisely in the
"large TLS write over a high‑latency link" corner, where the record layer can
need the *other* direction (or needs to process a buffered post‑handshake
record) at the moment the command is parked. When that happens the watcher
waits for an event that never arrives and the command stalls until
`total_timeout` — exactly the reported symptom.

Two facts make this acute on the reporter's environment (Debian 13):

* **TLS 1.3 is silently negotiated.** The protocol enum in `as_tls.c` has no
  1.3 bit and `as_tls_context_setup()` never sets `SSL_OP_NO_TLSv1_3`, so the
  "default `TLSv1.2`" configuration actually negotiates **TLS 1.3** on modern
  OpenSSL. The harness confirmed `version=TLSv1.3` with the stock client
  method. TLS 1.3 adds post‑handshake traffic (NewSessionTicket, KeyUpdate)
  that arrives interleaved with the upload — the harness showed the socket
  readable (`POLLIN=1`) with a NewSessionTicket pending *while* the client was
  blocked on writability (`POLLOUT=0`).
* Debian 13 ships **OpenSSL 3.5.x**; the verified-good runs above used 3.0.2.
  The record‑layer state in which `SSL_write` returns the cross‑direction
  status differs across 3.x releases.

**Why libuv is immune.** The libuv backend (`as_event_uv.c`) does *not* use
`SSL_set_fd`. It wires OpenSSL to an in‑memory **BIO pair**:

```c
SSL_set_bio(tls->ssl, tls->ibio, tls->ibio);   // memory BIO, never blocks
```

`SSL_write()` always succeeds into memory, and a separate, completion‑driven
writer (`as_uv_tls_send_pending` → `uv_write`) flushes the network BIO to the
socket with real backpressure. When `SSL_write` returns `WANT_READ`, libuv
explicitly `uv_read_start()`s and re‑drives. It never depends on an
unsatisfiable single‑direction fd event, so the send‑buffer‑full case simply
cannot strand it.

## 3. Recommended fix

Bring the libev/libevent TLS path to parity with libuv by replacing the
direct‑socket binding with a **BIO pair**, so SSL work happens against memory
and all socket I/O is explicit and completion‑driven:

1. In `as_tls_wrap()` (or an async‑specific wrap), instead of
   `SSL_set_fd()`, create an internal BIO and a network BIO
   (`BIO_new_bio_pair`) and `SSL_set_bio(ssl, ibio, ibio)`.
2. In the event backends, the write path becomes:
   `SSL_write(plaintext)` → drain `BIO_pending(nbio)` to the socket via the
   existing writable watcher (handling `EWOULDBLOCK` as the partial‑flush
   point); the read path feeds bytes from the socket into `nbio` via
   `BIO_write` then `SSL_read`. This is the same flow `as_event_uv.c` already
   implements with `as_uv_tls_try_send_pending` / `as_uv_tls_fill_buffer`.

This removes the entire class of "SSL_write blocked on the socket send buffer"
stalls and matches the backend that is already known to work.

### Smaller, related hardening (do regardless)

* **Explicitly pin the TLS version.** Add a `TLSv1.3` entry to the protocol
  enum and set `SSL_OP_NO_TLSv1_3` when 1.3 is not requested, so the documented
  "default TLSv1.2" actually means 1.2. Today 1.3 is negotiated unintentionally.

## 4. How to validate

The decisive test cannot be run from a single host. To confirm on the
reporter's setup:

1. Reproduce with their Docker image (`AS_BACKEND=libev`, 100 KB payload,
   ≥13 ms latency) — expect `FAIL ~3001ms`.
2. As a quick **diagnostic**, force TLS 1.2 end‑to‑end (and verify
   `SSL_OP_NO_TLSv1_3` is set). If the timeout changes/clears, it confirms the
   TLS‑1.3 post‑handshake interaction is the trigger.
3. Apply the BIO‑pair change and re‑run the libev/libevent image; it should
   pass like libuv.

## 5. Files involved

* `src/main/aerospike/as_tls.c` — `as_tls_wrap()` (`SSL_set_fd`),
  `as_tls_write_once()`, protocol/option setup.
* `src/main/aerospike/as_event_ev.c` — `as_ev_write()`, `as_ev_read()`,
  `as_ev_watch_write/read()`, `as_ev_callback_common()`.
* `src/main/aerospike/as_event_event.c` — identical structure
  (`as_event_write()` etc.).
* `src/main/aerospike/as_event_uv.c` — reference implementation that works
  (BIO‑pair design).
