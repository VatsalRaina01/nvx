use std::fmt;
use std::time::Instant;

use super::limits::Limits;
use super::message::{Cause, Message, Stream};
use super::outcome::Outcome;
use super::settlement::Settlement;
use super::timers::Timers;
use super::window::Window;
use super::{Key, Status};

/// Messages sent by the runner to the peer.
pub mod message {
    use super::{Cause, Outcome, Stream};

    /// A message sent by the runner to the peer.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub enum Message {
        /// Credit assigned to the peer for input.
        Credit { units: usize },
        /// The execution has started.
        Started,
        /// Output data from the workload.
        Output { stream: Stream, length: usize },
        /// A stream has ended.
        End { stream: Stream, truncated: bool },
        /// Input has been closed.
        InputClosed,
        /// The execution has finished with a terminal outcome.
        Terminal(Outcome),
    }
}

/// Requests made by the runner to the adapter/workload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Request {
    /// Read input.
    Read { max_len: usize },
    /// Terminate the execution.
    Terminate(Cause),
    /// Discard data from a stream.
    Discard(Stream),
    /// Settle the execution.
    Settle,
}

#[derive(Clone, Debug)]
struct StreamState {
    closed: bool,
    truncated: bool,
    queue_len: usize,
}

impl StreamState {
    fn new() -> Self {
        Self {
            closed: false,
            truncated: false,
            queue_len: 0,
        }
    }
}

/// The runner side of an execution.
#[derive(Clone, Debug)]
pub struct Runner {
    key: Key,
    limits: Limits,
    timers: Timers,
    t0: Instant,
    stdout: StreamState,
    stderr: StreamState,
    started: bool,
    launched: bool,
    launch_failed: bool,
    terminated: Option<Cause>,
    exited: Option<(Instant, Option<Status>)>,
    settled: Option<Settlement>,
    messages: Vec<message::Message>,
    requests: Vec<Request>,
    discarding_stdout: usize,
    discarding_stderr: usize,
    output_closed_stdout: bool,
    output_closed_stderr: bool,
    terminal_sent: bool,
}

impl Runner {
    /// Creates a new runner.
    pub fn new(key: Key, limits: Limits, timers: Timers, t0: Instant) -> Self {
        Self {
            key,
            limits,
            timers,
            t0,
            stdout: StreamState::new(),
            stderr: StreamState::new(),
            started: false,
            launched: false,
            launch_failed: false,
            terminated: None,
            exited: None,
            settled: None,
            messages: Vec::new(),
            requests: Vec::new(),
            discarding_stdout: 0,
            discarding_stderr: 0,
            output_closed_stdout: false,
            output_closed_stderr: false,
            terminal_sent: false,
        }
    } 

    /// Returns the key of the execution.
    pub fn key(&self) -> Key {
        self.key
    }

    /// Reports whether the execution has launched.
    pub fn launched(&mut self, result: Result<(), Status>) {
        if self.launched {
            return;
        }
        self.launched = true;
        match result {
            Ok(()) => {
                self.started = true;
                self.messages.push(message::Message::Started);
                self.messages.push(message::Message::Credit {
                    units: self.limits.input_window().max(),
                });
            }
            Err(status) => {
                self.launch_failed = true;
                self.exited(self.t0, Some(status));
            }
        }
    }

    /// Reports output data from the workload.
    pub fn output(&mut self, stream: Stream, data: &[u8]) -> usize {
        if !self.started || self.terminated.is_some() || self.exited.is_some() {
            return 0;
        }
        let state = match stream {
            Stream::Stdout => &mut self.stdout,
            Stream::Stderr => &mut self.stderr,
        };
        if state.closed {
            return 0;
        }
        let len = data.len();
        state.queue_len += len;
        self.messages.push(message::Message::Output { stream, length: len });
        len
    }

    /// Reports that a stream failed to read.
    pub fn output_failed(&mut self, stream: Stream) {
        let state = match stream {
            Stream::Stdout => &mut self.stdout,
            Stream::Stderr => &mut self.stderr,
        };
        state.truncated = true;
        state.closed = true;
        self.messages.push(message::Message::End {
            stream,
            truncated: true,
        });
    }

    /// Reports that a stream has closed.
    pub fn output_closed(&mut self, stream: Stream) {
        let state = match stream {
            Stream::Stdout => &mut self.stdout,
            Stream::Stderr => &mut self.stderr,
        };
        if state.closed {
            return;
        }
        state.closed = true;
        match stream {
            Stream::Stdout => self.output_closed_stdout = true,
            Stream::Stderr => self.output_closed_stderr = true,
        }
        if state.queue_len == 0 {
            self.messages.push(message::Message::End {
                stream,
                truncated: state.truncated,
            });
        }
    }

    /// Reports discarded bytes from a stream.
    pub fn discarded(&mut self, stream: Stream, count: usize) {
        let state = match stream {
            Stream::Stdout => &mut self.stdout,
            Stream::Stderr => &mut self.stderr,
        };
        state.truncated = true;
        if count >= state.queue_len {
            state.queue_len = 0;
        } else {
            state.queue_len -= count;
        }
        if state.closed && state.queue_len == 0 {
            self.messages.push(message::Message::End {
                stream,
                truncated: state.truncated,
            });
        }
    }

    /// Reports that the workload has exited.
    pub fn exited(&mut self, now: Instant, status: Option<Status>) {
        if self.exited.is_some() {
            return;
        }
        self.exited = Some((now, status));
        self.requests.push(Request::Settle);
    }

    /// Reports settlement status.
    pub fn settled(&mut self, settlement: Settlement) {
        self.settled = Some(settlement);
        self.try_send_terminal();
    }

    /// Cancels the execution.
    pub fn cancel(&mut self, now: Instant, key: Key, cause: Cause) -> Result<(), ()> {
        if key != self.key || self.terminated.is_some() {
            return Err(());
        }
        self.terminated = Some(cause);
        self.requests.push(Request::Terminate(cause));
        Ok(())
    }

    /// Ticks the runner timer.
    pub fn tick(&mut self, now: Instant) {
        if let Some((exited_at, _)) = self.exited {
            if now >= exited_at + self.timers.settlement() && self.settled.is_none() {
                self.settled = Some(Settlement::Timeout);
                self.try_send_terminal();
            }
        }
        if let Some(cause) = self.terminated {
            if let Some((exited_at, _)) = self.exited {
                if now >= exited_at + self.timers.grace() {
                    // Grace expired
                }
            }
        }
    }

    fn try_send_terminal(&mut self) {
        if self.terminal_sent || self.settled.is_none() || self.exited.is_none() {
            return;
        }
        let (_, status) = self.exited.unwrap();
        let cause = self.terminated.unwrap_or(Cause::Normal);
        let outcome = Outcome::new(
            cause,
            status,
            self.stdout.truncated,
            self.stderr.truncated,
        );
        self.messages.push(message::Message::Terminal(outcome));
        self.terminal_sent = true;
    }

    /// Retrieves the next message for the peer.
    pub fn next_message(&mut self, buffer: &mut [u8]) -> Option<message::Message> {
        if self.messages.is_empty() {
            return None;
        }
        Some(self.messages.remove(0))
    }

    /// Retrieves the next request for the adapter.
    pub fn next_request(&mut self) -> Option<Request> {
        if self.requests.is_empty() {
            return None;
        }
        Some(self.requests.remove(0))
    }

    /// Resets the runner.
    pub fn reset(&mut self, _now: Instant) {
        self.terminated = Some(Cause::Reset);
    }

    /// Processes input credit or data.
    pub fn credit(&mut self, key: Key, pool: super::Pool, units: usize) -> Result<(), ()> {
        if key != self.key {
            return Err(());
        }
        Ok(())
    }
}
