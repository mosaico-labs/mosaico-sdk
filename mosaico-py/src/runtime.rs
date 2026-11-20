use std::mem::ManuallyDrop;
use std::pin::pin;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::Thread;
use std::time::Duration;

use pyo3::prelude::*;
use tokio::runtime::Runtime;

use crate::errors::{MosaicoError, to_py};

pub(crate) const IO_THREADS: usize = 2;
pub(crate) const SIGNAL_CHECK_INTERVAL: Duration = Duration::from_millis(100);

struct ProcessRuntime {
    pid: u32,
    runtime: Runtime,
}

pub(crate) fn runtime() -> Result<&'static Runtime, String> {
    static CURRENT: AtomicPtr<ProcessRuntime> = AtomicPtr::new(ptr::null_mut());

    let pid = std::process::id();
    let mut current = CURRENT.load(Ordering::Acquire);
    loop {
        if let Some(installed) = unsafe { current.as_ref() }
            && installed.pid == pid
        {
            return Ok(&installed.runtime);
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(IO_THREADS)
            .enable_all()
            .thread_name("mosaico-io")
            .build()
            .map_err(|e| format!("cannot start the I/O threads: {e}"))?;
        let new = Box::into_raw(Box::new(ProcessRuntime { pid, runtime }));
        match CURRENT.compare_exchange(current, new, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return Ok(unsafe { &(*new).runtime }),
            Err(installed) => {
                drop(unsafe { Box::from_raw(new) });
                current = installed;
            }
        }
    }
}

pub(crate) fn block_on<F: Future>(runtime: &Runtime, future: F) -> F::Output {
    if tokio::runtime::Handle::try_current().is_err() {
        return runtime.block_on(future);
    }
    let _context = runtime.enter();
    let mut future = pin!(tokio::task::unconstrained(future));
    let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
        std::thread::park();
    }
}

struct Unpark(Thread);

impl Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

pub(crate) fn wait<F>(py: Python<'_>, future: F) -> PyResult<F::Output>
where
    F: Future + Send,
    F::Output: Send,
{
    let runtime = runtime().map_err(MosaicoError::new_err)?;
    let mut future = pin!(future);
    loop {
        let polled = py.detach(|| {
            block_on(runtime, async {
                tokio::time::timeout(SIGNAL_CHECK_INTERVAL, future.as_mut()).await
            })
        });
        if let Ok(output) = polled {
            return Ok(output);
        }
        py.check_signals()?;
    }
}

pub(crate) enum Closing<T> {
    Done(T),
    Interrupted(T, PyErr),
    Abandoned(PyErr),
}

pub(crate) fn wait_closing<F>(
    py: Python<'_>,
    future: F,
    interrupt: impl Fn(),
) -> PyResult<Closing<F::Output>>
where
    F: Future + Send,
    F::Output: Send,
{
    let runtime = runtime().map_err(MosaicoError::new_err)?;
    let mut future = pin!(future);
    let mut signal = None;
    loop {
        let polled = py.detach(|| {
            block_on(runtime, async {
                tokio::time::timeout(SIGNAL_CHECK_INTERVAL, future.as_mut()).await
            })
        });
        if let Ok(output) = polled {
            return Ok(match signal {
                Some(signal) => Closing::Interrupted(output, signal),
                None => Closing::Done(output),
            });
        }
        if let Err(error) = py.check_signals() {
            if signal.is_some() {
                return Ok(Closing::Abandoned(error));
            }
            interrupt();
            signal = Some(error);
        }
    }
}

pub(crate) fn run<F, T>(py: Python<'_>, future: F) -> PyResult<T>
where
    F: Future<Output = mosaico_core::Result<T>> + Send,
    T: Send,
{
    wait(py, future)?.map_err(to_py)
}

pub(crate) fn run_now<F, T>(py: Python<'_>, future: F) -> PyResult<T>
where
    F: Future<Output = mosaico_core::Result<T>> + Send,
    T: Send,
{
    let runtime = runtime().map_err(MosaicoError::new_err)?;
    let mut future = pin!(future);
    let polled = {
        let _context = runtime.enter();
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
    };
    match polled {
        Poll::Ready(result) => result.map_err(to_py),
        Poll::Pending => run(py, future),
    }
}

pub(crate) struct ProcessBound<T> {
    pid: u32,
    value: ManuallyDrop<T>,
}

impl<T> ProcessBound<T> {
    pub(crate) fn new(value: T) -> Self {
        Self {
            pid: std::process::id(),
            value: ManuallyDrop::new(value),
        }
    }

    pub(crate) fn get(&self) -> PyResult<&T> {
        if self.pid == std::process::id() {
            return Ok(&self.value);
        }
        Err(MosaicoError::new_err(fork_message(self.pid)))
    }

    pub(crate) fn get_mut(&mut self) -> Result<&mut T, String> {
        if self.pid == std::process::id() {
            return Ok(&mut self.value);
        }
        Err(fork_message(self.pid))
    }
}

pub(crate) fn fork_message(pid: u32) -> String {
    format!(
        "object created in process {pid} cannot be used after fork(): \
         connect a new client in each process"
    )
}

impl<T> Drop for ProcessBound<T> {
    fn drop(&mut self) {
        if self.pid == std::process::id() {
            unsafe { ManuallyDrop::drop(&mut self.value) }
        }
    }
}
