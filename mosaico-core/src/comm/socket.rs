use std::io::{self, IoSlice};
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use http::Uri;
use hyper_util::rt::TokioIo;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

const FLUSH_AT: usize = 256 << 10;

pub(crate) async fn connect(uri: Uri) -> io::Result<TokioIo<BufferedWrites<TcpStream>>> {
    let host = uri
        .host()
        .unwrap_or_default()
        .trim_start_matches('[')
        .trim_end_matches(']');
    let port = match uri.port_u16() {
        Some(port) => port,
        None if uri.scheme_str() == Some("https") => 443,
        None => 80,
    };
    let stream = TcpStream::connect((host, port)).await?;
    stream.set_nodelay(true)?;
    Ok(TokioIo::new(BufferedWrites::new(stream)))
}

pub(crate) struct BufferedWrites<T> {
    inner: T,
    buf: Vec<u8>,
    written: usize,
    deferred: bool,
}

impl<T> BufferedWrites<T> {
    fn new(inner: T) -> Self {
        Self {
            inner,
            buf: Vec::with_capacity(2 * FLUSH_AT),
            written: 0,
            deferred: false,
        }
    }
}

impl<T: AsyncWrite + Unpin> BufferedWrites<T> {
    fn poll_drain(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        while self.written < self.buf.len() {
            let n = ready!(Pin::new(&mut self.inner).poll_write(cx, &self.buf[self.written..]))?;
            if n == 0 {
                return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            }
            self.written += n;
        }
        self.buf.clear();
        self.written = 0;
        Poll::Ready(Ok(()))
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> AsyncRead for BufferedWrites<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = &mut *self;
        if !this.buf.is_empty()
            && let Poll::Ready(Err(error)) = this.poll_drain(cx)
        {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut this.inner).poll_read(cx, buf)
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for BufferedWrites<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_vectored(cx, &[IoSlice::new(data)])
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        slices: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = &mut *self;
        if this.buf.len() >= FLUSH_AT {
            ready!(this.poll_drain(cx))?;
        }
        let total: usize = slices.iter().map(|slice| slice.len()).sum();
        if this.buf.is_empty() && total >= FLUSH_AT {
            return Pin::new(&mut this.inner).poll_write_vectored(cx, slices);
        }
        for slice in slices {
            this.buf.extend_from_slice(slice);
        }
        this.deferred = false;
        Poll::Ready(Ok(total))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = &mut *self;
        if !this.deferred && !this.buf.is_empty() && this.buf.len() < FLUSH_AT {
            this.deferred = true;
            cx.waker().wake_by_ref();
            return Poll::Ready(Ok(()));
        }
        ready!(this.poll_drain(cx))?;
        this.deferred = false;
        Pin::new(&mut this.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = &mut *self;
        ready!(this.poll_drain(cx))?;
        Pin::new(&mut this.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::io::AsyncWriteExt;

    use super::*;

    #[derive(Default)]
    struct Recorder {
        data: Vec<u8>,
        writes: Arc<AtomicUsize>,
    }

    impl AsyncWrite for Recorder {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            data: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.writes.fetch_add(1, Ordering::Relaxed);
            self.data.extend_from_slice(data);
            Poll::Ready(Ok(data.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncRead for Recorder {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Pending
        }
    }

    fn pattern(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    #[tokio::test]
    async fn small_writes_reach_the_socket_together_and_in_order() {
        let writes = Arc::new(AtomicUsize::new(0));
        let mut io = BufferedWrites::new(Recorder {
            writes: writes.clone(),
            ..Recorder::default()
        });
        let frame = pattern(16 << 10);
        let mut expected = Vec::new();
        for _ in 0..64 {
            io.write_all(&frame).await.unwrap();
            io.flush().await.unwrap();
            expected.extend_from_slice(&frame);
        }
        io.flush().await.unwrap();
        assert_eq!(io.inner.data, expected);
        assert!(writes.load(Ordering::Relaxed) <= 8);
    }

    #[tokio::test]
    async fn a_second_flush_sends_what_is_buffered() {
        let mut io = BufferedWrites::new(Recorder::default());
        io.write_all(b"ping").await.unwrap();
        io.flush().await.unwrap();
        assert!(io.inner.data.is_empty());
        io.flush().await.unwrap();
        assert_eq!(io.inner.data, b"ping");
    }

    #[test]
    fn a_read_sends_what_a_deferred_flush_kept_first() {
        let mut io = BufferedWrites::new(Recorder::default());
        let mut cx = Context::from_waker(std::task::Waker::noop());
        let written = Pin::new(&mut io).poll_write(&mut cx, b"client hello");
        assert!(matches!(written, Poll::Ready(Ok(12))));
        assert!(Pin::new(&mut io).poll_flush(&mut cx).is_ready());
        assert!(io.inner.data.is_empty());
        let mut space = [0u8; 16];
        let mut buf = ReadBuf::new(&mut space);
        assert!(Pin::new(&mut io).poll_read(&mut cx, &mut buf).is_pending());
        assert_eq!(io.inner.data, b"client hello");
    }

    #[tokio::test]
    async fn large_writes_skip_the_buffer_and_shutdown_drains() {
        let mut io = BufferedWrites::new(Recorder::default());
        let large = pattern(FLUSH_AT + 1);
        io.write_all(b"head").await.unwrap();
        io.write_all(&large).await.unwrap();
        io.write_all(b"tail").await.unwrap();
        io.shutdown().await.unwrap();
        let mut expected = b"head".to_vec();
        expected.extend_from_slice(&large);
        expected.extend_from_slice(b"tail");
        assert_eq!(io.inner.data, expected);
    }
}
