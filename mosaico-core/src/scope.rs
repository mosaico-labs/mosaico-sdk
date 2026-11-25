use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tokio_util::sync::CancellationToken;

pub(crate) struct Token {
    closed: AtomicBool,
    cancel: CancellationToken,
    parent: Option<Arc<Token>>,
}

impl Token {
    fn new(parent: Option<Arc<Token>>) -> Arc<Self> {
        let cancel = match &parent {
            Some(parent) => parent.cancel.child_token(),
            None => CancellationToken::new(),
        };
        Arc::new(Self {
            closed: AtomicBool::new(false),
            cancel,
            parent,
        })
    }

    pub(crate) fn is_closed(&self) -> bool {
        let mut token = Some(self);
        while let Some(current) = token {
            if current.closed.load(Ordering::Acquire) {
                return true;
            }
            token = current.parent.as_deref();
        }
        false
    }

    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.cancel.cancel();
    }

    pub(crate) async fn closed(&self) {
        self.cancel.cancelled().await;
    }

    pub(crate) async fn run_until_closed<F: Future>(&self, future: F) -> Option<F::Output> {
        self.cancel.run_until_cancelled(future).await
    }
}

#[derive(Clone)]
pub(crate) struct Scope(Arc<ScopeState>);

struct ScopeState {
    parent: Option<Scope>,
    current: Mutex<Arc<Token>>,
}

impl Scope {
    pub(crate) fn root() -> Self {
        Self::new(None, Token::new(None))
    }

    pub(crate) fn child(&self) -> Self {
        Self::new(Some(self.clone()), self.token())
    }

    fn new(parent: Option<Scope>, current: Arc<Token>) -> Self {
        Self(Arc::new(ScopeState {
            parent,
            current: Mutex::new(current),
        }))
    }

    pub(crate) fn token(&self) -> Arc<Token> {
        let mut current = self.current();
        if current.is_closed()
            && let Some(parent) = &self.0.parent
        {
            *current = parent.token();
        }
        Token::new(Some(current.clone()))
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.current().is_closed()
    }

    pub(crate) fn close(&self) {
        self.current().close();
    }

    fn current(&self) -> MutexGuard<'_, Arc<Token>> {
        self.0
            .current
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closed_scope_reopens() {
        let root = Scope::root();
        let handler = root.child();
        let first = handler.token();
        handler.close();
        assert!(first.is_closed());
        assert!(!handler.token().is_closed());
        root.close();
        assert!(handler.token().is_closed());
    }

    #[tokio::test]
    async fn close_cancels_wait() {
        let scope = Scope::root().child();
        let token = scope.token();
        scope.close();
        let waited = token.run_until_closed(std::future::pending::<()>()).await;
        assert!(waited.is_none());
    }
}
