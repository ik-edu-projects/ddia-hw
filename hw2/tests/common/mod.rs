#![allow(dead_code)]
use resilience::{BoxFuture, Call, Sender, Strategy, StrategyError, WithTimeout};
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::time::Instant;

pub type Outcome = Result<&'static str, StrategyError>;
pub type Log = Arc<Mutex<Vec<(&'static str, Instant)>>>;
pub struct Step {
    pub delay: Duration,
    pub result: Option<Outcome>,
}
pub fn ok() -> Step {
    Step {
        delay: Duration::ZERO,
        result: Some(Ok("ok")),
    }
}
pub fn fail(error: StrategyError) -> Step {
    Step {
        delay: Duration::ZERO,
        result: Some(Err(error)),
    }
}
pub fn slow(delay: Duration) -> Step {
    Step { delay, ..ok() }
}
pub fn hang() -> Step {
    Step {
        delay: Duration::ZERO,
        result: None,
    }
}
pub fn server() -> StrategyError {
    StrategyError::server(503, true)
}

#[derive(Clone, Default)]
pub struct Tracker {
    pub log: Log,
    pub active: Arc<AtomicUsize>,
    pub canceled: Arc<AtomicUsize>,
}
impl Tracker {
    pub fn calls(&self) -> usize {
        self.log.lock().unwrap().len()
    }
    pub fn names(&self) -> Vec<&'static str> {
        self.log
            .lock()
            .unwrap()
            .iter()
            .map(|(name, _)| *name)
            .collect()
    }
    pub fn times(&self) -> Vec<Instant> {
        self.log
            .lock()
            .unwrap()
            .iter()
            .map(|(_, time)| *time)
            .collect()
    }
    pub fn active(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }
    pub fn canceled(&self) -> usize {
        self.canceled.load(Ordering::SeqCst)
    }
}

pub struct Mock {
    name: &'static str,
    steps: VecDeque<Step>,
    tracker: Tracker,
}
impl Mock {
    pub fn new(name: &'static str, steps: Vec<Step>, tracker: &Tracker) -> Self {
        Self {
            name,
            steps: steps.into(),
            tracker: tracker.clone(),
        }
    }
}
impl Sender for Mock {
    type Response = Outcome;
    fn send(&mut self) -> BoxFuture<'_, Outcome> {
        let step = self.steps.pop_front().expect("unexpected physical send");
        Box::pin(async move {
            self.tracker
                .log
                .lock()
                .unwrap()
                .push((self.name, Instant::now()));
            self.tracker.active.fetch_add(1, Ordering::SeqCst);
            let mut guard = Active {
                tracker: self.tracker.clone(),
                completed: false,
            };
            if !step.delay.is_zero() {
                tokio::time::sleep(step.delay).await;
            }
            let result = match step.result {
                Some(result) => result,
                None => std::future::pending().await,
            };
            guard.completed = true;
            result
        })
    }
}
struct Active {
    tracker: Tracker,
    completed: bool,
}
impl Drop for Active {
    fn drop(&mut self) {
        self.tracker.active.fetch_sub(1, Ordering::SeqCst);
        if !self.completed {
            self.tracker.canceled.fetch_add(1, Ordering::SeqCst);
        }
    }
}

pub fn call(
    name: &'static str,
    steps: Vec<Step>,
    tracker: &Tracker,
) -> impl Strategy<Output = &'static str> {
    Call::new(Mock::new(name, steps, tracker), |result| result)
}
pub fn default(
    name: &'static str,
    steps: Vec<Step>,
    tracker: &Tracker,
) -> impl Strategy<Output = &'static str> {
    WithTimeout::default_timeout(call(name, steps, tracker))
}
