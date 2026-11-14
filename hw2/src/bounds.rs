use std::{
    ops::{RangeFull, RangeToInclusive},
    time::Duration,
};

/// Upper bound on physical sends, including sends inside nested strategies.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttemptBudget {
    Limited(u64),
    Unlimited,
}

impl AttemptBudget {
    pub(crate) fn allows(self, used: u64) -> bool {
        match self {
            Self::Limited(n) => used < n,
            Self::Unlimited => true,
        }
    }

    pub(crate) fn add(self, other: Self) -> Self {
        match (self, other) {
            (Self::Limited(a), Self::Limited(b)) => {
                a.checked_add(b).map_or(Self::Unlimited, Self::Limited)
            }
            _ => Self::Unlimited,
        }
    }

    pub(crate) fn multiply(self, other: Self) -> Self {
        match (self, other) {
            (Self::Limited(0), _) | (_, Self::Limited(0)) => Self::Limited(0),
            (Self::Limited(a), Self::Limited(b)) => {
                a.checked_mul(b).map_or(Self::Unlimited, Self::Limited)
            }
            _ => Self::Unlimited,
        }
    }

    pub(crate) fn min(self, other: Self) -> Self {
        match (self, other) {
            (Self::Limited(a), Self::Limited(b)) => Self::Limited(a.min(b)),
            (Self::Unlimited, x) | (x, Self::Unlimited) => x,
        }
    }
}

impl From<RangeToInclusive<u64>> for AttemptBudget {
    fn from(value: RangeToInclusive<u64>) -> Self {
        Self::Limited(value.end)
    }
}
impl From<RangeFull> for AttemptBudget {
    fn from(_: RangeFull) -> Self {
        Self::Unlimited
    }
}

/// Upper bound on elapsed time. Overflow always widens a bound to `Unlimited`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimeBudget {
    Limited(Duration),
    Unlimited,
}

impl TimeBudget {
    pub(crate) fn add(self, other: Self) -> Self {
        match (self, other) {
            (Self::Limited(a), Self::Limited(b)) => {
                a.checked_add(b).map_or(Self::Unlimited, Self::Limited)
            }
            _ => Self::Unlimited,
        }
    }

    pub(crate) fn multiply(self, count: AttemptBudget) -> Self {
        match (self, count) {
            (_, AttemptBudget::Limited(0)) | (Self::Limited(Duration::ZERO), _) => {
                Self::Limited(Duration::ZERO)
            }
            (Self::Limited(time), AttemptBudget::Limited(n)) => {
                let nanos = time.as_nanos().checked_mul(u128::from(n));
                nanos
                    .and_then(|v| {
                        let secs = u64::try_from(v / 1_000_000_000).ok()?;
                        Some(Self::Limited(Duration::new(
                            secs,
                            (v % 1_000_000_000) as u32,
                        )))
                    })
                    .unwrap_or(Self::Unlimited)
            }
            _ => Self::Unlimited,
        }
    }

    pub(crate) fn min(self, other: Self) -> Self {
        match (self, other) {
            (Self::Limited(a), Self::Limited(b)) => Self::Limited(a.min(b)),
            (Self::Unlimited, x) | (x, Self::Unlimited) => x,
        }
    }
}

impl From<RangeToInclusive<Duration>> for TimeBudget {
    fn from(value: RangeToInclusive<Duration>) -> Self {
        Self::Limited(value.end)
    }
}
impl From<RangeFull> for TimeBudget {
    fn from(_: RangeFull) -> Self {
        Self::Unlimited
    }
}
