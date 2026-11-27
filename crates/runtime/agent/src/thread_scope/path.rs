use nanocode_scope::switches::{OnDisk, THREAD_PATH};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ThreadPath {
    #[default]
    Resident,
    Legacy,
}

impl ThreadPath {
    pub const SWITCH: OnDisk = THREAD_PATH;
    pub const ALL: [Self; 2] = [Self::Resident, Self::Legacy];

    #[must_use]
    pub const fn setting(self) -> &'static str {
        match self {
            Self::Resident => "resident",
            Self::Legacy => "legacy",
        }
    }

    #[must_use]
    pub fn named(setting: &str) -> Option<Self> {
        let path = Self::ALL.into_iter().find(|path| path.setting() == setting);
        assert!(
            path.is_none_or(|path| Self::SWITCH.settings().contains(&path.setting())),
            "every thread path is a registered setting of its switch"
        );
        path
    }

    #[must_use]
    pub const fn flipped(self) -> Self {
        match self {
            Self::Resident => Self::Legacy,
            Self::Legacy => Self::Resident,
        }
    }
}

pub trait ThreadPaths: Send + Sync + 'static {
    fn path(&self, thread: &str) -> ThreadPath;
}

pub(crate) fn legacy_refused(thread: &str) -> String {
    format!(
        "thread {thread} runs on the {} path, which the old writer serves through OneLog::over; it holds no resident scope",
        ThreadPath::Legacy.setting()
    )
}
