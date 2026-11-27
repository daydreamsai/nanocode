use std::fmt::Write as _;

use nanocode_durability_core::StoreError;

use crate::codec::{ARCHIVE, BOUNDARY, LINKED, RECORDS, SECTION, SNAPSHOT, STAMPED};

const HEX_DIGIT_BITS: u32 = 4;
const HEX_RADIX: u32 = 1 << HEX_DIGIT_BITS;
const STAMP_DIGITS: usize = (u64::BITS / HEX_DIGIT_BITS) as usize;
const STAMP_FIELDS: usize = 3;
const STAMP_CHARS: usize = STAMP_DIGITS * STAMP_FIELDS;
const TAGGED: [char; 6] = [ARCHIVE, BOUNDARY, LINKED, RECORDS, SECTION, SNAPSHOT];
const STAMPS: [char; 1] = [STAMPED];
const TAGGED_FIRST_RELEASE: &str = "build-db12e71";

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Format {
    Tagged,
    Stamped,
}

impl Format {
    #[must_use]
    pub const fn version(self) -> u64 {
        match self {
            Self::Tagged => 1,
            Self::Stamped => 2,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Release {
    First(&'static str),
    Unreleased,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Stamp {
    writer: u64,
    epoch: u64,
    op: u64,
}

impl Stamp {
    #[must_use]
    pub const fn new(writer: u64, epoch: u64, op: u64) -> Self {
        Self { writer, epoch, op }
    }

    #[must_use]
    pub const fn writer(&self) -> u64 {
        self.writer
    }

    #[must_use]
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    #[must_use]
    pub const fn op(&self) -> u64 {
        self.op
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FormatError {
    Unreleased { asked: Format, writable: Format },
}

impl std::fmt::Display for FormatError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unreleased { asked, writable } => write!(
                formatter,
                "row format {} is newer than format {}, the newest the previous release decodes",
                asked.version(),
                writable.version()
            ),
        }
    }
}

impl std::error::Error for FormatError {}

pub(crate) struct Opened {
    pub(crate) stamp: Option<Stamp>,
    pub(crate) tag: char,
    pub(crate) body: usize,
}

struct Decoder {
    format: Format,
    release: Release,
    tags: &'static [char],
    open: fn(u64, char, &str) -> Result<Opened, StoreError>,
}

const REGISTRY: [Decoder; 2] = [
    Decoder {
        format: Format::Tagged,
        release: Release::First(TAGGED_FIRST_RELEASE),
        tags: &TAGGED,
        open: tagged,
    },
    Decoder {
        format: Format::Stamped,
        release: Release::Unreleased,
        tags: &STAMPS,
        open: stamped,
    },
];

pub struct Formats;

impl Formats {
    #[must_use]
    pub fn all() -> Vec<(Format, Release)> {
        REGISTRY
            .iter()
            .map(|decoder| (decoder.format, decoder.release))
            .collect()
    }

    #[must_use]
    pub fn writable() -> Format {
        let newest = REGISTRY[REGISTRY.len() - 1].format;
        if crate::sim::mutated(crate::sim::Mutation::WritesUnreleasedFormat) {
            return newest;
        }
        let writable = REGISTRY
            .iter()
            .rev()
            .find(|decoder| decoder.release != Release::Unreleased)
            .map(|decoder| decoder.format)
            .expect("format 1 has a release that decodes it");
        assert!(writable <= newest, "the writable format is registered");
        writable
    }
}

pub struct Encoder {
    format: Format,
}

impl Encoder {
    pub fn new(format: Format) -> Result<Self, FormatError> {
        let writable = Formats::writable();
        if format > writable {
            return Err(FormatError::Unreleased {
                asked: format,
                writable,
            });
        }
        Ok(Self { format })
    }

    #[must_use]
    pub const fn format(&self) -> Format {
        self.format
    }

    #[must_use]
    pub fn row(&self, stamp: Stamp, body: String) -> String {
        match self.format {
            Format::Tagged => body,
            Format::Stamped => stamp_row(stamp, &body),
        }
    }
}

pub(crate) fn stamp_row(stamp: Stamp, body: &str) -> String {
    assert!(
        body.chars().next().is_some_and(|tag| TAGGED.contains(&tag)),
        "a stamp heads a format 1 row"
    );
    let mut row = String::with_capacity(STAMPED.len_utf8() + STAMP_CHARS + body.len());
    row.push(STAMPED);
    for field in [stamp.writer, stamp.epoch, stamp.op] {
        write!(row, "{field:0width$x}", width = STAMP_DIGITS).expect("a string takes every write");
    }
    assert_eq!(row.len(), STAMPED.len_utf8() + STAMP_CHARS);
    row.push_str(body);
    row
}

pub(crate) fn starts(tag: char) -> bool {
    REGISTRY.iter().any(|decoder| decoder.tags.contains(&tag))
}

pub(crate) fn open(seq: u64, text: &str) -> Result<Opened, StoreError> {
    let unknown = || StoreError::Backend(format!("turn log row {seq} has no known tag"));
    let tag = text.chars().next().ok_or_else(unknown)?;
    let decoder = REGISTRY
        .iter()
        .find(|decoder| decoder.tags.contains(&tag))
        .ok_or_else(unknown)?;
    let opened = (decoder.open)(seq, tag, text)?;
    assert!(
        TAGGED.contains(&opened.tag),
        "every row opens to a format 1 body"
    );
    assert!(opened.body <= text.len(), "a row's body lies inside it");
    Ok(opened)
}

#[expect(
    clippy::unnecessary_wraps,
    reason = "every decoder in the registry has one signature"
)]
fn tagged(_seq: u64, tag: char, _text: &str) -> Result<Opened, StoreError> {
    Ok(Opened {
        stamp: None,
        tag,
        body: tag.len_utf8(),
    })
}

fn stamped(seq: u64, tag: char, text: &str) -> Result<Opened, StoreError> {
    let header = tag.len_utf8() + STAMP_CHARS;
    let fields = text
        .get(tag.len_utf8()..header)
        .ok_or_else(|| StoreError::Backend(format!("turn log row {seq} ends inside its stamp")))?;
    if !fields
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(StoreError::Backend(format!(
            "turn log row {seq} has a stamp that is not hexadecimal"
        )));
    }
    let field = |index: usize| -> Result<u64, StoreError> {
        let digits = &fields[index * STAMP_DIGITS..(index + 1) * STAMP_DIGITS];
        u64::from_str_radix(digits, HEX_RADIX)
            .map_err(|error| StoreError::Backend(format!("turn log row {seq} stamp: {error}")))
    };
    let stamp = Stamp::new(field(0)?, field(1)?, field(2)?);
    let inner = text[header..]
        .chars()
        .next()
        .filter(|inner| TAGGED.contains(inner))
        .ok_or_else(|| StoreError::Backend(format!("turn log row {seq} stamps no format 1 row")))?;
    Ok(Opened {
        stamp: Some(stamp),
        tag: inner,
        body: header + inner.len_utf8(),
    })
}
