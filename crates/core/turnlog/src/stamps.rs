use nanocode_durability_core::StoreError;

use crate::{
    codec::{self, PART, STAMPED},
    formats::{self, Encoder, Format, Formats, Stamp},
    sim::{self, Mutation},
};

const WRITER_BYTES: usize = (u64::BITS / u8::BITS) as usize;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Landed {
    Absent,
    Sent,
    Own { op: u64 },
    Other,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Stamps {
    writer: Option<u64>,
    epoch: u64,
    op: u64,
}

fn drawn() -> u64 {
    let mut bytes = [0; WRITER_BYTES];
    nanocode_platform::fill_random(&mut bytes)
        .expect("the host provides randomness for a writer id");
    u64::from_le_bytes(bytes)
}

impl Stamps {
    #[must_use]
    pub const fn new(epoch: u64) -> Self {
        Self {
            writer: None,
            epoch,
            op: 0,
        }
    }

    #[must_use]
    pub const fn writer(&self) -> Option<u64> {
        self.writer
    }

    #[must_use]
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn op(&mut self) -> u64 {
        self.op = self.op.checked_add(1).expect("a writer's ops fit in u64");
        assert!(self.op > 0, "a writer's first op is 1");
        self.op
    }

    #[must_use]
    pub fn format() -> Format {
        sim::forced().unwrap_or_else(Formats::writable)
    }

    pub fn row(&mut self, op: u64, body: String) -> String {
        assert!(op > 0, "a row carries an op its writer issued");
        assert!(op <= self.op, "a row carries an op its writer issued");
        let format = Self::format();
        if format == Format::Tagged {
            return body;
        }
        let writer = *self.writer.get_or_insert_with(drawn);
        let stamp = Stamp::new(writer, self.epoch, op);
        match sim::forced() {
            Some(_) => formats::stamp_row(stamp, &body),
            None => Encoder::new(format)
                .expect("the writable format has a released decoder")
                .row(stamp, body),
        }
    }

    pub fn rows(&mut self, op: u64, rows: Vec<(u64, String)>) -> Vec<(u64, String)> {
        rows.into_iter()
            .map(|(seq, body)| {
                if body.starts_with(PART) {
                    (seq, body)
                } else {
                    (seq, self.row(op, body))
                }
            })
            .collect()
    }

    pub fn landed(
        &self,
        found: &[(u64, String)],
        sent: &[(u64, String)],
        unknown: bool,
    ) -> Result<Landed, StoreError> {
        if found.is_empty() {
            return Ok(Landed::Absent);
        }
        if sim::mutated(Mutation::OwnRowFences) {
            return Ok(if codec::same_rows(found, sent) {
                Landed::Sent
            } else {
                Landed::Other
            });
        }
        let mut own = None;
        for (seq, body) in found {
            if !body.starts_with(STAMPED) {
                continue;
            }
            let Some(stamp) = formats::open(*seq, body)?.stamp else {
                return Err(StoreError::Backend(format!(
                    "turn log row {seq} opens with a stamp tag and no stamp"
                )));
            };
            if Some(stamp.writer()) != self.writer || stamp.epoch() != self.epoch {
                return Ok(Landed::Other);
            }
            own.get_or_insert(stamp.op());
        }
        Ok(match own {
            Some(op) => Landed::Own { op },
            None => unstamped(found, sent, unknown),
        })
    }
}

fn unstamped(found: &[(u64, String)], sent: &[(u64, String)], unknown: bool) -> Landed {
    if codec::same_rows(found, sent) {
        Landed::Sent
    } else if unknown {
        Landed::Unknown
    } else {
        Landed::Other
    }
}

pub fn tagged_row(row: &str) -> Result<&str, StoreError> {
    if !row.starts_with(STAMPED) {
        return Ok(row);
    }
    let opened = formats::open(0, row)?;
    let start = opened.body - opened.tag.len_utf8();
    assert!(
        row[start..].starts_with(opened.tag),
        "a stamped row carries its format 1 row after the stamp"
    );
    Ok(&row[start..])
}

pub(crate) fn tag(row: &str) -> Option<char> {
    if row.starts_with(PART) {
        return None;
    }
    formats::open(0, row).ok().map(|opened| opened.tag)
}
