use nanocode_compression::{
    inflate::{Inflate, InflateFlush, InflateStatus},
    xz::{self, LzmaOptions, MatchFinder, PRESET_DEFAULT},
};
use nanocode_durability_core::StoreError;
use nanocode_sys::base64::STANDARD_NO_PAD;

const WINDOW: usize = 1 << WINDOW_BITS;
const WINDOW_BITS: i32 = 15;
const BYTE_BITS: usize = 8;
const BASE85_CHARS: usize = 5;
const BASE85_RADIX: u64 = 85;
const BASE85_ALPHABET: &[u8; BASE85_RADIX as usize] =
    b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";
const NOT_BASE85: u8 = u8::MAX;
const BASE85_DIGITS: [u8; 1 << BYTE_BITS] = {
    let mut digits = [NOT_BASE85; 1 << BYTE_BITS];
    let mut index = 0;
    while index < BASE85_ALPHABET.len() {
        digits[BASE85_ALPHABET[index] as usize] = index as u8;
        index += 1;
    }
    digits
};
const BASE85_TOP_DIGIT: u64 = BASE85_RADIX - 1;
const SEGMENT: char = '~';
const DEFLATE_FINAL_BLOCK: u8 = 1;
const DEFLATE_BLOCK_TYPE_SHIFT: u32 = 1;
const DEFLATE_RESERVED_BLOCK_TYPE: u8 = 0b11;
const LZMA_CONTINUED: u8 = DEFLATE_RESERVED_BLOCK_TYPE << DEFLATE_BLOCK_TYPE_SHIFT;
const LZMA_RESTARTED: u8 = LZMA_CONTINUED | DEFLATE_FINAL_BLOCK;
const LZMA2_CHUNK_BYTES_MAX: usize = 1 << 16;
const LZMA2_END: u8 = 0x00;
const LZMA2_UNCOMPRESSED_RESET: u8 = 0x01;
const LZMA2_UNCOMPRESSED: u8 = 0x02;
const LZMA2_COMPRESSED: u8 = 0x80;
const LZMA2_PROPERTIES_RESET: u8 = 0xC0;
const LZMA2_UNCOMPRESSED_HEADER_BYTES: usize = 3;
const LZMA2_COMPRESSED_HEADER_BYTES: usize = 5;
const LZMA2_PROPERTIES_BYTES: usize = 1;
const LZMA2_UNCOMPRESSED_SIZE_AT: usize = 1;
const LZMA2_COMPRESSED_SIZE_AT: usize = 3;
const LZMA2_SIZE_BYTES: usize = 2;
const LZMA_SEAL: [u8; 2] = [LZMA2_END, LZMA2_END];
const ARCHIVE: char = 'a';
const RECORDS: char = 'r';
const SECTION: char = 'p';
const SNAPSHOT: char = 's';
const PART: char = '+';
const BOUNDARY: char = 'b';
const LINKED: char = 'P';
const VARINT_BITS: u32 = 7;
const VARINT_MORE: u8 = 1 << VARINT_BITS;

pub struct Decoded {
    pub boundary: bool,
    pub plain: String,
    pub seq: u64,
    pub snapshot: bool,
}

#[derive(Clone, Debug, Default)]
struct Window(Vec<u8>);

impl Window {
    fn extend(&mut self, plain: &[u8]) {
        let kept = &plain[plain.len().saturating_sub(WINDOW)..];
        let excess = (self.0.len() + kept.len()).saturating_sub(WINDOW);
        self.0.drain(..excess);
        self.0.extend_from_slice(kept);
    }
}

#[derive(Default)]
struct Lzma {
    decoder: Option<xz::Stream>,
}

#[derive(Default)]
struct History {
    records: Window,
    records_lzma: Lzma,
    chain: Option<(u64, Window)>,
    chain_lzma: Lzma,
}

fn lzma_options() -> LzmaOptions {
    let mut options =
        LzmaOptions::preset(PRESET_DEFAULT).expect("the default preset is a valid preset");
    options.dict_size = u32::try_from(WINDOW).expect("the window fits in 32 bits");
    options.match_finder = MatchFinder::HashChain4;
    options
}

fn lzma_failed(action: &str, error: xz::Error) -> StoreError {
    failed(action, &error.to_string())
}

fn lzma2_chunk_bytes(chunk: &[u8]) -> Result<usize, StoreError> {
    let control = chunk[0];
    let (header, size_at) = match control {
        LZMA2_UNCOMPRESSED_RESET | LZMA2_UNCOMPRESSED => {
            (LZMA2_UNCOMPRESSED_HEADER_BYTES, LZMA2_UNCOMPRESSED_SIZE_AT)
        }
        LZMA2_PROPERTIES_RESET.. => (
            LZMA2_COMPRESSED_HEADER_BYTES + LZMA2_PROPERTIES_BYTES,
            LZMA2_COMPRESSED_SIZE_AT,
        ),
        LZMA2_COMPRESSED.. => (LZMA2_COMPRESSED_HEADER_BYTES, LZMA2_COMPRESSED_SIZE_AT),
        _ => {
            return Err(failed(
                "decoding",
                "a compressed chunk has no known control",
            ));
        }
    };
    let size = chunk
        .get(size_at..size_at + LZMA2_SIZE_BYTES)
        .ok_or_else(|| failed("decoding", "the row ends inside a chunk header"))?;
    let size = usize::from(u16::from_be_bytes([size[0], size[1]])) + 1;
    Ok(header + size)
}

fn lzma_packs(bytes: &[u8]) -> Result<Vec<(u8, std::ops::Range<usize>)>, StoreError> {
    let mut packs: Vec<(u8, std::ops::Range<usize>)> = Vec::new();
    let mut at = 0;
    while at < bytes.len() {
        match bytes[at] {
            format @ (LZMA_CONTINUED | LZMA_RESTARTED) => {
                at += 1;
                packs.push((format, at..at));
            }
            LZMA2_END if bytes[at..] == LZMA_SEAL => return Ok(packs),
            LZMA2_END => return Err(failed("decoding", "the row ends past its seal")),
            _ => {
                let Some((_, range)) = packs.last_mut() else {
                    return Err(failed("decoding", "a chunk comes before its format"));
                };
                at += lzma2_chunk_bytes(&bytes[at..])?;
                if at > bytes.len() {
                    return Err(failed("decoding", "the row ends inside a chunk"));
                }
                range.end = at;
            }
        }
    }
    Ok(packs)
}

fn lzma_decode(
    lzma: &mut Lzma,
    window: &[u8],
    seq: u64,
    format: u8,
    compressed: &[u8],
) -> Result<Vec<u8>, StoreError> {
    if format == LZMA_RESTARTED {
        lzma.decoder = Some(
            xz::Stream::lzma2_primed_decoder(&lzma_options(), window)
                .map_err(|error| lzma_failed("decompression", error))?,
        );
    }
    let Some(decoder) = lzma.decoder.as_mut() else {
        return Err(StoreError::Backend(format!(
            "turn log row {seq} continues a compressed stream this read does not hold"
        )));
    };
    let mut plain = Vec::with_capacity(compressed.len() * 2);
    let mut fed = 0;
    loop {
        plain.reserve(LZMA2_CHUNK_BYTES_MAX);
        let (before_in, before_out) = (decoder.total_in(), decoder.total_out());
        let status = decoder
            .process_vec(&compressed[fed..], &mut plain, xz::Action::Run)
            .map_err(|error| lzma_failed("decompression", error))?;
        fed +=
            usize::try_from(decoder.total_in() - before_in).expect("consumed input fits in usize");
        if status != xz::Status::Ok {
            return Err(failed(
                "decompression",
                "the row ends its compressed stream",
            ));
        }
        if fed == compressed.len() && decoder.total_out() == before_out {
            return Ok(plain);
        }
    }
}

impl History {
    fn extend(&mut self, plain: &[u8]) {
        self.records.extend(plain);
        self.records_lzma = Lzma::default();
    }

    fn clear(&mut self) {
        self.records.0.clear();
        self.records_lzma = Lzma::default();
        self.chain = None;
        self.chain_lzma = Lzma::default();
    }

    fn decode_records(&mut self, seq: u64, text: &str) -> Result<Vec<u8>, StoreError> {
        let compressed = decoded(text)?;
        let plain = match compressed.first().copied() {
            Some(LZMA_CONTINUED | LZMA_RESTARTED) => {
                let mut lzma = std::mem::take(&mut self.records_lzma);
                let mut plain = Vec::new();
                for (format, range) in lzma_packs(&compressed)? {
                    let piece =
                        lzma_decode(&mut lzma, &self.records.0, seq, format, &compressed[range])?;
                    self.records.extend(&piece);
                    plain.extend_from_slice(&piece);
                }
                self.records_lzma = lzma;
                return Ok(plain);
            }
            _ => inflate(&compressed, &self.records.0)?,
        };
        self.extend(&plain);
        Ok(plain)
    }

    fn decode_linked(
        &mut self,
        seq: u64,
        keyframe: u64,
        compressed: &[u8],
    ) -> Result<Vec<u8>, StoreError> {
        let held = self.chained(keyframe);
        if keyframe < seq && held.is_none() {
            return Err(StoreError::Backend(format!(
                "turn log row {seq} links to rows from {keyframe} that this read does not hold"
            )));
        }
        let window = held.unwrap_or_default().to_vec();
        match compressed.first().copied() {
            Some(LZMA_CONTINUED | LZMA_RESTARTED) => {
                let mut lzma = std::mem::take(&mut self.chain_lzma);
                if keyframe == seq {
                    lzma = Lzma::default();
                }
                let packs = lzma_packs(compressed)?;
                let [(format, range)] = packs.as_slice() else {
                    return Err(failed("decoding", "a linked row holds one compressed run"));
                };
                let plain =
                    lzma_decode(&mut lzma, &window, seq, *format, &compressed[range.clone()])?;
                self.link(keyframe, &plain);
                self.chain_lzma = lzma;
                Ok(plain)
            }
            _ => {
                let plain = inflate(compressed, &window)?;
                self.link(keyframe, &plain);
                Ok(plain)
            }
        }
    }

    fn chained(&self, keyframe: u64) -> Option<&[u8]> {
        match &self.chain {
            Some((current, window)) if *current == keyframe => Some(&window.0),
            _ => None,
        }
    }

    fn link(&mut self, keyframe: u64, plain: &[u8]) {
        self.chain_lzma = Lzma::default();
        match &mut self.chain {
            Some((current, window)) if *current == keyframe => {
                window.extend(plain);
            }
            _ => {
                let mut window = Window::default();
                window.extend(plain);
                self.chain = Some((keyframe, window));
            }
        }
    }
}

#[cold]
fn failed(action: &str, detail: &str) -> StoreError {
    StoreError::Backend(format!("turn log row {action} failed: {detail}"))
}

fn base85(segment: &str, bytes: &mut Vec<u8>) -> Result<(), StoreError> {
    for group in segment.as_bytes().chunks(BASE85_CHARS) {
        if group.len() == 1 {
            return Err(failed("decoding", "a base85 group holds one character"));
        }
        let mut value: u64 = 0;
        for index in 0..BASE85_CHARS {
            let digit = match group.get(index) {
                Some(&char) => match BASE85_DIGITS[usize::from(char)] {
                    NOT_BASE85 => return Err(failed("decoding", "a character is not base85")),
                    digit => u64::from(digit),
                },
                None => BASE85_TOP_DIGIT,
            };
            value = value * BASE85_RADIX + digit;
        }
        let word = u32::try_from(value)
            .map_err(|_| failed("decoding", "a base85 group overflows four bytes"))?;
        bytes.extend_from_slice(&word.to_be_bytes()[..group.len() - 1]);
    }
    Ok(())
}

fn decoded(text: &str) -> Result<Vec<u8>, StoreError> {
    let mut segments = text.split(SEGMENT);
    let first = segments.next().unwrap_or_default();
    let mut bytes = STANDARD_NO_PAD
        .decode(first)
        .map_err(|error| failed("decoding", &error.to_string()))?;
    for segment in segments {
        base85(segment, &mut bytes)?;
    }
    Ok(bytes)
}

fn unpack(text: &str, dictionary: &[u8]) -> Result<Vec<u8>, StoreError> {
    inflate(&decoded(text)?, dictionary)
}

fn linked(text: &str) -> Result<(u64, Vec<u8>), StoreError> {
    let mut compressed = decoded(text)?;
    let mut back: u64 = 0;
    let mut shift = 0;
    for (index, &byte) in compressed.iter().enumerate() {
        let low = u64::from(byte & (VARINT_MORE - 1));
        if shift >= u64::BITS || (low << shift) >> shift != low {
            return Err(failed("decoding", "the link back overflows"));
        }
        back |= low << shift;
        shift += VARINT_BITS;
        if byte & VARINT_MORE == 0 {
            compressed.drain(..=index);
            return Ok((back, compressed));
        }
    }
    Err(failed("decoding", "the row ends inside its link back"))
}

fn inflate(compressed: &[u8], dictionary: &[u8]) -> Result<Vec<u8>, StoreError> {
    let mut inflate =
        Inflate::new(-WINDOW_BITS).map_err(|error| failed("decompression", error.as_str()))?;
    if !dictionary.is_empty() {
        inflate
            .set_dictionary(dictionary)
            .map_err(|error| failed("decompression", error.as_str()))?;
    }
    let start = inflate.total_in();
    let mut plain = Vec::new();
    let mut chunk = vec![0; WINDOW];
    loop {
        let consumed =
            usize::try_from(inflate.total_in() - start).expect("consumed input fits in usize");
        let before = inflate.total_out();
        let status = inflate
            .inflate(&compressed[consumed..], &mut chunk, InflateFlush::NoFlush)
            .map_err(|error| failed("decompression", error.as_str()))?;
        let produced = usize::try_from(inflate.total_out() - before)
            .expect("one call produces at most its chunk");
        plain.extend_from_slice(&chunk[..produced]);
        match status {
            InflateStatus::StreamEnd => return Ok(plain),
            InflateStatus::Ok if produced > 0 || inflate.total_in() - start > consumed as u64 => {}
            InflateStatus::Ok | InflateStatus::BufError => {
                return Err(failed("decompression", "the row ends before its stream"));
            }
        }
    }
}

pub fn rows(stored: &[(u64, String)]) -> Result<Vec<Decoded>, StoreError> {
    let mut decoded = Vec::new();
    let mut history = History::default();
    let mut pending: Option<(u64, String)> = None;
    for (seq, body) in stored {
        let mut chars = body.chars();
        match chars.next() {
            Some(PART) => match &mut pending {
                Some((_, text)) => text.push_str(chars.as_str()),
                None => {
                    return Err(StoreError::Backend(format!(
                        "turn log row {seq} continues no row"
                    )));
                }
            },
            Some(ARCHIVE | BOUNDARY | LINKED | RECORDS | SECTION | SNAPSHOT) => {
                if let Some((first, text)) = pending.take() {
                    decoded.push(decode_row(first, &text, &mut history)?);
                }
                pending = Some((*seq, body.clone()));
            }
            _ => {
                return Err(StoreError::Backend(format!(
                    "turn log row {seq} has no known tag"
                )));
            }
        }
    }
    if let Some((first, text)) = pending {
        decoded.push(decode_row(first, &text, &mut history)?);
    }
    Ok(decoded)
}

fn decode_row(seq: u64, text: &str, history: &mut History) -> Result<Decoded, StoreError> {
    let tag = text
        .chars()
        .next()
        .expect("a logical row starts with its tag");
    let snapshot = tag == SNAPSHOT;
    let section = tag == SECTION || tag == LINKED;
    let boundary = snapshot || tag == BOUNDARY || tag == ARCHIVE;
    if boundary {
        history.clear();
    }
    let plain = if tag == LINKED {
        let (back, compressed) = linked(&text[tag.len_utf8()..])?;
        let keyframe = seq.checked_sub(back).ok_or_else(|| {
            StoreError::Backend(format!(
                "turn log row {seq} links back past the log's start"
            ))
        })?;
        history.decode_linked(seq, keyframe, &compressed)?
    } else if section {
        unpack(&text[tag.len_utf8()..], &[])?
    } else {
        history.decode_records(seq, &text[tag.len_utf8()..])?
    };
    let plain = String::from_utf8(plain).map_err(|error| {
        StoreError::Backend(format!("turn log row {seq} is not UTF-8: {error}"))
    })?;
    Ok(Decoded {
        boundary,
        plain,
        seq,
        snapshot,
    })
}
