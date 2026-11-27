use nanocode_compression::{
    deflate::{Deflate, DeflateConfig, DeflateFlush, DeflateStatus, Strategy},
    inflate::{Inflate, InflateFlush, InflateStatus},
    xz::{self, LzmaOptions, LzmaState, MatchFinder, PRESET_DEFAULT},
};
use nanocode_durability_core::StoreError;
use nanocode_sys::base64::{STANDARD, STANDARD_NO_PAD};

pub(crate) const WINDOW: usize = 1 << WINDOW_BITS;
const WINDOW_BITS: i32 = 15;
pub(crate) const DEFAULT_LEVEL: i32 = -1;
pub(crate) const FAST_LEVEL: i32 = 1;
const DEFAULT_MEM_LEVEL: i32 = 8;
const FINAL_EMPTY_BLOCK: [u8; 2] = [0x03, 0x00];
const BASE64_GROUP: usize = 3;
const BASE64_CHARS: usize = 4;
pub(crate) const SEAL: usize = (FINAL_EMPTY_BLOCK.len() * BASE64_CHARS).div_ceil(BASE64_GROUP);
const BASE85_GROUP: usize = 4;
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
const _: () = assert!(FINAL_EMPTY_BLOCK.len() < BASE85_GROUP);
const _: () = assert!(SEAL == FINAL_EMPTY_BLOCK.len() + 1);
pub(crate) const SEGMENT: char = '~';
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
const _: () = assert!(LZMA_SEAL.len() == FINAL_EMPTY_BLOCK.len());

pub(crate) const ARCHIVE: char = 'a';
pub(crate) const RECORDS: char = 'r';
pub(crate) const SECTION: char = 'p';
pub(crate) const SNAPSHOT: char = 's';
pub(crate) const PART: char = '+';
pub(crate) const BOUNDARY: char = 'b';
pub(crate) const LINKED: char = 'P';
pub(crate) const STAMPED: char = 'w';
const EMPTY_STORED_BLOCK: [u8; 5] = [0x00, 0x00, 0x00, 0xFF, 0xFF];
const STANDALONE_BLOCKS_MAX: usize = BASE85_GROUP * BASE64_GROUP;
const VARINT_BITS: u32 = 7;
const VARINT_MORE: u8 = 1 << VARINT_BITS;

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
    state: Option<LzmaState>,
    decoder: Option<xz::Stream>,
}

#[derive(Default)]
pub(crate) struct History {
    records: Window,
    records_lzma: Lzma,
    chain: Option<(u64, Window)>,
    chain_bytes: usize,
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

fn lzma_encode(
    window: &[u8],
    state: Option<&LzmaState>,
    plain: &[u8],
) -> Result<(Vec<u8>, LzmaState), StoreError> {
    let position = state.map_or(window.len() as u64, LzmaState::position);
    let mut encoder = xz::Stream::lzma2_resumed(&lzma_options(), window, position, state)
        .map_err(|error| lzma_failed("compression", error))?;
    let mut compressed = Vec::with_capacity(plain.len() / 2 + LZMA2_CHUNK_BYTES_MAX);
    compressed.push(if state.is_some() {
        LZMA_CONTINUED
    } else {
        LZMA_RESTARTED
    });
    let mut consumed = 0;
    loop {
        compressed.reserve(LZMA2_CHUNK_BYTES_MAX);
        let before = encoder.total_in();
        let status = encoder
            .process_vec(&plain[consumed..], &mut compressed, xz::Action::SyncFlush)
            .map_err(|error| lzma_failed("compression", error))?;
        consumed +=
            usize::try_from(encoder.total_in() - before).expect("consumed input fits in usize");
        if status == xz::Status::StreamEnd {
            break;
        }
    }
    assert_eq!(
        consumed,
        plain.len(),
        "a flushed encoder took the whole row"
    );
    let state = encoder.lzma_state().expect("a flushed encoder has a state");
    assert_eq!(state.position(), position + plain.len() as u64);
    Ok((compressed, state))
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
    pub(crate) fn extend(&mut self, plain: &[u8]) {
        self.records.extend(plain);
        self.records_lzma = Lzma::default();
    }

    pub(crate) fn clear(&mut self) {
        self.records.0.clear();
        self.records_lzma = Lzma::default();
        self.chain = None;
        self.chain_bytes = 0;
        self.chain_lzma = Lzma::default();
    }

    pub(crate) fn resume_records(&mut self, state: LzmaState) {
        self.records_lzma.state = Some(state);
    }

    pub(crate) fn resume_chain(&mut self, keyframe: u64, state: LzmaState) {
        assert_eq!(
            self.keyframe(),
            Some(keyframe),
            "a chain resumes after its link"
        );
        self.chain_lzma.state = Some(state);
    }

    pub(crate) fn decode_records(&mut self, seq: u64, text: &str) -> Result<Vec<u8>, StoreError> {
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
                lzma.state = None;
                self.records_lzma = lzma;
                return Ok(plain);
            }
            _ => inflate(&compressed, &self.records.0)?,
        };
        self.extend(&plain);
        Ok(plain)
    }

    pub(crate) fn decode_linked(
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
                lzma.state = None;
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

    pub(crate) fn keyframe(&self) -> Option<u64> {
        self.chain.as_ref().map(|(keyframe, _)| *keyframe)
    }

    pub(crate) fn chained(&self, keyframe: u64) -> Option<&[u8]> {
        match &self.chain {
            Some((current, window)) if *current == keyframe => Some(&window.0),
            _ => None,
        }
    }

    pub(crate) fn link(&mut self, keyframe: u64, plain: &[u8]) {
        self.chain_lzma = Lzma::default();
        match &mut self.chain {
            Some((current, window)) if *current == keyframe => {
                window.extend(plain);
                self.chain_bytes = self.chain_bytes.saturating_add(plain.len());
            }
            _ => {
                let mut window = Window::default();
                window.extend(plain);
                self.chain = Some((keyframe, window));
                self.chain_bytes = plain.len();
            }
        }
    }

    pub(crate) const fn chain_bytes(&self) -> usize {
        self.chain_bytes
    }
}

#[cold]
fn failed(action: &str, detail: &str) -> StoreError {
    StoreError::Backend(format!("turn log row {action} failed: {detail}"))
}

fn primed(deflate: &mut Deflate, dictionary: &[u8]) -> Result<(), StoreError> {
    if !dictionary.is_empty() {
        deflate
            .set_dictionary(dictionary)
            .map_err(|error| failed("compression", error.as_str()))?;
    }
    Ok(())
}

fn reused(mut deflate: Deflate, level: i32, dictionary: &[u8]) -> Result<Deflate, StoreError> {
    deflate
        .reset(level)
        .map_err(|error| failed("compression", error.as_str()))?;
    primed(&mut deflate, dictionary)?;
    Ok(deflate)
}

fn compressor(level: i32, dictionary: &[u8]) -> Result<Deflate, StoreError> {
    let mut deflate = Deflate::new(DeflateConfig {
        level,
        mem_level: DEFAULT_MEM_LEVEL,
        strategy: Strategy::Default,
        window_bits: -WINDOW_BITS,
    })
    .map_err(|error| failed("compression", error.as_str()))?;
    primed(&mut deflate, dictionary)?;
    Ok(deflate)
}

fn encode(tag: char, compressed: &[u8]) -> String {
    let mut body = String::with_capacity(
        tag.len_utf8()
            + SEGMENT.len_utf8()
            + compressed.len().div_ceil(BASE85_GROUP) * BASE85_CHARS,
    );
    body.push(tag);
    body.push(SEGMENT);
    let mut chars = [0; BASE85_CHARS];
    for group in compressed.chunks(BASE85_GROUP) {
        let mut word = [0; BASE85_GROUP];
        word[..group.len()].copy_from_slice(group);
        let mut value = u64::from(u32::from_be_bytes(word));
        for char in chars.iter_mut().rev() {
            *char = BASE85_ALPHABET[usize::try_from(value % BASE85_RADIX).expect("a digit")];
            value /= BASE85_RADIX;
        }
        assert_eq!(value, 0, "four bytes fit five base85 digits");
        for &char in &chars[..=group.len()] {
            body.push(char::from(char));
        }
    }
    body
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

pub(crate) fn pack(
    spare: &mut Spare,
    tag: char,
    plain: &[u8],
    dictionary: &[u8],
) -> Result<String, StoreError> {
    let mut packer = Packer::new(spare, DEFAULT_LEVEL, dictionary)?;
    packer.write(plain)?;
    Ok(packer.finish(tag, spare)?.packed)
}

#[derive(Default)]
pub(crate) struct Spare {
    chunk: Vec<u8>,
    deflate: Option<Deflate>,
}

pub(crate) struct Packer {
    chunk: Vec<u8>,
    compressed: Vec<u8>,
    deflate: Deflate,
    plain: usize,
}

pub(crate) struct Packed {
    pub(crate) packed: String,
    pub(crate) plain: usize,
}

impl Packer {
    pub(crate) fn new(
        spare: &mut Spare,
        level: i32,
        dictionary: &[u8],
    ) -> Result<Self, StoreError> {
        let deflate = match spare.deflate.take() {
            Some(deflate) => reused(deflate, level, dictionary)?,
            None => compressor(level, dictionary)?,
        };
        let mut chunk = std::mem::take(&mut spare.chunk);
        if chunk.is_empty() {
            chunk = vec![0; WINDOW];
        }
        Ok(Self {
            chunk,
            compressed: Vec::new(),
            deflate,
            plain: 0,
        })
    }

    pub(crate) const fn plain(&self) -> usize {
        self.plain
    }

    pub(crate) fn write(&mut self, plain: &[u8]) -> Result<(), StoreError> {
        let start = self.deflate.total_in();
        loop {
            let consumed = usize::try_from(self.deflate.total_in() - start)
                .expect("consumed input fits in usize");
            if consumed == plain.len() {
                break;
            }
            let before = self.deflate.total_out();
            self.deflate
                .deflate(&plain[consumed..], &mut self.chunk, DeflateFlush::NoFlush)
                .map_err(|error| failed("compression", error.as_str()))?;
            let produced = usize::try_from(self.deflate.total_out() - before)
                .expect("one call produces at most its chunk");
            self.compressed.extend_from_slice(&self.chunk[..produced]);
            let taken = usize::try_from(self.deflate.total_in() - start)
                .expect("consumed input fits in usize");
            if produced == 0 && taken == consumed {
                return Err(failed("compression", "no progress"));
            }
        }
        self.plain += plain.len();
        Ok(())
    }

    pub(crate) fn finish(self, tag: char, spare: &mut Spare) -> Result<Packed, StoreError> {
        let plain = self.plain;
        let compressed = self.finished(spare)?;
        Ok(Packed {
            packed: encode(tag, &compressed),
            plain,
        })
    }

    fn finished(mut self, spare: &mut Spare) -> Result<Vec<u8>, StoreError> {
        loop {
            let before = self.deflate.total_out();
            let status = self
                .deflate
                .deflate(&[], &mut self.chunk, DeflateFlush::Finish)
                .map_err(|error| failed("compression", error.as_str()))?;
            let produced = usize::try_from(self.deflate.total_out() - before)
                .expect("one call produces at most its chunk");
            self.compressed.extend_from_slice(&self.chunk[..produced]);
            match status {
                DeflateStatus::StreamEnd => break,
                DeflateStatus::Ok => {}
                DeflateStatus::BufError if produced > 0 => {}
                DeflateStatus::BufError => return Err(failed("compression", "no progress")),
            }
        }
        spare.chunk = self.chunk;
        spare.deflate = Some(self.deflate);
        Ok(self.compressed)
    }
}

pub(crate) struct Stream {
    window: Vec<u8>,
    state: Option<LzmaState>,
    packed: Option<LzmaState>,
}

impl Stream {
    pub(crate) fn new(history: &History) -> Self {
        Self {
            window: history.records.0.clone(),
            state: history.records_lzma.state.clone(),
            packed: None,
        }
    }

    pub(crate) fn pack(&mut self, tag: char, plain: &[u8]) -> Result<String, StoreError> {
        assert!(self.packed.is_none(), "a stream packs one row");
        let (compressed, state) = lzma_encode(&self.window, self.state.as_ref(), plain)?;
        self.packed = Some(state);
        let mut body = encode(tag, &compressed);
        let seal = encode(SEGMENT, &LZMA_SEAL);
        let seal = &seal[SEGMENT.len_utf8()..];
        assert_eq!(seal.len(), SEGMENT.len_utf8() + SEAL);
        body.push_str(seal);
        Ok(body)
    }

    pub(crate) fn state(self) -> LzmaState {
        self.packed.expect("a stream's state follows its row")
    }
}

pub(crate) fn link(
    history: &History,
    keyframe: u64,
    back: u64,
    plain: &[u8],
) -> Result<(String, LzmaState), StoreError> {
    let (window, state) = match &history.chain {
        Some((current, window)) if *current == keyframe => {
            (window.0.as_slice(), history.chain_lzma.state.as_ref())
        }
        _ => (&[][..], None),
    };
    let mut compressed = Vec::new();
    let mut rest = back;
    loop {
        let low = u8::try_from(rest & u64::from(VARINT_MORE - 1)).expect("seven bits fit a byte");
        rest >>= VARINT_BITS;
        if rest == 0 {
            compressed.push(low);
            break;
        }
        compressed.push(low | VARINT_MORE);
    }
    let (packed, state) = lzma_encode(window, state, plain)?;
    compressed.extend_from_slice(&packed);
    Ok((encode(LINKED, &compressed), state))
}

pub fn standalone_row(records: &[crate::Record]) -> Result<String, StoreError> {
    let plain = crate::record::encode_all(records)?;
    let mut spare = Spare::default();
    let mut packer = Packer::new(&mut spare, DEFAULT_LEVEL, &[])?;
    packer.write(&plain)?;
    let stream = packer.finished(&mut spare)?;
    let mut bytes =
        Vec::with_capacity(STANDALONE_BLOCKS_MAX * EMPTY_STORED_BLOCK.len() + stream.len());
    for _ in 0..STANDALONE_BLOCKS_MAX {
        bytes.extend_from_slice(&EMPTY_STORED_BLOCK);
        let blocks = bytes.len();
        bytes.extend_from_slice(&stream);
        let body = encode(RECORDS, &bytes);
        let binary = format!("{RECORDS}{}", STANDARD_NO_PAD.encode(&bytes));
        if !extensible(&body) && !extensible(&binary) {
            assert!(standalone(&body), "a standalone row is read as one");
            assert!(
                standalone(&binary),
                "a standalone row read back from binary storage is read as one"
            );
            return Ok(body);
        }
        bytes.truncate(blocks);
    }
    panic!(
        "one of {STANDALONE_BLOCKS_MAX} empty stored block prefixes leaves a row unextensible in both of its encodings"
    )
}

#[must_use]
pub fn standalone(body: &str) -> bool {
    body.strip_prefix(RECORDS)
        .and_then(|text| decoded(text).ok())
        .is_some_and(|bytes| bytes.starts_with(&EMPTY_STORED_BLOCK))
}

pub fn standalone_plain(body: &str) -> Result<Option<String>, StoreError> {
    let Some(text) = body.strip_prefix(RECORDS) else {
        return Ok(None);
    };
    let bytes = decoded(text)?;
    if !bytes.starts_with(&EMPTY_STORED_BLOCK) {
        return Ok(None);
    }
    let plain = inflate(&bytes, &[])?;
    String::from_utf8(plain)
        .map(Some)
        .map_err(|error| failed("decoding", &error.to_string()))
}

#[must_use]
pub fn extensible(body: &str) -> bool {
    if !body.starts_with(RECORDS) {
        return false;
    }
    let text = &body[RECORDS.len_utf8()..];
    let lzma = decoded(text).is_ok_and(|bytes| {
        matches!(
            bytes.first().copied(),
            Some(LZMA_CONTINUED | LZMA_RESTARTED)
        ) && bytes.ends_with(&LZMA_SEAL)
    });
    match text.rsplit_once(SEGMENT) {
        Some((_, last)) if lzma => last.len() == SEAL,
        Some((_, last)) => last.len() > SEAL && (last.len() - SEAL).is_multiple_of(BASE85_CHARS),
        None => text.len() > SEAL && (text.len() - SEAL).is_multiple_of(BASE64_CHARS),
    }
}

pub(crate) fn unpack(text: &str, dictionary: &[u8]) -> Result<Vec<u8>, StoreError> {
    inflate(&decoded(text)?, dictionary)
}

pub(crate) fn linked(text: &str) -> Result<(u64, Vec<u8>), StoreError> {
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

pub(crate) fn inflate(compressed: &[u8], dictionary: &[u8]) -> Result<Vec<u8>, StoreError> {
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

pub(crate) fn parts(body: &str, limit: Option<usize>) -> Result<Vec<&str>, StoreError> {
    let Some(limit) = limit else {
        return Ok(vec![body]);
    };
    if limit < 2 {
        return Err(StoreError::NotCommitted(format!(
            "a turn log row limit of {limit} bytes cannot hold a tag and a byte"
        )));
    }
    let (first, mut rest) = body.split_at(body.len().min(limit));
    let mut parts = vec![first];
    while !rest.is_empty() {
        let (part, next) = rest.split_at(rest.len().min(limit - 1));
        parts.push(part);
        rest = next;
    }
    Ok(parts)
}

const SEXTET_BITS: usize = 6;
const BYTE_BITS: usize = 8;
const PAD: char = '=';

const ALPHABET: &[u8; 1 << SEXTET_BITS] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
const NOT_BASE64: u8 = u8::MAX;
const SEXTETS: [u8; 1 << BYTE_BITS] = {
    let mut sextets = [NOT_BASE64; 1 << BYTE_BITS];
    let mut index = 0;
    while index < ALPHABET.len() {
        sextets[ALPHABET[index] as usize] = index as u8;
        index += 1;
    }
    sextets
};

pub(crate) fn padded(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    let remainder = bytes.len() % BASE64_CHARS;
    if bytes.is_empty() || remainder == 1 {
        return None;
    }
    let mut last = 0;
    for &byte in bytes {
        last = SEXTETS[usize::from(byte)];
        if last == NOT_BASE64 {
            return None;
        }
    }
    let spare = (remainder * SEXTET_BITS) % BYTE_BITS;
    if last & ((1 << spare) - 1) != 0 {
        return None;
    }
    let mut padded = String::with_capacity(bytes.len() + BASE64_CHARS);
    padded.push_str(text);
    while !padded.len().is_multiple_of(BASE64_CHARS) {
        padded.push(PAD);
    }
    Some(padded)
}

fn binary(body: &str, whole: bool) -> Option<(&str, String)> {
    let tag = body
        .chars()
        .next()
        .filter(|tag| tag.is_ascii() && *tag != STAMPED)?;
    let (tag, text) = body.split_at(tag.len_utf8());
    if text.contains(SEGMENT) {
        return whole
            .then(|| tail(text))
            .flatten()
            .map(|padded| (tag, padded));
    }
    padded(text).map(|padded| (tag, padded))
}

pub(crate) fn binaries(rows: &[(u64, String)]) -> Vec<Option<(&str, String)>> {
    rows.iter()
        .enumerate()
        .map(|(index, (_, body))| {
            let whole = !body.starts_with(PART)
                && rows
                    .get(index + 1)
                    .is_none_or(|(_, next)| !next.starts_with(PART));
            binary(body, whole)
        })
        .collect()
}

pub(crate) fn same_rows(found: &[(u64, String)], rows: &[(u64, String)]) -> bool {
    found.len() == rows.len()
        && found.iter().zip(rows).zip(binaries(rows)).all(
            |(((found_seq, found), (seq, body)), binary)| {
                found_seq == seq
                    && (found == body
                        || binary.is_some_and(|(tag, padded)| {
                            found.strip_prefix(tag) == Some(padded.trim_end_matches('='))
                        }))
            },
        )
}

pub(crate) fn tail(text: &str) -> Option<String> {
    if !text.contains(SEGMENT) {
        return padded(text);
    }
    let bytes = decoded(text).ok().filter(|bytes| !bytes.is_empty())?;
    Some(STANDARD.encode(bytes))
}

pub(crate) fn decoded_bytes(chars: usize) -> usize {
    assert_ne!(
        chars % BASE64_CHARS,
        1,
        "a cut of whole base64 groups and a final partial group"
    );
    chars * SEXTET_BITS / BYTE_BITS
}
