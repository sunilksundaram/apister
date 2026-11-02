use memmap2::Mmap;
use pyo3::exceptions::{PyIOError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 4] = b"AP1\n";
const HEADER_SIZE: usize = 44;
const FROM_BROKER_FLAG: u16 = 1;
const SEAL_SIZE: usize = 8;

pub struct SegmentInfo {
    pub base: u64,
    pub end: u64,
    pub path: PathBuf,
}

#[pyclass]
pub struct RustLedger {
    dir: PathBuf,
    segment_bytes: u64,
    commit_every: usize,
    retain_bytes: Option<u64>,

    segments: Vec<SegmentInfo>,
    segment_bases: Vec<u64>,
    mmaps: HashMap<PathBuf, Mmap>,

    active_base: u64,
    active_size: u64,
    active_path: PathBuf,
    writer: Option<BufWriter<File>>,
    end_offset: u64,
    since_commit: usize,
}

#[pymethods]
impl RustLedger {
    #[new]
    #[pyo3(signature = (directory, segment_bytes=67108864, commit_every=128, retain_bytes=None))]
    pub fn new(
        directory: PathBuf,
        segment_bytes: u64,
        commit_every: usize,
        retain_bytes: Option<u64>,
    ) -> PyResult<Self> {
        if segment_bytes < HEADER_SIZE as u64 {
            return Err(PyValueError::new_err("segment_bytes is too small"));
        }
        if commit_every < 1 {
            return Err(PyValueError::new_err("commit_every must be >= 1"));
        }
        if matches!(retain_bytes, Some(rb) if rb < 1) {
            return Err(PyValueError::new_err("retain_bytes must be >= 1 or None"));
        }

        fs::create_dir_all(&directory)
            .map_err(|e| PyIOError::new_err(format!("failed to create dir: {e}")))?;

        let mut ledger = Self {
            dir: directory.clone(),
            segment_bytes,
            commit_every,
            retain_bytes,
            segments: Vec::new(),
            segment_bases: Vec::new(),
            mmaps: HashMap::new(),
            active_base: 0,
            active_size: 0,
            active_path: directory.join(format!("{:016x}.seg", 0)),
            writer: None,
            end_offset: 0,
            since_commit: 0,
        };

        ledger.recover()?;
        Ok(ledger)
    }

    #[getter]
    pub fn end_offset(&self) -> u64 {
        self.end_offset
    }

    #[getter]
    pub fn bytes_retained(&self) -> u64 {
        if self.segments.is_empty() && self.end_offset == self.active_base {
            return self.active_size;
        }
        let start = if let Some(first) = self.segments.first() {
            first.base
        } else {
            self.active_base
        };
        self.end_offset.saturating_sub(start)
    }

    #[getter]
    pub fn uncommitted(&self) -> usize {
        self.since_commit
    }

    #[pyo3(signature = (topic, payload, message_id, timestamp_ns, from_broker, commit=None, crc=None))]
    pub fn append(
        &mut self,
        topic: &str,
        payload: &[u8],
        message_id: &[u8],
        timestamp_ns: u64,
        from_broker: bool,
        commit: Option<bool>,
        crc: Option<u32>,
    ) -> PyResult<(u64, u64)> {
        let topic_bytes = topic.as_bytes();
        let flags: u16 = if from_broker { FROM_BROKER_FLAG } else { 0 };

        let computed_crc = match crc {
            Some(c) => c,
            None => {
                let mut hasher = crc32fast::Hasher::new();
                hasher.update(topic_bytes);
                hasher.update(payload);
                hasher.finalize()
            }
        };

        let mut mid_16 = [0u8; 16];
        let copy_len = message_id.len().min(16);
        mid_16[..copy_len].copy_from_slice(&message_id[..copy_len]);

        let mut header = [0u8; HEADER_SIZE];
        header[0..4].copy_from_slice(MAGIC);
        header[4..6].copy_from_slice(&(topic_bytes.len() as u16).to_le_bytes());
        header[6..8].copy_from_slice(&flags.to_le_bytes());
        header[8..16].copy_from_slice(&(payload.len() as u64).to_le_bytes());
        header[16..24].copy_from_slice(&timestamp_ns.to_le_bytes());
        header[24..40].copy_from_slice(&mid_16);
        header[40..44].copy_from_slice(&computed_crc.to_le_bytes());

        if self.active_size >= self.segment_bytes && self.active_size > 0 {
            self.rotate()?;
        }

        let offset = self.end_offset;
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| PyIOError::new_err("ledger is closed"))?;

        writer
            .write_all(&header)
            .map_err(|e| PyIOError::new_err(format!("write error: {e}")))?;
        writer
            .write_all(topic_bytes)
            .map_err(|e| PyIOError::new_err(format!("write error: {e}")))?;
        writer
            .write_all(payload)
            .map_err(|e| PyIOError::new_err(format!("write error: {e}")))?;

        let size = (HEADER_SIZE + topic_bytes.len() + payload.len()) as u64;
        self.active_size += size;
        self.end_offset = offset + size;
        self.since_commit += 1;

        let should_commit = match commit {
            Some(true) => true,
            Some(false) => false,
            None => self.since_commit >= self.commit_every,
        };

        if should_commit {
            self.commit()?;
        }

        Ok((offset, offset + size))
    }

    pub fn read<'py>(
        &mut self,
        py: Python<'py>,
        offset: u64,
    ) -> PyResult<(u64, u64, String, Bound<'py, PyBytes>, Bound<'py, PyBytes>, u64, bool)> {
        self.read_at(py, offset, self.end_offset)
    }

    pub fn read_next<'py>(
        &mut self,
        py: Python<'py>,
        offset: u64,
        end: Option<u64>,
    ) -> PyResult<Option<(u64, u64, String, Bound<'py, PyBytes>, Bound<'py, PyBytes>, u64, bool)>> {
        let limit = match end {
            Some(e) => e.min(self.end_offset),
            None => self.end_offset,
        };
        if offset >= limit {
            return Ok(None);
        }
        Ok(Some(self.read_at(py, offset, limit)?))
    }

    pub fn commit_if_due(&mut self) -> PyResult<()> {
        if self.since_commit >= self.commit_every {
            self.commit()?;
        }
        Ok(())
    }

    pub fn commit(&mut self) -> PyResult<()> {
        if let Some(writer) = self.writer.as_mut() {
            writer
                .flush()
                .map_err(|e| PyIOError::new_err(format!("flush error: {e}")))?;
            writer
                .get_ref()
                .sync_data()
                .map_err(|e| PyIOError::new_err(format!("sync error: {e}")))?;
        }
        self.write_ckpt(self.end_offset)?;
        self.since_commit = 0;
        self.enforce_retention()?;
        Ok(())
    }

    pub fn close(&mut self) -> PyResult<()> {
        if let Some(w) = self.writer.take() {
            let _ = self.commit();
            if let Ok(file) = w.into_inner() {
                let _ = file.set_len(self.active_size);
                let _ = file.sync_data();
            }
            self.mmaps.clear();
        }
        Ok(())
    }
}

impl RustLedger {
    fn recover(&mut self) -> PyResult<()> {
        let mut paths: Vec<PathBuf> = fs::read_dir(&self.dir)
            .map_err(|e| PyIOError::new_err(e.to_string()))?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("seg"))
            .collect();
        paths.sort();

        if paths.is_empty() {
            self.open_active(0)?;
            self.write_ckpt(0)?;
            return Ok(());
        }

        let mut expected = 0u64;
        let num_sealed = paths.len().saturating_sub(1);
        for path in &paths[..num_sealed] {
            let base = base_of(path)?;
            if base != expected {
                return Err(PyValueError::new_err(format!("segment gap at {:?}", path)));
            }
            let end = self.sealed_end(path, base)?;
            self.segments.push(SegmentInfo {
                base,
                end,
                path: path.clone(),
            });
            self.segment_bases.push(base);
            expected = end;
        }

        let active = paths.last().unwrap();
        let base = base_of(active)?;
        if base != expected {
            return Err(PyValueError::new_err(format!(
                "active segment gap at {:?}",
                active
            )));
        }

        let ckpt = self.read_ckpt(base)?;
        let end = self.walk(active, base, ckpt)?;
        self.end_offset = end;
        self.open_active(base)?;
        self.active_size = end.saturating_sub(base);

        let active_file_size = fs::metadata(&self.active_path)
            .map_err(|e| PyIOError::new_err(e.to_string()))?
            .len();

        if active_file_size != self.active_size {
            let file = OpenOptions::new()
                .write(true)
                .open(&self.active_path)
                .map_err(|e| PyIOError::new_err(e.to_string()))?;
            file.set_len(self.active_size)
                .map_err(|e| PyIOError::new_err(e.to_string()))?;
        }

        Ok(())
    }

    fn open_active(&mut self, base: u64) -> PyResult<()> {
        let path = self.dir.join(format!("{:016x}.seg", base));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .map_err(|e| PyIOError::new_err(format!("open active error: {e}")))?;

        let size = file
            .metadata()
            .map_err(|e| PyIOError::new_err(e.to_string()))?
            .len();

        // Sparse contiguous pre-allocation: if brand-new segment, pre-allocate disk blocks
        if size == 0 && self.segment_bytes > 0 {
            let _ = file.set_len(self.segment_bytes);
        }

        self.active_path = path;
        self.active_base = base;
        self.active_size = size;
        self.writer = Some(BufWriter::with_capacity(256 * 1024, file));
        Ok(())
    }

    fn rotate(&mut self) -> PyResult<()> {
        if let Some(mut w) = self.writer.take() {
            w.flush()
                .map_err(|e| PyIOError::new_err(format!("flush error: {e}")))?;
            let file = w.into_inner().map_err(|e| PyIOError::new_err(e.to_string()))?;
            // Truncate any unused pre-allocated capacity to exact active size
            let _ = file.set_len(self.active_size);
            file.sync_data()
                .map_err(|e| PyIOError::new_err(format!("sync error: {e}")))?;
        }

        let end = self.end_offset;
        self.write_seal(&self.active_path, end)?;
        self.mmaps.remove(&self.active_path);

        self.segments.push(SegmentInfo {
            base: self.active_base,
            end,
            path: self.active_path.clone(),
        });
        self.segment_bases.push(self.active_base);

        self.open_active(end)?;
        self.active_size = 0;
        self.enforce_retention()?;
        Ok(())
    }

    fn enforce_retention(&mut self) -> PyResult<()> {
        if let Some(retain) = self.retain_bytes {
            let horizon = self.end_offset.saturating_sub(retain);
            while !self.segments.is_empty() && self.segments[0].end <= horizon {
                let seg = self.segments.remove(0);
                self.segment_bases.remove(0);
                self.mmaps.remove(&seg.path);
                let _ = fs::remove_file(&seg.path);
                let seal_path = seg.path.with_extension("end");
                let _ = fs::remove_file(seal_path);
            }
        }
        Ok(())
    }

    fn read_at<'py>(
        &mut self,
        py: Python<'py>,
        offset: u64,
        limit: u64,
    ) -> PyResult<(u64, u64, String, Bound<'py, PyBytes>, Bound<'py, PyBytes>, u64, bool)> {
        if offset >= limit {
            return Err(PyValueError::new_err(format!("offset {} unavailable", offset)));
        }

        let (path, local, is_active) = self.locate(offset)?;

        if is_active {
            if let Some(writer) = self.writer.as_mut() {
                writer
                    .flush()
                    .map_err(|e| PyIOError::new_err(format!("flush error: {e}")))?;
            }
            let mut file = File::open(&path)
                .map_err(|e| PyIOError::new_err(format!("open reader error: {e}")))?;
            file.seek(SeekFrom::Start(local))
                .map_err(|e| PyIOError::new_err(format!("seek error: {e}")))?;

            let mut header = [0u8; HEADER_SIZE];
            file.read_exact(&mut header)
                .map_err(|_| PyValueError::new_err(format!("short header at {}", offset)))?;

            let (magic, topic_len, flags, payload_len, timestamp_ns, mid, crc) =
                parse_header(&header)?;
            if &magic != MAGIC {
                return Err(PyValueError::new_err(format!("bad magic at {}", offset)));
            }

            let body_len = topic_len as usize + payload_len as usize;
            let mut body = vec![0u8; body_len];
            file.read_exact(&mut body)
                .map_err(|_| PyValueError::new_err(format!("short body at {}", offset)))?;

            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&body);
            if hasher.finalize() != crc {
                return Err(PyValueError::new_err(format!("crc mismatch at {}", offset)));
            }

            let next_offset = offset + (HEADER_SIZE + body_len) as u64;
            if next_offset > limit {
                return Err(PyValueError::new_err(format!("offset {} unavailable", offset)));
            }

            let topic = String::from_utf8_lossy(&body[..topic_len as usize]).to_string();
            let payload_bytes = PyBytes::new(py, &body[topic_len as usize..]);
            let mid_bytes = PyBytes::new(py, &mid);

            return Ok((
                offset,
                next_offset,
                topic,
                payload_bytes,
                mid_bytes,
                timestamp_ns,
                (flags & FROM_BROKER_FLAG) != 0,
            ));
        }

        // Sealed segment: zero-syscall memory-mapped read!
        if !self.mmaps.contains_key(&path) {
            let file = File::open(&path)
                .map_err(|e| PyIOError::new_err(format!("open mmap file error: {e}")))?;
            let mmap = unsafe {
                Mmap::map(&file)
                    .map_err(|e| PyIOError::new_err(format!("mmap error: {e}")))?
            };
            self.mmaps.insert(path.clone(), mmap);
        }

        let mmap = self.mmaps.get(&path).unwrap();
        let start = local as usize;
        if start + HEADER_SIZE > mmap.len() {
            return Err(PyValueError::new_err(format!("short header at {}", offset)));
        }

        let header: &[u8; HEADER_SIZE] = mmap[start..start + HEADER_SIZE].try_into().unwrap();
        let (magic, topic_len, flags, payload_len, timestamp_ns, mid, crc) = parse_header(header)?;
        if &magic != MAGIC {
            return Err(PyValueError::new_err(format!("bad magic at {}", offset)));
        }

        let body_len = topic_len as usize + payload_len as usize;
        let total_len = HEADER_SIZE + body_len;
        if start + total_len > mmap.len() {
            return Err(PyValueError::new_err(format!("short body at {}", offset)));
        }

        let body = &mmap[start + HEADER_SIZE..start + total_len];
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(body);
        if hasher.finalize() != crc {
            return Err(PyValueError::new_err(format!("crc mismatch at {}", offset)));
        }

        let next_offset = offset + total_len as u64;
        if next_offset > limit {
            return Err(PyValueError::new_err(format!("offset {} unavailable", offset)));
        }

        let topic = String::from_utf8_lossy(&body[..topic_len as usize]).to_string();
        let payload_bytes = PyBytes::new(py, &body[topic_len as usize..]);
        let mid_bytes = PyBytes::new(py, &mid);

        Ok((
            offset,
            next_offset,
            topic,
            payload_bytes,
            mid_bytes,
            timestamp_ns,
            (flags & FROM_BROKER_FLAG) != 0,
        ))
    }

    fn locate(&self, offset: u64) -> PyResult<(PathBuf, u64, bool)> {
        // Fast-path: active segment O(1)
        if offset >= self.active_base && offset < self.end_offset {
            return Ok((self.active_path.clone(), offset - self.active_base, true));
        }

        // Binary search sealed segments O(log N)
        if !self.segment_bases.is_empty() {
            let idx = match self.segment_bases.binary_search(&offset) {
                Ok(exact) => exact,
                Err(pos) => pos.saturating_sub(1),
            };
            if idx < self.segments.len() {
                let seg = &self.segments[idx];
                if offset >= seg.base && offset < seg.end {
                    return Ok((seg.path.clone(), offset - seg.base, false));
                }
            }
        }

        Err(PyValueError::new_err(format!("offset {} unavailable", offset)))
    }

    fn sealed_end(&mut self, path: &Path, base: u64) -> PyResult<u64> {
        let seal_path = path.with_extension("end");
        if seal_path.exists() {
            let data = fs::read(&seal_path)
                .map_err(|e| PyIOError::new_err(format!("read seal error: {e}")))?;
            if data.len() == SEAL_SIZE {
                let end = u64::from_le_bytes(data[0..8].try_into().unwrap());
                return Ok(end);
            }
        }
        let end = self.walk(path, base, 0)?;
        self.write_seal(path, end)?;
        Ok(end)
    }

    fn walk(&self, path: &Path, base: u64, start_local: u64) -> PyResult<u64> {
        let size = fs::metadata(path)
            .map_err(|e| PyIOError::new_err(e.to_string()))?
            .len();

        let mut start = start_local;
        if start > size {
            start = 0;
        }

        let mut good = start;
        let mut file = File::open(path)
            .map_err(|e| PyIOError::new_err(format!("open walk error: {e}")))?;
        file.seek(SeekFrom::Start(start))
            .map_err(|e| PyIOError::new_err(format!("seek walk error: {e}")))?;

        let mut header = [0u8; HEADER_SIZE];
        loop {
            let local = file
                .stream_position()
                .map_err(|e| PyIOError::new_err(e.to_string()))?;

            if file.read_exact(&mut header).is_err() {
                break;
            }

            let (magic, topic_len, _flags, payload_len, _ts, _mid, crc) = match parse_header(&header)
            {
                Ok(h) => h,
                Err(_) => break,
            };

            if &magic != MAGIC || topic_len > 1024 {
                break;
            }

            let body_len = topic_len as usize + payload_len as usize;
            if local + HEADER_SIZE as u64 + body_len as u64 > size {
                break;
            }

            let mut body = vec![0u8; body_len];
            if file.read_exact(&mut body).is_err() {
                break;
            }

            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&body);
            if hasher.finalize() != crc {
                break;
            }

            good = local + HEADER_SIZE as u64 + body_len as u64;
        }

        if good < size {
            let trunc_file = OpenOptions::new()
                .write(true)
                .open(path)
                .map_err(|e| PyIOError::new_err(e.to_string()))?;
            trunc_file
                .set_len(good)
                .map_err(|e| PyIOError::new_err(e.to_string()))?;
            trunc_file
                .sync_all()
                .map_err(|e| PyIOError::new_err(e.to_string()))?;
        }

        Ok(base + good)
    }

    fn read_ckpt(&self, base: u64) -> PyResult<u64> {
        let ckpt_path = self.dir.join("ckpt");
        if !ckpt_path.exists() {
            return Ok(0);
        }
        let data = fs::read(&ckpt_path).unwrap_or_default();
        if data.len() != 8 {
            return Ok(0);
        }
        let ckpt = u64::from_le_bytes(data[0..8].try_into().unwrap());
        if ckpt < base {
            return Ok(0);
        }
        Ok(ckpt - base)
    }

    fn write_ckpt(&self, offset: u64) -> PyResult<()> {
        let temp_path = self.dir.join("ckpt.tmp");
        let final_path = self.dir.join("ckpt");
        fs::write(&temp_path, offset.to_le_bytes())
            .map_err(|e| PyIOError::new_err(e.to_string()))?;
        fs::rename(&temp_path, &final_path).map_err(|e| PyIOError::new_err(e.to_string()))?;
        Ok(())
    }

    fn write_seal(&self, path: &Path, end: u64) -> PyResult<()> {
        let seal_path = path.with_extension("end");
        let temp_path = path.with_extension("end.tmp");
        fs::write(&temp_path, end.to_le_bytes()).map_err(|e| PyIOError::new_err(e.to_string()))?;
        fs::rename(&temp_path, &seal_path).map_err(|e| PyIOError::new_err(e.to_string()))?;
        Ok(())
    }
}

fn base_of(path: &Path) -> PyResult<u64> {
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| PyValueError::new_err(format!("bad segment name {:?}", path)))?;
    u64::from_str_radix(stem, 16)
        .map_err(|_| PyValueError::new_err(format!("bad segment name {:?}", path)))
}

fn parse_header(
    h: &[u8; HEADER_SIZE],
) -> PyResult<([u8; 4], u16, u16, u64, u64, [u8; 16], u32)> {
    let magic: [u8; 4] = h[0..4].try_into().unwrap();
    let topic_len = u16::from_le_bytes(h[4..6].try_into().unwrap());
    let flags = u16::from_le_bytes(h[6..8].try_into().unwrap());
    let payload_len = u64::from_le_bytes(h[8..16].try_into().unwrap());
    let timestamp_ns = u64::from_le_bytes(h[16..24].try_into().unwrap());
    let mid: [u8; 16] = h[24..40].try_into().unwrap();
    let crc = u32::from_le_bytes(h[40..44].try_into().unwrap());
    Ok((
        magic,
        topic_len,
        flags,
        payload_len,
        timestamp_ns,
        mid,
        crc,
    ))
}

