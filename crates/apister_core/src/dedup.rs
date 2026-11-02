use pyo3::exceptions::{PyIOError, PyValueError};
use pyo3::prelude::*;
use std::collections::{HashSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

const ID_LEN: usize = 16;

#[pyclass]
pub struct RustDedupLog {
    path: PathBuf,
    max_ids: usize,
    queue: VecDeque<[u8; ID_LEN]>,
    seen: HashSet<[u8; ID_LEN]>,
    file: Option<File>,
    buffer: Vec<u8>,
}

#[pymethods]
impl RustDedupLog {
    #[new]
    #[pyo3(signature = (path, max_ids=200000))]
    pub fn new(path: PathBuf, max_ids: usize) -> PyResult<Self> {
        if max_ids < 1 {
            return Err(PyValueError::new_err("max_ids must be >= 1"));
        }

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| PyIOError::new_err(format!("failed to create parent dir: {e}")))?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .map_err(|e| PyIOError::new_err(format!("open dedup log error: {e}")))?;

        let mut log = Self {
            path,
            max_ids,
            queue: VecDeque::with_capacity(max_ids.min(250_000)),
            seen: HashSet::with_capacity(max_ids.min(250_000)),
            file: Some(file),
            buffer: Vec::with_capacity(64 * 1024),
        };

        log.load_tail()?;
        Ok(log)
    }

    pub fn remember(&mut self, message_id: &[u8]) -> PyResult<bool> {
        if message_id.len() != ID_LEN {
            return Err(PyValueError::new_err("message_id must be 16 bytes"));
        }

        let mut mid = [0u8; ID_LEN];
        mid.copy_from_slice(message_id);

        if self.seen.contains(&mid) {
            return Ok(false);
        }

        self.seen.insert(mid);
        self.queue.push_back(mid);
        self.buffer.extend_from_slice(&mid);

        while self.queue.len() > self.max_ids {
            if let Some(oldest) = self.queue.pop_front() {
                self.seen.remove(&oldest);
            }
        }

        if self.buffer.len() >= 64 * 1024 {
            self.flush()?;
        }

        Ok(true)
    }

    pub fn flush(&mut self) -> PyResult<()> {
        if let Some(f) = self.file.as_mut() {
            if !self.buffer.is_empty() {
                f.write_all(&self.buffer)
                    .map_err(|e| PyIOError::new_err(format!("write error: {e}")))?;
                self.buffer.clear();
            }
            f.flush()
                .map_err(|e| PyIOError::new_err(format!("flush error: {e}")))?;

            let pos = f
                .stream_position()
                .map_err(|e| PyIOError::new_err(e.to_string()))?;
            let threshold = (self.max_ids * ID_LEN * 4) as u64;
            if pos > threshold {
                self.compact()?;
            }
        }
        Ok(())
    }

    pub fn close(&mut self) -> PyResult<()> {
        let _ = self.flush();
        self.file = None;
        Ok(())
    }
}

impl RustDedupLog {
    fn load_tail(&mut self) -> PyResult<()> {
        let size = fs::metadata(&self.path)
            .map_err(|e| PyIOError::new_err(e.to_string()))?
            .len();

        let max_bytes = (self.max_ids * ID_LEN) as u64;
        let mut start = size.saturating_sub(max_bytes);
        start -= start % (ID_LEN as u64);

        let mut f = File::open(&self.path)
            .map_err(|e| PyIOError::new_err(format!("open tail error: {e}")))?;
        f.seek(SeekFrom::Start(start))
            .map_err(|e| PyIOError::new_err(format!("seek tail error: {e}")))?;

        let mut data = Vec::new();
        f.read_to_end(&mut data)
            .map_err(|e| PyIOError::new_err(format!("read tail error: {e}")))?;

        let usable = data.len() - (data.len() % ID_LEN);
        for chunk in data[..usable].chunks_exact(ID_LEN) {
            let mut mid = [0u8; ID_LEN];
            mid.copy_from_slice(chunk);
            if !self.seen.contains(&mid) {
                self.seen.insert(mid);
                self.queue.push_back(mid);
            }
        }

        while self.queue.len() > self.max_ids {
            if let Some(oldest) = self.queue.pop_front() {
                self.seen.remove(&oldest);
            }
        }

        Ok(())
    }

    fn compact(&mut self) -> PyResult<()> {
        let temp_path = self.path.with_extension("tmp");
        {
            let mut temp_file = File::create(&temp_path)
                .map_err(|e| PyIOError::new_err(format!("create temp error: {e}")))?;
            for mid in &self.queue {
                temp_file
                    .write_all(mid)
                    .map_err(|e| PyIOError::new_err(format!("write temp error: {e}")))?;
            }
            temp_file
                .flush()
                .map_err(|e| PyIOError::new_err(format!("flush temp error: {e}")))?;
            temp_file
                .sync_all()
                .map_err(|e| PyIOError::new_err(format!("sync temp error: {e}")))?;
        }

        self.file = None;
        fs::rename(&temp_path, &self.path)
            .map_err(|e| PyIOError::new_err(format!("rename error: {e}")))?;

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&self.path)
            .map_err(|e| PyIOError::new_err(format!("reopen error: {e}")))?;
        self.file = Some(file);
        Ok(())
    }
}
