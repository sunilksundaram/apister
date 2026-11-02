use pyo3::exceptions::{PyIOError, PyValueError};
use pyo3::prelude::*;
use std::io::Cursor;

#[pyfunction]
#[pyo3(signature = (data, level=3, dict=None))]
pub fn rust_zstd_compress(
    data: &[u8],
    level: i32,
    dict: Option<&[u8]>,
) -> PyResult<Vec<u8>> {
    match dict {
        Some(dict_bytes) => {
            let mut compressor = zstd::bulk::Compressor::with_dictionary(level, dict_bytes)
                .map_err(|e| PyIOError::new_err(format!("zstd dict compressor init error: {e}")))?;
            compressor
                .compress(data)
                .map_err(|e| PyIOError::new_err(format!("zstd compression error: {e}")))
        }
        None => zstd::encode_all(Cursor::new(data), level)
            .map_err(|e| PyIOError::new_err(format!("zstd encode error: {e}"))),
    }
}

#[pyfunction]
#[pyo3(signature = (data, dict=None))]
pub fn rust_zstd_decompress(
    data: &[u8],
    dict: Option<&[u8]>,
) -> PyResult<Vec<u8>> {
    match dict {
        Some(dict_bytes) => {
            let mut decompressor = zstd::bulk::Decompressor::with_dictionary(dict_bytes)
                .map_err(|e| PyIOError::new_err(format!("zstd dict decompressor init error: {e}")))?;
            // 8x capacity heuristic with fallback to stream if needed
            let capacity = data.len().saturating_mul(8).max(64 * 1024);
            decompressor
                .decompress(data, capacity)
                .map_err(|e| PyIOError::new_err(format!("zstd decompression error: {e}")))
        }
        None => zstd::decode_all(Cursor::new(data))
            .map_err(|e| PyIOError::new_err(format!("zstd decode error: {e}"))),
    }
}

#[pyfunction]
#[pyo3(signature = (samples, dict_size=65536))]
pub fn rust_zstd_train_dict(
    samples: Vec<Vec<u8>>,
    dict_size: usize,
) -> PyResult<Vec<u8>> {
    if samples.is_empty() {
        return Err(PyValueError::new_err("samples cannot be empty"));
    }
    let sample_refs: Vec<&[u8]> = samples.iter().map(|s| s.as_slice()).collect();
    zstd::dict::from_samples(&sample_refs, dict_size)
        .map_err(|e| PyIOError::new_err(format!("zstd train dictionary error: {e}")))
}

#[pyfunction]
pub fn rust_lz4_compress(data: &[u8]) -> PyResult<Vec<u8>> {
    Ok(lz4_flex::compress_prepend_size(data))
}

#[pyfunction]
pub fn rust_lz4_decompress(data: &[u8]) -> PyResult<Vec<u8>> {
    lz4_flex::decompress_size_prepended(data)
        .map_err(|e| PyIOError::new_err(format!("lz4 decompression error: {e}")))
}
