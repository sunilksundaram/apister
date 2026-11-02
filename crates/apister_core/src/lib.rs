#![allow(
    clippy::type_complexity,
    clippy::too_many_arguments,
    clippy::chunks_exact_to_as_chunks
)]

pub mod compression;
pub mod dedup;
pub mod ledger;
pub mod tracing;
pub mod trie;

use pyo3::prelude::*;

#[pyfunction]
fn rust_crc32(data: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

#[pymodule]
fn _apister_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ledger::RustLedger>()?;
    m.add_class::<trie::RustTopicTrie>()?;
    m.add_class::<dedup::RustDedupLog>()?;
    m.add_function(wrap_pyfunction!(rust_crc32, m)?)?;
    m.add_function(wrap_pyfunction!(compression::rust_zstd_compress, m)?)?;
    m.add_function(wrap_pyfunction!(compression::rust_zstd_decompress, m)?)?;
    m.add_function(wrap_pyfunction!(compression::rust_zstd_train_dict, m)?)?;
    m.add_function(wrap_pyfunction!(compression::rust_lz4_compress, m)?)?;
    m.add_function(wrap_pyfunction!(compression::rust_lz4_decompress, m)?)?;
    m.add_function(wrap_pyfunction!(tracing::rust_inject_trace, m)?)?;
    m.add_function(wrap_pyfunction!(tracing::rust_extract_trace, m)?)?;
    Ok(())
}
