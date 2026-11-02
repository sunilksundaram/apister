use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

const TRACE_MAGIC: &[u8; 4] = b"\x00TRC";
const TRACEPARENT_LEN: usize = 55;

#[pyfunction]
#[pyo3(signature = (payload, traceparent=None))]
pub fn rust_inject_trace(payload: &[u8], traceparent: Option<&str>) -> PyResult<Vec<u8>> {
    let mut out = Vec::with_capacity(4 + TRACEPARENT_LEN + payload.len());
    out.extend_from_slice(TRACE_MAGIC);

    match traceparent {
        Some(tp) => {
            if tp.len() != TRACEPARENT_LEN {
                return Err(PyValueError::new_err(format!(
                    "traceparent must be {} bytes, got {}",
                    TRACEPARENT_LEN,
                    tp.len()
                )));
            }
            out.extend_from_slice(tp.as_bytes());
        }
        None => {
            let trace_id = format!(
                "{:032x}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            );
            let span_id = format!(
                "{:016x}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64
            );
            let tp = format!("00-{}-{}-01", &trace_id[..32], &span_id[..16]);
            out.extend_from_slice(tp.as_bytes());
        }
    }

    out.extend_from_slice(payload);
    Ok(out)
}

#[pyfunction]
pub fn rust_extract_trace<'py>(
    py: Python<'py>,
    payload: &'py [u8],
) -> PyResult<(Option<String>, Bound<'py, PyBytes>)> {
    let min_len = 4 + TRACEPARENT_LEN;
    if payload.len() >= min_len && &payload[..4] == TRACE_MAGIC {
        let tp_str = String::from_utf8_lossy(&payload[4..min_len]).to_string();
        let inner_bytes = PyBytes::new(py, &payload[min_len..]);
        Ok((Some(tp_str), inner_bytes))
    } else {
        let all_bytes = PyBytes::new(py, payload);
        Ok((None, all_bytes))
    }
}
