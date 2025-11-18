use std::fmt::Write;
use std::sync::Arc;

use crate::RUNTIME;
use crate::serialize::PyAnyWrapper;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyString;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::Row;

#[pyclass]
pub(crate) struct Session {
    pub(crate) _inner: Arc<scylla::client::session::Session>,
}

#[pymethods]
impl Session {
    async fn execute(&self, request: Py<PyString>) -> PyResult<RequestResult> {
        let request_string = Python::with_gil(|py| request.to_str(py))?.to_string();
        let session_clone = Arc::clone(&self._inner);
        let result = RUNTIME
            .spawn(async move {
                session_clone
                    .query_unpaged(request_string, &[])
                    .await
                    .map_err(|e| {
                        PyRuntimeError::new_err(format!("Failed to deserialize metadata: {}", e))
                    })
            })
            .await
            .expect("Driver should not panic")?;
        Ok(RequestResult { inner: result })
    }

    async fn execute_with_values(
        &self,
        request: Py<PyString>,
        values: Py<PyAny>,
    ) -> PyResult<RequestResult> {
        let request_string = Python::with_gil(|py| request.to_str(py))?.to_string();
        let session_clone = Arc::clone(&self._inner);
        let result = RUNTIME
            .spawn(async move {
                session_clone
                    .query_unpaged(request_string, PyAnyWrapper(values))
                    .await
                    .map_err(|e| {
                        PyRuntimeError::new_err(format!("Failed to deserialize metadata: {}", e))
                    })
            })
            .await
            .expect("Driver should not panic")?;
        Ok(RequestResult { inner: result })
    }

    async fn prepare(&self, request: Py<PyString>) -> PyResult<PyPreparedStatement> {
        let request_string = Python::with_gil(|py| request.to_str(py))?.to_string();
        let session_clone = Arc::clone(&self._inner);
        let result = RUNTIME
            .spawn(async move {
                session_clone.prepare(request_string).await.map_err(|e| {
                    PyRuntimeError::new_err(format!("Failed to deserialize metadata: {}", e))
                })
            })
            .await
            .expect("Driver should not panic")?;
        Ok(PyPreparedStatement {
            _inner: Arc::new(result),
        })
    }
}

#[pyclass]
pub(crate) struct RequestResult {
    pub(crate) inner: scylla::response::query_result::QueryResult,
}

#[pymethods]
impl RequestResult {
    fn __str__<'gil>(&mut self, py: Python<'gil>) -> PyResult<Bound<'gil, PyString>> {
        let mut result = String::new();
        let rows_result = match self.inner.clone().into_rows_result() {
            Ok(r) => r,
            Err(e) => return Ok(PyString::new(py, &format!("non-rows result: {}", e))),
        };
        for r in rows_result.rows::<Row>().map_err(|e| {
            PyRuntimeError::new_err(format!("Failed to deserialize metadata: {}", e))
        })? {
            let row = match r {
                Ok(r) => r,
                Err(e) => {
                    return Err(PyRuntimeError::new_err(format!(
                        "Failed to deserialize row: {}",
                        e
                    )));
                }
            };
            write!(result, "|").unwrap();
            for col in row.columns {
                match col {
                    Some(c) => write!(result, "{}", c).unwrap(),
                    None => write!(result, "null").unwrap(),
                };
                write!(result, "|").unwrap();
            }
            writeln!(result).unwrap();
        }
        Ok(PyString::new(py, &result))
    }
}

#[pyfunction]
fn serialize_rust(values: Py<PyAny>, prepared: PyRef<'_, PyPreparedStatement>) -> PyResult<()> {
    let values = PyAnyWrapper(values);

    let res = prepared._inner.serialize_values(&values).map_err(|e| {
        PyRuntimeError::new_err(format!("Serialization error: {e}"))
    })?;

    let _ = std::hint::black_box(res);

    Ok(())
}

#[pyfunction]
fn serialize_rust_and_return(
    values: Py<PyAny>,
    prepared: PyRef<'_, PyPreparedStatement>,
) -> PyResult<(u16, Vec<u8>)> {
    let values = PyAnyWrapper(values);

    let res = prepared._inner.serialize_values(&values).map_err(|e| {
        PyRuntimeError::new_err(format!("Serialization error: {e}"))
    })?;

    let count = res.element_count();

    let values = res.get_contents().to_vec();

    Ok((count, values))
}

#[pyclass]
#[derive(Clone)]
pub struct PyPreparedStatement {
    pub _inner: Arc<PreparedStatement>,
}

#[pymodule]
pub(crate) fn session(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Session>()?;
    module.add_class::<RequestResult>()?;

    module.add_class::<PyPreparedStatement>()?;
    module.add_function(wrap_pyfunction!(serialize_rust, module)?)?;
    module.add_function(wrap_pyfunction!(serialize_rust_and_return, module)?)?;

    Ok(())
}
