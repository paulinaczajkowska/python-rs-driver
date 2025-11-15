use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};
use pyo3::{Py, PyAny};
use scylla::_macro_internal::{ColumnType, RowSerializationContext};
use scylla::cluster::metadata::NativeType;
use scylla_cql::_macro_internal::{RowWriter, SerializeRow, SerializeValue, WrittenCellProof};
use scylla_cql::frame::response::result::{CollectionType, UserDefinedType};
use scylla_cql::serialize::CellWriter;
use scylla_cql::serialize::SerializationError;
use std::ops::Deref;
use std::sync::Arc;

fn invalid_input(msg: &str) -> SerializationError {
    SerializationError::new(std::io::Error::new(std::io::ErrorKind::InvalidInput, msg))
}

#[derive(Debug)]
pub struct PyAnyWrapper(pub Py<PyAny>);

impl Deref for PyAnyWrapper {
    type Target = Py<PyAny>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PyAnyWrapper {
    fn serialize_natives<'a>(
        &self,
        typ: &ColumnType,
        native_type: &NativeType,
        writer: CellWriter<'a>,
    ) -> Result<WrittenCellProof<'a>, SerializationError> {
        match native_type {
            NativeType::Boolean => self.serialize_native::<bool>(typ, writer),
            NativeType::BigInt => self.serialize_native::<i64>(typ, writer),
            NativeType::Double => self.serialize_native::<f64>(typ, writer),
            NativeType::Int => self.serialize_native::<i32>(typ, writer),
            NativeType::Text => self.serialize_native::<String>(typ, writer),
            _ => unimplemented!("other native types not supported yet"),
        }
    }

    fn serialize_native<'a, T>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'a>,
    ) -> Result<WrittenCellProof<'a>, SerializationError>
    where
        T: for<'b> pyo3::FromPyObject<'b> + SerializeValue,
    {
        let value = Python::with_gil(|py| {
            self.bind(py)
                .extract::<T>()
                .map_err(SerializationError::new)
        })?;

        value.serialize(typ, writer)
    }

    fn serialize_types<'a>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'a>,
    ) -> Result<WrittenCellProof<'a>, SerializationError> {
        match typ {
            ColumnType::Native(native_type) => self.serialize_natives(typ, native_type, writer),
            ColumnType::Collection {
                frozen: _,
                typ: collection_typ,
            } => match collection_typ {
                CollectionType::List(element_type) => {
                    let list = PyListWrapper::new(self, typ).map_err(SerializationError::new)?;
                    list.serialize(element_type, writer)
                }
                _ => unimplemented!("other collection types not supported yet"),
            },
            // Supports UDTs passed as Python dicts.
            // For Python dataclass instances, convert to dict first (e.g., using dataclasses.asdict()).
            ColumnType::UserDefinedType { definition, .. } => {
                let dict = PyUdtWrapper::new(self, definition).map_err(SerializationError::new)?;
                dict.serialize(typ, writer)
            }
            _ => unimplemented!("other types not supported yet"),
        }
    }
}

impl SerializeRow for PyAnyWrapper {
    fn serialize(
        &self,
        ctx: &RowSerializationContext<'_>,
        writer: &mut RowWriter,
    ) -> Result<(), SerializationError> {
        Python::with_gil(|py| {
            let val = self.bind(py);

            match val.downcast::<PyTuple>() {
                Ok(py_tuple) => {
                    for (col, val) in ctx.columns().iter().zip(py_tuple.iter()) {
                        let wrapper = PyAnyWrapper(val.into());
                        let sub_writer = writer.make_cell_writer();
                        SerializeValue::serialize(&wrapper, col.typ(), sub_writer)?;
                    }
                    Ok(())
                }
                Err(_) => Err(invalid_input("expected Python tuple")),
            }
        })
    }

    fn is_empty(&self) -> bool {
        Python::with_gil(|py| self.is_none(py))
    }
}

impl SerializeValue for PyAnyWrapper {
    fn serialize<'a>(
        &self,
        typ: &ColumnType,
        writer: CellWriter<'a>,
    ) -> Result<WrittenCellProof<'a>, SerializationError> {
        if self.is_empty() {
            return Ok(writer.set_null());
        }

        self.serialize_types(typ, writer)
    }
}

#[derive(Debug)]
pub struct PyListWrapper<'a> {
    pub inner: Py<PyList>,
    pub list_type: &'a ColumnType<'a>,
}

impl Deref for PyListWrapper<'_> {
    type Target = Py<PyList>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'a> PyListWrapper<'a> {
    fn new(value: &PyAnyWrapper, list_type: &'a ColumnType<'a>) -> PyResult<Self> {
        Python::with_gil(|py| {
            let val = value.bind(py);
            let list: &Bound<PyList> = val.downcast::<PyList>()?;
            Ok(PyListWrapper {
                inner: list.clone().unbind(),
                list_type,
            })
        })
    }
}

impl SerializeValue for PyListWrapper<'_> {
    fn serialize<'b>(
        &self,
        _typ: &ColumnType,
        writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        let items: Vec<PyAnyWrapper> = Python::with_gil(|py| {
            self.bind(py)
                .iter()
                .map(|elem| PyAnyWrapper(elem.into()))
                .collect::<Vec<_>>()
        });

        SerializeValue::serialize(&items, self.list_type, writer)
    }
}

pub struct PyUdtWrapper<'a> {
    pub inner: Py<PyDict>,
    pub definition: &'a Arc<UserDefinedType<'a>>,
}

impl Deref for PyUdtWrapper<'_> {
    type Target = Py<PyDict>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'a> PyUdtWrapper<'a> {
    fn new(value: &PyAnyWrapper, definition: &'a Arc<UserDefinedType<'a>>) -> PyResult<Self> {
        Python::with_gil(|py| {
            let val = value.bind(py);
            let dict: &Bound<PyDict> = val.downcast::<PyDict>()?;
            Ok(PyUdtWrapper {
                inner: dict.clone().unbind(),
                definition,
            })
        })
    }
}

impl SerializeValue for PyUdtWrapper<'_> {
    fn serialize<'a>(
        &self,
        _typ: &ColumnType,
        writer: CellWriter<'a>,
    ) -> Result<WrittenCellProof<'a>, SerializationError> {
        let mut builder = writer.into_value_builder();

        for (field_name, field_type) in &self.definition.field_types {
            let item: Py<PyAny> = Python::with_gil(|py| {
                let dict = self.inner.bind(py);

                dict.get_item(field_name)
                    .map_err(SerializationError::new)?
                    .map(|val| val.unbind())
                    .ok_or_else(|| invalid_input("UDT missing field"))
            })?;

            PyAnyWrapper(item).serialize_types(field_type, builder.make_sub_writer())?;
        }

        builder.finish().map_err(SerializationError::new)
    }
}
