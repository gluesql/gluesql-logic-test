use gluesql::prelude::{Glue, MemoryStorage, Payload};

use crate::{
    db::{Row, Type},
    error::LogicTestError,
};

use super::{Execute, Output};

pub struct GlueSQL {
    storage: Option<Glue<MemoryStorage>>,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("gluesql error: {0}")]
    GlueSQL(#[from] gluesql::prelude::Error),
}

impl sqllogictest::DB for GlueSQL {
    type Error = Error;
    type ColumnType = Type;

    fn run(&mut self, sql: &str) -> Result<sqllogictest::DBOutput<Self::ColumnType>, Self::Error> {
        use std::sync::{Arc, Mutex};
        use std::thread;

        let storage = Arc::new(Mutex::new(self.storage.take().unwrap()));
        let sql = sql.to_string();
        let storage_clone = Arc::clone(&storage);

        let handle = thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let mut storage = storage_clone.lock().unwrap();
            rt.block_on(async {
                let payloads = storage.execute(&sql).await.map_err(Error::GlueSQL)?;

                assert_eq!(payloads.len(), 1, "only one payload is supported");
                let payload = payloads.into_iter().next().expect("payload is not empty");

                let output = match payload {
                    // select
                    Payload::Select { labels, rows } => {
                        let types = infer_result_types(&labels, &rows);
                        let rows = rows
                            .into_iter()
                            .map(|row| row.into_iter().map(Into::into).collect())
                            .collect();

                        sqllogictest::DBOutput::Rows { types, rows }
                    }
                    Payload::SelectMap(rows) => {
                        // Convert SelectMap to the same format as Select for type inference
                        let row_values: Vec<Vec<gluesql::prelude::Value>> = rows
                            .iter()
                            .map(|row| row.values().cloned().collect())
                            .collect();

                        // Get column labels from the first row's keys
                        let labels: Vec<String> = if let Some(first_row) = rows.first() {
                            first_row.keys().cloned().collect()
                        } else {
                            Vec::new()
                        };

                        let types = infer_result_types(&labels, &row_values);
                        let rows = rows
                            .into_iter()
                            .map(|row| row.into_values().map(Into::into).collect())
                            .collect();

                        sqllogictest::DBOutput::Rows { types, rows }
                    }
                    // execute
                    Payload::Insert(_)
                    | Payload::Delete(_)
                    | Payload::Update(_)
                    | Payload::Create
                    | Payload::DropTable(_)
                    | Payload::AlterTable
                    | Payload::CreateIndex
                    | Payload::DropIndex
                    | Payload::StartTransaction
                    | Payload::Commit
                    | Payload::Rollback
                    | Payload::DropFunction => sqllogictest::DBOutput::StatementComplete(0),

                    Payload::ShowVariable(_) | Payload::ShowColumns(_) => {
                        return Err(Error::GlueSQL(gluesql::prelude::Error::StorageMsg(
                            "ShowVariable and ShowColumns not supported".to_string(),
                        )));
                    }
                };

                Ok(output)
            })
        });

        let result = handle.join().unwrap()?;

        // Put the storage back
        if let Ok(storage_guard) = Arc::try_unwrap(storage) {
            self.storage = Some(storage_guard.into_inner().unwrap());
        } else {
            // Fallback: create a new storage if Arc couldn't be unwrapped
            self.storage = Some(Glue::new(MemoryStorage::default()));
        }

        Ok(result)
    }
}

impl GlueSQL {
    pub fn new_memory() -> Self {
        Self {
            storage: Some(Glue::new(MemoryStorage::default())),
        }
    }
}

impl Execute for GlueSQL {
    fn execute_inner(&mut self, sql: impl AsRef<str>) -> Result<Output, LogicTestError> {
        use std::sync::{Arc, Mutex};
        use std::thread;

        let storage = Arc::new(Mutex::new(self.storage.take().unwrap()));
        let sql = sql.as_ref().to_string();
        let storage_clone = Arc::clone(&storage);

        let handle = thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let mut storage = storage_clone.lock().unwrap();
            rt.block_on(async {
                let payloads = storage
                    .execute(&sql)
                    .await
                    .map_err(|e| LogicTestError::GlueSQL(Error::GlueSQL(e)))?;

                assert_eq!(payloads.len(), 1, "only one payload is supported");
                let payload = payloads.into_iter().next().expect("payload is not empty");

                Ok::<Output, LogicTestError>(match payload {
                    // select
                    Payload::Select { labels, rows } => {
                        let types = infer_result_types(&labels, &rows);

                        Output::Rows {
                            types,
                            rows: rows
                                .into_iter()
                                .map(|row| Row(row.into_iter().map(Into::into).collect()))
                                .collect(),
                        }
                    }
                    Payload::SelectMap(rows) => {
                        // Convert SelectMap to the same format as Select for type inference
                        let row_values: Vec<Vec<gluesql::prelude::Value>> = rows
                            .iter()
                            .map(|row| row.values().cloned().collect())
                            .collect();

                        // Get column labels from the first row's keys
                        let labels: Vec<String> = if let Some(first_row) = rows.first() {
                            first_row.keys().cloned().collect()
                        } else {
                            Vec::new()
                        };

                        let types = infer_result_types(&labels, &row_values);

                        Output::Rows {
                            types,
                            rows: rows
                                .into_iter()
                                .map(|row| Row(row.into_values().map(Into::into).collect()))
                                .collect(),
                        }
                    }
                    // execute
                    Payload::Insert(_)
                    | Payload::Delete(_)
                    | Payload::Update(_)
                    | Payload::Create
                    | Payload::DropTable(_)
                    | Payload::AlterTable
                    | Payload::CreateIndex
                    | Payload::DropIndex
                    | Payload::StartTransaction
                    | Payload::Commit
                    | Payload::Rollback
                    | Payload::DropFunction => Output::StatementComplete(0),

                    Payload::ShowVariable(_) | Payload::ShowColumns(_) => unimplemented!(),
                })
            })
        });

        let result = handle.join().unwrap()?;

        // Put the storage back
        if let Ok(storage_guard) = Arc::try_unwrap(storage) {
            self.storage = Some(storage_guard.into_inner().unwrap());
        } else {
            // Fallback: create a new storage if Arc couldn't be unwrapped
            self.storage = Some(Glue::new(MemoryStorage::default()));
        }

        Ok(result)
    }
}

fn infer_result_types(labels: &[String], rows: &[Vec<gluesql::prelude::Value>]) -> Vec<Type> {
    (0..labels.len())
        .map(|col_idx| {
            // Infer type from non-null values in this column
            for row in rows {
                if let Some(value) = row.get(col_idx) {
                    match value {
                        gluesql::prelude::Value::I8(_)
                        | gluesql::prelude::Value::I16(_)
                        | gluesql::prelude::Value::I32(_)
                        | gluesql::prelude::Value::I64(_)
                        | gluesql::prelude::Value::I128(_)
                        | gluesql::prelude::Value::U8(_)
                        | gluesql::prelude::Value::U16(_)
                        | gluesql::prelude::Value::U32(_)
                        | gluesql::prelude::Value::U64(_)
                        | gluesql::prelude::Value::U128(_)
                        | gluesql::prelude::Value::Bool(_) => return Type::Integer,
                        gluesql::prelude::Value::F32(_) | gluesql::prelude::Value::F64(_) => {
                            return Type::FloatingPoint;
                        }
                        gluesql::prelude::Value::Str(_) => return Type::Text,
                        gluesql::prelude::Value::Null => continue, // Skip nulls
                        _ => return Type::Any,
                    }
                }
            }
            // If all values are null or no rows, default to Any
            Type::Any
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::db::execute_test;

    use super::*;

    #[test]
    fn execute() {
        let mut gluesql = GlueSQL::new_memory();
        execute_test(&mut gluesql);
    }

    #[test]
    fn test_result_type_inference() {
        use gluesql::prelude::Value;

        // Test with different value types
        let labels = vec![
            "id".to_string(),
            "name".to_string(),
            "score".to_string(),
            "active".to_string(),
        ];
        let rows = vec![
            vec![
                Value::I32(1),
                Value::Str("Alice".to_string()),
                Value::F64(95.5),
                Value::Bool(true),
            ],
            vec![
                Value::I32(2),
                Value::Str("Bob".to_string()),
                Value::F64(87.2),
                Value::Bool(false),
            ],
        ];

        let types = infer_result_types(&labels, &rows);

        assert_eq!(types.len(), 4);
        assert_eq!(types[0], Type::Integer); // id
        assert_eq!(types[1], Type::Text); // name
        assert_eq!(types[2], Type::FloatingPoint); // score
        assert_eq!(types[3], Type::Integer); // active (bool maps to integer)
    }

    #[test]
    fn test_result_type_inference_with_nulls() {
        use gluesql::prelude::Value;

        // Test with null values - should infer from non-null values
        let labels = vec!["value".to_string()];
        let rows = vec![
            vec![Value::Null],
            vec![Value::Str("test".to_string())],
            vec![Value::Null],
        ];

        let types = infer_result_types(&labels, &rows);

        assert_eq!(types.len(), 1);
        assert_eq!(types[0], Type::Text); // Should infer Text from the non-null value
    }

    #[test]
    fn test_result_type_inference_all_nulls() {
        use gluesql::prelude::Value;

        // Test with all null values - should default to Any
        let labels = vec!["value".to_string()];
        let rows = vec![vec![Value::Null], vec![Value::Null]];

        let types = infer_result_types(&labels, &rows);

        assert_eq!(types.len(), 1);
        assert_eq!(types[0], Type::Any); // Should default to Any when all values are null
    }

    #[test]
    fn test_result_type_inference_empty_result() {
        // Test with empty result set - should default to Any
        let labels = vec!["value".to_string()];
        let rows: Vec<Vec<gluesql::prelude::Value>> = vec![];

        let types = infer_result_types(&labels, &rows);

        assert_eq!(types.len(), 1);
        assert_eq!(types[0], Type::Any); // Should default to Any when no rows
    }
}
