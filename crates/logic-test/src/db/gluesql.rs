use std::ops::ControlFlow;

use gluesql::{
    core::ast_builder::{table, Build},
    prelude::{Glue, MemoryStorage, Payload},
};
use sqlparser::{
    ast::{ObjectName, Visit, Visitor},
    dialect::GenericDialect,
    parser::Parser,
    tokenizer::Tokenizer,
};

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
            rt.block_on(async {
                let mut storage = storage_clone.lock().unwrap();
                let payloads = storage.execute(&sql).await.map_err(Error::GlueSQL)?;

                assert_eq!(payloads.len(), 1, "only one payload is supported");
                let payload = payloads.into_iter().next().expect("payload is not empty");

                let output = match payload {
                    // select
                    Payload::Select { rows, .. } => {
                        let table_name = get_table_name(&sql);
                        let types = get_table_types_sync(&mut *storage, &table_name).await?;
                        let rows = rows
                            .into_iter()
                            .map(|row| row.into_iter().map(Into::into).collect())
                            .collect();

                        sqllogictest::DBOutput::Rows { types, rows }
                    }
                    Payload::SelectMap(rows) => {
                        let table_name = get_table_name(&sql);
                        let types = get_table_types_sync(&mut *storage, &table_name).await?;
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

async fn get_table_types_sync(
    storage: &mut Glue<MemoryStorage>,
    table_name: &str,
) -> Result<Vec<Type>, Error> {
    let payload = storage
        .execute_stmt(&table(table_name).show_columns().build().unwrap())
        .await
        .map_err(Error::GlueSQL)?;

    let Payload::ShowColumns(show_columns) = payload else {
        unreachable!("SHOW COLUMNS should return Payload::ShowColumns")
    };

    Ok(show_columns
        .into_iter()
        .map(|(_, data_type)| {
            let data_type = data_type.to_string();

            match from_gluesql_type(&data_type) {
                Some(t) => t,
                None => {
                    tracing::warn!("column type is not found: {:?}", data_type);
                    Type::Any
                }
            }
        })
        .collect())
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
            rt.block_on(async {
                let mut storage = storage_clone.lock().unwrap();
                let payloads = storage
                    .execute(&sql)
                    .await
                    .map_err(|e| LogicTestError::GlueSQL(Error::GlueSQL(e)))?;

                assert_eq!(payloads.len(), 1, "only one payload is supported");
                let payload = payloads.into_iter().next().expect("payload is not empty");

                Ok::<Output, LogicTestError>(match payload {
                    // select
                    Payload::Select { rows, .. } => {
                        let table_name = get_table_name(&sql);
                        let types = get_table_types_sync(&mut *storage, &table_name)
                            .await
                            .map_err(|e| LogicTestError::GlueSQL(e))?;

                        Output::Rows {
                            types,
                            rows: rows
                                .into_iter()
                                .map(|row| Row(row.into_iter().map(Into::into).collect()))
                                .collect(),
                        }
                    }
                    Payload::SelectMap(rows) => {
                        let table_name = get_table_name(&sql);
                        let types = get_table_types_sync(&mut *storage, &table_name)
                            .await
                            .map_err(|e| LogicTestError::GlueSQL(e))?;

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

fn get_table_name(sql: &str) -> String {
    let mut tokenizer = Tokenizer::new(&GenericDialect, sql);
    let tokens = tokenizer.tokenize().unwrap();
    let s = Parser::new(&GenericDialect)
        .with_tokens(tokens)
        .parse_statement()
        .unwrap();

    #[derive(Default)]
    struct TableNameVisitor(Option<String>);

    impl Visitor for TableNameVisitor {
        type Break = ();

        fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Self::Break> {
            self.0 = Some(relation.to_string());
            ControlFlow::Break(())
        }
    }

    let mut visitor = TableNameVisitor::default();
    let _ = s.visit(&mut visitor);

    visitor.0.unwrap()
}

fn from_gluesql_type(value: &str) -> Option<Type> {
    Some(match value {
        "INT" => Type::Integer,
        _ => return Type::from_sql_type(value),
    })
}

#[cfg(test)]
mod tests {
    use crate::db::execute_test;

    use super::*;

    #[test]
    fn get_table_name() {
        let sql = "CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT NOT NULL)";
        let table_name = super::get_table_name(sql);
        assert_eq!(table_name, "test");

        let sql = "SELECT * FROM test";
        let table_name = super::get_table_name(sql);
        assert_eq!(table_name, "test");
    }

    #[test]
    fn execute() {
        let mut gluesql = GlueSQL::new_memory();
        execute_test(&mut gluesql);
    }
}
