use std::ops::ControlFlow;

use gluesql::prelude::{Glue, MemoryStorage, Payload};
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
    storage: Glue<MemoryStorage>,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    GlueSQL(#[from] gluesql::prelude::Error),
}

impl GlueSQL {
    pub fn new_memory() -> Self {
        Self {
            storage: Glue::new(MemoryStorage::default()),
        }
    }

    pub fn table_types(&mut self, table_name: &str) -> Result<Vec<Type>, LogicTestError> {
        let mut payloads = self
            .storage
            .execute(format!("SHOW COLUMNS FROM {table_name}"))
            .into_gluesql_error()?;
        let payload = payloads.remove(0);

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
}

impl Execute for GlueSQL {
    fn execute_inner(&mut self, sql: impl AsRef<str>) -> Result<Output, LogicTestError> {
        let payloads = self.storage.execute(sql.as_ref()).into_gluesql_error()?;
        assert_eq!(payloads.len(), 1, "only one payload is supported");
        let payload = payloads.into_iter().next().expect("payload is not empty");

        Ok(match payload {
            // select
            Payload::Select { rows, .. } => {
                let table_name = get_table_name(sql.as_ref());
                let types = self.table_types(&table_name)?;

                Output::Rows {
                    types,
                    rows: rows
                        .into_iter()
                        .map(|row| Row(row.into_iter().map(Into::into).collect()))
                        .collect(),
                }
            }
            Payload::SelectMap(rows) => {
                let table_name = get_table_name(sql.as_ref());
                let types = self.table_types(&table_name)?;

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
            | Payload::DropTable
            | Payload::AlterTable
            | Payload::CreateIndex
            | Payload::DropIndex
            | Payload::StartTransaction
            | Payload::Commit
            | Payload::Rollback => Output::StatementComplete(0),

            Payload::ShowVariable(_) | Payload::ShowColumns(_) => unimplemented!(),
        })
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
    s.visit(&mut visitor);

    visitor.0.unwrap()
}

fn from_gluesql_type(value: &str) -> Option<Type> {
    Some(match value {
        "INT" => Type::Integer,

        _ => return Type::from_sql_type(value),
    })
}

trait ResultExt<T> {
    fn into_gluesql_error(self) -> Result<T, Error>;
}

impl<T> ResultExt<T> for Result<T, gluesql::prelude::Error> {
    fn into_gluesql_error(self) -> Result<T, Error> {
        self.map_err(|e| e.into())
    }
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
