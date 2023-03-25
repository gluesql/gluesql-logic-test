use std::{convert::Infallible, str::FromStr};

use rusqlite::{Connection, Statement};

use crate::error::LogicTestError;

use super::{Execute, Output, Row, Type};

pub struct Sqlite {
    connection: Connection,
}

pub enum Query {
    /// Selct
    Query,
    /// Other
    Execute,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),

    #[error("query is empty")]
    EmptyQuery,
}

impl FromStr for Query {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(s.to_uppercase()
            .starts_with("SELECT")
            .then_some(Query::Query)
            .unwrap_or(Query::Execute))
    }
}

impl Sqlite {
    pub fn new_memory() -> Self {
        Self {
            connection: Connection::open_in_memory().unwrap(),
        }
    }
}

impl Execute for Sqlite {
    fn execute_inner(&mut self, sql: impl AsRef<str>) -> Result<Output, LogicTestError> {
        let query: Query = sql
            .as_ref()
            .split_whitespace()
            .next()
            .ok_or(Error::EmptyQuery)?
            .parse()
            .unwrap();

        let mut stmt = self.connection.prepare(sql.as_ref()).into_sqlite_result()?;
        match query {
            Query::Query => parse_query(stmt),
            Query::Execute => {
                stmt.execute([]).into_sqlite_result()?;
                Ok(Output::StatementComplete(0))
            }
        }
    }
}

fn parse_query(mut stmt: Statement) -> Result<Output, LogicTestError> {
    let types = stmt
        .columns()
        .into_iter()
        .map(|column| {
            let decl_type = column.decl_type();
            let decl_type = decl_type.and_then(Type::from_sql_type);

            match decl_type {
                Some(decl_type) => decl_type,
                None => {
                    tracing::warn!("column type is not found: {:?}", column);
                    Type::Any
                }
            }
        })
        .collect::<Vec<_>>();

    let mut result = vec![];
    let mut rows = stmt.query([]).into_sqlite_result()?;
    while let Some(row) = rows.next().into_sqlite_result()? {
        let row = types
            .iter()
            .enumerate()
            .map(|(idx, ty)| match ty {
                Type::Text => row.get::<_, String>(idx),
                Type::Integer => row.get::<_, i64>(idx).map(|v| v.to_string()),
                Type::FloatingPoint => row.get::<_, f64>(idx).map(|v| v.to_string()),

                Type::Any => row.get::<_, String>(idx),
            })
            .collect::<Result<Vec<String>, _>>()
            .into_sqlite_result()?;

        result.push(Row(row))
    }

    Ok(Output::Rows {
        types,
        rows: result,
    })
}

trait ResultExt<T> {
    fn into_sqlite_result(self) -> Result<T, Error>;
}

impl<T> ResultExt<T> for Result<T, rusqlite::Error> {
    fn into_sqlite_result(self) -> Result<T, Error> {
        self.map_err(|e| e.into())
    }
}

#[cfg(test)]
mod tests {
    use crate::db::execute_test;

    use super::*;

    #[test]
    fn constructor() {
        let _ = Sqlite::new_memory();
    }

    #[test]
    fn execute() {
        let mut db = Sqlite::new_memory();
        execute_test(&mut db);
    }
}
