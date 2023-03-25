use crate::db;

#[derive(thiserror::Error, Debug)]
pub enum LogicTestError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] db::sqlite::Error),

    #[error("gluesql error: {0}")]
    GlueSQL(#[from] db::gluesql::Error),
}
