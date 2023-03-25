use crate::db;

#[derive(thiserror::Error, Debug)]
pub enum LogicTestError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] db::sqlite::Error),
}
