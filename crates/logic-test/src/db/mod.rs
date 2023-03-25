use crate::error::LogicTestError;

pub mod sqlite;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Type {
    /// T
    Text,
    /// I
    Integer,
    /// R
    FloatingPoint,

    /// ?
    Any,
}

impl Type {
    pub fn from_sql_type(value: &str) -> Option<Self> {
        Some(match value.to_uppercase().as_str() {
            "TEXT" => Self::Text,
            "INTEGER" => Self::Integer,
            "FLOAT" => Self::FloatingPoint,

            _ => return None,
        })
    }
}

impl sqllogictest::ColumnType for Type {
    fn from_char(value: char) -> Option<Self> {
        match value {
            'T' => Some(Self::Text),
            'I' => Some(Self::Integer),
            'R' => Some(Self::FloatingPoint),

            _ => Some(Self::Any),
        }
    }

    fn to_char(&self) -> char {
        match self {
            Self::Text => 'T',
            Self::Integer => 'I',
            Self::FloatingPoint => 'R',

            Self::Any => '?',
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Row(Vec<String>);

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Output {
    StatementComplete(u64),
    Rows { types: Vec<Type>, rows: Vec<Row> },
}

impl Output {
    pub fn into(self) -> sqllogictest::DBOutput<Type> {
        match self {
            Self::StatementComplete(count) => sqllogictest::DBOutput::StatementComplete(count),
            Self::Rows { rows, types } => sqllogictest::DBOutput::Rows {
                types,
                rows: rows.into_iter().map(|row| row.0).collect(),
            },
        }
    }
}

pub trait Execute {
    fn execute_inner(&self, sql: impl AsRef<str>) -> Result<Output, LogicTestError>;

    fn execute(
        &self,
        sql: impl AsRef<str>,
    ) -> Result<sqllogictest::DBOutput<Type>, LogicTestError> {
        self.execute_inner(sql).map(Output::into)
    }
}

#[cfg(test)]
pub(crate) fn execute_test(db: &impl Execute) {
    macro_rules! exec {
        ($sql:literal, $count:literal) => {
            let output = db.execute_inner($sql).unwrap();
            assert_eq!(output, Output::StatementComplete($count));
        };
    }

    macro_rules! query {
        ($sql:literal, rows: $rows:expr,types: $types:expr) => {
            let output = db.execute_inner($sql).unwrap();
            assert_eq!(
                output,
                Output::Rows {
                    rows: $rows
                        .into_iter()
                        .map(|row| Row(row.into_iter().map(ToOwned::to_owned).collect()))
                        .collect(),
                    types: $types
                        .into_iter()
                        .map(Type::from_sql_type)
                        .flatten()
                        .collect()
                }
            );
        };
    }

    exec!("CREATE TABLE Foo (a INTEGER, b TEXT)", 0);
    exec!("INSERT INTO Foo VALUES (1, 'a')", 1);
    exec!("INSERT INTO Foo VALUES (2, 'b'), (3, 'c')", 2);

    query!("SELECT * FROM Foo",
        rows: [["1", "a"], ["2", "b"], ["3", "c"]],
        types: ["INTEGER", "TEXT"]
    );

    exec!("UPDATE Foo SET a = 10 WHERE a = 1", 1);
    query!("SELECT * FROM Foo",
        rows: [["10", "a"], ["2", "b"], ["3", "c"]],
        types: ["INTEGER", "TEXT"]
    );

    exec!("DELETE FROM Foo WHERE a = 10", 1);
    query!("SELECT * FROM Foo",
        rows: [["2", "b"], ["3", "c"]],
        types: ["INTEGER", "TEXT"]
    );
}
