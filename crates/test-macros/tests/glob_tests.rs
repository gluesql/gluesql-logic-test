use std::path::PathBuf;

use test_macros::glob_tests;

#[glob_tests("tests/**/*.slt")]
pub fn execute_example(_path: PathBuf) {}
