# GlueSQL logic test

[![Coverage Status](https://coveralls.io/repos/github/gluesql/gluesql-logic-test/badge.svg?branch=main)](https://coveralls.io/github/gluesql/gluesql-logic-test?branch=main)

You can run test by running commands like following examples:

```
# Run a single test file
cargo run --bin gluesql-logic-test -- test.slt

# Run all tests in directory
cargo run --bin gluesql-logic-test -- slt/

# Run with verbose output
cargo run --bin gluesql-logic-test -- --verbose test.slt
```

_slt/_ directory may be from https://sqlite.org/sqllogictest/tree?name=test&type=tree.

---

TODO items
----------

 - [x] Implement `sqllogictest::AsyncDB` trait for GlueSQL in-memory storage backend.
   - [ ] Repeat test with other storage backends that supports CRUD.
 - [ ] Implement `sqllogictest-bin` `ExternalDriver`-compatible GlueSQL runner.
 - [ ] (Not sure) Merge this repository to gluesql/gluesql repository to follow changes of GlueSQL.
