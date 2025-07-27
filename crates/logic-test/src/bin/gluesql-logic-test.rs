use clap::Parser;
use gluesql_logic_test::db::gluesql::GlueSQL;
use regex::Regex;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "gluesql-logic-test")]
#[command(about = "Run SQL logic tests with GlueSQL")]
struct Args {
    /// Path to the test file or directory
    #[arg(help = "Path to .slt test file or directory containing test files")]
    path: PathBuf,

    /// Verbose output
    #[arg(short, long, help = "Enable verbose output")]
    verbose: bool,

    /// Stop on first test failure
    #[arg(long, help = "Stop on first test failure")]
    fail_fast: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    if args.verbose {
        tracing_subscriber::fmt::init();
    }

    if args.path.is_file() {
        run_single_test(&args.path).await?;
    } else if args.path.is_dir() {
        run_directory_tests(&args.path, args.fail_fast).await?;
    } else {
        eprintln!("Error: Path {} does not exist", args.path.display());
        std::process::exit(1);
    }

    println!("All tests completed successfully!");
    Ok(())
}

static TRAILING_COMMENTS_ESCAPE_REGEX: std::sync::LazyLock<regex::Regex> =
    std::sync::LazyLock::new(|| {
        Regex::new(r"(skipif|onlyif) ([^ ]+) #([^\n]+)\n").expect("Invalid regex")
    });

async fn run_file_async<D: sqllogictest::AsyncDB, M: sqllogictest::MakeConnection<Conn = D>>(
    runner: &mut sqllogictest::Runner<D, M>,
    path: &PathBuf,
) -> Result<(), sqllogictest::TestError> {
    println!("Running test: {}", path.display());

    // Escape trailing comments
    let script = TRAILING_COMMENTS_ESCAPE_REGEX
        .replace_all(
            &std::fs::read_to_string(path).expect("Must exist"),
            "$1 $2\n",
        )
        .to_string();

    runner
        .run_script_with_name_async(&script, path.to_str().expect("Must exist."))
        .await
}

fn value_wise_sql_result_validator(
    normalizer: sqllogictest::Normalizer,
    actual: &[Vec<String>],
    expected: &[String],
) -> bool {
    // NOTE: copied from sqllogictest-rs but flatten actual rows for compatibility with SQLite testsuite.
    // https://github.com/risinglightdb/sqllogictest-rs/blob/dc6c6d4c666a8972e4398235ccfae688c202dd4b/sqllogictest/src/runner.rs#L528C1-L535
    let expected_results: Vec<String> = expected.iter().map(normalizer).collect();
    let normalized_rows: Vec<String> = actual
        .iter()
        .flat_map(|strs| strs.iter().map(normalizer))
        .collect();
    normalized_rows == expected_results
}

async fn run_single_test(path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let mut runner = sqllogictest::Runner::new(|| async { Ok(GlueSQL::new_memory()) });
    runner.with_validator(value_wise_sql_result_validator);
    run_file_async(&mut runner, path).await?;

    Ok(())
}

async fn run_directory_tests(
    dir: &PathBuf,
    fail_fast: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut test_files = Vec::new();
    collect_test_files(&dir, &mut test_files)?;

    test_files.sort();

    println!("Found {} test files", test_files.len());

    for test_file in test_files {
        let mut runner = sqllogictest::Runner::new(|| async { Ok(GlueSQL::new_memory()) });
        runner.with_validator(value_wise_sql_result_validator);
        match run_file_async(&mut runner, &test_file).await {
            Ok(_) => println!("✓ {}", test_file.display()),
            Err(e) => {
                eprintln!("✗ {} - Error: {}", test_file.display(), e);
                if fail_fast {
                    return Err(e.into());
                }
            }
        }
    }

    Ok(())
}

fn collect_test_files(dir: &PathBuf, files: &mut Vec<PathBuf>) -> Result<(), std::io::Error> {
    use std::fs;

    let entries = fs::read_dir(dir)?;

    for entry in entries {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            if let Some(ext) = path.extension() {
                if ext == "slt" || ext == "test" {
                    files.push(path);
                }
            }
        } else if path.is_dir() {
            collect_test_files(&path, files)?;
        }
    }

    Ok(())
}
