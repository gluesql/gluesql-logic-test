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
        run_directory_tests(&args.path).await?;
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

async fn run_single_test(path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let mut runner = sqllogictest::Runner::new(|| async { Ok(GlueSQL::new_memory()) });
    run_file_async(&mut runner, path).await?;

    Ok(())
}

async fn run_directory_tests(dir: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    use std::fs;

    let entries = fs::read_dir(dir)?;
    let mut test_files = Vec::new();

    for entry in entries {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            if let Some(ext) = path.extension() {
                if ext == "slt" || ext == "test" {
                    test_files.push(path);
                }
            }
        } else if path.is_dir() {
            // Recursively find test files
            collect_test_files(&path, &mut test_files)?;
        }
    }

    test_files.sort();

    println!("Found {} test files", test_files.len());

    for test_file in test_files {
        let mut runner = sqllogictest::Runner::new(|| async { Ok(GlueSQL::new_memory()) });
        match run_file_async(&mut runner, &test_file).await {
            Ok(_) => println!("✓ {}", test_file.display()),
            Err(e) => {
                eprintln!("✗ {} - Error: {}", test_file.display(), e);
                return Err(e.into());
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
