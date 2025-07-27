use clap::Parser;
use gluesql_logic_test::db::gluesql::GlueSQL;
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

async fn run_single_test(path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    println!("Running test: {}", path.display());

    let mut runner = sqllogictest::Runner::new(|| async { Ok(GlueSQL::new_memory()) });
    runner.run_file_async(path).await?;

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
        println!("Running test: {}", test_file.display());

        let mut runner = sqllogictest::Runner::new(|| async { Ok(GlueSQL::new_memory()) });

        match runner.run_file_async(&test_file).await {
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
