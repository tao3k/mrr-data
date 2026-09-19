//! Dedicated external MRR resource process.
use std::process::ExitCode;
fn main() -> ExitCode {
    match mrr_data_poo_flow::run_cli() {
        Ok(receipt) => {
            println!("{receipt}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("mrr-data-poo-flow: {error:#}");
            ExitCode::FAILURE
        }
    }
}
