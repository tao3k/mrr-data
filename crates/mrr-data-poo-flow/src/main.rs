//! Dedicated external MRR resource process.
use std::process::ExitCode;
fn main() -> ExitCode {
    let command = std::env::args().skip(1).collect::<Vec<_>>();
    let result = match command.as_slice() {
        [] => mrr_data_poo_flow::run_cli(),
        [command] if command == "--sync-pending" => mrr_data_poo_flow::run_sync_pending(),
        [command] if command == "--serve" => {
            mrr_data_poo_flow::run_sync_service().map(|()| String::new())
        }
        _ => Err(anyhow::anyhow!(
            "usage: mrr-data-poo-flow [--sync-pending|--serve]"
        )),
    };
    match result {
        Ok(receipt) => {
            if !receipt.is_empty() {
                println!("{receipt}");
            }
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("mrr-data-poo-flow: {error:#}");
            ExitCode::FAILURE
        }
    }
}
