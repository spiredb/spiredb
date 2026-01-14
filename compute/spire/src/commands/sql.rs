use spire_common::SpireError;
use std::process::Command;

pub async fn handle_sql_command(spiresql_addr: String) -> Result<(), SpireError> {
    println!("Connecting to SpireSQL at {}...", spiresql_addr);

    // Split host and port
    let parts: Vec<&str> = spiresql_addr.split(':').collect();
    if parts.len() != 2 {
        return Err(SpireError::Internal(
            "Invalid SpireSQL address format. Expected host:port".to_string(),
        ));
    }
    let host = parts[0];
    let port = parts[1];

    // Try to run psql
    // Environment variables for psql can be set if needed
    let status = Command::new("psql")
        .arg("-h")
        .arg(host)
        .arg("-p")
        .arg(port)
        .arg("-d")
        .arg("postgres") // Default db
        .status();

    match status {
        Ok(s) => {
            if !s.success() {
                println!("psql exited with error");
            }
        }
        Err(e) => {
            return Err(SpireError::Internal(format!(
                "Failed to start psql: {}. Please ensure psql is installed.",
                e
            )));
        }
    }

    Ok(())
}
