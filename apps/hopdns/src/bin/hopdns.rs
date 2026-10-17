//! hopdns: DNS-service-discovery voor Hop, de host-daemon.
//!
//! Dezelfde vlaggen als de Go-daemon (zie `hopdns::config`):
//!
//! ```text
//! hopdns -listen :5353 -peer http://127.0.0.1:8080 -peer http://key@10.0.1.100:8080
//! ```

use std::process::ExitCode;

use hopdns::config::USAGE;
use hopdns::host::Daemon;
use hopdns::{Error, Flags};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let flags = match Flags::parse(args.iter().map(String::as_str)) {
        Ok(f) => f,
        Err(Error::Help) => {
            println!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Err(e) => {
            let at = match e {
                Error::UnknownFlag { at }
                | Error::MissingValue { at }
                | Error::UnexpectedArgument { at } => args.get(at).map(String::as_str),
                _ => None,
            };
            match at {
                Some(arg) => eprintln!("hopdns: {arg}: {e}\n{USAGE}"),
                None => eprintln!("hopdns: {e}\n{USAGE}"),
            }
            return ExitCode::from(2);
        }
    };
    let result = Daemon::start(&flags).and_then(Daemon::serve);
    if let Err(e) = result {
        eprintln!("hopdns: DNS server error: {e} HOPDNS_FATAL");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
