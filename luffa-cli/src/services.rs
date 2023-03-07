use anyhow::{anyhow, Result};
use crossterm::terminal::{Clear, ClearType};
use crossterm::{cursor, style, style::Stylize, QueueableCommand};
use futures::StreamExt;
use luffa_rpc_client::{ClientStatus, ServiceStatus, StatusType};
use luffa_util::luffa_cache_path;
use std::collections::BTreeSet;
use std::io::{stdout, Write};
use std::ops::Deref;
use std::time::SystemTime;
use sysinfo::PidExt;
use tracing::info;

use luffa_util::lock::{LockError, ProgramLock};

use crate::api::Api;
use crate::error::ApiError;

const SERVICE_START_TIMEOUT_SECONDS: u64 = 15;

/// Start any given services that aren't currently running.
pub async fn start(api: &Api, services: &Vec<String>) -> Result<()> {
    let services = match services.is_empty() {
        true => BTreeSet::from(["node", "relay"]),
        false => {
            let mut hs: BTreeSet<&str> = BTreeSet::new();
            for s in services {
                hs.insert(s.as_str());
            }
            hs
        }
    };
    start_services(api, services).await
}

async fn start_services(api: &Api, services: BTreeSet<&str>) -> Result<()> {
    // check for any running luffa services
    let table = api.check().await;

    let mut expected_services = BTreeSet::new();
    let expected_services = table
        .iter()
        .fold(&mut expected_services, |accum, status_row| {
            accum.insert(status_row.name());
            accum
        });

    let unknown_services: BTreeSet<&str> =
        services.difference(expected_services).copied().collect();

    if !unknown_services.is_empty() {
        let u = unknown_services.into_iter().collect::<Vec<&str>>();
        let mut e = "Unknown services";
        if u.len() == 1 {
            e = "Unknown service";
        }
        return Err(anyhow!("{} {}.", e, u.join(", ")));
    }

    let mut missing_services = BTreeSet::new();
    let missing_services = table
        .iter()
        .fold(&mut missing_services, |accum, status_row| {
            match status_row.status() {
                StatusType::Serving => (),
                StatusType::Unknown => {
                    accum.insert(status_row.name());
                }
                StatusType::NotServing => {
                    accum.insert(status_row.name());
                }
                StatusType::Down => {
                    accum.insert(status_row.name());
                    // TODO(b5) - warn user that a service is down & exit
                }
            }
            accum
        });

    let missing_services: BTreeSet<&str> = services
        .intersection(missing_services)
        .map(Deref::deref)
        .collect();

    if missing_services.is_empty() {
        println!(
            "{}",
            "All luffa daemons are already running. all systems normal.".green()
        );
        return Ok(());
    }

    for service in missing_services.iter() {
        let daemon_name = format!("luffa-{service}");
        let log_path = luffa_cache_path(format!("luffa-{service}.log").as_str())?;

        // check if a binary by this name exists
        let bin_path = which::which(&daemon_name).map_err(|_| {
            anyhow!(format!(
                "can't find {} daemon binary on your $PATH. please install {}.",
                &daemon_name, &daemon_name
            ))
        })?;

        print!("starting {}... ", &daemon_name.bold());

        luffa_process::process::daemonize(bin_path, log_path.clone())?;

        let is_up = poll_until_status(api, service, StatusType::Serving).await?;
        if is_up {
            println!("{}", "success".green());
        } else {
            eprintln!(
                "{}",
                format!(
                    "error: took more than {}s start.\ncheck log file for details: {}",
                    SERVICE_START_TIMEOUT_SECONDS,
                    log_path.display(),
                )
                .red()
            );
        }
    }

    Ok(())
}

/// stop the default set of services by sending SIGINT to any active daemons
/// identified by lockfiles
pub async fn stop(api: &Api, services: &Vec<String>) -> Result<()> {
    let services = match services.is_empty() {
        true => BTreeSet::from(["node", "relay"]),
        false => {
            let mut hs: BTreeSet<&str> = BTreeSet::new();
            for s in services {
                hs.insert(s.as_str());
            }
            hs
        }
    };
    stop_services(api, services).await
}

pub async fn stop_services(api: &Api, services: BTreeSet<&str>) -> Result<()> {
    for service in services {
        let daemon_name = format!("luffa-{service}");
        info!("checking daemon {} lock", daemon_name);
        let mut lock = ProgramLock::new(&daemon_name)?;
        match lock.active_pid() {
            Ok(pid) => {
                info!("stopping {} pid: {}", daemon_name, pid);
                print!("stopping {}... ", &daemon_name);
                match luffa_process::process::stop(pid.as_u32()) {
                    Ok(_) => {
                        let is_down = poll_until_status(api, service, StatusType::Down).await?;
                        if is_down {
                            println!("{}", "stopped".red());
                        } else {
                            eprintln!("{}", format!("{service} API is still running, but the lock is removed.\nYou may need to manually stop luffa via your operating system").red());
                        }
                    }
                    Err(error) => {
                        println!("{}: {}", "error".yellow(), error);
                    }
                }
            }
            Err(e) => match e {
                LockError::NoLock(_) => {
                    eprintln!("{}", format!("{daemon_name} is already stopped").white());
                }
                LockError::NoSuchProcess(_, _) => {
                    lock.destroy_without_checking().unwrap();
                    println!(
                        "stopping {}:, {}",
                        daemon_name,
                        "removed zombie lockfile".red()
                    );
                }
                e => {
                    eprintln!("{daemon_name} lock error: {e}");
                    continue;
                }
            },
        }
    }
    Ok(())
}

pub async fn status(api: &Api, watch: bool) -> Result<()> {
    let mut stdout = stdout();
    if watch {
        let status_stream = api.watch().await;
        tokio::pin!(status_stream);
        stdout.queue(Clear(ClearType::All))?;
        while let Some(table) = status_stream.next().await {
            stdout
                .queue(cursor::RestorePosition)?
                .queue(Clear(ClearType::FromCursorUp))?
                .queue(cursor::MoveTo(0, 1))?;
            queue_table(&table, &stdout)?;
            stdout
                .queue(cursor::SavePosition)?
                .queue(style::Print("\n"))?
                .flush()?;
        }
        Ok(())
    } else {
        let table = api.check().await;
        queue_table(&table, &stdout)?;
        stdout.flush()?;
        Ok(())
    }
}

/// queues the table for printing
/// you must call `writer.flush()` to execute the queue
pub fn queue_table<W>(table: &ClientStatus, mut w: W) -> Result<()>
where
    W: Write,
{
    w.queue(style::PrintStyledContent(
        "Service\t\tVersion\t\tStatus\n".bold(),
    ))?;
    queue_row(&table.p2p, &mut w)?;
    queue_row(&table.store, &mut w)?;
    Ok(())
}

// queue queues this row of the ServiceStatus to be written
// You must call `writer.flush()` to actually write the content to the writer
pub fn queue_row<W>(row: &ServiceStatus, w: &mut W) -> Result<()>
where
    W: Write,
{
    w.queue(style::Print(format!("{}\t\t", row.name())))?
        .queue(style::Print(format!("{}\t\t", row.version())))?;
    match row.status() {
        StatusType::Unknown => {
            w.queue(style::PrintStyledContent("Unknown".dark_yellow()))?;
        }
        StatusType::NotServing => {
            w.queue(style::PrintStyledContent("Not Serving".dark_yellow()))?;
        }
        StatusType::Serving => {
            w.queue(style::PrintStyledContent("Serving".green()))?;
        }
        StatusType::Down => {
            w.queue(style::PrintStyledContent("Down".grey()))?
                .queue(style::Print("\tThe service is currently unavailable"))?;
        }
    };
    w.queue(style::Print("\n"))?;
    Ok(())
}

/// require a set of services is up. returns the underlying status table of all
/// services for additional scrutiny
pub async fn require_services(api: &Api, services: BTreeSet<&str>) -> Result<ClientStatus> {
    let table = api.check().await;
    for service in table.iter() {
        if services.contains(service.name()) && service.status() != StatusType::Serving {
            return Err(anyhow!(ApiError::ConnectionRefused {
                service: service.name()
            }));
        }
    }
    Ok(table)
}

/// poll until a service matches the desired status. returns Ok(true) if status was matched,
/// and Ok(false) if desired status isn't reported before SERVICE_START_TIMEOUT_SECONDS
async fn poll_until_status(api: &Api, service: &str, status: StatusType) -> Result<bool> {
    let status_stream = api.watch().await;
    tokio::pin!(status_stream);
    let start = SystemTime::now();
    while let Some(table) = status_stream.next().await {
        let is_status = table
            .iter()
            .filter(|row| row.name() == service)
            .map(|row| row.status() == status)
            .next()
            .unwrap();
        if is_status {
            return Ok(true);
        }
        if let Ok(elapsed) = start.elapsed() {
            if elapsed.as_secs() > SERVICE_START_TIMEOUT_SECONDS {
                return Ok(false);
            }
        }
    }
    Err(anyhow!(""))
}
