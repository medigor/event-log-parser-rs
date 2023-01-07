use std::{
    collections::HashMap,
    env,
    fs::{metadata, read_dir},
    io,
    path::Path,
    time::Instant,
};

use event_log_parser::references::References;

fn main() -> io::Result<()> {
    
    let now = Instant::now();

    let Some(dir_name) = env::args().skip(1).next() else {
        println!("Usage: parse-events /path/to/log/dir");
        return Ok(());
    };

    let mut refs = References::new();
    refs.parse(Path::new(&dir_name).join("1Cv8.lgf"))?;

    let session_start_id = *refs
        .events()
        .iter()
        .position(|x| x == "_$Session$_.Start")
        .get_or_insert(0);
    let data_new_id = *refs
        .events()
        .iter()
        .position(|x| x == "_$Data$_.New")
        .get_or_insert(0);
    let data_update_id = *refs
        .events()
        .iter()
        .position(|x| x == "_$Data$_.Update")
        .get_or_insert(0);

    let mut total_events = 0;
    let mut total_log_size = 0;

    let mut total_error = 0;
    let mut total_information = 0;
    let mut total_note = 0;
    let mut total_warning = 0;

    let mut total_session_start = 0;
    let mut total_data_new = 0;
    let mut total_data_update = 0;

    let mut top_errors = HashMap::<usize, usize>::new();

    for entry in read_dir(dir_name)? {
        let entry = entry?;
        if entry.file_name() == "1Cv8.lgf" {
            continue;
        }
        let meta = metadata(entry.path())?;
        total_log_size += meta.len();
        event_log_parser::events::parse(entry.path(), &mut |event| {
            match event.log_level() {
                event_log_parser::events::EventLogLevel::Error => {
                    total_error += 1;
                    top_errors
                        .entry(event.event_id())
                        .and_modify(|counter| *counter += 1)
                        .or_insert(1);
                }
                event_log_parser::events::EventLogLevel::Information => total_information += 1,
                event_log_parser::events::EventLogLevel::Note => total_note += 1,
                event_log_parser::events::EventLogLevel::Warning => total_warning += 1,
            }

            if event.event_id() == session_start_id {
                total_session_start += 1;
            } else if event.event_id() == data_new_id {
                total_data_new += 1;
            } else if event.event_id() == data_update_id {
                total_data_update += 1;
            }

            total_events += 1;
        })?;
    }

    println!(
        "duration: {} ms",
        (now.elapsed().as_nanos() as f64) / 1_000_000f64
    );
    println!(
        "Total log size: {:.3} Mb",
        total_log_size as f64 / 1024f64 / 1024f64
    );
    println!("Total Events: {total_events}");

    println!("==========");
    println!("LogLevel.Error: {total_error}");
    println!("LogLevel.Warning: {total_warning}");
    println!("LogLevel.Information: {total_information}");
    println!("LogLevel.Note: {total_note}");

    println!("==========");
    println!("Session.Start: {total_session_start}");
    println!("Data.New: {total_data_new}");
    println!("Data.Update: {total_data_update}");

    println!("==========");
    println!("Top 10 errors:");

    let mut top_errors: Vec<(usize, usize)> = top_errors.into_iter().collect();
    top_errors.sort_by(|a, b| b.1.cmp(&a.1));
    for error in top_errors.iter().take(10) {
        println!("  {}: {}", refs.events()[error.0], error.1);
    }

    Ok(())
}
