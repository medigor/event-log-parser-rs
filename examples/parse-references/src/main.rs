use std::{env, io, time::Instant};

use event_log_parser_1c::references::References;

fn main() -> io::Result<()> {
    let Some(file_name) = env::args().nth(1) else {
        println!("Usage: parse-references /path/to/file/1Cv8.lgf");
        return Ok(());
    };
    let now = Instant::now();
    let mut refs = References::default();
    refs.parse_file(file_name)?;
    println!(
        "duration: {} ms",
        (now.elapsed().as_nanos() as f64) / 1_000_000f64
    );
    Ok(())
}
