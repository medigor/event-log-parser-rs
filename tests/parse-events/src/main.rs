use std::{env, hint::black_box, io, time::Instant};

fn main() -> io::Result<()> {
    let Some(file_name) = env::args().nth(1) else {
        println!("Usage: parse-events /path/to/file/*.lgp");
        return Ok(());
    };

    let mut count = 0;
    let now = Instant::now();
    event_log_parser::events::parse(file_name, &mut |event| {
        black_box(event);
        count += 1;
    })?;
    println!(
        "duration: {} ms",
        (now.elapsed().as_nanos() as f64) / 1_000_000f64
    );
    println!("count: {count}");
    Ok(())
}
