use crate::{
    parser::{LogStr, Parser},
    references::{Metadata, References, User},
};
use chrono::{NaiveDate, NaiveDateTime};
use std::{borrow::Cow, io, path::Path};
use std::{fs::File, io::Read};

pub enum TransactionStatus {
    Unfinished,
    NotApplicable,
    Committed,
    RolledBack,
}

pub enum EventLogLevel {
    Error,
    Information,
    Note,
    Warning,
}

pub struct Event<'a> {
    date: NaiveDateTime,
    transaction_status: TransactionStatus,
    transaction_data: &'a str,
    user_id: usize,
    computer_id: usize,
    application_id: usize,
    connection: usize,
    event_id: usize,
    log_level: EventLogLevel,
    comment: LogStr<'a>,
    metadata_id: usize,
    data: &'a str,
    data_presentation: LogStr<'a>,
    worker_server_id: usize,
    port_id: usize,
    sync_port_id: usize,
    session: usize,
    unknown1: usize,
    unknown2: &'a str,
}

impl<'a> Event<'a> {
    pub fn date(&self) -> NaiveDateTime {
        self.date
    }

    pub fn transaction_status(&self) -> &TransactionStatus {
        &self.transaction_status
    }

    pub fn transaction_data(&self) -> &str {
        self.transaction_data
    }

    pub fn user_id(&self) -> usize {
        self.user_id
    }

    pub fn user<'refs>(&self, refs: &'refs References) -> &'refs User {
        &refs.users()[self.user_id]
    }

    pub fn computer_id(&self) -> usize {
        self.computer_id
    }

    pub fn computer<'refs>(&self, refs: &'refs References) -> &'refs str {
        &refs.computers()[self.computer_id]
    }

    pub fn application_id(&self) -> usize {
        self.application_id
    }

    pub fn application<'refs>(&self, refs: &'refs References) -> &'refs str {
        &refs.applications()[self.application_id]
    }

    pub fn connection(&self) -> usize {
        self.connection
    }

    pub fn event_id(&self) -> usize {
        self.event_id
    }

    pub fn event<'refs>(&self, refs: &'refs References) -> &'refs str {
        &refs.events()[self.event_id]
    }

    pub fn log_level(&self) -> &EventLogLevel {
        &self.log_level
    }

    pub fn comment(&self) -> Cow<'a, str> {
        self.comment.str()
    }

    pub fn metadata_id(&self) -> usize {
        self.metadata_id
    }

    pub fn metadata<'refs>(&self, refs: &'refs References) -> &'refs Metadata {
        &refs.metadata()[self.metadata_id]
    }

    pub fn data(&self) -> &str {
        self.data
    }

    pub fn data_presentation(&self) -> Cow<'a, str> {
        self.data_presentation.str()
    }

    pub fn worker_server_id(&self) -> usize {
        self.worker_server_id
    }

    pub fn worker_server<'refs>(&self, refs: &'refs References) -> &'refs str {
        &refs.worker_servers()[self.worker_server_id]
    }

    pub fn port_id(&self) -> usize {
        self.port_id
    }

    pub fn port(&self, refs: &References) -> u32 {
        refs.ports()[self.port_id]
    }

    pub fn sync_port_id(&self) -> usize {
        self.sync_port_id
    }

    pub fn sync_port(&self, refs: &References) -> u32 {
        refs.sync_ports()[self.sync_port_id]
    }

    pub fn session(&self) -> usize {
        self.session
    }

    pub fn unknown1(&self) -> usize {
        self.unknown1
    }

    pub fn unknown2(&self) -> &str {
        self.unknown2
    }
}

pub fn parse<F, P>(file_name: P, action: &mut F) -> io::Result<()>
where
    F: FnMut(Event),
    P: AsRef<Path>,
{
    let mut reader = File::open(file_name)?;

    let mut buffer = Box::new([0u8; 1024 * 1024]);
    let mut offset = 0usize;

    loop {
        let len = reader.read(&mut buffer[offset..])?;
        if len == 0 {
            break;
        }
        let len = len + offset;
        let read = parse_buffer(&buffer[0..len], action);

        if read == 0 {
            panic!("buffer too small")
        }

        for i in read..len {
            buffer[i - read] = buffer[i];
        }
        offset = len - read;
    }

    Ok(())
}

fn parse_buffer<F>(buffer: &[u8], action: &mut F) -> usize
where
    F: FnMut(Event),
{
    let mut parser = Parser::new(buffer);
    loop {
        let position = parser.position();
        match parse_record(&mut parser) {
            Some(event) => action(event),
            None => return position,
        }
    }
}

fn parse_record<'a>(parser: &'a mut Parser) -> Option<Event<'a>> {
    while parser.next()? != b'{' {}

    let date = parse_datetime(parser)?;
    let transaction_status = parse_transaction_status(parser)?;
    let transaction_data = parser.parse_object()?;
    let user_id = parser.parse_usize()?;
    let computer_id = parser.parse_usize()?;
    let application_id = parser.parse_usize()?;
    let connection = parser.parse_usize()?;
    let event_id = parser.parse_usize()?;
    let log_level = parse_log_level(parser)?;
    let comment = parser.parse_str()?;
    let metadata_id = parser.parse_usize()?;
    let data = parser.parse_object()?;
    let data_presentation = parser.parse_str()?;
    let worker_server_id = parser.parse_usize()?;
    let port_id = parser.parse_usize()?;
    let sync_port_id = parser.parse_usize()?;
    let session = parser.parse_usize()?;
    let unknown1 = parser.parse_usize()?;
    let unknown2 = parser.parse_object()?;

    Some(Event {
        date,
        transaction_status,
        transaction_data,
        user_id,
        computer_id,
        application_id,
        connection,
        event_id,
        log_level,
        comment,
        metadata_id,
        data,
        data_presentation,
        worker_server_id,
        port_id,
        sync_port_id,
        session,
        unknown1,
        unknown2,
    })
}

fn parse_datetime(parser: &mut Parser) -> Option<NaiveDateTime> {
    fn next2(parser: &mut Parser) -> Option<u32> {
        Some((parser.next()? - b'0') as u32 * 10 + (parser.next()? - b'0') as u32)
    }

    let year = next2(parser)? * 100 + next2(parser)?;
    let month = next2(parser)?;
    let day = next2(parser)?;
    let hour = next2(parser)?;
    let min = next2(parser)?;
    let sec = next2(parser)?;
    parser.skip(1)?;

    let date = NaiveDate::from_ymd_opt(year as i32, month, day)
        .expect("Invalid file format")
        .and_hms_opt(hour, min, sec)
        .expect("Invalid file format");
    Some(date)
}

fn parse_transaction_status(parser: &mut Parser) -> Option<TransactionStatus> {
    let ch = parser.next()?;
    parser.skip(1)?;
    Some(match ch {
        b'R' => TransactionStatus::RolledBack,
        b'N' => TransactionStatus::NotApplicable,
        b'U' => TransactionStatus::Unfinished,
        b'C' => TransactionStatus::Committed,
        _ => panic!("Unknown transaction status: {ch}"),
    })
}

fn parse_log_level(parser: &mut Parser) -> Option<EventLogLevel> {
    let ch = parser.next()?;
    parser.skip(1)?;
    Some(match ch {
        b'E' => EventLogLevel::Error,
        b'I' => EventLogLevel::Information,
        b'N' => EventLogLevel::Note,
        b'W' => EventLogLevel::Warning,
        _ => panic!("Unknown log level: {ch}"),
    })
}
