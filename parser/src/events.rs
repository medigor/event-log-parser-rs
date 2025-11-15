use crate::{
    parser::{LogStr, ParseError, ParseResult, Parser},
    references::{Metadata, References, User},
};
use chrono::{NaiveDate, NaiveDateTime};
use core::str;
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

    pub fn user<'refs>(&self, refs: &'refs References) -> Option<&'refs User> {
        refs.users().get(self.user_id)
    }

    pub fn computer_id(&self) -> usize {
        self.computer_id
    }

    pub fn computer<'refs>(&self, refs: &'refs References) -> Option<&'refs str> {
        refs.computers().get(self.computer_id).map(|x| x.as_str())
    }

    pub fn application_id(&self) -> usize {
        self.application_id
    }

    pub fn application<'refs>(&self, refs: &'refs References) -> Option<&'refs str> {
        refs.applications()
            .get(self.application_id)
            .map(|x| x.as_str())
    }

    pub fn connection(&self) -> usize {
        self.connection
    }

    pub fn event_id(&self) -> usize {
        self.event_id
    }

    pub fn event<'refs>(&self, refs: &'refs References) -> Option<&'refs str> {
        refs.events().get(self.event_id).map(|s| s.as_str())
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

    pub fn metadata<'refs>(&self, refs: &'refs References) -> Option<&'refs Metadata> {
        refs.metadata().get(self.metadata_id())
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

    pub fn worker_server<'refs>(&self, refs: &'refs References) -> Option<&'refs str> {
        refs.worker_servers().get(self.worker_server_id).map(|x| x.as_str())
    }

    pub fn port_id(&self) -> usize {
        self.port_id
    }

    pub fn port(&self, refs: &References) -> Option<u32> {
        refs.ports().get(self.port_id).copied()
    }

    pub fn sync_port_id(&self) -> usize {
        self.sync_port_id
    }

    pub fn sync_port(&self, refs: &References) -> Option<u32> {
        refs.sync_ports().get(self.sync_port_id).copied()
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

pub fn parse_file<F, P>(file_name: P, action: &mut F) -> io::Result<()>
where
    F: FnMut(Event),
    P: AsRef<Path>,
{
    let mut reader = File::open(file_name.as_ref())?;

    let mut buffer = vec![0_u8; 512 * 1024];
    let mut offset = 0usize;

    loop {
        let len = reader.read(&mut buffer[offset..])?;
        if len == 0 {
            break;
        }
        let len = len + offset;
        let read = parse_buffer(&buffer[0..len], action);

        if read == 0 {
            buffer.extend((0..buffer.len()).map(|_| 0));
        } else {
            for i in read..len {
                buffer[i - read] = buffer[i];
            }
            offset = len - read;
        }
    }

    Ok(())
}

pub fn parse_buffer<F>(buffer: &[u8], action: &mut F) -> usize
where
    F: FnMut(Event),
{
    let mut parser = Parser::new(buffer);
    loop {
        let position = parser.position();
        match parse_record(&mut parser) {
            Ok(event) => action(event),
            Err(ParseError::End) => return position,
            Err(ParseError::InvalidFormat) => {
                if parser.skip_to(b'\r').is_err() {
                    return position;
                }
            }
        }
    }
}

fn parse_record<'a>(parser: &'a mut Parser) -> ParseResult<Event<'a>> {
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

    Ok(Event {
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

fn parse_datetime(parser: &mut Parser) -> ParseResult<NaiveDateTime> {
    fn next2(parser: &mut Parser) -> ParseResult<u32> {
        Ok((parser.next()? - b'0') as u32 * 10 + (parser.next()? - b'0') as u32)
    }

    let year = next2(parser)? * 100 + next2(parser)?;
    let month = next2(parser)?;
    let day = next2(parser)?;
    let hour = next2(parser)?;
    let min = next2(parser)?;
    let sec = next2(parser)?;
    parser.skip(1)?;

    let date = NaiveDate::from_ymd_opt(year as i32, month, day)
        .ok_or(ParseError::InvalidFormat)?
        .and_hms_opt(hour, min, sec)
        .ok_or(ParseError::InvalidFormat)?;
    Ok(date)
}

fn parse_transaction_status(parser: &mut Parser) -> ParseResult<TransactionStatus> {
    let ch = parser.next()?;
    parser.skip(1)?;
    Ok(match ch {
        b'R' => TransactionStatus::RolledBack,
        b'N' => TransactionStatus::NotApplicable,
        b'U' => TransactionStatus::Unfinished,
        b'C' => TransactionStatus::Committed,
        _ => return Err(ParseError::InvalidFormat),
    })
}

fn parse_log_level(parser: &mut Parser) -> ParseResult<EventLogLevel> {
    let ch = parser.next()?;
    parser.skip(1)?;
    Ok(match ch {
        b'E' => EventLogLevel::Error,
        b'I' => EventLogLevel::Information,
        b'N' => EventLogLevel::Note,
        b'W' => EventLogLevel::Warning,
        _ => return Err(ParseError::InvalidFormat),
    })
}
