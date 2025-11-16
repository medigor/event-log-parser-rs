use crate::parser::{ParseError, ParseResult, Parser};
use std::cmp::Ordering;
use std::{fs::File, io::Read};
use std::{io, path::Path};
use uuid::Uuid;

#[derive(Default, Debug)]
pub struct User {
    id: Uuid,
    name: String,
}

impl User {
    pub fn name(&self) -> &str {
        self.name.as_ref()
    }

    pub fn id(&self) -> Uuid {
        self.id
    }
}

#[derive(Default)]
pub struct Metadata {
    id: Uuid,
    name: String,
}

impl Metadata {
    pub fn name(&self) -> &str {
        self.name.as_ref()
    }

    pub fn id(&self) -> Uuid {
        self.id
    }
}

#[cfg(feature = "data-separation")]
#[derive(Default)]
pub struct DataSeparation {
    id: Uuid,
    name: String,
    values: Vec<String>,
}

#[cfg(feature = "data-separation")]
impl DataSeparation {
    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn name(&self) -> &str {
        self.name.as_ref()
    }

    pub fn values(&self) -> &[String] {
        self.values.as_ref()
    }
}

#[derive(Default)]
pub struct References {
    users: Vec<User>,
    computers: Vec<String>,
    applications: Vec<String>,
    events: Vec<String>,
    metadata: Vec<Metadata>,
    worker_servers: Vec<String>,
    ports: Vec<u32>,
    sync_ports: Vec<u32>,
    #[cfg(feature = "data-separation")]
    data_separation: Vec<DataSeparation>,
}

impl References {
    pub fn parse_file<P: AsRef<Path>>(&mut self, path: P) -> io::Result<()> {
        let mut reader = File::open(path)?;

        let mut buffer = vec![0u8; 512 * 1024];
        let mut offset = 0usize;

        loop {
            let len = reader.read(&mut buffer[offset..])?;
            if len == 0 {
                break;
            }
            let len = len + offset;
            let read = self.parse_buffer(&buffer[0..len]);

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

    pub fn parse_buffer(&mut self, buffer: &[u8]) -> usize {
        let mut parser = Parser::new(buffer);
        loop {
            let position = parser.position();
            match self.parser_record(&mut parser) {
                Ok(_) => (),
                Err(ParseError::End) => return position,
                Err(ParseError::InvalidFormat) => {
                    if parser.skip_to(b'\r').is_err() {
                        return position;
                    }
                }
            }
        }
    }

    fn parser_record(&mut self, parser: &mut Parser) -> ParseResult<()> {
        fn add_ref<T: Default>(vec: &mut Vec<T>, value: T, num: usize) {
            match num.cmp(&vec.len()) {
                Ordering::Less => vec[num] = value,
                Ordering::Equal => vec.push(value),
                Ordering::Greater => {
                    for _ in 0..num - vec.len() {
                        vec.push(T::default());
                    }
                    vec.push(value);
                }
            }
        }

        while parser.next()? != b'{' {}

        match parser.parse_usize()? {
            1 => {
                let id = parser.parse_uuid()?;
                let name = parser.parse_str()?.str().to_string();
                let num = parser.parse_usize()?;
                let user = User { name, id };
                add_ref(&mut self.users, user, num);
            }
            2 => {
                let name = parser.parse_str()?.str().to_string();
                let num = parser.parse_usize()?;
                add_ref(&mut self.computers, name, num);
            }
            3 => {
                let name = parser.parse_str()?.str().to_string();
                let num = parser.parse_usize()?;
                add_ref(&mut self.applications, name, num);
            }
            4 => {
                let name = parser.parse_str()?.str().to_string();
                let num = parser.parse_usize()?;
                add_ref(&mut self.events, name, num);
            }
            5 => {
                let id = parser.parse_uuid()?;
                let name = parser.parse_str()?.str().to_string();
                let num = parser.parse_usize()?;
                let metadata = Metadata { name, id };
                add_ref(&mut self.metadata, metadata, num);
            }
            6 => {
                let name = parser.parse_str()?.str().to_string();
                let num = parser.parse_usize()?;
                add_ref(&mut self.worker_servers, name, num);
            }
            7 => {
                let port = parser.parse_usize()? as u32;
                let num = parser.parse_usize()?;
                add_ref(&mut self.ports, port, num);
            }
            8 => {
                let port = parser.parse_usize()? as u32;
                let num = parser.parse_usize()?;
                add_ref(&mut self.sync_ports, port, num);
            }
            9 => {
                #[cfg(not(feature = "data-separation"))]
                {
                    parser.parse_uuid()?;
                    parser.parse_str()?;
                    parser.parse_usize()?;
                }
                #[cfg(feature = "data-separation")]
                {
                    let id = parser.parse_uuid()?;
                    let name = parser.parse_str()?.str().to_string();
                    let num = parser.parse_usize()?;
                    let data_separation = DataSeparation {
                        id,
                        name,
                        values: Vec::new(),
                    };
                    add_ref(&mut self.data_separation, data_separation, num);
                }
            }
            10 => {
                #[cfg(not(feature = "data-separation"))]
                {
                    parser.parse_object()?;
                    parser.parse_usize()?;
                    parser.parse_usize()?;
                }
                #[cfg(feature = "data-separation")]
                {
                    let obj = parser.parse_object()?.to_string();
                    let ind = parser.parse_usize()?;
                    let num = parser.parse_usize()?;
                    let vec = &mut self.data_separation[ind].values;
                    add_ref(vec, obj, num);
                }
            }
            11 | 12 => {
                let _obj = parser.parse_object()?;
                let _num = parser.parse_usize()?;
            }
            13 => {
                let _num = parser.parse_usize()?;
                let _num = parser.parse_usize()?;
            }
            _ => (),
        }
        Ok(())
    }

    pub fn users(&self) -> &[User] {
        self.users.as_ref()
    }

    pub fn computers(&self) -> &[String] {
        self.computers.as_ref()
    }

    pub fn applications(&self) -> &[String] {
        self.applications.as_ref()
    }

    pub fn events(&self) -> &[String] {
        self.events.as_ref()
    }

    pub fn metadata(&self) -> &[Metadata] {
        self.metadata.as_ref()
    }

    pub fn worker_servers(&self) -> &[String] {
        self.worker_servers.as_ref()
    }

    pub fn ports(&self) -> &[u32] {
        self.ports.as_ref()
    }

    pub fn sync_ports(&self) -> &[u32] {
        self.sync_ports.as_ref()
    }
}

#[cfg(test)]
mod tests {

    use uuid::uuid;

    use crate::{
        parser::{ParseResult, Parser},
        references::References,
    };

    #[test]
    fn test_parse_record_1() -> ParseResult<()> {
        let mut references = References::default();
        let buf = br#" {1,d303f30c-9e76-412f-95d2-3c3622e6b6e1,"Executor",1}"#;
        let mut parser = Parser::new(buf);

        references.parser_record(&mut parser)?;
        let user = &references.users[1];

        assert_eq!(user.id, uuid!("d303f30c-9e76-412f-95d2-3c3622e6b6e1"));
        assert_eq!(user.name, "Executor");
        Ok(())
    }
}
