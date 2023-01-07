use std::{borrow::Cow, str::FromStr};

use uuid::Uuid;

pub struct LogStr<'a> {
    s: &'a str,
    need_replace_quotes: bool,
}

impl<'a> LogStr<'a> {
    pub fn str(&self) -> Cow<'a, str> {
        if self.need_replace_quotes {
            Cow::Owned(self.s.replace(r#""""#, r#"""#))
        } else {
            Cow::Borrowed(self.s)
        }
    }
}

pub struct Parser<'a> {
    buffer: &'a [u8],
    position: usize,
}

impl<'a> Parser<'a> {
    pub fn new(buffer: &[u8]) -> Parser {
        Parser {
            buffer,
            position: 0,
        }
    }

    pub fn position(&self) -> usize {
        self.position
    }

    pub fn next(&mut self) -> Option<u8> {
        if self.position < self.buffer.len() {
            let r = self.buffer[self.position];
            self.position += 1;
            Some(r)
        } else {
            None
        }
    }

    pub fn skip(&mut self, count: usize) -> Option<()> {
        if (self.position + count) < self.buffer.len() {
            self.position += count;
            Some(())
        } else {
            None
        }
    }

    pub fn current(&self) -> u8 {
        if self.position == 0 {
            panic!("before need to call next()")
        }
        self.buffer[self.position - 1]
    }

    pub fn peek(&self) -> Option<u8> {
        if (self.position) < self.buffer.len() {
            Some(self.buffer[self.position])
        } else {
            None
        }
    }

    pub fn parse_usize(&mut self) -> Option<usize> {
        let mut number: usize = 0;
        loop {
            let next = self.next()?;
            if next == b',' || next == b'}' {
                break;
            }
            number = number * 10 + (next - b'0') as usize;
        }
        Some(number)
    }

    pub fn parse_raw(&mut self) -> Option<&'a [u8]> {
        let start = self.position;
        loop {
            let next = self.next()?;
            if next == b',' || next == b'}' {
                break;
            }
        }
        Some(&self.buffer[start..self.position - 1])
    }

    pub fn parse_uuid(&mut self) -> Option<Uuid> {
        let raw = self.parse_raw()?;
        unsafe {
            let s = std::str::from_utf8_unchecked(raw);
            Some(Uuid::from_str(s).unwrap())
        }
    }

    pub fn parse_str(&mut self) -> Option<LogStr<'a>> {
        let ch = self.next()?;
        if ch != b'"' {
            let s = &self.buffer[self.position..self.position + 20];
            unsafe {
                let s = std::str::from_utf8_unchecked(s);
                panic!("Invalid data 1: {}", s);
            }
        }
        let position = self.position;
        let mut need_replace_quotes = false;

        loop {
            if self.next()? == b'"' {
                let next = self.next()?;
                if next == b',' || next == b'}' {
                    break;
                } else if next == b'"' {
                    need_replace_quotes = true;
                }
            }
        }

        let s = unsafe { std::str::from_utf8_unchecked(&self.buffer[position..self.position - 2]) };
        Some(LogStr {
            s,
            need_replace_quotes,
        })
    }

    pub fn parse_object(&mut self) -> Option<&'a str> {
        // Перейти к '{'
        while self.next()? != b'{' {}

        // Запомнить начало строки
        let position = self.position - 1;
        let mut end_of_record = false;

        while !end_of_record {
            let peek = self.peek()?;
            match peek {
                b'"' => {
                    self.parse_str()?;
                }
                b'{' => {
                    self.parse_object()?;
                }
                b'\r' => self.skip(2)?,
                _ => {
                    self.parse_raw()?;
                }
            }
            end_of_record = self.current() == b'}';
        }
        let mut last = self.next()?;
        if last == b'\r' {
            self.skip(1)?;
            last = self.next()?;
        }
        if last != b',' && last != b'}' {
            let mut s = &self.buffer[self.position..];
            if s.len() > 20 {
                s = &s[..20];
            }
            unsafe {
                let s = std::str::from_utf8_unchecked(s);
                panic!("Invalid data 2: {}", s);
            }
        }
        unsafe {
            Some(std::str::from_utf8_unchecked(
                &self.buffer[position..self.position - 1],
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_u32() {
        let buf = b"12345,";
        let mut parser = Parser::new(buf);
        let n = parser.parse_usize().unwrap();
        assert_eq!(n, 12345);
    }

    #[test]
    fn test_parse_raw() {
        let buf = b"12345,";
        let mut parser = Parser::new(buf);
        let r = parser.parse_raw().unwrap();
        assert_eq!(r, b"12345");
    }

    #[test]
    fn test_parse_none() {
        let buf = b"1111,12345";
        let mut parser = Parser::new(buf);
        parser.skip(5);
        let r = parser.parse_raw();
        assert_eq!(r, None)
    }

    #[test]
    fn test_parse_uuid() {
        let buf = b"71ada582-5c75-466a-b17c-7b9a48af5f0b}";
        let mut parser = Parser::new(buf);
        let uuid = parser.parse_uuid().unwrap();
        assert_eq!(
            uuid,
            Uuid::from_str("71ada582-5c75-466a-b17c-7b9a48af5f0b").unwrap()
        );
    }

    #[test]
    fn test_parse_str_1() {
        let buf = b"\"12345\"}";
        let mut parser = Parser::new(buf);
        let str = parser.parse_str().unwrap();
        assert_eq!(str.str(), "12345");
    }

    #[test]
    fn test_parse_str_2() {
        let buf = br#""123""45"}"#;
        let mut parser = Parser::new(buf);
        let str = parser.parse_str().unwrap();

        assert_eq!(str.str(), r#"123"45"#);
    }

    #[test]
    fn test_parse_object_1() {
        let buf = br#"   {1,"N"}, 321"#;
        let mut parser = Parser::new(buf);
        let res = parser.parse_object().unwrap();
        assert_eq!(res, r#"{1,"N"}"#);
    }

    #[test]
    fn test_parse_object_2() {
        let buf = br#"   {1,2,3,"123",{1,"N"}}, 321"#;
        let mut parser = Parser::new(buf);
        let res = parser.parse_object().unwrap();
        assert_eq!(res, r#"{1,2,3,"123",{1,"N"}}"#);
    }
}
