use std::{borrow::Cow, cmp::min, marker::PhantomData, str::FromStr};
use uuid::Uuid;

pub struct LogStr<'a> {
    str: &'a [u8],
    need_replace_quotes: bool,
}

impl<'a> LogStr<'a> {
    pub fn new(str: &'a [u8], need_replace_quotes: bool) -> LogStr {
        LogStr {
            str,
            need_replace_quotes,
        }
    }
    pub fn str(&self) -> Cow<'a, str> {
        let str = String::from_utf8_lossy(self.str);
        match self.need_replace_quotes {
            true => Cow::Owned(str.replace(r#""""#, r#"""#)),
            _ => str,
        }
    }
}

pub struct Parser<'a> {
    source: *const u8,
    ptr: *const u8,
    end: *const u8,
    _marker: PhantomData<&'a u8>,
}

impl<'a> Parser<'a> {
    pub fn new(buffer: &[u8]) -> Parser {
        let ptr = buffer.as_ptr();
        let end = unsafe { ptr.add(buffer.len()) };
        Parser {
            source: ptr,
            ptr,
            end,
            _marker: PhantomData,
        }
    }

    pub fn position(&self) -> usize {
        unsafe { self.ptr.offset_from(self.source) as usize }
    }

    pub fn next(&mut self) -> Option<u8> {
        if self.ptr == self.end {
            None
        } else {
            let v = unsafe { *self.ptr };
            self.ptr = unsafe { self.ptr.add(1) };
            Some(v)
        }
    }

    pub fn skip(&mut self, count: usize) -> Option<()> {
        let new_ptr = unsafe { self.ptr.add(count) };
        if new_ptr > self.end {
            None
        } else {
            self.ptr = new_ptr;
            Some(())
        }
    }

    pub fn skip_to(&mut self, ch: u8) -> Option<()> {
        let len = unsafe { self.end.offset_from(self.ptr) } as usize;
        let haystack = unsafe { std::slice::from_raw_parts(self.ptr, len) };
        let i = memchr::memchr(ch, haystack)?;
        self.skip(i + 1)
    }

    pub fn skip_to2(&mut self, ch1: u8, ch2: u8) -> Option<()> {
        let len = unsafe { self.end.offset_from(self.ptr) } as usize;
        let haystack = unsafe { std::slice::from_raw_parts(self.ptr, len) };
        let i = memchr::memchr2(ch1, ch2, haystack)?;
        self.skip(i + 1)
    }

    pub fn current(&self) -> u8 {
        if self.ptr == self.source {
            panic!("before need to call next()")
        }
        unsafe { *self.ptr.sub(1) }
    }

    pub fn peek(&self) -> Option<u8> {
        if self.ptr == self.end {
            None
        } else {
            let v = unsafe { *self.ptr };
            Some(v)
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
        let ptr = self.ptr;
        self.skip_to2(b',', b'}')?;
        Some(unsafe { std::slice::from_raw_parts(ptr, self.ptr.offset_from(ptr) as usize - 1) })
    }

    pub fn parse_uuid(&mut self) -> Option<Uuid> {
        let raw = self.parse_raw()?;
        let s = std::str::from_utf8(raw).expect("Invalid file format");
        Some(Uuid::from_str(s).expect("Invalid file format"))
    }

    pub fn parse_str(&mut self) -> Option<LogStr<'a>> {
        let ch = self.next()?;
        if ch != b'"' {
            let len = min(20, unsafe { self.end.offset_from(self.ptr) as usize });
            let s = unsafe { std::slice::from_raw_parts(self.ptr, len) };
            panic!("Invalid data 1: {}", String::from_utf8_lossy(s));
        }
        let ptr = self.ptr;
        let mut need_replace_quotes = false;

        loop {
            self.skip_to(b'"')?;
            let next = self.next()?;
            if next == b',' || next == b'}' {
                break;
            } else if next == b'"' {
                need_replace_quotes = true;
            }
        }

        let s = unsafe { std::slice::from_raw_parts(ptr, self.ptr.offset_from(ptr) as usize - 2) };
        Some(LogStr::new(s, need_replace_quotes))
    }

    pub fn parse_object(&mut self) -> Option<&'a str> {
        // Перейти к '{'
        while self.next()? != b'{' {}

        // Запомнить начало строки
        let ptr = unsafe { self.ptr.sub(1) };
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
            unsafe {
                let len = min(20, self.ptr.offset_from(self.end) as usize);
                let s = std::slice::from_raw_parts(self.ptr, len);
                panic!("Invalid data 2: {}", String::from_utf8_lossy(s));
            }
        }

        let s = unsafe { std::slice::from_raw_parts(ptr, self.ptr.offset_from(ptr) as usize - 1) };
        Some(std::str::from_utf8(s).expect("Invalid file format"))
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
