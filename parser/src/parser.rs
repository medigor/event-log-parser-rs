use libc::c_void;
use std::{borrow::Cow, cmp::min, ffi::c_int, marker::PhantomData, str::FromStr};
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
        let new_ptr = unsafe { libc::memchr(self.ptr as *const c_void, ch as c_int, len) };
        if new_ptr.is_null() {
            return None;
        }
        self.ptr = unsafe { new_ptr.add(1) } as *mut u8;
        Some(())
    }

    pub fn skip_to2(&mut self, ch1: u8, ch2: u8) -> Option<()> {
        let len = unsafe { self.end.offset_from(self.ptr) } as usize;

        let new_ptr1 =
            unsafe { libc::memchr(self.ptr as *const c_void, ch1 as c_int, len) } as *const u8;
        let new_ptr2 =
            unsafe { libc::memchr(self.ptr as *const c_void, ch2 as c_int, len) } as *const u8;

        let new_ptr = {
            if new_ptr1.is_null() {
                new_ptr2
            } else if new_ptr2.is_null() {
                new_ptr1
            } else if new_ptr1 < new_ptr2 {
                new_ptr1
            } else {
                new_ptr2
            }
        };
        if new_ptr.is_null() {
            None
        } else {
            self.ptr = unsafe { new_ptr.add(1) };
            Some(())
        }
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
        unsafe {
            let s = std::str::from_utf8_unchecked(raw);
            Some(Uuid::from_str(s).unwrap())
        }
    }

    pub fn parse_str(&mut self) -> Option<LogStr<'a>> {
        let ch = self.next()?;
        if ch != b'"' {
            let len = min(20, unsafe { self.end.offset_from(self.ptr) as usize });
            let s = unsafe { std::slice::from_raw_parts(self.ptr, len) };
            unsafe {
                let s = std::str::from_utf8_unchecked(s);
                panic!("Invalid data 1: {}", s);
            }
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

        let s = unsafe {
            std::str::from_utf8_unchecked(std::slice::from_raw_parts(
                ptr,
                self.ptr.offset_from(ptr) as usize - 2,
            ))
        };
        Some(LogStr {
            s,
            need_replace_quotes,
        })
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
                let s = std::str::from_utf8_unchecked(s);
                panic!("Invalid data 2: {}", s);
            }
        }
        unsafe {
            Some(std::str::from_utf8_unchecked(std::slice::from_raw_parts(
                ptr,
                self.ptr.offset_from(ptr) as usize - 1,
            )))
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
