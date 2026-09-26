//! A small, strict, allocation-free JSON pull reader for venue messages.
//!
//! Venue decoders walk a message field by field and read only what they need, straight from
//! the received bytes. Nothing is copied or allocated, which keeps decoding on the hot path.
//! Every JSON venue adapter uses this one reader (CLAUDE.md §2, DRY).
//!
//! The reader validates the structure it walks: commas, colons, nesting and string
//! termination. Strings read with [`Reader::str`] must not contain escapes, which venue keys
//! and values never do; escaped strings can still be skipped. Nesting is limited to
//! [`MAX_DEPTH`] so hostile input cannot exhaust the stack.
//!
//! Usage pattern:
//!
//! ```
//! use bowst_venue::json::Reader;
//!
//! let mut r = Reader::new(br#"{"id": 7, "tags": ["a", "b"], "skip": {"x": null}}"#);
//! r.begin_object()?;
//! let (mut id, mut tags) = (0, 0);
//! while let Some(key) = r.next_key()? {
//!     match key {
//!         "id" => id = r.u64()?,
//!         "tags" => {
//!             r.begin_array()?;
//!             while r.next_element()? {
//!                 r.str()?;
//!                 tags += 1;
//!             }
//!         }
//!         _ => r.skip()?,
//!     }
//! }
//! r.finish()?;
//! assert_eq!((id, tags), (7, 2));
//! # Ok::<(), bowst_venue::json::JsonError>(())
//! ```

/// Deepest nesting of objects and arrays accepted.
pub const MAX_DEPTH: u8 = 32;

/// Malformed or unsupported JSON, with the byte offset where it was found.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum JsonError {
    /// Input ended in the middle of a value.
    #[error("unexpected end of input")]
    UnexpectedEnd,
    /// A byte that cannot appear here.
    #[error("expected {expected} at byte {pos}")]
    Unexpected {
        /// Byte offset.
        pos: usize,
        /// What the reader was looking for.
        expected: &'static str,
    },
    /// Nesting deeper than [`MAX_DEPTH`].
    #[error("nesting deeper than {MAX_DEPTH} at byte {pos}")]
    TooDeep {
        /// Byte offset.
        pos: usize,
    },
    /// A string read with [`Reader::str`] contained an escape sequence.
    #[error("escaped string at byte {pos}")]
    EscapedString {
        /// Byte offset.
        pos: usize,
    },
    /// A number that is not a valid unsigned 64-bit integer.
    #[error("invalid integer at byte {pos}")]
    InvalidInteger {
        /// Byte offset.
        pos: usize,
    },
}

/// Pull reader over one JSON document. See the module docs.
#[derive(Debug)]
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
    depth: u8,
    /// Whether a value has just completed inside the current container, so the next member
    /// must be preceded by a comma.
    after_value: bool,
}

impl<'a> Reader<'a> {
    /// Starts reading `buf` from the beginning.
    #[must_use]
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            buf,
            pos: 0,
            depth: 0,
            after_value: false,
        }
    }

    /// Current byte offset, for error context.
    #[must_use]
    pub fn position(&self) -> usize {
        self.pos
    }

    fn peek(&self) -> Option<u8> {
        self.buf.get(self.pos).copied()
    }

    fn advance(&mut self) {
        self.pos = self.pos.saturating_add(1);
    }

    fn skip_whitespace(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.advance();
        }
    }

    fn unexpected(&self, expected: &'static str) -> JsonError {
        if self.pos >= self.buf.len() {
            JsonError::UnexpectedEnd
        } else {
            JsonError::Unexpected {
                pos: self.pos,
                expected,
            }
        }
    }

    fn expect_byte(&mut self, byte: u8, expected: &'static str) -> Result<(), JsonError> {
        self.skip_whitespace();
        if self.peek() == Some(byte) {
            self.advance();
            Ok(())
        } else {
            Err(self.unexpected(expected))
        }
    }

    fn enter(&mut self, open: u8, expected: &'static str) -> Result<(), JsonError> {
        self.expect_byte(open, expected)?;
        if self.depth >= MAX_DEPTH {
            return Err(JsonError::TooDeep { pos: self.pos });
        }
        self.depth = self.depth.saturating_add(1);
        self.after_value = false;
        Ok(())
    }

    fn leave(&mut self) {
        self.advance();
        self.depth = self.depth.saturating_sub(1);
        self.after_value = true;
    }

    /// Consumes the separator before the next member of the current container. Returns
    /// `false` (and consumes `close`) at the end of the container.
    fn next_member(&mut self, close: u8, expected: &'static str) -> Result<bool, JsonError> {
        self.skip_whitespace();
        if self.peek() == Some(close) {
            self.leave();
            return Ok(false);
        }
        if self.after_value {
            self.expect_byte(b',', expected)?;
        }
        Ok(true)
    }

    /// Consumes `{`.
    ///
    /// # Errors
    /// If the next value is not an object, or nesting is too deep.
    pub fn begin_object(&mut self) -> Result<(), JsonError> {
        self.enter(b'{', "'{'")
    }

    /// Reads the next key of the current object and its `:`. `None` at the closing `}`.
    ///
    /// # Errors
    /// On malformed input or an escaped key.
    pub fn next_key(&mut self) -> Result<Option<&'a str>, JsonError> {
        if !self.next_member(b'}', "',' or '}'")? {
            return Ok(None);
        }
        let key = self.str()?;
        self.after_value = false;
        self.expect_byte(b':', "':'")?;
        Ok(Some(key))
    }

    /// Consumes `[`.
    ///
    /// # Errors
    /// If the next value is not an array, or nesting is too deep.
    pub fn begin_array(&mut self) -> Result<(), JsonError> {
        self.enter(b'[', "'['")
    }

    /// Moves to the next element of the current array. `false` at the closing `]`.
    ///
    /// # Errors
    /// On malformed input.
    pub fn next_element(&mut self) -> Result<bool, JsonError> {
        self.next_member(b']', "',' or ']'")
    }

    /// Reads a string without escape sequences, borrowed from the input.
    ///
    /// # Errors
    /// If the next value is not a string, contains an escape or control character, or is
    /// not valid UTF-8.
    pub fn str(&mut self) -> Result<&'a str, JsonError> {
        self.expect_byte(b'"', "string")?;
        let start = self.pos;
        let end = self.string_end()?;
        if self.buf.get(end) == Some(&b'\\') {
            return Err(JsonError::EscapedString { pos: end });
        }
        let bytes = self.buf.get(start..end).ok_or(JsonError::UnexpectedEnd)?;
        let text = core::str::from_utf8(bytes).map_err(|_| JsonError::Unexpected {
            pos: start,
            expected: "UTF-8",
        })?;
        self.pos = end.saturating_add(1);
        self.after_value = true;
        Ok(text)
    }

    /// Reads a string without escape sequences as raw bytes, borrowed from the input and not
    /// checked for UTF-8. For values the caller parses strictly (numbers), where any
    /// non-ASCII byte is rejected anyway, this saves a validation pass.
    ///
    /// # Errors
    /// If the next value is not a string, or contains an escape or control character.
    #[inline]
    pub fn str_bytes(&mut self) -> Result<&'a [u8], JsonError> {
        self.expect_byte(b'"', "string")?;
        let start = self.pos;
        let end = self.string_end()?;
        if self.buf.get(end) == Some(&b'\\') {
            return Err(JsonError::EscapedString { pos: end });
        }
        let bytes = self.buf.get(start..end).ok_or(JsonError::UnexpectedEnd)?;
        self.pos = end.saturating_add(1);
        self.after_value = true;
        Ok(bytes)
    }

    /// Offset of the first `"` or `\\` at or after the current position, rejecting control
    /// characters. One tight scan instead of a bounds-checked step per byte.
    fn string_end(&mut self) -> Result<usize, JsonError> {
        let rest = self.buf.get(self.pos..).unwrap_or_default();
        let found = rest
            .iter()
            .position(|&b| b == b'"' || b == b'\\' || b < 0x20)
            .ok_or(JsonError::UnexpectedEnd)?;
        let end = self.pos.saturating_add(found);
        if self.buf.get(end).is_some_and(|&b| b < 0x20) {
            self.pos = end;
            return Err(self.unexpected("string character"));
        }
        Ok(end)
    }

    /// Reads a non-negative integer that fits in a `u64`.
    ///
    /// # Errors
    /// If the next value is not such an integer.
    pub fn u64(&mut self) -> Result<u64, JsonError> {
        self.skip_whitespace();
        let start = self.pos;
        let mut value: u64 = 0;
        while let Some(byte @ b'0'..=b'9') = self.peek() {
            value = value
                .checked_mul(10)
                .and_then(|v| v.checked_add(u64::from(byte.wrapping_sub(b'0'))))
                .ok_or(JsonError::InvalidInteger { pos: start })?;
            self.advance();
        }
        let digits = self.pos.saturating_sub(start);
        let leading_zero = digits > 1 && self.buf.get(start) == Some(&b'0');
        let continues = matches!(self.peek(), Some(b'.' | b'e' | b'E' | b'-' | b'+'));
        if digits == 0 || leading_zero || continues {
            return Err(JsonError::InvalidInteger { pos: start });
        }
        self.after_value = true;
        Ok(value)
    }

    /// Reads `true` or `false`.
    ///
    /// # Errors
    /// If the next value is not a boolean.
    pub fn bool(&mut self) -> Result<bool, JsonError> {
        self.skip_whitespace();
        if self.literal(b"true") {
            Ok(true)
        } else if self.literal(b"false") {
            Ok(false)
        } else {
            Err(self.unexpected("boolean"))
        }
    }

    fn literal(&mut self, word: &[u8]) -> bool {
        let end = self.pos.saturating_add(word.len());
        if self.buf.get(self.pos..end) == Some(word) {
            self.pos = end;
            self.after_value = true;
            true
        } else {
            false
        }
    }

    /// Skips the next value of any type, validating its structure.
    ///
    /// # Errors
    /// On malformed input or nesting deeper than [`MAX_DEPTH`].
    pub fn skip(&mut self) -> Result<(), JsonError> {
        self.skip_whitespace();
        match self.peek() {
            None => Err(JsonError::UnexpectedEnd),
            Some(b'{') => {
                self.begin_object()?;
                while self.next_key()?.is_some() {
                    self.skip()?;
                }
                Ok(())
            }
            Some(b'[') => {
                self.begin_array()?;
                while self.next_element()? {
                    self.skip()?;
                }
                Ok(())
            }
            Some(b'"') => self.skip_string(),
            Some(b't' | b'f' | b'n') => {
                if self.literal(b"true") || self.literal(b"false") || self.literal(b"null") {
                    Ok(())
                } else {
                    Err(self.unexpected("value"))
                }
            }
            Some(b'-' | b'0'..=b'9') => self.skip_number(),
            Some(_) => Err(self.unexpected("value")),
        }
    }

    fn skip_string(&mut self) -> Result<(), JsonError> {
        self.advance(); // opening quote
        loop {
            let end = self.string_end()?;
            if self.buf.get(end) == Some(&b'"') {
                self.pos = end.saturating_add(1);
                self.after_value = true;
                return Ok(());
            }
            // Backslash: skip it and the escaped byte.
            if end.saturating_add(1) >= self.buf.len() {
                return Err(JsonError::UnexpectedEnd);
            }
            self.pos = end.saturating_add(2);
        }
    }

    fn skip_number(&mut self) -> Result<(), JsonError> {
        let start = self.pos;
        while matches!(
            self.peek(),
            Some(b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9')
        ) {
            self.advance();
        }
        if self.pos == start {
            return Err(self.unexpected("number"));
        }
        self.after_value = true;
        Ok(())
    }

    /// Checks that only whitespace remains after the document.
    ///
    /// # Errors
    /// If there is trailing content or a container is still open.
    pub fn finish(&mut self) -> Result<(), JsonError> {
        self.skip_whitespace();
        if self.depth != 0 {
            return Err(JsonError::UnexpectedEnd);
        }
        if self.pos < self.buf.len() {
            return Err(self.unexpected("end of input"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys_and_skip(input: &str) -> Result<Vec<String>, JsonError> {
        let mut r = Reader::new(input.as_bytes());
        r.begin_object()?;
        let mut keys = Vec::new();
        while let Some(key) = r.next_key()? {
            keys.push(key.to_owned());
            r.skip()?;
        }
        r.finish()?;
        Ok(keys)
    }

    #[test]
    fn walks_nested_documents() {
        let keys = keys_and_skip(
            r#" { "a" : 1 , "b":[1,-2.5e3,"x\"y",true,false,null,{"c":[]}], "d":{} } "#,
        )
        .unwrap();
        assert_eq!(keys, ["a", "b", "d"]);
        assert_eq!(keys_and_skip("{}").unwrap(), Vec::<String>::new());
    }

    #[test]
    fn rejects_malformed_structure() {
        for input in [
            "",
            "{",
            r#"{"a":1,}"#,
            r#"{,"a":1}"#,
            r#"{"a" 1}"#,
            r#"{"a":1 "b":2}"#,
            r#"{"a":[1 2]}"#,
            r#"{"a":[1,]}"#,
            r#"{"a":tru}"#,
            r#"{"a":"unterminated}"#,
            "{\"a\":\"ctrl\u{1}\"}",
            r#"{"a":1}x"#,
            r#"{"a":@}"#,
            r"{a:1}",
        ] {
            assert!(keys_and_skip(input).is_err(), "accepted {input:?}");
        }
    }

    #[test]
    fn limits_nesting_depth() {
        let deep = format!("{{\"a\":{}{}}}", "[".repeat(40), "]".repeat(40));
        assert!(matches!(
            keys_and_skip(&deep),
            Err(JsonError::TooDeep { .. })
        ));
    }

    #[test]
    fn reads_integers_strictly() {
        let read = |s: &str| Reader::new(s.as_bytes()).u64();
        assert_eq!(read("0"), Ok(0));
        assert_eq!(read(" 18446744073709551615"), Ok(u64::MAX));
        for bad in [
            "18446744073709551616",
            "-1",
            "01",
            "1.5",
            "1e3",
            "",
            "\"1\"",
        ] {
            assert!(read(bad).is_err(), "accepted {bad:?}");
        }
    }

    #[test]
    fn reads_strings_and_booleans() {
        let mut r = Reader::new(br#"["BTCUSDT", true, false, "esc\"aped", "caf\xc3\xa9"]"#);
        r.begin_array().unwrap();
        assert!(r.next_element().unwrap());
        assert_eq!(r.str(), Ok("BTCUSDT"));
        assert!(r.next_element().unwrap());
        assert_eq!(r.bool(), Ok(true));
        assert!(r.next_element().unwrap());
        assert_eq!(r.bool(), Ok(false));
        assert!(r.next_element().unwrap());
        assert!(matches!(r.str(), Err(JsonError::EscapedString { .. })));
        let mut r = Reader::new(b"\"caf\xc3\xa9\"");
        assert_eq!(r.str(), Ok("caf\u{e9}"));
        let mut r = Reader::new(b"\"\xff\"");
        assert!(r.str().is_err());
    }
}
