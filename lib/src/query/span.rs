#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub pos: usize,  // 시작 문자 오프셋
    pub len: usize,  // 문자 길이
    pub line: usize, // 줄 번호 (1-based)
    pub col: usize,  // 열 번호 (1-based)
}

impl Default for Span {
    fn default() -> Self {
        Self { pos: 0, len: 0, line: 1, col: 1 }
    }
}
