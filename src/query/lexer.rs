pub enum Token {
    Null,
    Num(String),
    Text(String),
    // 식별자
    Ident(String),
    // 키워드
    Create,  // CREATE
    Table,   // TABLE
    Select,  // SELECT
    From,    // FROM
    Where,   // WHERE
    Update,  // UPDATE
    Alter,   // ALTER
    Delete,  // DELETE
    Drop,    // DROP
    // 구분자
    Dot,       // .
    Comma,     // ,
    Semicolon, // ;
    LParen,    // (
    RParen,    // )
    // 연산자
    Not,       // NOT
    And,       // AND
    Or,        // OR
    Assign,    // =
    Gt,        // >
    Lt,        // <
    Ge,        // >=
    Le,        // <=
    Add,       // +
    Sub,       // -
    Mul,       // *
    Div,       // /
}