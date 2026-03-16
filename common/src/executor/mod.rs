/*
기본 SQL 문법

SQL에서 데이터는 엑셀 표처럼 생긴 ‘테이블’에 저장됩니다.
SQL은 이 테이블에서 어떤 열(정보)을 선택하고,
어떤 조건의 행(데이터)을 가져올지를 지정하는 언어입니다.

SQL의 문(Statement)은 크게 6가지가 있습니다.
1. 테이블을 생성하는 CREATE
2. 테이블에 데이터를 추가하는 INSERT
3. 테이블의 데이터를 가져오는 SELECT
4. 테이블의 데이터를 수정하는 UPDATE
5. 테이블의 데이터를 삭제하는 DELETE
6. 테이블을 삭제하는 DROP


--- 예시 1. CREATE ---

먼저 테이블을 생성해 보겠습니다.
    `CREATE TABLE friends(name TEXT, male BOOL, age INT);`
    >>> SUCCESS
        ------ <friends> ------
        |  name  | male | age |
        |  ----  | ---- | --- |

'friends' 테이블이 생성되었습니다.


--- 예시 2. INSERT ---

이제 데이터를 여러 개 추가해봅시다.
    `INSERT INTO friends VALUES('Alpha', TRUE, 18);`
    `INSERT INTO friends VALUES('Beta', FALSE, 20);`
    `INSERT INTO friends VALUES('Gamma', FALSE, 25);`
    `INSERT INTO (name, age) friends VALUES('Delta', 31);`
                 ^^^^^^^^^^^ 이렇게 특정 컬럼만 지정할 수도 있습니다.
    >>> SUCCESS
        SUCCESS
        SUCCESS
        SUCCESS
        ------ <friends> ------
        |  name  | male | age |
        |:------:|:----:|:---:|
        | Alpha  | yes  | 18  |
        | Beta   | no   | 20  |
        | Gamma  | no   | 25  |
        | Delta  | null | 31  |


--- 예시 3. SELECT ---

일단, INSERT가 제대로 되었는지 확인해봅시다.
    `SELECT * FROM friends;`
    >>> Rows:
        |  name  | male | age |
        |:------:|:----:|:---:|
        | Alpha  | yes  | 18  |
        | Beta   | no   | 20  |

이제 '여자'인 친구의 '이름과 나이'를 가져와봅시다.
    `SELECT name, age FROM friends WHERE male = FALSE;`
    >>> Rows:
        |  name  | age |
        |:------:|:---:|
        | Beta   | 20  |
        | Gamma  | 25  |

이번엔 '나이가 30보다 어린' 친구의 '이름과 성별'을 가져와봅시다.
    `SELECT name, male FROM friends WHERE age < 30;`
    >>> Rows:
        |  name  | male |
        |:------:|:----:|
        | Alpha  | yes  |
        | Beta   | no   |
        | Gamma  | no   |


--- 예시 4. UPDATE ---

Delta의 성별이 누락되었으니, 이를 갱신해봅시다.
    `UPDATE friends SET male = TRUE WHERE name = 'Delta';`
    >>> SUCCESS
        ------ <friends> ------
        |  name  | male | age |
        |:------:|:----:|:---:|
        | Alpha  | yes  | 18  |
        | Beta   | no   | 20  |
        | Gamma  | no   | 25  |
        | Delta  | yes  | 31  |


--- 예시 5. DELETE ---

이번엔 '나이가 20 미만'인 친구를 테이블에서 삭제해봅시다.
    `DELETE FROM friends WHERE age < 20;`
    >>> SUCCESS
        ------ <friends> ------
        |  name  | male | age |
        |:------:|:----:|:---:|
        | Beta   | no   | 20  |
        | Gamma  | no   | 25  |
        | Delta  | yes  | 31  |

--- 예시 6. DROP ---

이제 모든 친구 데이터를 삭제해봅시다.
    `DROP TABLE friends;`
    >>> SUCCESS
*/

/*
해야할 것:

1. Lexer와 Parser를 이용하여 쿼리 문자열을 해석하기
  - main.rs에서는 executor.run(src: String); 을 기대하고 있습니다.

2. 해석한 문자열을 match하여 run_create, run_insert ...와 같은
   하위 메서드로 전달하여 처리하기

3. 각 메서드에서 Executor의 mock 속성을 조작하여 쿼리를 처리하기
   - mock 속성은 임시로 데이터를 저장하는 용도입니다.
   - 나중에 storage 모듈을 이용하여 데이터를 조작해야 합니다.

우선 목표는 CREATE와 INSERT를 처리하는 것입니다.
*/

use crate::query::{Expr, Lexer, Parser, Stmt};
use crate::storage::{
    DataValue, create_column, create_row, create_table, get_table_hash, read_all_rows, read_schema,
};
use crate::var_char::VarChar;

pub struct ColumnId(pub u64);
pub struct RowId(pub u64);

#[derive(PartialEq, Eq, Hash, Clone, Copy, Debug)]
pub struct TableId(pub u64);

pub enum QueryResult {
    Rows(Vec<Vec<String>>),
    // Count(usize), TODO: COUNT 함수 구현 후 사용
    Success,
    Error(String),
}

pub struct TableView {
    pub name: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

pub struct Executor;

impl Executor {
    pub fn new() -> Self {
        Self
    }

    fn format_value(&self, value: &DataValue) -> String {
        match value {
            DataValue::Int(i) => i.to_string(),
            DataValue::Float(f) => f.to_string(),
            DataValue::Bool(b) => b.to_string(),
            DataValue::VChar(s) => s.to_string(),
        }
    }

    fn eval_expr(&self, expr: &Expr) -> DataValue {
        match expr {
            Expr::Int(i) => DataValue::Int(*i),
            Expr::Bool(b) => DataValue::Bool(*b),
            Expr::Float(f) => DataValue::Float(*f),
            Expr::Text(s) => DataValue::VChar(
                VarChar::try_from(s.as_ref())
                    .unwrap_or_else(|_| panic!("String too long for VarChar: '{}'", s)),
            ),
            _ => unimplemented!("Expression type not supported yet"),
        }
    }

    pub async fn run(&mut self, src: String) -> QueryResult {
        let lexer = Lexer::new(&src);
        let parser = Parser::new(lexer);
        let stmts = parser.unwrap().parse(); // TODO: 오류 처리
        if let Err(e) = stmts {
            return QueryResult::Error(e.to_string());
        }
        let stmts = stmts.unwrap();
        for stmt in stmts {
            match stmt {
                Stmt::Create { table, columns, .. } => {
                    println!("Creating table: {}", table);
                    let table_id = match create_table(table.to_string()).await {
                        Ok(id) => id,
                        Err(e) => {
                            return QueryResult::Error(format!(
                                "Failed to create table '{}': {}",
                                table, e
                            ));
                        }
                    };
                    for (col_name, col_type) in columns {
                        println!("  Column: {} ({:?})", col_name, col_type);
                        let Ok(_) = create_column(table_id, col_name.to_string(), col_type).await
                        else {
                            return QueryResult::Error(format!(
                                "Failed to create column: {} ({:?})",
                                col_name, col_type
                            ));
                        };
                    }
                }
                Stmt::InsertValues { table, rows, .. } => {
                    println!("Inserting data into: {}", table);
                    let table_id = get_table_hash(&table);
                    // Vec<Expr> [1 + [1 * 2]]

                    for values in rows {
                        println!("  Row: {:?}", values);
                        let values = values.iter().map(|v| self.eval_expr(v)).collect::<Vec<_>>();
                        match create_row(table_id, values).await {
                            Ok(_) => (),
                            Err(e) => {
                                return QueryResult::Error(format!(
                                    "Failed to insert row into '{}': {}",
                                    table, e
                                ));
                            }
                        }
                    }
                }
                _ => {
                    println!("Other statement: {:?}", stmt);
                }
            }
        }
        QueryResult::Success
    }

    pub async fn load_table(&self, table_name: &str) -> Result<TableView, String> {
        let table_id = get_table_hash(table_name);

        let (_, name, columns_info) = read_schema(table_id)
            .await
            .map_err(|err| format!("Failed to read schema for table '{}': {}", table_name, err))?;

        let columns = columns_info
            .into_iter()
            .map(|(_, _, col_name)| col_name)
            .collect::<Vec<_>>();

        let rows_data = read_all_rows(table_id)
            .await
            .map_err(|err| format!("Failed to load rows for table '{}': {}", table_name, err))?;

        let rows = rows_data
            .iter()
            .map(|row| row.iter().map(|value| self.format_value(value)).collect())
            .collect();

        Ok(TableView {
            name,
            columns,
            rows,
        })
    }
}
