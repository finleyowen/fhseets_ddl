#![allow(dead_code)]

use rlrl::prelude::*;
use std::{error::Error, fmt::Display};

/// Enum representing the tokens available to the lexer.
#[derive(PartialEq, Debug)]
pub enum Token {
    // chars
    OParen,
    CParen,
    OAngle,
    CAngle,
    Colon,
    Semicolon,
    Comma,
    Equals,
    QMark,

    // kwds
    TypeKwd,
    TableKwd,
    SchemaKwd,

    // ident
    Ident(String),

    // literals
    IntLiteral(i32),
    DblLiteral(f64),
    StrLiteral(String),
}

impl Token {
    // helper function for handling identifiers
    fn is_ident_or_str_literal_tok(&self) -> bool {
        match self {
            Self::Ident(_) | Self::StrLiteral(_) => true,
            _ => false,
        }
    }

    // helper function for handling identifiers
    fn get_ident_or_str_literal(&self) -> Option<String> {
        match &self {
            Self::Ident(ident) => Some(ident.clone()),
            Self::StrLiteral(strlit) => Some(strlit.clone()),
            _ => None,
        }
    }

    fn is_int_literal_tok(&self) -> bool {
        if let Self::IntLiteral(_) = self {
            return true;
        }
        false
    }

    fn get_int_literal(&self) -> Result<i32, ParseError> {
        if let Self::IntLiteral(val) = self {
            return Ok(*val);
        }
        Err(ParseError::new(""))
    }

    fn is_double_literal_tok(&self) -> bool {
        if let Self::DblLiteral(_) | Self::IntLiteral(_) = self {
            return true;
        }
        false
    }

    fn get_double_literal(&self) -> Result<f64, ParseError> {
        if let Self::DblLiteral(val) = self {
            return Ok(*val);
        } else if let Self::IntLiteral(val) = self {
            return Ok(*val as f64);
        }
        Err(ParseError::new(""))
    }
}

/// Function to setup the lexer for testing
fn setup_lexer() -> Lexer<Token> {
    let mut lexer: Lexer<Token> = Lexer::new();

    lexer.add_rule(r"[\s\n\t]+", |_| LexResult::Ignore);

    // chars
    lexer.add_rule(r"\(", |_| LexResult::Token(Token::OParen));
    lexer.add_rule(r"\)", |_| LexResult::Token(Token::CParen));
    lexer.add_rule(r"<", |_| LexResult::Token(Token::OAngle));
    lexer.add_rule(r">", |_| LexResult::Token(Token::CAngle));
    lexer.add_rule(r":", |_| LexResult::Token(Token::Colon));
    lexer.add_rule(r";", |_| LexResult::Token(Token::Semicolon));
    lexer.add_rule(r"\,", |_| LexResult::Token(Token::Comma));
    lexer.add_rule(r"=", |_| LexResult::Token(Token::Equals));
    lexer.add_rule(r"\?", |_| LexResult::Token(Token::QMark));

    // kwds
    lexer.add_rule(r"type", |_| LexResult::Token(Token::TypeKwd));
    lexer.add_rule(r"table", |_| LexResult::Token(Token::TableKwd));

    // idents
    lexer.add_rule(r"[a-zA-Z][a-zA-Z0-9_]*", |re_match| {
        LexResult::Token(Token::Ident(re_match.as_str().into()))
    });

    // literals
    lexer.add_rule(r"\-?[0-9]+", |re_match| {
        match re_match.as_str().parse::<i32>() {
            Ok(v) => LexResult::Token(Token::IntLiteral(v)),
            Err(e) => LexResult::Error(e.into()),
        }
    });
    lexer.add_rule(r"\-?[0-9]+(\.[0-9]+)?", |re_match| {
        match re_match.as_str().parse::<f64>() {
            Ok(v) => LexResult::Token(Token::DblLiteral(v)),
            Err(e) => LexResult::Error(e.into()),
        }
    });
    lexer.add_rule("\"[^\"]*\"", |re_match| {
        LexResult::Token(Token::StrLiteral(re_match.as_str().into()))
    });

    lexer.add_rule(".", |re_match| {
        LexResult::Error(
            format!("Unmatched input at position {}", re_match.start()).into(),
        )
    });

    lexer
}

/// A doubly-ended range of type `T`.
#[derive(Debug, PartialEq)]
pub struct Range<T> {
    min: Option<T>,
    max: Option<T>,
}

impl<T: Display> Display for Range<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<")?;
        if let Some(min) = &self.min {
            min.fmt(f)?;
        }
        write!(f, ",")?;
        if let Some(max) = &self.max {
            max.fmt(f)?;
        }
        write!(f, ">")
    }
}

/// Type alias over a `Range<T> where T = i32`
///
type IntRange = Range<i32>;

pub fn parse_int_range(
    tq: &TokenQueue<Token>,
) -> Result<(IntRange, usize), Box<dyn Error>> {
    // create a mutable copy
    let mut tq = tq.clone();

    tq.consume_eq(Token::OAngle)?;

    // clone to avoid mixing mutable and immutable borrows - note cloning
    // tq is cheap
    let min = match tq.clone().peek_matching(|token| token.is_int_literal_tok())
    {
        Ok(token) => {
            tq.increment();
            Some(token.get_int_literal()?)
        }
        Err(_) => None,
    };

    tq.consume_eq(Token::Comma)?;

    let max = match tq.clone().peek_matching(|token| token.is_int_literal_tok())
    {
        Ok(token) => {
            tq.increment();
            Some(token.get_int_literal()?)
        }
        Err(_) => None,
    };

    tq.consume_eq(Token::CAngle)?;

    Ok((IntRange { min, max }, tq.get_idx()))
}

/// Type alias over a `Range<T> where T = f64`
///
/// /// Offers syntactic convenience when parsing - allows use of
/// `DblRange::try_from(&tq)` rather than `Range::<f64>::try_from(&tq)`
type DblRange = Range<f64>;

pub fn parse_dbl_range(
    tq: &TokenQueue<Token>,
) -> Result<(DblRange, usize), Box<dyn Error>> {
    let mut tq = tq.clone();

    // consume '<'
    tq.consume_eq(Token::OAngle)?;

    // consume min
    let min = match tq.clone().peek_matching(|token| {
        token.is_double_literal_tok() || token.is_int_literal_tok()
    }) {
        Ok(token) => {
            tq.increment();
            Some(token.get_double_literal()?)
        }
        Err(_) => None,
    };

    // consume ','
    tq.consume_eq(Token::Comma)?;

    // consume max
    let max = match tq
        .clone()
        .peek_matching(|token| token.is_double_literal_tok())
    {
        Ok(token) => {
            tq.increment();
            Some(token.get_double_literal()?)
        }
        Err(_) => None,
    };

    // consume '>'
    tq.consume_eq(Token::CAngle)?;

    // done
    Ok((DblRange { min, max }, tq.get_idx()))
}

/// The basic types available in the application.
#[derive(Debug, PartialEq)]
pub enum ParentType {
    Int(IntRange),
    Str(IntRange),
    Dbl(DblRange),
}

/// Derived data types in the applicaiton.
#[derive(Debug, PartialEq)]
pub struct DType {
    parent: ParentType,
    nullable: bool,
}

impl Display for DType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.parent {
            ParentType::Int(range) => {
                write!(
                    f,
                    "int{}{}",
                    if self.nullable { "?" } else { "" },
                    range
                )
            }
            ParentType::Dbl(range) => {
                write!(
                    f,
                    "dbl{}{}",
                    if self.nullable { "?" } else { "" },
                    range
                )
            }
            ParentType::Str(range) => {
                write!(
                    f,
                    "str{}{}",
                    if self.nullable { "?" } else { "" },
                    range
                )
            }
        }
    }
}

pub fn parse_data_type(
    tq: &TokenQueue<Token>,
) -> Result<(DType, usize), Box<dyn Error>> {
    let mut tq = tq.clone();

    let parent_name = tq
        .consume_matching(|tok| tok.is_ident_or_str_literal_tok())?
        .get_ident_or_str_literal()
        .ok_or::<Box<dyn Error>>("Couldn't parse base type name!".into())?
        .clone();

    let nullable = tq.consume_eq(Token::QMark).is_ok();

    match parent_name.to_lowercase().as_str() {
        "int" | "integer" => {
            if let Ok((range, end)) = parse_int_range(&tq) {
                return Ok((
                    DType {
                        nullable,
                        parent: ParentType::Int(range),
                    },
                    end,
                ));
            }
            return Ok((
                DType {
                    nullable,
                    parent: ParentType::Int(Range {
                        min: None,
                        max: None,
                    }),
                },
                tq.get_idx(),
            ));
        }
        "str" | "string" | "text" => {
            if let Ok((range, end)) = parse_int_range(&tq) {
                return Ok((
                    DType {
                        nullable,
                        parent: ParentType::Str(range),
                    },
                    end,
                ));
            }
            return Ok((
                DType {
                    nullable,
                    parent: ParentType::Str(Range {
                        min: None,
                        max: None,
                    }),
                },
                tq.get_idx(),
            ));
        }
        "dbl" | "double" | "float" => {
            if let Ok((range, end)) = parse_dbl_range(&tq) {
                return Ok((
                    DType {
                        nullable,
                        parent: ParentType::Dbl(range),
                    },
                    end,
                ));
            }
            return Ok((
                DType {
                    nullable,
                    parent: ParentType::Dbl(Range {
                        min: None,
                        max: None,
                    }),
                },
                tq.get_idx(),
            ));
        }
        _ => {
            return Err(format!(
                "Couldn't parse data type from '{}'!",
                parent_name
            )
            .into());
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct ColumnSchema {
    column_name: String,
    dtype: DType,
}

impl Display for ColumnSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.column_name, self.dtype)
    }
}

pub fn parse_column_schema(
    tq: &TokenQueue<Token>,
) -> Result<(ColumnSchema, usize), Box<dyn Error>> {
    let mut tq: TokenQueue<Token> = tq.clone();

    let column_name = tq
        .consume_matching(|tok| tok.is_ident_or_str_literal_tok())?
        .get_ident_or_str_literal()
        .ok_or::<Box<dyn Error>>("Couldn't get column name!".into())?
        .clone();

    tq.consume_eq(Token::Colon)?;

    let dtype = tq.parse(parse_data_type)?;

    Ok((ColumnSchema { column_name, dtype }, tq.get_idx()))
}

#[derive(Debug, PartialEq)]
pub struct TableSchema {
    table_name: String,
    columns: Vec<ColumnSchema>,
}

pub fn parse_table_schema(tq: &TokenQueue<Token>) -> ParseResult<TableSchema> {
    let mut tq = tq.clone();

    let table_name = tq
        .consume_matching(|tok| tok.is_ident_or_str_literal_tok())?
        .get_ident_or_str_literal()
        .ok_or("Couldn't get table name!")?;

    tq.consume_eq(Token::OParen)?;

    let mut columns = vec![];

    while let Ok(column) = tq.parse(parse_column_schema) {
        columns.push(column);
        if tq.consume_eq(Token::Comma).is_err() {
            break;
        }
    }

    tq.consume_eq(Token::CParen)?;
    Ok((
        TableSchema {
            table_name,
            columns,
        },
        tq.get_idx(),
    ))
}

#[derive(Debug, PartialEq)]
pub enum Stmt {
    DType(String, DType),
    Table(TableSchema),
}

pub fn parse_stmt(tq: &TokenQueue<Token>) -> ParseResult<Stmt> {
    let mut tq = tq.clone();

    match tq.consume() {
        Ok(Token::TypeKwd) => {
            let type_name = tq
                .consume_matching(|tok| tok.is_ident_or_str_literal_tok())?
                .get_ident_or_str_literal()
                .ok_or::<Box<dyn Error>>("Couldn't get type name!".into())?
                .clone();

            let (dtype, end) = parse_data_type(&tq)?;
            // tq.consume_eq(Token::)
            Ok((Stmt::DType(type_name.into(), dtype), end))
        }
        Ok(Token::TableKwd) => {
            let (table_schema, end) = parse_table_schema(&tq)?;
            Ok((Stmt::Table(table_schema), end))
        }
        Ok(tok) => {
            dbg!(tok);
            Err(format!("Couldn't parse statement!").into())
        }
        Err(_) => Err("Couldn't parse statement!".into()),
    }
}

#[derive(Debug, PartialEq)]
pub struct Prgm {
    stmts: Vec<Stmt>,
}

pub fn parse_prgm(tq: &TokenQueue<Token>) -> ParseResult<Prgm> {
    let mut tq = tq.clone();
    let mut stmts = vec![];

    while let Ok((stmt, end)) = parse_stmt(&tq) {
        tq.go_to(end);
        stmts.push(stmt);
        if tq.consume_eq(Token::Semicolon).is_err() {
            return Err("Missing semicolon!".into());
        }
    }

    Ok((Prgm { stmts }, tq.get_idx()))
}

#[cfg(test)]
mod tests {
    use crate::*;
    use std::error::Error;

    fn lex(s: &str) -> Result<TokenQueue<Token>, Box<dyn Error>> {
        let lexer = setup_lexer();
        let tq = TokenQueue::new(lexer.lex(s)?);
        Ok(tq)
    }

    fn maps_to_int_range(s: &str) -> Result<bool, Box<dyn Error>> {
        let out = parse_int_range(&lex(&s)?)?.0.to_string();
        Ok(out == s)
    }

    fn maps_to_dbl_range(s: &str) -> Result<bool, Box<dyn Error>> {
        let out = parse_dbl_range(&lex(&s)?)?.0.to_string();
        Ok(out == s)
    }

    fn maps_to_data_type(s: &str) -> Result<bool, Box<dyn Error>> {
        let out = parse_data_type(&lex(&s)?)?.0.to_string();
        Ok(out == s)
    }

    fn maps_to_column_schema(s: &str) -> Result<bool, Box<dyn Error>> {
        let out = parse_column_schema(&lex(&s)?)?.0.to_string();
        Ok(out == s)
    }

    #[test]
    fn int_range_test() -> Result<(), Box<dyn Error>> {
        assert!(maps_to_int_range("<5,10>")?);
        assert!(maps_to_int_range("<10,10>")?);
        assert!(maps_to_int_range("<,10>")?);
        assert!(maps_to_int_range("<5,>")?);

        Ok(())
    }

    #[test]
    fn dbl_range_test() -> Result<(), Box<dyn Error>> {
        assert!(maps_to_dbl_range("<5,>")?);
        assert!(maps_to_dbl_range("<,10>")?);
        assert!(maps_to_dbl_range("<1.1,9.9>")?);
        assert!(maps_to_dbl_range("<1.1,9.900009>")?);

        Ok(())
    }

    #[test]
    fn data_type_test() -> Result<(), Box<dyn Error>> {
        assert!(maps_to_data_type("int?<5,10>")?);
        assert!(maps_to_data_type("int<5,10>")?);
        assert!(maps_to_data_type("str<5,10>")?);
        assert!(maps_to_data_type("str?<5,10>")?);
        assert!(maps_to_data_type("dbl?<5.4,10.3>")?);
        assert!(maps_to_data_type("dbl<5,10>")?);
        Ok(())
    }

    #[test]
    fn column_schema_test() -> Result<(), Box<dyn Error>> {
        assert!(maps_to_column_schema("abc: int<5,5>")?);
        assert!(maps_to_column_schema("\"My column\": int<5,5>")?);

        Ok(())
    }

    #[test]
    fn stmt_test() -> Result<(), Box<dyn Error>> {
        let tokens = setup_lexer().lex("type i1to5 int<1,5>")?;
        let (stmt, _) = parse_stmt(&TokenQueue::new(tokens))?;

        assert!(
            stmt == Stmt::DType(
                "i1to5".into(),
                DType {
                    parent: ParentType::Int(Range {
                        min: Some(1),
                        max: Some(5)
                    }),
                    nullable: false
                }
            )
        );

        let tokens = setup_lexer().lex("table Table1(a:int,b:dbl,c:str)")?;
        let (_, _) = parse_stmt(&TokenQueue::new(tokens))?;

        Ok(())
    }

    #[test]
    fn ident_test() -> Result<(), Box<dyn Error>> {
        let ident = "helloWorld";
        let str_lit = "\"helloWorld\"";

        let ident_tok = Token::Ident(ident.into());
        let str_lit_tok = Token::StrLiteral(str_lit.into());
        assert!(
            ident_tok.is_ident_or_str_literal_tok()
                && ident_tok
                    .get_ident_or_str_literal()
                    .ok_or::<Box<dyn Error>>("ident".into())?
                    == ident,
        );
        assert!(
            str_lit_tok.is_ident_or_str_literal_tok()
                && str_lit_tok
                    .get_ident_or_str_literal()
                    .ok_or::<Box<dyn Error>>("ident".into())?
                    == str_lit,
        );
        Ok(())
    }

    #[test]
    fn table_schema_test() -> Result<(), Box<dyn Error>> {
        let mut tq = lex("Table(
            col1: int<1, 5>, 
            col2: dbl<1, 5>, 
            col3: str<5, 10>,
            col4: int?<1, 6>
        )")?;

        let table = tq.parse(parse_table_schema)?;

        assert!(
            table
                == TableSchema {
                    table_name: "Table".into(),
                    columns: vec![
                        ColumnSchema {
                            column_name: "col1".into(),
                            dtype: DType {
                                parent: ParentType::Int(Range {
                                    min: Some(1),
                                    max: Some(5)
                                }),
                                nullable: false
                            }
                        },
                        ColumnSchema {
                            column_name: "col2".into(),
                            dtype: DType {
                                parent: ParentType::Dbl(Range {
                                    min: Some(1.0),
                                    max: Some(5.0)
                                }),
                                nullable: false
                            }
                        },
                        ColumnSchema {
                            column_name: "col3".into(),
                            dtype: DType {
                                parent: ParentType::Str(Range {
                                    min: Some(5),
                                    max: Some(10)
                                }),
                                nullable: false
                            }
                        },
                        ColumnSchema {
                            column_name: "col4".into(),
                            dtype: DType {
                                parent: ParentType::Int(Range {
                                    min: Some(1),
                                    max: Some(6)
                                }),
                                nullable: true
                            }
                        }
                    ]
                }
        );

        Ok(())
    }

    #[test]
    fn parse_prgm_test() -> Result<(), Box<dyn Error>> {
        let tq = lex("
        table Table1(a: int, b: dbl, c: str, d: int?);
        table Table2(a: int, b: dbl, c: str, d: int?);
        ")?;

        parse_prgm(&tq)?;

        Ok(())
    }
}
