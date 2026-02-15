use super::lex::*;
use rlrl::parse::*;
use std::fmt::Display;

/// A doubly-ended range of type `T`.
#[derive(Debug, PartialEq)]
pub struct Range<T> {
    pub min: Option<T>,
    pub max: Option<T>,
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
type IntRange = Range<i32>;

pub fn parse_int_range(tq: &TokenQueue<Token>) -> ParseResult<IntRange> {
    // create a mutable copy
    let mut tq = tq.clone();

    tq.consume_eq(Token::OAngle)?;

    // consume min
    let min = match tq.clone().peek_matching(|token| token.is_literal()) {
        Ok(token) => {
            tq.increment();
            let literal = token.get_literal().unwrap();
            if literal.is_int_literal() {
                Some(literal.get_int().unwrap())
            } else {
                return Err(anyhow::anyhow!("Couldn't parse int literal!"));
            }
        }
        Err(_) => None,
    };

    tq.consume_eq(Token::Comma)?;

    // consume max
    let max = match tq.clone().peek_matching(|token| token.is_literal()) {
        Ok(token) => {
            tq.increment();
            let literal = token.get_literal().unwrap();
            if literal.is_int_literal() {
                Some(literal.get_int().unwrap())
            } else {
                return Err(anyhow::anyhow!("Couldn't parse int literal!"));
            }
        }
        Err(_) => None,
    };

    tq.consume_eq(Token::CAngle)?;

    Ok((IntRange { min, max }, tq.get_idx()))
}

/// Type alias over a `Range<T> where T = f64`
type DblRange = Range<f64>;

pub fn parse_dbl_range(tq: &TokenQueue<Token>) -> ParseResult<DblRange> {
    let mut tq = tq.clone();

    // consume '<'
    tq.consume_eq(Token::OAngle)?;

    // consume min
    let min = match tq.clone().peek_matching(|token| token.is_literal()) {
        Ok(token) => {
            tq.increment();
            let literal = token.get_literal().unwrap();
            if literal.is_dbl_literal() {
                Some(literal.get_dbl().unwrap())
            } else {
                return Err(anyhow::anyhow!("Couldn't parse dbl literal!"));
            }
        }
        Err(_) => None,
    };

    // consume ','
    tq.consume_eq(Token::Comma)?;

    // consume max
    let max = match tq.clone().peek_matching(|token| token.is_literal()) {
        Ok(token) => {
            tq.increment();
            let literal = token.get_literal().unwrap();
            if literal.is_dbl_literal() {
                Some(literal.get_dbl().unwrap())
            } else {
                return Err(anyhow::anyhow!("Couldn't parse dbl literal!"));
            }
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
    Ident(String),
}

pub fn parse_parent_type(tq: &TokenQueue<Token>) -> ParseResult<ParentType> {
    let mut tq = tq.clone();

    let parent_name = tq
        .consume_matching(|tok| tok.is_ident_or_str_literal_tok())?
        .get_ident_or_str_literal()
        .ok_or(anyhow::anyhow!("Couldn't get type name"))?
        .clone();

    match parent_name.to_lowercase().as_str() {
        "int" | "integer" => {
            if let Ok((range, end)) = parse_int_range(&tq) {
                return Ok((ParentType::Int(range), end));
            }
            return Ok((
                ParentType::Int(Range {
                    min: None,
                    max: None,
                }),
                tq.get_idx(),
            ));
        }
        "str" | "string" | "text" => {
            if let Ok((range, end)) = parse_int_range(&tq) {
                return Ok((ParentType::Str(range), end));
            }
            return Ok((
                ParentType::Str(Range {
                    min: None,
                    max: None,
                }),
                tq.get_idx(),
            ));
        }
        "dbl" | "double" | "float" => {
            if let Ok((range, end)) = parse_dbl_range(&tq) {
                return Ok((ParentType::Dbl(range), end));
            }
            return Ok((
                ParentType::Dbl(Range {
                    min: None,
                    max: None,
                }),
                tq.get_idx(),
            ));
        }
        _ => Ok((ParentType::Ident(parent_name), tq.get_idx())),
    }
}

impl Display for ParentType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParentType::Int(range) => {
                if range.min.is_none() && range.max.is_none() {
                    return write!(f, "int");
                }
                write!(f, "int{}", range)
            }
            ParentType::Dbl(range) => {
                if range.min.is_none() && range.max.is_none() {
                    return write!(f, "dbl");
                }
                write!(f, "dbl{}", range)
            }
            ParentType::Str(range) => {
                if range.min.is_none() && range.max.is_none() {
                    return write!(f, "str");
                }
                write!(f, "str{}", range,)
            }
            ParentType::Ident(val) => write!(f, "{}", val),
        }
    }
}

/// Derived data types in the applicaiton.
#[derive(Debug, PartialEq)]
pub struct DType {
    pub parent: ParentType,
    pub nullable: bool,
}

impl Display for DType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}", self.parent, if self.nullable { "?" } else { "" })
    }
}

pub fn parse_dtype(tq: &TokenQueue<Token>) -> ParseResult<DType> {
    let mut tq = tq.clone();

    let parent = tq.parse(parse_parent_type)?;

    let nullable = tq.consume_eq(Token::QMark).is_ok();

    Ok((DType { parent, nullable }, tq.get_idx()))
}

#[derive(Debug, PartialEq)]
pub struct ColumnSchema {
    pub column_name: String,
    pub dtype: DType,
    pub default_value: Option<Literal>,
}

impl Display for ColumnSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.default_value {
            Some(val) => {
                write!(f, "{}: {} = {}", self.column_name, self.dtype, val)
            }
            None => write!(f, "{}: {}", self.column_name, self.dtype),
        }
    }
}

pub fn parse_column_schema(
    tq: &TokenQueue<Token>,
) -> ParseResult<ColumnSchema> {
    let mut tq: TokenQueue<Token> = tq.clone();

    let column_name = tq
        .consume_matching(|tok| tok.is_ident_or_str_literal_tok())?
        .get_ident_or_str_literal()
        .ok_or(anyhow::anyhow!("Couldn't get column name!"))?
        .clone();

    tq.consume_eq(Token::Colon)?;

    let dtype = tq.parse(parse_dtype)?;

    let mut default_value = None;
    if tq.consume_eq(Token::Equals).is_ok() {
        default_value = Some(
            tq.consume_matching(|tok| tok.is_literal())?
                .get_literal()
                .ok_or(anyhow::anyhow!(
                    "Couldn't get default value literal!"
                ))?,
        );
    }

    Ok((
        ColumnSchema {
            column_name,
            dtype,
            default_value: default_value.cloned(),
        },
        tq.get_idx(),
    ))
}

#[derive(Debug, PartialEq)]
pub struct TableSchema {
    pub table_name: String,
    pub columns: Vec<ColumnSchema>,
}

pub fn parse_table_schema(tq: &TokenQueue<Token>) -> ParseResult<TableSchema> {
    let mut tq_mut = tq.clone();

    let table_name = tq
        .peek_matching(|tok| tok.is_ident_or_str_literal_tok())?
        .get_ident_or_str_literal()
        .ok_or(anyhow::anyhow!("Couldn't get table name!"))?;
    tq_mut.increment();

    tq_mut.consume_eq(Token::OParen)?;

    let mut columns = vec![];

    while let Ok(column) = tq_mut.parse(parse_column_schema) {
        columns.push(column);
        if tq_mut.consume_eq(Token::Comma).is_err() {
            break;
        }
    }

    tq_mut.consume_eq(Token::CParen)?;
    Ok((
        TableSchema {
            table_name: table_name.clone(),
            columns,
        },
        tq_mut.get_idx(),
    ))
}

#[derive(Debug, PartialEq)]
pub enum Stmt {
    ParentType(String, ParentType),
    Table(TableSchema),
}

pub fn parse_stmt(tq: &TokenQueue<Token>) -> ParseResult<Stmt> {
    let mut tq = tq.clone();

    match tq.consume() {
        Ok(Token::TypeKwd) => {
            let type_name = tq
                .consume_matching(|tok| tok.is_ident_or_str_literal_tok())?
                .get_ident_or_str_literal()
                .ok_or(anyhow::anyhow!("Couldn't get type name!"))?
                .clone();

            let (parent_type, end) = parse_parent_type(&tq)?;
            // tq.consume_eq(Token::)
            Ok((Stmt::ParentType(type_name.into(), parent_type), end))
        }
        Ok(Token::TableKwd) => {
            let (table_schema, end) = parse_table_schema(&tq)?;
            Ok((Stmt::Table(table_schema), end))
        }
        Ok(tok) => {
            dbg!(tok);
            Err(anyhow::anyhow!("Couldn't parse statement!"))
        }
        Err(_) => Err(anyhow::anyhow!("Couldn't parse statement!")),
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
            return Err(anyhow::anyhow!("Missing semicolon!"));
        }
    }

    if !tq.is_consumed() {
        dbg!(&tq);
        return Err(anyhow::anyhow!("Program ends without a valid statement"));
    }

    Ok((Prgm { stmts }, tq.get_idx()))
}
