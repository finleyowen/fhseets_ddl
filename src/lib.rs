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
    Comma,
    Equals,
    QMark,

    // kwds
    TypeKwd,
    FnKwd,

    // ident
    Ident(String),

    // literals
    IntLiteral(i32),
    DblLiteral(f64),
    StrLiteral(String),
}

impl Token {
    // helper function for handling identifiers
    fn is_ident_tok(&self) -> bool {
        if let Self::Ident(_) = self {
            return true;
        }
        false
    }

    // helper function for handling identifiers
    fn get_ident(&self) -> Result<&String, ParseError> {
        if let Self::Ident(ident) = self {
            return Ok(ident);
        }
        Err(ParseError::new(""))
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
    lexer.add_rule(r"\,", |_| LexResult::Token(Token::Comma));
    lexer.add_rule(r"=", |_| LexResult::Token(Token::Equals));
    lexer.add_rule(r"\?", |_| LexResult::Token(Token::QMark));

    // kwds
    lexer.add_rule(r"type", |_| LexResult::Token(Token::TypeKwd));
    lexer.add_rule(r"fn", |_| LexResult::Token(Token::FnKwd));

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
        LexResult::Token(Token::StrLiteral(
            re_match.as_str().trim_matches('"').into(),
        ))
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

    Ok((IntRange { min, max }, 0))
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
        .consume_matching(|tok| tok.is_ident_tok())?
        .get_ident()?
        .clone();

    let nullable = tq.consume_eq(Token::QMark).is_ok();

    match parent_name.to_lowercase().as_str() {
        "int" | "integer" => {
            let (range, end) = parse_int_range(&tq)?;
            return Ok((
                DType {
                    nullable,
                    parent: ParentType::Int(range),
                },
                end,
            ));
        }
        "str" | "string" | "text" => {
            let (range, end) = parse_int_range(&tq)?;
            return Ok((
                DType {
                    nullable,
                    parent: ParentType::Str(range),
                },
                end,
            ));
        }
        "dbl" | "double" | "float" => {
            let (range, end) = parse_dbl_range(&tq)?;
            return Ok((
                DType {
                    nullable,
                    parent: ParentType::Dbl(range),
                },
                end,
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
pub enum Stmt {
    TypeDef(String, DType),
}

pub fn parse_stmt(
    tq: &TokenQueue<Token>,
) -> Result<(Stmt, usize), Box<dyn Error>> {
    let mut tq = tq.clone();

    match tq.consume() {
        Ok(Token::TypeKwd) => {
            // dbg!("consumed type keyword");

            let type_name = tq
                .consume_matching(|tok| tok.is_ident_tok())?
                .get_ident()?
                .clone();

            // dbg!(&type_name);

            let (dtype, end) = parse_data_type(&tq)?;
            Ok((Stmt::TypeDef(type_name.into(), dtype), end))
        }
        Ok(_) => {
            // dbg!(tok);
            Err("Couldn't parse statement!".into())
        }
        Err(_) => {
            // dbg!(err);
            Err("Couldn't parse statement!".into())
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::*;
    use rlrl::parse::TokenQueue;
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

        Ok(())
    }

    #[test]
    fn stmt_test() -> Result<(), Box<dyn Error>> {
        let tokens = setup_lexer().lex("type i1to5 int<1,5>")?;
        let (stmt, _) = parse_stmt(&TokenQueue::new(tokens))?;

        assert!(
            stmt == Stmt::TypeDef(
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

        Ok(())
    }
}
