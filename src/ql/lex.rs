use rlrl::lex::*;
use std::{fmt::Display, rc::Rc};

/// A literal in the query language.
#[derive(Clone, Debug, PartialEq)]
pub enum Literal {
    Int(i32),
    Dbl(f64),
    Str(Rc<str>),
}

impl Literal {
    pub fn is_str(&self) -> bool {
        match self {
            Self::Str(_) => true,
            _ => false,
        }
    }

    /// Clones the pointer to the string value if `self` is a `Literal::Str`,
    /// returns `None`.
    pub fn get_str(&self) -> Option<Rc<str>> {
        match self {
            Self::Str(val) => Some(val.clone()),
            _ => None,
        }
    }

    pub fn is_i32(&self) -> bool {
        match self {
            Self::Int(_) => true,
            _ => false,
        }
    }

    /// Clones the int value if `self` is a `Literal::Int`, otherwise returns
    /// `None`.
    pub fn get_i32(&self) -> Option<i32> {
        match self {
            Self::Int(val) => Some(val.clone()),
            _ => None,
        }
    }

    pub fn is_f64(&self) -> bool {
        match self {
            Self::Dbl(_) | Self::Int(_) => true,
            _ => false,
        }
    }

    /// Clones the double value if `self` is a `Literal::Dbl`, otherwise returns
    /// `None`.
    pub fn get_f64(&self) -> Option<f64> {
        match self {
            Self::Dbl(val) => Some(val.clone()),
            Self::Int(val) => Some(*val as f64),
            _ => None,
        }
    }
}

impl Display for Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Literal::Int(val) => write!(f, "{val}"),
            Literal::Dbl(val) => write!(f, "{val}"),
            Literal::Str(val) => write!(f, "{val}"),
        }
    }
}

/// Enum representing the tokens available to the lexer.
#[derive(Clone, Debug, PartialEq)]
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
    Ident(Rc<str>),

    // literals
    Literal(Literal),
}

impl Token {
    // helper function for handling identifiers
    pub fn is_ident_or_str_literal_tok(&self) -> bool {
        match self {
            Self::Ident(_) => true,
            Self::Literal(literal) => literal.is_str(),
            _ => false,
        }
    }

    // helper function for handling identifiers
    pub fn get_ident_or_str_literal(&self) -> Option<Rc<str>> {
        match &self {
            Self::Ident(ident) => Some(ident.clone()),
            Self::Literal(literal) => literal.get_str(),
            _ => None,
        }
    }

    pub fn is_literal(&self) -> bool {
        match &self {
            Self::Literal(_) => true,
            _ => false,
        }
    }

    pub fn get_literal(&self) -> Option<&Literal> {
        match &self {
            Self::Literal(literal) => Some(literal),
            _ => None,
        }
    }
}

/// Function to setup the lexer for testing
pub fn setup_lexer() -> Lexer<Token> {
    let mut lexer: Lexer<Token> = Lexer::new();

    // comments
    lexer.add_rule(r"//[^\n\r]*", |_| LexResult::Ignore);

    // whitespace
    lexer.add_rule(r"[\s]+", |_| LexResult::Ignore);

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
    lexer.add_rule(r"tab", |_| LexResult::Token(Token::TableKwd));
    lexer.add_rule(r"table", |_| LexResult::Token(Token::TableKwd));

    // idents
    lexer.add_rule(r"[a-zA-Z][a-zA-Z0-9_]*", |re_match| {
        LexResult::Token(Token::Ident(re_match.as_str().into()))
    });

    // literals
    lexer.add_rule(r"\-?[0-9]+", |re_match| {
        match re_match.as_str().parse::<i32>() {
            Ok(v) => LexResult::Token(Token::Literal(Literal::Int(v))),
            Err(e) => LexResult::Error(e.into()),
        }
    });
    lexer.add_rule(r"\-?[0-9]+(\.[0-9]+)?", |re_match| {
        match re_match.as_str().parse::<f64>() {
            Ok(v) => LexResult::Token(Token::Literal(Literal::Dbl(v))),
            Err(e) => LexResult::Error(e.into()),
        }
    });
    lexer.add_rule("\"[^\"]*\"", |re_match| {
        LexResult::Token(Token::Literal(Literal::Str(
            re_match.as_str()[1..re_match.len() - 1].into(),
        )))
    });

    lexer
}
