pub mod lex;
pub mod parse;

use std::{collections::HashMap, rc::Rc};

use crate::core::schema::{SharedDataType, SharedTableSchema};

/// A symbol in the symbol table; the value of a variable.
pub enum Symbol {
    TableSchema(SharedTableSchema),
    DataType(SharedDataType),
}

/// A symbol table whose keys are identifiers and values are of type `Symbol`.
pub type SymbolTable = HashMap<Rc<str>, Symbol>;

/// A statement in the query language.
pub enum Stmt {
    TableSchema(SharedTableSchema),
    // Note Stmt::TypeDef stores a name for the type while Symbol::DataType
    // does not
    TypeDef(Rc<str>, SharedDataType),
}
