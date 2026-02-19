use rlrl::parse::TokenQueue;

use crate::{
    core::schema::SpreadsheetSchema,
    ql::{lex::setup_lexer, parse::parse_spreadsheet_schema},
};

pub mod core;
pub mod json;
pub mod ql;

#[cfg(test)]
mod tests;

pub fn parse_valid_schema_from_str(
    s: &str,
) -> anyhow::Result<SpreadsheetSchema> {
    let tq = TokenQueue::from(setup_lexer().lex(s)?);
    let schema = parse_spreadsheet_schema(&tq)?;
    schema.validate_spreadsheet_schema()?;
    Ok(schema)
}
