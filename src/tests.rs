use crate::{
    core::schema::SpreadsheetSchema,
    ql::{
        Stmt,
        lex::{Token, setup_lexer},
        parse::{Parse, parse_spreadsheet_schema},
    },
};
use rlrl::parse::TokenQueue;
use std::{collections::HashMap, fs};

const NUM_VALID_TEST_SCHEMA: usize = 2;

fn lex_file(path: &str) -> anyhow::Result<TokenQueue<Token>> {
    let s = fs::read_to_string(path)?;
    let mut tokens = vec![];
    let lexer = setup_lexer();

    for line in s.lines() {
        tokens.extend_from_slice(&lexer.lex(line)?);
    }

    Ok(TokenQueue::from(tokens))
}

fn lex(s: &str) -> anyhow::Result<TokenQueue<Token>> {
    let toks = setup_lexer().lex(s)?;
    Ok(TokenQueue::from(toks))
}

fn assert_maps_to_stmt(expected: &str) -> anyhow::Result<()> {
    let mut tq = lex(expected)?;
    let stmt = tq.parse_with_mut(Stmt::parse, &mut HashMap::new())?;
    let actual = stmt.to_string();
    if actual != expected {
        return Err(anyhow::anyhow!(
            "Expected stmt {expected}, got {}",
            actual
        ));
    }
    Ok(())
}

fn assert_maps_to_schema(expected: &str) -> anyhow::Result<()> {
    let tq = lex(expected)?;
    let schema = parse_spreadsheet_schema(&tq)?;
    let actual = schema.to_string();
    if actual.trim() != expected.trim() {
        return Err(anyhow::anyhow!(
            "Expected schema:
'{expected}'
Got:
'{}'",
            actual
        ));
    }
    Ok(())
}

fn parse_schema_from_str(s: &str) -> anyhow::Result<SpreadsheetSchema> {
    let tq = lex(s)?;
    let schema = parse_spreadsheet_schema(&tq)?;
    Ok(schema)
}

#[test]
fn test_ddl_1() -> anyhow::Result<()> {
    let stmt1 = "table T (a: int<, >);";
    let stmt2 = "type myType int<1, 5>;";

    // test the program parses simple statements
    assert_maps_to_stmt(stmt1)?;
    assert_maps_to_stmt(stmt2)?;

    // test the program parses simple single-statement schemas
    assert_maps_to_schema(stmt1)?;

    Ok(())
}

#[test]
fn test_ddl_2() -> anyhow::Result<()> {
    // test the program parses slightly more complex schemas

    // table with multiple columns
    assert_maps_to_schema(
        "table Users (userId: int<0, >, userName: str<2, 32>);",
    )?;

    // multi-table schemas
    assert_maps_to_schema(
        "table T1 (a: int<, >);
table T2 (b: int<, >);",
    )?;

    // schemas with comments
    assert!(
        parse_schema_from_str(
            "
			// comment
			table T (a: int);
			"
        )?
        .to_string()
        .trim()
            == "table T (a: int<, >);"
    );

    // schemas with typedefs
    assert!(
        parse_schema_from_str(
            "type uIntType int<0, >; table T (id: uIntType);"
        )?
        .to_string()
        .trim()
            == "table T (id: int<0, >);"
    );

    Ok(())
}

#[test]
fn test_ddl_3() -> anyhow::Result<()> {
    // test the program parses more complex schemas without throwing errors
    for i in 1..(NUM_VALID_TEST_SCHEMA + 1) {
        let path_to_input =
            format!("test_artifacts/valid_schemas/input/input_{i}.txt");
        let path_to_output =
            format!("test_artifacts/valid_schemas/output/output_{i}.txt");
        let tq = lex_file(&path_to_input)?;
        let schema = parse_spreadsheet_schema(&tq)?;

        let expected = fs::read_to_string(path_to_output)?;
        let actual = schema.to_string();

        assert!(
            actual.trim() == expected.trim(),
            "'{}' != '{}'",
            actual.trim(),
            expected.trim()
        );
    }

    Ok(())
}
