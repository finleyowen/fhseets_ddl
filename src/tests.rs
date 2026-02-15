
use crate::lex::*;
use crate::parse::*;
use rlrl::prelude::*;

fn lex(s: &str) -> anyhow::Result<TokenQueue<Token>> {
    let lexer = setup_lexer();
    let tokens = lexer.lex(s)?;
    let tq = TokenQueue::from(tokens);
    Ok(tq)
}

fn maps_to_int_range(s: &str) -> anyhow::Result<bool> {
    let out = parse_int_range(&lex(&s)?)?.0.to_string();
    Ok(out == s)
}

fn maps_to_dbl_range(s: &str) -> anyhow::Result<bool> {
    let out = parse_dbl_range(&lex(&s)?)?.0.to_string();
    Ok(out == s)
}

fn maps_to_data_type(s: &str) -> anyhow::Result<bool> {
    let out = parse_dtype(&lex(&s)?)?.0.to_string();
    Ok(out == s)
}

fn maps_to_column_schema(s: &str) -> anyhow::Result<bool> {
    let out = parse_column_schema(&lex(&s)?)?.0.to_string();
    Ok(out == s)
}

fn parses_to_prgm(s: &str) -> anyhow::Result<bool> {
    let mut tq = lex(s)?;
    let _prgm = tq.parse(parse_prgm)?;
    Ok(true)
}

#[test]
fn int_range_test() -> anyhow::Result<()> {
    assert!(maps_to_int_range("<5,10>")?);
    assert!(maps_to_int_range("<10,10>")?);
    assert!(maps_to_int_range("<,10>")?);
    assert!(maps_to_int_range("<5,>")?);

    Ok(())
}

#[test]
fn dbl_range_test() -> anyhow::Result<()> {
    assert!(maps_to_dbl_range("<5,>")?);
    assert!(maps_to_dbl_range("<,10>")?);
    assert!(maps_to_dbl_range("<1.1,9.9>")?);
    assert!(maps_to_dbl_range("<1.1,9.900009>")?);

    Ok(())
}

#[test]
fn data_type_test() -> anyhow::Result<()> {
    assert!(maps_to_data_type("int<5,10>")?);
    assert!(maps_to_data_type("int<5,10>?")?);
    assert!(maps_to_data_type("str<5,10>")?);
    assert!(maps_to_data_type("str<5,10>?")?);
    assert!(maps_to_data_type("dbl<5.4,10.3>")?);
    assert!(maps_to_data_type("dbl<5,10>?")?);
    assert!(maps_to_data_type("int1to5?")?);
    Ok(())
}

#[test]
fn column_schema_test() -> anyhow::Result<()> {
    assert!(maps_to_column_schema("abc: int<5,5>")?);
    assert!(maps_to_column_schema("\"My column\": int<5,5>")?);
    assert!(maps_to_column_schema("\"My optional column\": int<5,5>?")?);

    Ok(())
}

#[test]
fn stmt_test() -> anyhow::Result<()> {
    let mut tq = lex("type i1to5 int<1,5>")?;
    let stmt = tq.parse(parse_stmt)?;
    assert!(
        stmt == Stmt::ParentType(
            "i1to5".into(),
            ParentType::Int(Range {
                min: Some(1),
                max: Some(5)
            })
        )
    );

    let tokens = setup_lexer().lex("table Table1(a: int?, b: dbl?, c:str?)")?;
    assert!(parse_stmt(&TokenQueue::from(tokens)).is_ok());

    Ok(())
}

#[test]
fn ident_test() -> anyhow::Result<()> {
    let ident = "helloWorld";
    let str_lit = "\"helloWorld\"";

    let ident_tok = Token::Ident(ident.into());
    let str_lit_tok = Token::Literal(Literal::Str(str_lit.into()));
    assert!(
        ident_tok.is_ident_or_str_literal_tok()
            && ident_tok.get_ident_or_str_literal() == Some(&ident.into()),
    );
    assert!(
        str_lit_tok.is_ident_or_str_literal_tok()
            && str_lit_tok.get_ident_or_str_literal() == Some(&str_lit.into())
    );
    Ok(())
}

#[test]
fn table_schema_test() -> anyhow::Result<()> {
    let mut tq = lex("Table(
            col1: int<1, 5>, 
            col2: dbl<1, 5>, 
            col3: str<5, 10>,
            col4: int<1, 6>?
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
                        },
                        default_value: None
                    },
                    ColumnSchema {
                        column_name: "col2".into(),
                        dtype: DType {
                            parent: ParentType::Dbl(Range {
                                min: Some(1.0),
                                max: Some(5.0)
                            }),
                            nullable: false
                        },
                        default_value: None
                    },
                    ColumnSchema {
                        column_name: "col3".into(),
                        dtype: DType {
                            parent: ParentType::Str(Range {
                                min: Some(5),
                                max: Some(10)
                            }),
                            nullable: false
                        },
                        default_value: None
                    },
                    ColumnSchema {
                        column_name: "col4".into(),
                        dtype: DType {
                            parent: ParentType::Int(Range {
                                min: Some(1),
                                max: Some(6)
                            }),
                            nullable: true
                        },
                        default_value: None
                    }
                ]
            }
    );

    Ok(())
}

#[test]
fn parse_prgm_test() -> anyhow::Result<()> {
    assert!(parses_to_prgm(include_str!(
        "../test_artifacts/valid_schemas/valid_schema_1.txt"
    ))?);
    assert!(parses_to_prgm(include_str!(
        "../test_artifacts/valid_schemas/valid_schema_2.txt"
    ))?);

    Ok(())
}
