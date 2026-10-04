//! Postgres's error codes (SQLSTATE) for Pondra's errors, whichever door they leave by (ADR-036
//! §4): the Postgres port sends them as Postgres does, HTTP as `code` beside `error` (and the
//! `x-pondra-sqlstate` header), Flight SQL as the nearest gRPC status, the Python client as
//! `PondraError.sqlstate`. So a client can tell a constraint from a syntax error, and retry what a
//! conflict refused (40001), without reading words. A typed error says its code; DataFusion's and
//! the rest are known by their words; anything else is XX000 (internal_error), as in Postgres.

/// The SQLSTATE of an error.
pub fn of(e: &anyhow::Error) -> &'static str {
    if crate::views::refused(e) {
        return "23514"; // check_violation (a table's CHECK, a view's expectation)
    }
    if let Some(c) = e.chain().find_map(|c| c.downcast_ref::<Coded>()) {
        return c.0;
    }
    by_words(&crate::ext::said(e))
}

/// An error that says its own SQLSTATE.
#[derive(Debug)]
pub struct Coded(pub &'static str, pub String);

impl std::fmt::Display for Coded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str(&self.1) }
}

impl std::error::Error for Coded {}

/// `code`: an error with that SQLSTATE.
pub fn coded(code: &'static str, message: impl Into<String>) -> anyhow::Error { anyhow::Error::new(Coded(code, message.into())) }

/// The code an error's words point to (the first that matches, most particular first).
fn by_words(text: &str) -> &'static str {
    let t = text.to_lowercase();
    let has = |w: &str| t.contains(w);
    match () {
        _ if has("could not serialize") => "40001",                                             // serialization_failure
        _ if has("cannot insert a non-default value") || has("can only be updated to default") => "428C9", // generated_always
        _ if has("nextval: reached") => "2200H",                                                // sequence_generator_limit_exceeded
        _ if has("is not yet defined in this session") => "55000",                              // object_not_in_prerequisite_state
        _ if has("violates check constraint") => "23514",                                       // check_violation
        _ if has("is not null, and a row gives it no value") || has("violates not-null") => "23502", // not_null_violation
        _ if has("duplicate key") => "23505",                                                   // unique_violation
        _ if has("permission denied") || has("may not ") && (has("this user") || has("this token")) => "42501", // insufficient_privilege
        _ if has("wrong token") || has("password authentication failed") => "28P01",            // invalid_password
        _ if has("sql error: parsererror") || has("syntax error") || has("expected: ") && has("found: ") => "42601", // syntax_error
        _ if has("no field named") || has("column") && has("not found") || has("no column ") => "42703", // undefined_column
        _ if has("invalid function") || has("function") && (has("not found") || has("does not exist")) || has("no function ") => "42883", // undefined_function
        _ if has("table '") && has("not found") || has("no table ") || has("no table or view") || has("relation") && has("does not exist") => "42P01", // undefined_table
        _ if has("no schema ") || has("schema") && has("does not exist") => "3F000",            // invalid_schema_name
        _ if has("already exists") || has("exists already") => "42P07",                        // duplicate_table
        _ if has("divide by zero") || has("division by zero") => "22012",                       // division_by_zero
        _ if has("overflow") || has("out of range") => "22003",                                 // numeric_value_out_of_range
        _ if has("cast error") || has("cannot cast") || has("can't cast") || has("invalid input syntax") || has("could not parse") => "22P02", // invalid_text_representation
        _ if has("can't reach the bucket") => "57P03",                                         // cannot_connect_now (a leader cut off: ask another node)
        _ if has("error sending request") => "58030",                                           // io_error (the bucket or another node didn't answer: try again)
        _ if has("statement timeout") || has("canceling statement") || has("timed out") => "57014", // query_canceled
        _ if has("resources exhausted") || has("out of memory") || has("memory limit") => "53200", // out_of_memory
        _ if has("too many") && has("at once") => "53300",                                      // too_many_connections (a user's queries at once)
        _ if has("not supported") || has("isn't supported") || has("not implemented") || has("not yet supported") => "0A000", // feature_not_supported
        _ if has("read-only") || has("read only") => "25006",                                   // read_only_sql_transaction
        _ => "XX000",                                                                          // internal_error
    }
}

/// The SQLSTATE of one of Postgres's condition names (a script's `EXCEPTION WHEN unique_violation`).
pub fn named(name: &str) -> Option<&'static str> {
    const NAMES: &[(&str, &str)] = &[
        ("serialization_failure", "40001"), ("check_violation", "23514"), ("not_null_violation", "23502"), ("unique_violation", "23505"),
        ("insufficient_privilege", "42501"), ("invalid_password", "28P01"), ("syntax_error", "42601"), ("undefined_column", "42703"),
        ("undefined_function", "42883"), ("undefined_table", "42P01"), ("invalid_schema_name", "3F000"), ("duplicate_table", "42P07"),
        ("division_by_zero", "22012"), ("numeric_value_out_of_range", "22003"), ("invalid_text_representation", "22P02"),
        ("query_canceled", "57014"), ("out_of_memory", "53200"), ("too_many_connections", "53300"), ("feature_not_supported", "0A000"),
        ("read_only_sql_transaction", "25006"), ("raise_exception", "P0001"), ("assert_failure", "P0004"), ("internal_error", "XX000"),
    ];
    NAMES.iter().find(|(n, _)| n.eq_ignore_ascii_case(name)).map(|(_, c)| *c)
}

/// The gRPC status nearest a SQLSTATE (Flight SQL's errors).
pub fn grpc(code: &str) -> tonic::Code {
    match code {
        "40001" => tonic::Code::Aborted,
        "42501" => tonic::Code::PermissionDenied,
        "28P01" | "28000" => tonic::Code::Unauthenticated,
        "42P01" | "42703" | "42883" | "3F000" => tonic::Code::NotFound,
        "42P07" | "23505" => tonic::Code::AlreadyExists,
        "57014" => tonic::Code::DeadlineExceeded,
        "53200" | "53300" => tonic::Code::ResourceExhausted,
        "0A000" => tonic::Code::Unimplemented,
        "23514" | "23502" | "25006" => tonic::Code::FailedPrecondition,
        c if c.starts_with("42") || c.starts_with("22") => tonic::Code::InvalidArgument,
        _ => tonic::Code::Internal,
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn known_by_words() {
        let of = |s: &str| super::of(&anyhow::anyhow!("{s}"));
        assert_eq!(of("Error during planning: table 'datafusion.public.nope' not found"), "42P01");
        assert_eq!(of("Schema error: No field named nope. Valid fields are t.a."), "42703");
        assert_eq!(of("SQL error: ParserError(\"Expected: an SQL statement, found: SELEC at Line: 1, Column: 1\")"), "42601");
        assert_eq!(of("Arrow error: Divide by zero error"), "22012");
        assert_eq!(of("orders.id is NOT NULL, and a row gives it no value"), "23502");
        assert_eq!(of("permission denied: INSERT on t (GRANT INSERT ON t TO ann)"), "42501");
        assert_eq!(of("Invalid function 'nope'.\nDid you mean 'now'?"), "42883");
        assert_eq!(of("table t already exists"), "42P07");
        assert_eq!(of(crate::cluster::CUT_OFF_SAYS), "57P03");
        assert_eq!(of("error sending request for url (http://10.0.0.2:8080/cluster/commit)"), "58030");
        assert_eq!(of("cannot insert a non-DEFAULT value into column \"id\" of t: it is an identity column defined as GENERATED ALWAYS"), "428C9");
        assert_eq!(of("nextval: reached maximum value of sequence \"s\" (3)"), "2200H");
        assert_eq!(of("currval of sequence \"s\" is not yet defined in this session"), "55000");
        assert_eq!(of("relation \"s\" does not exist"), "42P01");
        assert_eq!(of("something else"), "XX000");
        assert_eq!(super::of(&super::coded("40001", "could not serialize access due to concurrent update")), "40001");
        assert_eq!(super::of(&anyhow::Error::new(crate::views::Violation("new row for relation \"t\" violates check constraint \"c\"".into()))), "23514");
    }
}
