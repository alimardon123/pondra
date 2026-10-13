//! Postgres's error codes (SQLSTATE) for Pondra's errors, whichever door they leave by (ADR-036
//! §4): the Postgres port sends them as Postgres does, HTTP as `code` beside `error` (and the
//! `x-pondra-sqlstate` header), Flight SQL as the nearest gRPC status, the Python client as
//! `PondraError.sqlstate`. So a client can tell a constraint from a syntax error, and retry what a
//! conflict refused (40001), without reading words. A typed error says its code; DataFusion's and
//! the rest are known by their words; anything else is XX000 (internal_error), as in Postgres.

/// The SQLSTATE of an error.
pub fn of(e: &anyhow::Error) -> &'static str {
    if let Some(v) = e.chain().find_map(|c| c.downcast_ref::<crate::views::Violation>()) {
        return v.1; // check_violation (a table's CHECK, a view's expectation), or an enum's invalid_text_representation
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

/// A code another node answered with (`x-pondra-sqlstate`), as one of this node's: five digits or
/// capital letters, each kept once (a code is `&'static`). XX000 says nothing, so it is none.
pub fn received(code: &str) -> Option<&'static str> {
    static SEEN: std::sync::LazyLock<std::sync::Mutex<std::collections::HashSet<&'static str>>> = std::sync::LazyLock::new(Default::default);
    if code.len() != 5 || code == "XX000" || !code.bytes().all(|b| b.is_ascii_digit() || b.is_ascii_uppercase()) {
        return None;
    }
    let mut seen = SEEN.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(c) = seen.get(code) {
        return Some(c);
    }
    let c: &'static str = Box::leak(code.to_string().into_boxed_str());
    seen.insert(c);
    Some(c)
}

/// The code an error's words point to (the first that matches, most particular first).
fn by_words(text: &str) -> &'static str {
    let t = text.to_lowercase();
    let has = |w: &str| t.contains(w);
    match () {
        _ if has("could not serialize") => "40001",                                             // serialization_failure
        _ if has("cannot insert a non-default value") || has("can only be updated to default") => "428C9", // generated_always
        _ if has("nextval: reached") => "2200H",                                                // sequence_generator_limit_exceeded
        _ if has("is not yet defined in this session") => "55000",                              // object_not_in_prerequisite_state
        _ if has("is not a variable which can be viewed with 'show'") => "42704",                // undefined_object (Postgres: unrecognized configuration parameter)
        _ if has("is a session's:") => "55000",                                                 // (a SET, BEGIN or TEMP table sent with no session: send x-pondra-session)
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
        _ if has("type \"") && has("does not exist") => "42704",                                // undefined_object (`types.rs`)
        _ if has("because column") && (has("uses it") || has("use it")) => "2BP01",              // dependent_objects_still_exist (DROP TYPE)
        _ if has("already exists") || has("exists already") => "42P07",                        // duplicate_table
        _ if has("multiple primary keys") => "42P16",                                          // invalid_table_definition
        _ if has("divide by zero") || has("division by zero") => "22012",                       // division_by_zero
        _ if has("overflow") || has("out of range") => "22003",                                 // numeric_value_out_of_range
        _ if has("cast error") || has("cannot cast") || has("can't cast") || has("invalid input syntax") || has("could not parse") || has("invalid input value for enum") => "22P02", // invalid_text_representation
        _ if has("can't reach the bucket") || has("catching up with its leader") => "57P03",   // cannot_connect_now (a leader cut off, a node just started: ask another node)
        _ if has("error sending request") => "58030",                                           // io_error (the bucket or another node didn't answer: try again)
        _ if has("deadlock detected") => "40P01",                                               // deadlock_detected (`locks.rs`)
        _ if has("lock timeout") || has("could not obtain lock") => "55P03",                    // lock_not_available (a lock wait, or NOWAIT)
        _ if has("this node is stopping") => "57P01",                                           // admin_shutdown (a drain ends a wait)
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
        ("undefined_function", "42883"), ("undefined_table", "42P01"), ("invalid_schema_name", "3F000"), ("invalid_catalog_name", "3D000"), ("duplicate_table", "42P07"),
        ("division_by_zero", "22012"), ("numeric_value_out_of_range", "22003"), ("invalid_text_representation", "22P02"),
        ("query_canceled", "57014"), ("out_of_memory", "53200"), ("too_many_connections", "53300"), ("feature_not_supported", "0A000"),
        ("read_only_sql_transaction", "25006"), ("raise_exception", "P0001"), ("assert_failure", "P0004"), ("internal_error", "XX000"),
    ];
    NAMES.iter().find(|(n, _)| n.eq_ignore_ascii_case(name)).map(|(_, c)| *c)
}

/// The HTTP status nearest a SQLSTATE (ADR-036 §4). A client's own mistake — bad SQL, a value that
/// breaks a constraint, a name that isn't there — is a 4xx, so a proxy, a monitor or a tool reads it
/// as the user's error, not a server fault; a transient condition (catching up, a timeout, the
/// bucket not answering) is 503, to try again; only a real fault (XX, 58, an unknown code) is 500.
/// The body still carries the message and `x-pondra-sqlstate` the exact code, for a client that
/// wants more than the class.
pub fn http(code: &str) -> u16 {
    match code {
        "28P01" | "28000" => 401,                                         // invalid_authorization: sign in
        "42501" => 403,                                                   // insufficient_privilege
        "42P01" | "42703" | "42883" | "42704" | "3F000" | "3D000" => 404, // undefined table / column / function / type / schema / database
        "42P07" | "23505" => 409,                                         // already exists, duplicate key
        "53300" => 429,                                                   // too many of a user's queries at once (a quota)
        "0A000" => 501,                                                   // feature_not_supported
        "57014" | "57P03" | "58030" | "53200" => 503,                     // timeout, catching up / cut off, bucket or node silent, out of memory: try again
        c if c.starts_with("40") || c.starts_with("55") || c.starts_with("2B") => 409, // serialize / deadlock, wrong state, still depended on
        c if c.starts_with("08") || c.starts_with("53") || c.starts_with("57") => 503, // connection, resources, operator intervention
        c if c.starts_with("22") || c.starts_with("23") || c.starts_with("25") || c.starts_with("42") || c.starts_with("P0") => 400, // a bad request (data, constraint, transaction state, syntax, a procedure's RAISE)
        _ => 500,                                                         // XX, 58, unknown: a real fault
    }
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
        "23514" | "23502" | "25006" | "25P01" | "25P02" | "25P03" => tonic::Code::FailedPrecondition, // (a transaction that isn't open, failed, or idle-ended)
        c if c.starts_with("42") || c.starts_with("22") => tonic::Code::InvalidArgument,
        _ => tonic::Code::Internal,
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn received_from_another_node() {
        assert_eq!([super::received("42P16"), super::received("42P16"), super::received("XX000"), super::received("4"), super::received("42p16")], [Some("42P16"), Some("42P16"), None, None, None]);
    }

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
        assert_eq!(of("multiple primary keys for table \"t\" are not allowed"), "42P16");
        assert_eq!(of("Error during planning: 'nope' is not a variable which can be viewed with 'SHOW'"), "42704");
        assert_eq!(of("a transaction is a session's: a Postgres connection's, or the Python or JavaScript client's (over HTTP, send x-pondra-session: <id>)"), "55000");
        assert_eq!(of("External error: invalid input value for enum mood: \"angry\" (its labels: 'sad', 'ok')"), "22P02"); // (a bulk INSERT's stream)
        assert_eq!(of("type \"mood\" does not exist"), "42704");
        assert_eq!(of("cannot drop type mood because column t.m uses it"), "2BP01");
        assert_eq!(of("cannot drop type mood because columns use it: a.m, b.n; drop or change them first"), "2BP01");
        assert_eq!(of(crate::cluster::CUT_OFF_SAYS), "57P03");
        assert_eq!(of(crate::store::CATCHING_UP), "57P03");
        assert_eq!(of("error sending request for url (http://10.0.0.2:8080/cluster/commit)"), "58030");
        assert_eq!(of("cannot insert a non-DEFAULT value into column \"id\" of t: it is an identity column defined as GENERATED ALWAYS"), "428C9");
        assert_eq!(of("nextval: reached maximum value of sequence \"s\" (3)"), "2200H");
        assert_eq!(of("currval of sequence \"s\" is not yet defined in this session"), "55000");
        assert_eq!(of("relation \"s\" does not exist"), "42P01");
        assert_eq!(of("something else"), "XX000");
        assert_eq!(super::of(&super::coded("40001", "could not serialize access due to concurrent update")), "40001");
        assert_eq!(super::of(&super::coded("3D000", "no database x (CREATE DATABASE x)")), "3D000"); // (USE of a database that isn't there)
        assert_eq!(super::named("invalid_catalog_name"), Some("3D000"));
        assert_eq!(super::of(&anyhow::Error::new(crate::views::Violation("new row for relation \"t\" violates check constraint \"c\"".into(), "23514"))), "23514");
        assert_eq!(super::of(&anyhow::Error::new(crate::views::Violation("invalid input value for enum mood: \"x\"".into(), "22P02"))), "22P02");
        // (a lock refusal a follower reads back as words keeps its code: "canceling statement" is a cancel, 57014, otherwise)
        assert_eq!(of("the leader at http://10.0.0.1:8080: canceling statement due to lock timeout"), "55P03");
        assert_eq!(of("deadlock detected: this write waits for a row another transaction holds"), "40P01");
        assert_eq!(of("could not obtain lock on a row: it is held by another transaction (NOWAIT)"), "55P03");
        assert_eq!(of("this node is stopping: its lock waits end, and its transaction with them"), "57P01");
        assert_eq!(of("canceling statement due to statement timeout"), "57014");
    }

    #[test]
    fn http_status() {
        let h = super::http;
        // A client's mistake is a 4xx, not a 500 a proxy or a monitor would read as a server fault.
        assert_eq!(h("28P01"), 401); // sign in
        assert_eq!(h("42501"), 403); // insufficient_privilege
        assert_eq!(h("42P01"), 404); // undefined_table
        assert_eq!(h("42703"), 404); // undefined_column
        assert_eq!(h("42883"), 404); // undefined_function
        assert_eq!(h("42704"), 404); // undefined_object (a type)
        assert_eq!(h("3F000"), 404); // invalid_schema_name
        assert_eq!(h("3D000"), 404); // invalid_catalog_name (unknown database)
        assert_eq!(h("42P07"), 409); // duplicate_table
        assert_eq!(h("23505"), 409); // unique_violation
        assert_eq!(h("40001"), 409); // serialization_failure (retry)
        assert_eq!(h("55000"), 409); // object_not_in_prerequisite_state
        assert_eq!(h("2BP01"), 409); // dependent_objects_still_exist
        assert_eq!(h("23514"), 400); // check_violation
        assert_eq!(h("23502"), 400); // not_null_violation
        assert_eq!(h("22P02"), 400); // invalid_text_representation
        assert_eq!(h("22003"), 400); // numeric_value_out_of_range
        assert_eq!(h("428C9"), 400); // generated_always
        assert_eq!(h("42601"), 400); // syntax_error
        assert_eq!(h("25006"), 400); // read_only_sql_transaction
        assert_eq!(h("P0001"), 400); // raise_exception (a procedure's RAISE)
        assert_eq!(h("53300"), 429); // too many of a user's queries at once
        assert_eq!(h("0A000"), 501); // feature_not_supported
        assert_eq!(h("57014"), 503); // query_canceled (timeout)
        assert_eq!(h("57P03"), 503); // cannot_connect_now (catching up / cut off)
        assert_eq!(h("58030"), 503); // io_error (bucket or node silent)
        assert_eq!(h("53200"), 503); // out_of_memory
        // Only a real fault stays a 500.
        assert_eq!(h("XX000"), 500); // internal_error
        assert_eq!(h("58000"), 500); // system_error
    }
}
