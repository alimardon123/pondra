//! Postgres's error codes (SQLSTATE) for Pondra's errors, whichever door they leave by (ADR-036
//! §4): the Postgres port sends them as Postgres does, HTTP as `code` beside `error` (and the
//! `x-pondra-sqlstate` header), Flight SQL as the nearest gRPC status, the Python client as
//! `PondraError.sqlstate`. So a client can tell a constraint from a syntax error, and retry what a
//! conflict refused (40001), without reading words. A typed error says its code; DataFusion's and
//! the rest are known by their words; anything else is XX000 (internal_error), as in Postgres.

/// The SQLSTATE of an error.
pub fn of(e: &anyhow::Error) -> &'static str {
    if let Some(v) = e.chain().find_map(|c| c.downcast_ref::<Violation>()) {
        return v.1; // check_violation (a table's CHECK, a view's expectation), or an enum's invalid_text_representation
    }
    if let Some(c) = e.chain().find_map(|c| c.downcast_ref::<Coded>()) {
        return c.0;
    }
    // (without the lines a Python routine's error quotes, for every caller: they are its code, not what went wrong, so they never pick the code)
    by_words(&unquoted(&words(e)))
}

/// An error in words: its causes after it, each said once, DataFusion's and Arrow's labels taken off, and the files it
/// names as SQL named them (`ext_names::readable`, the same at every door). A node leaves out more: `ext::said`.
pub fn words(e: &anyhow::Error) -> String {
    let mut out = String::new();
    for c in e.chain().map(|c| c.to_string()) {
        // Unwrap DataFusion's and Arrow's wrappers, repeatedly, so the inner message — the part a
        // user can act on — leads, not an engine-internal label.
        let mut c = c;
        while let Some(inner) = ["External error: ", "Error during planning: ", "Arrow error: ", "Execution error: "]
            .iter()
            .find_map(|p| c.strip_prefix(p))
        {
            c = inner.to_string();
        }
        if !out.contains(c.trim()) {
            out += if out.is_empty() { "" } else { ": " };
            out += &c;
        }
    }
    crate::ext_names::readable(&SPILL_SAYS.iter().fold(out, |out, (from, to)| out.replace(from, to))) // (DataFusion's words for spilling, in the setting Pondra asks for; files as SQL named them)
}

/// An error in words for whoever sent the statement (`words`), a Python routine's quoted lines cut when the caller
/// may not see its code (S27: `unquoted`).
pub fn told(e: &anyhow::Error, shows_code: bool) -> String {
    match shows_code {
        true => words(e),
        false => unquoted(&words(e)),
    }
}

/// An error as every door answers it: the HTTP status its SQLSTATE takes, the SQLSTATE, and its words (`told`).
pub fn answer(e: &anyhow::Error, shows_code: bool) -> (u16, &'static str, String) {
    let code = of(e);
    (http(code), code, told(e, shows_code))
}

/// DataFusion's words for a spill that ran past its cap, and for spilling turned off, said with the
/// setting a user can change: `PONDRA_SPILL_GB`, not DataFusion's `datafusion.runtime.*` (which Pondra
/// refuses, invariant 188). The first words stay, so `codes::of` still reads them. Spilling is off on a page too (it has no
/// disk), so the words for it are true of both.
const SPILL_SAYS: &[(&str, &str)] = &[
    ("Please try increasing the config: `datafusion.runtime.max_temp_directory_size`.", "Raise PONDRA_SPILL_GB to let it spill more."),
    ("(DiskManager is disabled)", "(nothing spills to disk: PONDRA_SPILL_GB is 0, or this is a page)"),
];

/// A worker's error without the code it quotes, for a caller not shown it (S27): `failure` in worker.py ends it
/// with `  line 3: <the line>` for each place in the body, which become `  line 3`. Here for a procedure (and its
/// run's log) and in `ext::said` for every door, since a function's batches may run where no caller is known, and in
/// `codes::of`, so no quoted line picks an error's code.
pub fn unquoted(error: &str) -> String {
    let line = |l: &str| match l.strip_prefix("  line ").and_then(|r| r.split_once(": ")) {
        Some((n, _)) if n.chars().all(|c| c.is_ascii_digit()) => format!("  line {n}"),
        _ => l.to_string(),
    };
    error.lines().map(line).collect::<Vec<_>>().join("\n")
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

/// A row an expectation or a table's CHECK refuses (Postgres's `check_violation`, 23514), or a value
/// its column's enum doesn't list (22P02): its words, and its SQLSTATE.
#[derive(Debug)]
pub struct Violation(pub String, pub &'static str);

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str(&self.0) }
}

impl std::error::Error for Violation {}

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
    let first_said = |w: &str| t.split_once(' ').is_some_and(|(_, rest)| rest.starts_with(w)); // (`v is a view…`: what a name is, said first)
    match () {
        _ if has("could not serialize") => "40001",                                             // serialization_failure
        _ if has("cannot insert a non-default value") || has("can only be updated to default") => "428C9", // generated_always
        _ if has("nextval: reached") => "2200H",                                                // sequence_generator_limit_exceeded
        _ if has("is not yet defined in this session") => "55000",                              // object_not_in_prerequisite_state
        _ if has("is not a variable which can be viewed with 'show'") => "42704",                // undefined_object (Postgres: unrecognized configuration parameter)
        _ if has("is a session's:") => "55000",                                                 // (a SET, BEGIN or TEMP table sent with no session: send x-pondra-session)
        _ if has("violates check constraint") => "23514",                                       // check_violation
        _ if has("is not null, and a row gives it no value") || has("violates not-null") || has("has null but schema specifies non-nullable") || has("declared as non-nullable but contains null") => "23502", // not_null_violation (a page's INSERT and UPDATE: DataFusion's and Arrow's words)
        _ if has("duplicate key") => "23505",                                                   // unique_violation
        _ if has("permission denied") || has("may not ") && (has("this user") || has("this token")) => "42501", // insufficient_privilege
        _ if has("wrong token") || has("password authentication failed") => "28P01",            // invalid_password
        _ if has("sql error: parsererror") || has("syntax error") || has("expected: ") && has("found: ") => "42601", // syntax_error
        _ if has("inconsistent data length across values list") => "42601", // (a VALUES whose rows differ in width: Postgres says syntax_error too)
        _ if has("no field named") || has("column") && has("not found") || has("no column ") || has("column \"") && has("\" of relation \"") && has("does not exist") => "42703", // undefined_column
        _ if has("constraint \"") && has("\" of relation \"") && has("does not exist") => "42704", // undefined_object (Postgres's code for a constraint that isn't there)
        _ if has("invalid function") || has("function") && (has("not found") || has("does not exist")) || has("no function ") => "42883", // undefined_function
        _ if has("table '") && has("not found") || has("no table ") || has("no table or view") || has("no view ") || has("relation") && has("does not exist") => "42P01", // undefined_table
        _ if has("no schema ") || has("schema") && has("does not exist") => "3F000",            // invalid_schema_name
        _ if has("type \"") && has("does not exist") => "42704",                                // undefined_object (`types.rs`)
        _ if has("because column") && (has("uses it") || has("use it")) => "2BP01",              // dependent_objects_still_exist (DROP TYPE)
        _ if has(" is read by ") => "2BP01",                                                     // dependent_objects_still_exist (DROP TABLE that a view or a task reads)
        _ if first_said("is a view") || first_said("is a table") => "42809",                    // wrong_object_type (a view's name where a table's goes, or the other way)
        _ if has("already has a column") => "42701",                                           // duplicate_column (ALTER TABLE … ADD or RENAME COLUMN)
        _ if has("already exists") || has("exists already") => "42P07",                        // duplicate_table
        _ if has("multiple primary keys") => "42P16",                                          // invalid_table_definition
        _ if has("divide by zero") || has("division by zero") => "22012",                       // division_by_zero
        _ if has("overflow") || has("out of range") => "22003",                                 // numeric_value_out_of_range
        _ if has("can't cast value") || has("too large to store in a decimal") || int_overflow_text(&t) => "22003", // (Arrow's words for a number that doesn't fit: a value cast to a narrower integer, a decimal's precision, digits that overflow an integer)
        _ if has("csv error") => "22P04", // bad_copy_file_format (a file read as CSV is malformed: Arrow's words, e.g. a line with more fields than the schema has)
        _ if nan_into_decimal(&t) => "0A000", // feature_not_supported (NaN into a decimal: the planner's own cast, as `casts::into` refuses it)
        _ if has("cast error") || has("cannot cast") || has("can't cast") || has("invalid input syntax") || has("could not parse") || has("invalid input value for enum") => "22P02", // invalid_text_representation
        _ if has("can't reach the bucket") || has("catching up with its leader") => "57P03",   // cannot_connect_now (a leader cut off, a node just started: ask another node)
        _ if has("error sending request") => "58030",                                           // io_error (the bucket or another node didn't answer: try again)
        _ if has("deadlock detected") => "40P01",                                               // deadlock_detected (`locks.rs`)
        _ if has("lock timeout") || has("could not obtain lock") => "55P03",                    // lock_not_available (a lock wait, or NOWAIT)
        _ if has("this node is stopping") => "57P01",                                           // admin_shutdown (a drain ends a wait)
        _ if has("statement timeout") || has("canceling statement") || has("timed out") => "57014", // query_canceled
        _ if has("used disk space during the spilling") => "53100", // disk_full (a query spilled past PONDRA_SPILL_GB: DataFusion's words, before "out of memory" could match)
        _ if has(crate::page::FULL) => "53100",                                                 // disk_full (a page's browser storage is over its quota: `page::FULL`, `web/`'s words)
        _ if has("or move the lake to a folder (alter database") => "53100", // a lake in memory past its spill cap or the disk's room (memory::full)
        _ if has("resources exhausted") || has("out of memory") || has("memory limit") => "53200", // out_of_memory
        _ if has("too many") && has("at once") => "53300",                                      // too_many_connections (a user's queries at once)
        _ if has("come in 1.1") || has("comes in 1.1") || has("coming in 1.1") => "0A000", // feature_not_supported (a statement 1.1 brings, refused by name where it is read as text: `kept.rs`)
        _ if has("not supported") || has("isn't supported") || has("not implemented") || has("not yet supported") => "0A000", // feature_not_supported
        _ if has("would have no value for it") || has(" column: it stays") || has("takes its type alone") || has("one table at a time") || has("'s last column (drop table") => "0A000", // (a node's ALTER TABLE: a column filled over rows already there, or a key, partition, TTL or order column dropped)
        _ if has("read-only") || has("read only") => "25006",                                   // read_only_sql_transaction
        _ => "XX000",                                                                          // internal_error
    }
}

/// Whether lowercase `t` holds Arrow's words for digits (an optional sign) that overflow an integer type:
/// `cannot cast string '99999999999' to value of int32 type`. Other text (`'abc'`) is not an overflow.
fn int_overflow_text(t: &str) -> bool {
    let Some((_, rest)) = t.split_once("cannot cast string '") else { return false };
    let Some((digits, after)) = rest.split_once('\'') else { return false };
    let digits = digits.strip_prefix(['+', '-']).unwrap_or(digits);
    !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()) && after.starts_with(" to value of int")
}

/// Whether lowercase `t` holds Arrow's words for NaN cast to a decimal (`cannot cast string 'nan' to value of decimal128(5, 2)
/// type`): a decimal here holds numbers only, so it is refused as `casts::into` refuses it (0A000), wherever the cast is made.
fn nan_into_decimal(t: &str) -> bool { t.contains("cannot cast string 'nan' to value of decimal") }

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
        "42P07" | "42701" | "23505" => 409,                               // already exists (a table, a column), duplicate key
        "53300" => 429,                                                   // too many of a user's queries at once (a quota)
        "0A000" => 501,                                                   // feature_not_supported
        "54000" => 413,                                                   // program_limit_exceeded (a page's rows past PONDRA_WEB_SEND_MB: too big to send)
        "57014" | "57P03" | "58030" | "53200" => 503,                     // timeout, catching up / cut off, bucket or node silent, out of memory: try again
        c if c.starts_with("40") || c.starts_with("55") || c.starts_with("2B") => 409, // serialize / deadlock, wrong state, still depended on
        c if c.starts_with("08") || c.starts_with("53") || c.starts_with("57") => 503, // connection, resources, operator intervention
        c if c.starts_with("21") || c.starts_with("22") || c.starts_with("23") || c.starts_with("25") || c.starts_with("42") || c.starts_with("P0") => 400, // a bad request (cardinality, data, constraint, transaction state, syntax, a procedure's RAISE)
        _ => 500,                                                         // XX, 58, unknown: a real fault
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
        assert_eq!(of("Inconsistent data length across values list: got 1 values in row 2 but expected 2"), "42601");
        assert_eq!(of("Arrow error: Divide by zero error"), "22012");
        assert_eq!(of("orders.id is NOT NULL, and a row gives it no value"), "23502");
        assert_eq!(of("Invalid batch column at '0' has null but schema specifies non-nullable"), "23502"); // (a page's INSERT of a NULL: DataFusion's words)
        assert_eq!(of("Column 'id' is declared as non-nullable but contains null values"), "23502"); // (a page's UPDATE of one: Arrow's words)
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
        assert_eq!(of("t is read by view v: DROP TABLE t CASCADE drops them too, or drop them first"), "2BP01");
        assert_eq!(of("no view nov"), "42P01");
        assert_eq!(of("column \"d2\" of relation \"at2\" does not exist"), "42703");
        assert_eq!(of("constraint \"c\" of relation \"t\" does not exist"), "42704");
        assert_eq!(of("t already has a column w"), "42701");
        assert_eq!(of("key is t's key column: it stays"), "0A000");
        assert_eq!(of("a is t's last column (DROP TABLE t)"), "0A000");
        assert_eq!(of("ALTER TABLE t ADD COLUMN d … NOT NULL, DEFAULT or an identity: the rows already there would have no value for it (add it, then UPDATE t SET d = …)"), "0A000");
        assert_eq!(of("v is a view: write to the tables it reads"), "42809");
        assert_eq!(of("t is a table"), "42809");
        assert_ne!(of("no topic t on this Pondra node: a topic is a table (CREATE TABLE t …)"), "42809"); // (said of a topic, not a name)
        assert_eq!(of("error sending request for url (http://10.0.0.2:8080/cluster/commit)"), "58030");
        assert_eq!(of("cannot insert a non-DEFAULT value into column \"id\" of t: it is an identity column defined as GENERATED ALWAYS"), "428C9");
        assert_eq!(of("nextval: reached maximum value of sequence \"s\" (3)"), "2200H");
        assert_eq!(of("currval of sequence \"s\" is not yet defined in this session"), "55000");
        assert_eq!(of("relation \"s\" does not exist"), "42P01");
        assert_eq!(of("something else"), "XX000");
        // Arrow's words for a number that doesn't fit are 22003; a text that isn't a number is still 22P02
        assert_eq!(of("Cast error: Can't cast value 99999999999 to type Int32"), "22003");
        assert_eq!(of("Invalid argument error: 12345.0 is too large to store in a Decimal128 of precision 3. Max is 99.9"), "22003");
        assert_eq!(of("Cast error: Cannot cast string '99999999999' to value of Int32 type"), "22003");
        assert_eq!(of("Cast error: Cannot cast string '-99999999999999999999' to value of Int64 type"), "22003");
        assert_eq!(of("Cast error: Cannot cast string 'abc' to value of Int32 type"), "22P02");
        assert_eq!(of("Cast error: Cannot cast string '1.5' to value of Int32 type"), "22P02");
        assert_eq!(of("Cast error: Cannot cast string '4.4.5' to value of Decimal128(38, 10) type"), "22P02");
        assert_eq!(of("Cast error: Cannot cast string 'NaN' to value of Decimal128(5, 2) type"), "0A000"); // (a page's INSERT, `CAST('NaN' AS NUMERIC)`)
        assert_eq!(of("Arrow error: Csv error: incorrect number of fields for line 1, expected 0 got more than 0"), "22P04"); // (a read_csv over a file of blank lines: a malformed file, not a 500)
        assert_eq!(super::of(&super::coded("40001", "could not serialize access due to concurrent update")), "40001");
        assert_eq!(super::of(&super::coded("3D000", "no database x (CREATE DATABASE x)")), "3D000"); // (USE of a database that isn't there)
        assert_eq!(super::named("invalid_catalog_name"), Some("3D000"));
        assert_eq!(super::of(&anyhow::Error::new(super::Violation("new row for relation \"t\" violates check constraint \"c\"".into(), "23514"))), "23514");
        assert_eq!(super::of(&anyhow::Error::new(super::Violation("invalid input value for enum mood: \"x\"".into(), "22P02"))), "22P02");
        // (a lock refusal a follower reads back as words keeps its code: "canceling statement" is a cancel, 57014, otherwise)
        assert_eq!(of("the leader at http://10.0.0.1:8080: canceling statement due to lock timeout"), "55P03");
        assert_eq!(of("deadlock detected: this write waits for a row another transaction holds"), "40P01");
        assert_eq!(of("could not obtain lock on a row: it is held by another transaction (NOWAIT)"), "55P03");
        assert_eq!(of("this node is stopping: its lock waits end, and its transaction with them"), "57P01");
        assert_eq!(of("canceling statement due to statement timeout"), "57014");
        assert_eq!(of("External error: The used disk space during the spilling process has exceeded the allowable limit of 10.00 MB. Raise PONDRA_SPILL_GB to let it spill more."), "53100");
        assert_eq!(of("Resources exhausted: Memory Exhausted while Sorting (DiskManager is disabled)"), "53200"); // (PONDRA_SPILL_GB=0: nothing spills)
        // a lake in memory past its spill cap, or the room its disk keeps (memory::full: the same way out for both)
        assert_eq!(of("Generic memory error: this process's spills may take 2 MB of disk and hold 2 MB there: raise PONDRA_SPILL_GB or PONDRA_MEMORY_LAKE_GB, or move the lake to a folder (ALTER DATABASE … SET LOCATION)"), "53100");
        assert_eq!(of("Generic memory error: the disk a lake in memory spills to has 3 MB free, and a lake keeps 256 MB: raise PONDRA_SPILL_GB or PONDRA_MEMORY_LAKE_GB, or move the lake to a folder (ALTER DATABASE … SET LOCATION)"), "53100");
        // a page's browser storage over its quota, and a write the page then undid for it: disk_full, which HTTP answers 503
        let full = of("Generic opfs error: this site's browser storage is full: Failed to execute 'write' on 'FileSystemWritableFileStream'");
        assert_eq!((full, super::http(full)), ("53100", 503));
        assert_eq!(of("t: the write was undone: the page couldn't keep it: Generic test error: this site's browser storage is full"), "53100");
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
        assert_eq!(h("21000"), 400); // cardinality_violation (a row changed twice in one statement)
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
        assert_eq!(h("54000"), 413); // program_limit_exceeded (a page's rows past PONDRA_WEB_SEND_MB)
        assert_eq!(h("57014"), 503); // query_canceled (timeout)
        assert_eq!(h("57P03"), 503); // cannot_connect_now (catching up / cut off)
        assert_eq!(h("58030"), 503); // io_error (bucket or node silent)
        assert_eq!(h("53200"), 503); // out_of_memory
        // Only a real fault stays a 500.
        assert_eq!(h("XX000"), 500); // internal_error
        assert_eq!(h("58000"), 500); // system_error
    }

    #[test]
    fn an_error_shown_without_its_code_keeps_its_lines_numbers() {
        let said = "ZeroDivisionError: division by zero\n  line 2: total = secret_rate / 0\n  line 5: return helper(x)";
        assert_eq!(super::unquoted(said), "ZeroDivisionError: division by zero\n  line 2\n  line 5");
        assert_eq!(super::unquoted("ValueError: a line: with a colon"), "ValueError: a line: with a colon");
    }

    #[test]
    fn answer_gives_the_status_the_code_and_the_words_together() {
        assert_eq!(super::answer(&super::coded("23505", "x"), true), (409, "23505", "x".to_string()));
        assert_eq!(super::answer(&super::coded("42P01", "no file here"), true), (404, "42P01", "no file here".to_string()));
    }

    #[test]
    fn told_cuts_a_quoted_line_only_for_a_caller_not_shown_the_code() {
        let e = anyhow::anyhow!("ZeroDivisionError: division by zero\n  line 2: total = rate / 0");
        assert_eq!(super::told(&e, true), "ZeroDivisionError: division by zero\n  line 2: total = rate / 0");
        assert_eq!(super::told(&e, false), "ZeroDivisionError: division by zero\n  line 2");
    }

    #[test]
    fn a_quoted_line_never_picks_the_code() {
        let e = anyhow::anyhow!("ValueError: bad row\n  line 3: raise ValueError('duplicate key value violates unique constraint')");
        assert_eq!(super::of(&e), "XX000");
    }
}
