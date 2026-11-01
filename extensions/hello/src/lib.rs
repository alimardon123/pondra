//! `hello`: an extension written with `pondra-extension`, the kit. Two scalar functions and a table function, their types
//! taken from their signatures.
//!
//! ```sql
//! INSTALL hello FROM 'hello-0.1.wasm';
//! SELECT greet('Ada'), add(2, 3), initials(NULL);   -- 'hello, Ada', 5, NULL
//! SELECT * FROM count_to(3);                        -- i: 1, 2, 3, and their squares
//! ```
use pondra_extension::Extension;

fn hello() -> Extension {
    Extension::new("hello", "0.1")
        .scalar("greet", |name: String| format!("hello, {name}"))
        .scalar("add", |a: i64, b: i64| a + b)
        // (an Option argument sees NULL; a plain one gives NULL without a call)
        .scalar("initials", |name: Option<String>| name.map(|n| n.split_whitespace().filter_map(|w| w.chars().next()).collect::<String>()))
        .scalar("checked_div", |a: i64, b: i64| if b == 0 { Err("checked_div: division by zero".to_string()) } else { Ok(a / b) })
        .table("count_to", ["i", "square"], |n: i64| (1..=n).map(|i| (i, i * i)))
}

pondra_extension::export!(hello);
