//! Pondra: a streamhouse in one binary (see ADR-002 to ADR-005).
//! Object storage — a local dir or s3://bucket/prefix (S3, R2, MinIO) — is the only state.
#![recursion_limit = "256"] // (the Send check of a query's future, many awaits deep)

// On a page, `tokio` is the stand-in in `web/tokio`, under its own name in Cargo.toml: this makes it `tokio` for every
// module, as the native build's dependency is.
#[cfg(target_arch = "wasm32")]
extern crate pondra_tokio_web as tokio;

// The parts every build has, a browser's too (feature `server` off): SQL's functions and
// rewrites that need no lake.
pub mod finish;
pub mod env;
pub mod reset;
pub mod fsum;
pub mod avg;
pub mod casts;
pub mod intervals;
pub mod xlsx;
pub mod answer;
pub mod engine;
pub mod in_task;
pub mod page;
pub mod request_body;
pub mod text;
// The statements 1.1 brings, refused by name in every form (`page.rs` and the node read the same list).
mod kept;
pub mod extension;
pub mod ext_names;
pub mod lake_files;
pub mod sketch;
mod asof;
mod explain;
mod multiply;
mod sparksql;
mod past_names;
mod diff;
#[cfg_attr(not(feature = "server"), allow(dead_code))] // (the front's passes; a page calls them from step 4)
mod expander;
mod front;
#[cfg_attr(not(feature = "server"), allow(dead_code))] // (the front's passes; a page calls them from step 4)
mod codes;
mod friendly;
mod matcher;
mod pattern;
mod lateral;
#[cfg_attr(not(feature = "server"), allow(dead_code))] // (the front's passes; a page calls them from step 4)
mod measures;
// Postgres's JSON and text functions: every session has them (`engine::session`), a page's too.
#[cfg_attr(not(feature = "server"), allow(dead_code))]
mod pg_formats;
#[cfg_attr(not(feature = "server"), allow(dead_code))]
mod pg_functions;
#[cfg_attr(not(feature = "server"), allow(dead_code))]
mod pg_json;
#[cfg_attr(not(feature = "server"), allow(dead_code))]
mod pg_json_make;
#[cfg_attr(not(feature = "server"), allow(dead_code))]
mod pg_json_edit;
mod pg_json_sets;
#[cfg_attr(not(feature = "server"), allow(dead_code))]
mod pg_text;
#[cfg_attr(not(feature = "server"), allow(dead_code))] // (the length and category columns are the node's catalog's: a page's pg_typeof reads names)
mod pg_oids;
mod pg_casts;
mod pg_trace;
pub mod pg_bytea;
mod optimize;
mod held;
#[cfg_attr(not(feature = "server"), allow(dead_code))] // (another engine's files: a page reads them from step 4i-3)
mod data_file;
#[cfg_attr(not(feature = "server"), allow(dead_code))] // (another engine's files: a page reads them from step 4i-3)
mod scan;
#[cfg_attr(not(feature = "server"), allow(dead_code))] // (a page reads with it from step 4i-3)
mod reach;
#[cfg_attr(not(feature = "server"), allow(dead_code))] // (a page reads with it from step 4i-3)
mod read_delta;
#[cfg_attr(not(feature = "server"), allow(dead_code))] // (a write's rules: a node makes its table from its entry; a page's come with `shape`)
mod rules;

// A node: the catalog in the bucket, the doors, the cluster and the command line (`cli.rs`).
// Feature `server`, on by default; every module the node is made of is one of these.
#[cfg(feature = "doors")] pub mod cli;
#[cfg(feature = "lake")] mod adopt;
#[cfg(feature = "lake")] mod app;
#[cfg(feature = "lake")] mod audit;
#[cfg(feature = "lake")] mod ai;
#[cfg(feature = "lake")] mod avro;
#[cfg(feature = "lake")] mod backup;
#[cfg(feature = "lake")] mod branch;
#[cfg(feature = "lake")] mod bridge;
#[cfg(feature = "lake")] mod rerun;
#[cfg(feature = "lake")] mod auth;
#[cfg(feature = "lake")] mod budget;
#[cfg(feature = "lake")] mod byte_range;
#[cfg(feature = "lake")] mod cache;
#[cfg(feature = "lake")] mod catalogs;
#[cfg(feature = "lake")] mod change;
#[cfg(feature = "lake")] mod copy;
#[cfg(feature = "lake")] pub mod crypto;
#[cfg(feature = "lake")] mod cursors;
#[cfg(feature = "lake")] mod guard;
#[cfg(feature = "lake")] mod hilbert;
#[cfg(feature = "lake")] mod history;
#[cfg(feature = "lake")] mod ddl;
#[cfg(feature = "lake")] mod apply;
#[cfg(feature = "lake")] mod defaults;
#[cfg(feature = "lake")] mod delta;
#[cfg(feature = "lake")] mod ext;
#[cfg(feature = "lake")] mod fetch;
#[cfg(feature = "lake")] mod facts;
#[cfg(feature = "lake")] mod feeds;
#[cfg(feature = "lake")] mod files;
#[cfg(feature = "lake")] mod format;
#[cfg(feature = "doors")] mod flight;
#[cfg(feature = "lake")] mod fresh;
#[cfg(feature = "lake")] mod frugal;
#[cfg(feature = "lake")] mod find;
#[cfg(feature = "lake")] mod git;
#[cfg(feature = "lake")] mod hot;
#[cfg(feature = "lake")] mod index;
#[cfg(feature = "lake")] mod types;
#[cfg(feature = "lake")] mod constraints;
#[cfg(feature = "lake")] mod layout;
#[cfg(feature = "lake")] mod learned;
#[cfg(feature = "lake")] mod iceberg;
#[cfg(feature = "doors")] mod iceberg_rest;
#[cfg(feature = "lake")] mod inbox;
#[cfg(feature = "doors")] mod kafka;
#[cfg(feature = "lake")] mod kafka_client;
#[cfg(feature = "lake")] mod kafka_wire;
#[cfg(feature = "lake")] mod json_rows;
#[cfg(feature = "lake")] mod lake_scan;
#[cfg(feature = "doors")] mod live;
#[cfg(feature = "lake")] mod locks;
#[cfg(feature = "lake")] mod serve;
#[cfg(feature = "doors")] mod served;
#[cfg(feature = "lake")] mod settings;
#[cfg(feature = "lake")] mod shares;
#[cfg(feature = "doors")] mod sharing;
#[cfg(feature = "lake")] mod sharing_client;
#[cfg(feature = "doors")] mod shell;
#[cfg(feature = "doors")] mod cluster;
#[cfg(feature = "doors")] mod console;
#[cfg(feature = "doors")] mod dbserver;
#[cfg(feature = "lake")] mod drain;
#[cfg(feature = "lake")] mod log;
#[cfg(feature = "lake")] mod manifest;
#[cfg(feature = "lake")] pub mod memory;
#[cfg(feature = "lake")] mod objects;
#[cfg(feature = "lake")] mod oom;
#[cfg(all(feature = "doors", target_os = "linux", target_env = "gnu"))] mod jemalloc;
#[cfg(feature = "lake")] mod once;
#[cfg(feature = "doors")] mod origins;
#[cfg(feature = "lake")] mod metrics;
#[cfg(feature = "doors")] pub mod node;
#[cfg(feature = "doors")] pub mod library;
#[cfg(feature = "lake")] mod pages;
#[cfg(feature = "lake")] mod panics;
#[cfg(feature = "doors")] mod tls;
#[cfg(feature = "lake")] mod txn;
#[cfg(feature = "lake")] mod returning;
#[cfg(feature = "doors")] mod mcp;
#[cfg(feature = "doors")] mod pg;
#[cfg(feature = "lake")] mod past;
#[cfg(feature = "lake")] mod path;
#[cfg(feature = "lake")] mod pg_catalog;
#[cfg(feature = "lake")] mod pg_names;
#[cfg(feature = "lake")] mod pg_types;
#[cfg(feature = "doors")] mod pg_values;
#[cfg(feature = "lake")] mod pg_copy;
#[cfg(feature = "lake")] mod literals;
#[cfg(feature = "doors")] mod ci;
#[cfg(feature = "doors")] mod project;
#[cfg(feature = "lake")] mod protect;
#[cfg(feature = "lake")] mod public;
#[cfg(feature = "lake")] mod query;
#[cfg(feature = "lake")] mod read_iceberg;
#[cfg(feature = "lake")] mod ranges;
#[cfg(feature = "lake")] mod pyfn;
#[cfg(feature = "lake")] mod python;
#[cfg(feature = "lake")] mod runs;
#[cfg(feature = "lake")] mod skew;
#[cfg(feature = "lake")] mod replace;
#[cfg(feature = "doors")] mod replica;
#[cfg(feature = "lake")] mod routines;
#[cfg(feature = "lake")] mod host;
#[cfg(feature = "lake")] mod script;
#[cfg(feature = "lake")] mod seq;
#[cfg(feature = "doors")] mod server;
#[cfg(feature = "doors")] mod service;
#[cfg(feature = "lake")] mod spill;
#[cfg(feature = "lake")] mod spmd;
#[cfg(feature = "lake")] mod statement;
#[cfg(feature = "lake")] mod status;
#[cfg(feature = "lake")] mod kv;
#[cfg(feature = "lake")] mod store;
#[cfg(feature = "doors")] mod sync;
#[cfg(feature = "lake")] mod sys;
#[cfg(feature = "lake")] mod tasks;
#[cfg(feature = "lake")] mod temp;
#[cfg(feature = "lake")] mod terms;
#[cfg(feature = "lake")] mod tier;
#[cfg(feature = "lake")] mod udf;
#[cfg(feature = "lake")] mod users;
#[cfg(feature = "lake")] mod sso;
#[cfg(feature = "doors")] mod sso_flow;
#[cfg(feature = "lake")] mod vend;
#[cfg(feature = "lake")] mod vars;
#[cfg(feature = "lake")] mod views;
#[cfg(feature = "lake")] mod write;
#[cfg(feature = "lake")] mod write_outside;
#[cfg(feature = "lake")] mod workspace;
