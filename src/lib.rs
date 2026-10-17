//! Pondra: a streamhouse in one binary (see ADR-002 to ADR-005).
//! Object storage — a local dir or s3://bucket/prefix (S3, R2, MinIO) — is the only state.
#![recursion_limit = "256"] // (the Send check of a query's future, many awaits deep)

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
pub mod text;
pub mod ext_names;
#[cfg_attr(not(feature = "server"), allow(dead_code))]
mod asof;
#[cfg_attr(not(feature = "server"), allow(dead_code))] // (the front's passes; a page calls them from step 4)
mod sparksql;
#[cfg_attr(not(feature = "server"), allow(dead_code))] // (the front's passes; a page calls them from step 4)
mod past;
#[cfg_attr(not(feature = "server"), allow(dead_code))] // (the front's passes; a page calls them from step 4)
mod diff;
#[cfg_attr(not(feature = "server"), allow(dead_code))] // (the front's passes; a page calls them from step 4)
mod expander;
#[cfg_attr(not(feature = "server"), allow(dead_code))] // (the front's passes; a page calls them from step 4)
mod front;
#[cfg_attr(not(feature = "server"), allow(dead_code))] // (the front's passes; a page calls them from step 4)
mod codes;
#[cfg_attr(not(feature = "server"), allow(dead_code))] // (the front's passes; a page calls them from step 4)
mod friendly;
#[cfg_attr(not(feature = "server"), allow(dead_code))] // (the front's passes; a page calls them from step 4)
mod measures;
mod optimize;

// A node: the catalog in the bucket, the doors, the cluster and the command line (`cli.rs`).
// Feature `server`, on by default; every module the node is made of is one of these.
#[cfg(feature = "server")] pub mod cli;
#[cfg(feature = "server")] mod adopt;
#[cfg(feature = "server")] mod audit;
#[cfg(feature = "server")] mod ai;
#[cfg(feature = "server")] mod avro;
#[cfg(feature = "server")] mod backup;
#[cfg(feature = "server")] mod branch;
#[cfg(feature = "server")] mod bridge;
#[cfg(feature = "server")] mod rerun;
#[cfg(feature = "server")] mod auth;
#[cfg(feature = "server")] mod budget;
#[cfg(feature = "server")] mod cache;
#[cfg(feature = "server")] mod catalogs;
#[cfg(feature = "server")] mod change;
#[cfg(feature = "server")] mod copy;
#[cfg(feature = "server")] mod cursors;
#[cfg(feature = "server")] mod guard;
#[cfg(feature = "server")] mod held;
#[cfg(feature = "server")] mod hilbert;
#[cfg(feature = "server")] mod history;
#[cfg(feature = "server")] mod ddl;
#[cfg(feature = "server")] mod apply;
#[cfg(feature = "server")] mod defaults;
#[cfg(feature = "server")] mod delta;
#[cfg(feature = "server")] mod ext;
#[cfg(feature = "server")] mod facts;
#[cfg(feature = "server")] mod feeds;
#[cfg(feature = "server")] mod files;
#[cfg(feature = "server")] mod format;
#[cfg(feature = "server")] mod flight;
#[cfg(feature = "server")] mod fresh;
#[cfg(feature = "server")] mod find;
#[cfg(feature = "server")] mod git;
#[cfg(feature = "server")] mod hot;
#[cfg(feature = "server")] mod index;
#[cfg(feature = "server")] mod types;
#[cfg(feature = "server")] mod constraints;
#[cfg(feature = "server")] mod layout;
#[cfg(feature = "server")] mod learned;
#[cfg(feature = "server")] mod iceberg;
#[cfg(feature = "server")] mod inbox;
#[cfg(feature = "server")] mod kafka;
#[cfg(feature = "server")] mod kafka_client;
#[cfg(feature = "server")] mod live;
#[cfg(feature = "server")] mod locks;
#[cfg(feature = "server")] mod serve;
#[cfg(feature = "server")] mod served;
#[cfg(feature = "server")] mod settings;
#[cfg(feature = "server")] mod shares;
#[cfg(feature = "server")] mod sharing;
#[cfg(feature = "server")] mod shell;
#[cfg(feature = "server")] mod cluster;
#[cfg(feature = "server")] mod console;
#[cfg(feature = "server")] mod dbserver;
#[cfg(feature = "server")] mod drain;
#[cfg(feature = "server")] mod log;
#[cfg(feature = "server")] mod manifest;
#[cfg(feature = "server")] pub mod memory;
#[cfg(feature = "server")] mod objects;
#[cfg(feature = "server")] mod oom;
#[cfg(feature = "server")] mod once;
#[cfg(feature = "server")] mod metrics;
#[cfg(feature = "server")] pub mod node;
#[cfg(feature = "server")] pub mod library;
#[cfg(feature = "server")] mod pages;
#[cfg(feature = "server")] mod panics;
#[cfg(feature = "server")] mod tls;
#[cfg(feature = "server")] mod txn;
#[cfg(feature = "server")] mod returning;
#[cfg(feature = "server")] mod mcp;
#[cfg(feature = "server")] mod pg;
#[cfg(feature = "server")] mod path;
#[cfg(feature = "server")] mod pg_catalog;
#[cfg(feature = "server")] mod pg_formats;
#[cfg(feature = "server")] mod pg_functions;
#[cfg(feature = "server")] mod pg_names;
#[cfg(feature = "server")] mod pg_text;
#[cfg(feature = "server")] mod pg_types;
#[cfg(feature = "server")] mod pg_values;
#[cfg(feature = "server")] mod literals;
#[cfg(feature = "server")] mod ci;
#[cfg(feature = "server")] mod project;
#[cfg(feature = "server")] mod protect;
#[cfg(feature = "server")] mod public;
#[cfg(feature = "server")] mod query;
#[cfg(feature = "server")] mod read_delta;
#[cfg(feature = "server")] mod read_iceberg;
#[cfg(feature = "server")] mod ranges;
#[cfg(feature = "server")] mod pyfn;
#[cfg(feature = "server")] mod python;
#[cfg(feature = "server")] mod runs;
#[cfg(feature = "server")] mod sketch;
#[cfg(feature = "server")] mod skew;
#[cfg(feature = "server")] mod replica;
#[cfg(feature = "server")] mod routines;
#[cfg(feature = "server")] mod host;
#[cfg(feature = "server")] mod scan;
#[cfg(feature = "server")] mod script;
#[cfg(feature = "server")] mod seq;
#[cfg(feature = "server")] mod server;
#[cfg(feature = "server")] mod service;
#[cfg(feature = "server")] mod spill;
#[cfg(feature = "server")] mod spmd;
#[cfg(feature = "server")] mod status;
#[cfg(feature = "server")] mod store;
#[cfg(feature = "server")] mod sync;
#[cfg(feature = "server")] mod sys;
#[cfg(feature = "server")] mod tasks;
#[cfg(feature = "server")] mod temp;
#[cfg(feature = "server")] mod tier;
#[cfg(feature = "server")] mod udf;
#[cfg(feature = "server")] mod users;
#[cfg(feature = "server")] mod sso;
#[cfg(feature = "server")] mod sso_flow;
#[cfg(feature = "server")] mod vend;
#[cfg(feature = "server")] mod vars;
#[cfg(feature = "server")] mod views;
#[cfg(feature = "server")] mod write;
#[cfg(feature = "server")] mod write_outside;
#[cfg(feature = "server")] mod workspace;
