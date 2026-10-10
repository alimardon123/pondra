# Glossary

Generated from `pondra.kinds` (`src/objects.rs`): don't edit it here; `python3 tools/docs_check.py --write-glossary` writes it again.

## Objects

**database**: another lake this one reads, attached by name, or a branch cloned from one. Other products call it catalog, lake, workspace.

**external table**: a view of files outside the lake, read in place where they are. Other products call it foreign table.

**function**: a named computation, in SQL or Python, run per row, per batch or in a query. Other products call it UDF, user-defined function.

**index**: a named set of a table's columns, kept as a definition: nothing is built.

**macro**: a named SQL expression or query, expanded where it is written. Other products call it SQL macro.

**materialized view**: a query's result kept current as its tables change, read like a table. Other products call it dynamic table, live table, continuous query.

**procedure**: a named piece of work, run with CALL as its caller. Other products call it stored procedure.

**recipient**: a company that a share is granted to, with the token it reads with. Other products call it data consumer.

**role**: a group of grants, given to users. Other products call it group.

**schema**: a namespace for a database's objects, named `schema.name`. Other products call it namespace, dataset.

**secret**: credentials sealed in the lake, handed to a procedure and never shown. Other products call it credential, connection.

**sequence**: numbers handed out one at a time, never twice (`nextval`). Other products call it serial, auto increment.

**share**: tables handed to other companies at published versions, through links that end. Other products call it Delta Share, data share.

**table**: rows in the lake's files and log, queried and changed with SQL. Other products call it relation.

**table function**: a function that returns the rows of a table, used where a table is named. Other products call it UDTF.

**task**: a statement run on a schedule, or after other tasks (AFTER). Other products call it job, schedule, cron.

**type**: a named set of labels a column may hold. Other products call it enum.

**user**: a person who signs in, with a password or token, and grants. Other products call it login.

**view**: a stored query with a name, run where it is used. Other products call it virtual table.

## Parts

**check**, inside a table: CHECK: a condition every row written must meet. Other products call it constraint.

**column**, inside a table, view, materialized view, external table: a named, typed field of every row. Other products call it field, attribute.

**expectation**, inside a materialized view: a condition a view's new rows are counted against, and kept, dropped or failed by. Other products call it data quality check, assertion.

**key**, inside a table: PRIMARY KEY or UNIQUE: the columns that name a row. Other products call it primary key, identifier.

**label**, inside a type: one value an enum type allows. Other products call it enum value.

**link**, inside a table: FOREIGN KEY … NOT ENFORCED: a fact that one table's rows name another's. Other products call it foreign key, relationship, reference.

**parameter**, inside a function, macro, table function, procedure: an argument a routine takes, with its type and default. Other products call it argument.

## Patterns

**branch**: a database cloned from another with no data copied, and refreshed from it; listed by `pondra.databases`. Other products call it zero-copy clone, fork, dev environment.

**flow**: materialized views of views, moved in one commit; listed by `pondra.flows`. Other products call it pipeline, DAG.

**history view**: a materialized view keeping every version of each row; listed by `pondra.tables`. Other products call it SCD type 2, slowly changing dimension.

**task graph**: tasks that run after others (AFTER), once per tick of the first; listed by `pondra.tasks`. Other products call it workflow, job.
