// Frames: queries written in JavaScript, run as SQL (ADR-023): a port of python/pondra/frame.py with
// nodejs-polars' names (`withColumns`, `groupBy`, `isNull`, `nUnique`, `joinAsof`…).
//
//   const top = db.table("orders").filter(col("amount").gt(100)).groupBy("user").agg(col("amount").sum());
//   for (const row of await top) …             // (awaiting a frame runs it: its rows)
//   await top.writeTable("big_spenders");     // (a table, as the rows of the frame)
//
// A frame is a query that hasn't run yet. Each method returns a new frame, each step a CTE of one SQL
// statement (`await frame.sql()`), and running it sends that statement to the node: the same plan,
// speed and answer as the SQL.
//
// Python asks the node for a step's columns while the step is built. Here that answer comes later, so
// a step that needs the columns keeps its SQL (or its sort) as a promise. The CTE names are still made
// when each step is built, in frame.py's order, so the SQL text is the one frame.py writes.

const isPromise = (x) => x instanceof Promise;
const isPlain = (x) => x !== null && typeof x === "object" && Object.getPrototypeOf(x) === Object.prototype;
const handled = (p) => { p.catch(() => {}); return p; }; // (a promise nobody waits for is no unhandled error)
const then = (p, fn) => handled(p.then(fn));
const WORD = /^[A-Za-z_][A-Za-z0-9_]*$/;
const WITH_START = /^\s*(?:(?:--[^\n]*\n|\/\*[\s\S]*?\*\/)\s*)*WITH\s+(RECURSIVE\s+)?/i;
const STATEMENT = /^\s*(?:(?:--[^\n]*\n|\/\*[\s\S]*?\*\/)\s*)*(EXPLAIN|SHOW|DESCRIBE)\b/i;

let ids = 0; // (frame.py's `_ids`: the names of the steps, _1, _2…, and of the joins' sides)
const nextId = () => ++ids;

/** `fn` of `values` at once, or, when one is a promise, once they all are. */
function later(values, fn) {
  return values.some(isPromise) ? then(Promise.all(values), fn) : fn(values);
}

// ---------------------------------------------------------------- SQL text of values

export const quote = (name) => `"${String(name).replaceAll('"', '""')}"`;
const ident = (name) => (name === "*" ? "*" : String(name).split(".").map(quote).join("."));

/** A float as Python writes it (frame.py's `repr`): the shortest digits, an exponent past 1e16 or below 1e-4. */
function floatText(x) {
  if (Number.isNaN(x)) return "nan";
  if (!Number.isFinite(x)) return x > 0 ? "inf" : "-inf";
  const [mantissa, exponent] = x.toExponential().split("e");
  const e = Number(exponent);
  if (e >= -4 && e < 16) return String(x);
  return `${mantissa}e${e < 0 ? "-" : "+"}${String(Math.abs(e)).padStart(2, "0")}`;
}

/** A float that is whole, as Python's `str` has it ("1.0"): frame.py's `float(fraction)`. */
const pyFloat = (x) => (Number.isInteger(x) ? `${x}.0` : floatText(x));

/** A Date as a naive timestamp in UTC (frame.py's `isoformat(sep=" ")`: microseconds only if any). */
function timestampText(d) {
  const iso = d.toISOString();
  const ms = iso.slice(20, 23);
  return `${iso.slice(0, 10)} ${iso.slice(11, 19)}${ms === "000" ? "" : `.${ms}000`}`;
}

/** A JavaScript value as a SQL literal. A whole number is an integer (write `.cast("DOUBLE")` for a
 * double); a fraction is a double, as frame.py writes Python floats. */
export function literal(v) {
  if (v === null || v === undefined) return "NULL";
  if (typeof v === "boolean") return v ? "TRUE" : "FALSE";
  if (typeof v === "bigint") return String(v);
  if (typeof v === "number") return Number.isInteger(v) ? BigInt(v).toString() : `CAST('${floatText(v)}' AS DOUBLE)`;
  if (typeof v === "string") return `'${v.replaceAll("'", "''")}'`;
  if (v instanceof Date) return `TIMESTAMP '${timestampText(v)}'`;
  if (Array.isArray(v)) return `[${v.map((x) => literal(x)).join(", ")}]`;
  throw new TypeError(`${typeof v} isn't a SQL value here: use numbers, text, true or false, Dates, arrays or null`);
}

const SQL_TYPES = new Map(Object.entries({
  int8: "TINYINT", int16: "SMALLINT", int32: "INT", int64: "BIGINT", uint8: "TINYINT UNSIGNED", uint16: "SMALLINT UNSIGNED",
  uint32: "INT UNSIGNED", uint64: "BIGINT UNSIGNED", float32: "REAL", float64: "DOUBLE", utf8: "VARCHAR", string: "VARCHAR",
  str: "VARCHAR", boolean: "BOOLEAN", bool: "BOOLEAN", date: "DATE", datetime: "TIMESTAMP", int: "BIGINT", float: "DOUBLE",
}));

/** A SQL type from a Polars-style name, a JavaScript one (`"int"`, `"str"`), or SQL itself: `"BIGINT"`. */
export function sqlType(t) {
  const name = String(t);
  return SQL_TYPES.get(name.split("(")[0].toLowerCase()) ?? name;
}

const sqlOf = (x) => (x instanceof Expr ? x.sql : literal(x));

/** Python's `re.escape`, for `replace(…, {literal: true})`. */
const RE_SPECIAL = new Set("()[]{}?*+-|^$\\.&~# \t\n\r\v\f");
const pyEscape = (s) => [...s].map((c) => (RE_SPECIAL.has(c) ? `\\${c}` : c)).join("");

// ---------------------------------------------------------------- expressions

/** A column expression: its SQL, and the name its column gets (Polars': the first column in it). */
export class Expr {
  constructor(sql, name = null, over = null) {
    this.sql = sql;
    this.name = name;
    this._over = over; // (how `.over(…)` places it in a window; null: `sql OVER (…)`)
    this._text = false; // text for certain (a string, or text joined to one): `+` joins it, as Polars' does
  }

  _op(op, other, right = false) {
    const o = expr(other);
    const [a, b] = right ? [o, this] : [this, o];
    if (op === "+" && (a._text || b._text)) {
      const out = new Expr(`(${a.sql} || ${b.sql})`, a.name || b.name);
      out._text = true;
      return out;
    }
    return new Expr(`(${a.sql} ${op} ${b.sql})`, a.name || b.name);
  }

  _fn(f, args = [], name = null) {
    return new Expr(`${f}(${[this.sql, ...args.map(sqlOf)].join(", ")})`, name || this.name);
  }

  // (Python's operators: `2 - col("x")` is `lit(2).sub(col("x"))`)
  add(o) { return this._op("+", o); }
  sub(o) { return this._op("-", o); }
  mul(o) { return this._op("*", o); }
  mod(o) { return this._op("%", o); }
  eq(o) { return this._op("=", o); }
  neq(o) { return this._op("<>", o); }
  lt(o) { return this._op("<", o); }
  ltEq(o) { return this._op("<=", o); }
  gt(o) { return this._op(">", o); }
  gtEq(o) { return this._op(">=", o); }
  and(o) { return this._op("AND", o); }
  or(o) { return this._op("OR", o); }
  not() { return new Expr(`(NOT ${this.sql})`, this.name); }
  neg() { return new Expr(`(- ${this.sql})`, this.name); }
  pow(o) { return new Expr(`power(${this.sql}, ${expr(o).sql})`, this.name); }
  div(o) { return new Expr(`(CAST(${this.sql} AS DOUBLE) / ${expr(o).sql})`, this.name); } // (Polars: always a true division, as a float)
  floorDiv(o) { return floordiv(this, expr(o), this.name); }

  alias(name) {
    const out = new Expr(this.sql, name, this._over);
    out._text = this._text;
    return out;
  }

  cast(dtype) { return new Expr(`CAST(${this.sql} AS ${sqlType(dtype)})`, this.name); }
  isNull() { return new Expr(`(${this.sql} IS NULL)`, this.name); }
  isNotNull() { return new Expr(`(${this.sql} IS NOT NULL)`, this.name); }
  isNan() { return this._fn("isnan"); }

  isIn(values) {
    if (values instanceof Frame) throw new Error("isIn takes a list of values here: a frame's values are a join or a semi join");
    return values.length ? new Expr(`(${this.sql} IN (${values.map((v) => literal(v)).join(", ")}))`, this.name) : lit(false).alias(this.name);
  }

  isBetween(lower, upper, { closed = "both" } = {}) {
    const lo = closed === "both" || closed === "left" ? ">=" : ">";
    const hi = closed === "both" || closed === "right" ? "<=" : "<";
    return new Expr(`(${this.sql} ${lo} ${expr(lower).sql} AND ${this.sql} ${hi} ${expr(upper).sql})`, this.name);
  }

  fillNull(value) { return this._fn("coalesce", [value]); }
  fillNan(value) { return new Expr(`CASE WHEN isnan(${this.sql}) THEN ${expr(value).sql} ELSE ${this.sql} END`, this.name); }
  abs() { return this._fn("abs"); }

  /** Polars' rounding: a half to the even neighbour (SQL's round() goes away from zero). */
  round(decimals = 0, { mode = "half_to_even" } = {}) {
    if (mode !== "half_to_even") return this._fn("round", [lit(decimals)]);
    const k = 10 ** decimals;
    const x = `(${this.sql} * ${k})`;
    return new Expr(`(CASE WHEN abs(${x} - trunc(${x})) = 0.5 THEN 2 * round(${x} / 2) ELSE round(${x}) END / ${k})`, this.name);
  }

  sqrt() { return this._fn("sqrt"); }
  exp() { return this._fn("exp"); }
  log(base = Math.E) { return base === Math.E ? this._fn("ln") : new Expr(`log(${literal(base)}, ${this.sql})`, this.name); }
  floor() { return this._fn("floor"); }
  ceil() { return this._fn("ceil"); }

  clip(lower = null, upper = null) { // (null stays null: SQL's greatest() skips nulls)
    let e = lower == null ? this : new Expr(`greatest(${this.sql}, ${expr(lower).sql})`, this.name);
    e = upper == null ? e : new Expr(`least(${e.sql}, ${expr(upper).sql})`, this.name);
    return new Expr(`CASE WHEN ${this.sql} IS NULL THEN NULL ELSE ${e.sql} END`, this.name);
  }

  // aggregations (Polars' names; std and var with ddof 1)
  sum() { return this._fn("sum"); }
  mean() { return this._fn("avg"); }
  min() { return this._fn("min"); }
  max() { return this._fn("max"); }
  count() { return this._fn("count"); }
  nUnique() { return new Expr(`count(DISTINCT ${this.sql}) + max(CASE WHEN ${this.sql} IS NULL THEN 1 ELSE 0 END)`, this.name); } // (Polars counts null as a value)
  first() { return this._fn("first_value"); }
  last() { return this._fn("last_value"); }
  median() { return this._fn("median"); }
  std(ddof = 1) { return this._fn(ddof === 1 ? "stddev_samp" : "stddev_pop"); }
  var(ddof = 1) { return this._fn(ddof === 1 ? "var_samp" : "var_pop"); }
  quantile(q) { return new Expr(`approx_percentile_cont(${literal(q)}) WITHIN GROUP (ORDER BY ${this.sql})`, this.name); }
  any() { return this._fn("bool_or"); }
  all() { return this._fn("bool_and"); }

  // windows
  /** This expression per group of rows (SQL's `OVER (PARTITION BY … ORDER BY …)`). */
  over(partitionBy = null, { orderBy = null, descending = false } = {}) {
    const parts = exprs(partitionBy).map((c) => c.sql);
    const order = exprs(orderBy).map((c) => `${c.sql} ${descending ? "DESC" : "ASC"}`);
    const clause = [parts.length ? `PARTITION BY ${parts.join(", ")}` : "", order.length ? `ORDER BY ${order.join(", ")}` : ""].filter(Boolean).join(" ");
    return new Expr(this._over ? this._over(clause, parts) : `${this.sql} OVER (${clause})`, this.name);
  }

  shift(n = 1) {
    const f = n >= 0 ? "lag" : "lead";
    return new Expr(`${f}(${this.sql}, ${Math.abs(n)})`, this.name, (w) => `${f}(${this.sql}, ${Math.abs(n)}) OVER (${w})`);
  }

  cumSum() { return new Expr(`sum(${this.sql})`, this.name, (w) => `sum(${this.sql}) OVER (${w} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`); }

  /** Rank by this column's values (Polars' methods: average, min, max, dense, ordinal). */
  rank(method = "average", { descending = false } = {}) {
    const forms = new Map(["min", "dense", "ordinal", "max", "average"].map((m) => [m, m]));
    if (!forms.has(method)) throw new Error(`rank("${method}"): average, min, max, dense or ordinal`);
    const way = `ORDER BY ${this.sql} ${descending ? "DESC" : "ASC"}`;
    const place = (_, parts) => {
      const p = parts.length ? `PARTITION BY ${parts.join(", ")} ` : "";
      const ties = `count(*) OVER (PARTITION BY ${[...parts, this.sql].join(", ")})`;
      const r = {
        min: `rank() OVER (${p}${way})`, dense: `dense_rank() OVER (${p}${way})`, ordinal: `row_number() OVER (${p}${way})`,
        max: `(rank() OVER (${p}${way}) + ${ties} - 1)`, average: `(rank() OVER (${p}${way}) + (${ties} - 1) / 2.0)`,
      }[method];
      return `CASE WHEN ${this.sql} IS NULL THEN NULL ELSE ${r} END`;
    };
    return new Expr(place("", []), this.name, place);
  }

  get str() { return new StrNs(this); }
  get dt() { return new DtNs(this); }
}

/** `.str`: text functions, with Polars' names. */
class StrNs {
  constructor(e) { this.e = e; }
  _f(f, ...args) { return this.e._fn(f, args.map((a) => (a instanceof Expr ? a : lit(a)))); }
  toLowercase() { return this._f("lower"); }
  toUppercase() { return this._f("upper"); }
  lenChars() { return this._f("character_length"); }
  lenBytes() { return this._f("octet_length"); }

  contains(pattern, { literal: asText = false } = {}) {
    return asText ? new Expr(`(strpos(${this.e.sql}, ${literal(pattern)}) > 0)`, this.e.name) : this._f("regexp_like", pattern);
  }

  startsWith(prefix) { return this._f("starts_with", prefix); }
  endsWith(suffix) { return this._f("ends_with", suffix); }

  replace(pattern, value, { literal: asText = false } = {}) { return this._f("regexp_replace", asText ? pyEscape(pattern) : pattern, value); }
  replaceAll(pattern, value, { literal: asText = false } = {}) { return this._f("regexp_replace", asText ? pyEscape(pattern) : pattern, value, "g"); }

  slice(offset, length = null) { return length == null ? this._f("substr", offset + 1) : this._f("substr", offset + 1, length); }
  stripChars(characters = null) { return characters == null ? this._f("trim") : new Expr(`btrim(${this.e.sql}, ${literal(characters)})`, this.e.name); }
  split(by) { return this._f("string_to_array", by); }
  toDate(format = null) { return format == null ? this._f("to_date") : this._f("to_date", format); }
  toDatetime(format = null) { return format == null ? this._f("to_timestamp") : this._f("to_timestamp", format); }
}

const UNITS = new Map(Object.entries({ us: "microsecond", ms: "millisecond", s: "second", m: "minute", h: "hour", d: "day", w: "week", mo: "month", q: "quarter", y: "year" }));
const SCALES = new Map([["s", 1], ["ms", 1_000], ["us", 1_000_000], ["ns", 1_000_000_000]]);

/** `.dt`: date and time functions, with Polars' names. */
class DtNs {
  constructor(e) { this.e = e; }
  _part(part) { return new Expr(`CAST(date_part('${part}', ${this.e.sql}) AS BIGINT)`, this.e.name); }
  year() { return this._part("year"); }
  month() { return this._part("month"); }
  day() { return this._part("day"); }
  hour() { return this._part("hour"); }
  minute() { return this._part("minute"); }
  second() { return this._part("second"); }
  quarter() { return this._part("quarter"); }
  ordinalDay() { return this._part("doy"); }
  weekday() { return new Expr(`CAST((date_part('dow', ${this.e.sql}) + 6) % 7 + 1 AS BIGINT)`, this.e.name); } // (Monday 1 … Sunday 7)
  date() { return new Expr(`CAST(${this.e.sql} AS DATE)`, this.e.name); }

  /** Down to the start of its `every`: "1h", "15m", "1d", "1mo"… */
  truncate(every) {
    const m = /^(\d+)([a-z]+)$/.exec(every);
    if (!m || !UNITS.has(m[2])) throw new Error(`truncate("${every}"): a count and a unit, as "1h" or "15m"`);
    const [, n, u] = m;
    const unit = UNITS.get(u);
    return n === "1"
      ? new Expr(`date_trunc('${unit}', ${this.e.sql})`, this.e.name)
      : new Expr(`date_bin(INTERVAL '${n} ${unit}s', ${this.e.sql}, TIMESTAMP '1970-01-01')`, this.e.name);
  }

  strftime(format) { return new Expr(`to_char(${this.e.sql}, ${literal(format)})`, this.e.name); }

  epoch(timeUnit = "us") {
    if (!SCALES.has(timeUnit)) throw new Error(`epoch("${timeUnit}"): s, ms, us or ns`);
    return new Expr(`CAST(date_part('epoch', ${this.e.sql}) * ${SCALES.get(timeUnit)} AS BIGINT)`, this.e.name);
  }
}

/** A column: `col("amount")`, `col("s.amount")` (of the table called s), `col("*")`. */
export function col(name) {
  return new Expr(ident(name), name === "*" ? null : String(name).split(".").at(-1));
}

/** A value as an expression; a string is text, not SQL. */
export function lit(value) {
  if (value instanceof Expr) return value;
  const out = new Expr(literal(value), "literal");
  out._text = typeof value === "string";
  return out;
}

/** A SQL expression as it is, `AS name` included: `sqlExpr("CASE WHEN qty > 10 THEN 'big' END AS size")`. */
export function sqlExpr(sql) {
  const m = /^\s*([\s\S]+?)\s+AS\s+("(?:[^"]|"")+"|[A-Za-z_][A-Za-z0-9_]*)\s*$/i.exec(sql);
  if (m && balanced(m[1])) {
    const name = m[2];
    return new Expr(`(${m[1]})`, name.startsWith('"') ? name.slice(1, -1).replaceAll('""', '"') : name.toLowerCase());
  }
  const text = sql.trim();
  return new Expr(`(${text})`, WORD.test(text) ? text.toLowerCase() : null);
}

/** No AS inside parentheses or a CASE: the last AS is the alias's. */
function balanced(s) {
  let depth = 0;
  for (const ch of s.replace(/'(?:[^']|'')*'/g, "''")) {
    depth += ch === "(" ? 1 : ch === ")" ? -1 : 0;
    if (depth < 0) return false;
  }
  return depth === 0 && !/\bCASE\b(?!.*\bEND\b)/i.test(s);
}

/** The number of rows (in each group): `count(*)`. */
export const len = () => new Expr("count(*)", "len");

/** Every column: `*`. */
export const all = () => col("*");

/** `when(cond).then(value).when(cond).then(value).otherwise(value)`. */
export function when(condition) {
  return new When([], expr(condition, true));
}

class When {
  constructor(done, cond) {
    this.done = done;
    this.cond = cond;
  }

  then(value, reject) {
    // (`await when(…)` calls `then` as a promise does: a condition is not a value, so refuse it by name)
    if (typeof reject === "function") throw new TypeError("when(…) isn't a value yet: add .then(value) and .otherwise(value)");
    return new Then([...this.done, [this.cond, expr(value)]]);
  }
}

class Then extends Expr {
  constructor(cases, otherwise = null) {
    const body = cases.map(([c, v]) => `WHEN ${c.sql} THEN ${v.sql}`).join(" ");
    super(`CASE ${body}${otherwise !== null ? ` ELSE ${otherwise.sql}` : ""} END`, cases[0][1].name);
    this.cases = cases;
  }

  when(condition) { return new When(this.cases, expr(condition, true)); }
  otherwise(value) { return new Then(this.cases, expr(value)); }
}

/** A call of one of the lake's functions (`CREATE FUNCTION`, `db.function`), or any SQL function, in
 * an expression: `fn.slug(col("title"))`, or `fn("ops.slug", col("title"))` for one in a schema. */
function callFn(name, ...args) {
  const first = args.find((a) => a instanceof Expr);
  return new Expr(`${name}(${args.map(sqlOf).join(", ")})`, first ? first.name : name.split(".").at(-1));
}

export const fn = new Proxy(callFn, {
  get: (target, name) => (typeof name === "symbol" || name === "then" ? Reflect.get(target, name) : (...args) => callFn(name, ...args)),
});

/** The first of the values that isn't null, per row. */
export function coalesce(...xs) {
  const es = xs.map((e) => expr(e));
  return new Expr(`coalesce(${es.map((e) => e.sql).join(", ")})`, es[0].name);
}

/** Text joined: `concatStr([col("a"), "-", col("b")], "")`, nulls skipped (SQL's concat_ws). */
export function concatStr(xs, separator = "") {
  const es = xs.map((e) => expr(e));
  return new Expr(`concat_ws(${literal(separator)}, ${es.map((e) => e.sql).join(", ")})`, es[0].name);
}

export const sum = (name) => col(name).sum();
export const mean = (name) => col(name).mean();
export const min = (name) => col(name).min();
export const max = (name) => col(name).max();
export const count = (name) => col(name).count();
export const nUnique = (name) => col(name).nUnique();
export const median = (name) => col(name).median();
export const first = (name) => col(name).first();
export const last = (name) => col(name).last();

function floordiv(a, b, name) { // (a // b as Python and Polars have it: rounded toward minus infinity, of a's and b's type)
  return new Expr(`((${a.sql}) - (((${a.sql}) % (${b.sql})) + (${b.sql})) % (${b.sql})) / (${b.sql})`, name);
}

/** An expression from what a method was given: itself, a SQL snippet (`sql`), or a value. */
function expr(x, sql = false) {
  if (x instanceof Expr) return x;
  if (sql && typeof x === "string") return sqlExpr(x);
  return lit(x);
}

/** A string names a column where Polars' would (`select("a")`); one that isn't a plain name is SQL. */
function column(x) {
  if (typeof x === "string") return x === "*" || WORD.test(x) ? col(x) : sqlExpr(x);
  return expr(x);
}

function exprs(xs) {
  if (xs === null || xs === undefined) return [];
  return (Array.isArray(xs) ? xs : [xs]).map(column);
}

const flat = (xs) => xs.flatMap((x) => (Array.isArray(x) ? flat(x) : [x]));

/** `expr AS name`, unless the expression is the column itself. */
const named = (e) => (e.name && e.sql !== ident(e.name) ? `${e.sql} AS ${quote(e.name)}` : e.sql);

/** The keyword arguments of a call taking expressions: a trailing plain object is `name: expr` pairs. */
function takeNamed(args) {
  const last = args[args.length - 1];
  return isPlain(last) ? [args.slice(0, -1), Object.entries(last)] : [args, []];
}

// Arrow's type names, as the node reports them (`format=typed`), for `fillNull`'s kinds.
const INTEGER = /^U?Int(8|16|32|64)$/;
const FLOAT = /^Float(16|32|64)$/;
const DECIMAL = /^Decimal/;
const STRING = /^(Utf8|LargeUtf8|Utf8View)$/;
const TEMPORAL = /^(Date(32|64)|Time(32|64)|Timestamp|Duration|Interval)/;

/** Does a column of this type take a fill of this kind (frame.py's pyarrow checks)? */
function kindOf(type, kind) {
  switch (kind) {
    case "boolean": return type === "Boolean";
    case "integer": return INTEGER.test(type) || FLOAT.test(type) || DECIMAL.test(type);
    case "float": return FLOAT.test(type) || DECIMAL.test(type);
    case "string": return STRING.test(type);
    default: return TEMPORAL.test(type);
  }
}

/** The kind a fill value gives: an integer fills whole numbers too; a fraction, doubles only. */
function fillKind(v) {
  if (typeof v === "boolean") return "boolean";
  if (typeof v === "string") return "string";
  if (typeof v === "number") return Number.isInteger(v) ? "integer" : "float";
  return "temporal";
}

// ---------------------------------------------------------------- frames

/** A query that hasn't run yet, over a lake. `await frame.sql()` is its statement. */
export class Frame {
  /**
   * `query`: the SQL, or a promise of it. `ctes`: name → SQL (text or a promise), in order. `rel`: the
   * name this frame is, if it is one (a table, or a step that keeps its input). `order`: the sort in
   * effect, `[SQL, column name]` pairs (or a promise of them). `waits`: promises a run waits for (a view
   * `toView` is making). `table`: the table a write changes.
   */
  constructor(con, query, { ctes = new Map(), rel = null, params = {}, order = null, table = null, waits = [] } = {}) {
    this._con = con;
    this._query = query;
    this._ctes = new Map(ctes);
    this._rel = rel;
    this._params = { ...params };
    this._order = order;
    this._table = table;
    this._waits = waits;
    this._types = null; // (the columns: asked of the node once, as frame.py's `schema`)
    for (const p of [query, order, ...ctes.values()]) if (isPromise(p)) handled(p);
  }

  static fromSql(con, query, params = {}) {
    return new Frame(con, query, { params, order: trailingOrder(query) });
  }

  // what it is

  /** The one SQL statement this frame is: each step a CTE. */
  async sql() {
    await Promise.all(this._waits);
    const query = await this._query;
    if (!this._ctes.size) return query;
    const steps = await Promise.all([...this._ctes].map(async ([name, q]) => `${name} AS (${await q})`));
    const ours = steps.join(",\n     ");
    const m = WITH_START.exec(query);
    if (m) return `WITH ${m[1] || ""}${ours},\n     ${query.slice(m[0].length)}`; // (SQL with a WITH of its own: ours go first in it)
    return `WITH ${ours}\n${query}`;
  }

  /** Is this a statement rather than a query (`EXPLAIN …`, `SHOW …`)? It runs as it is. */
  _statement() {
    return !this._ctes.size && typeof this._query === "string" && STATEMENT.test(this._query);
  }

  _first(n) { return this._statement() ? this : this.limit(n); }

  /** Its columns and their types: `[{ name, type }]`, the types as Arrow names them (`Int64`, `Utf8`). */
  schema() { return this._probe(); }

  /** Its column names. */
  columns() { return this._names(); }

  _probe() {
    if (!this._types) this._types = handled(this._step((r) => `SELECT * FROM ${r} LIMIT 0`)._typed());
    return this._types;
  }

  _names() { return then(this._probe(), (ts) => ts.map((t) => t.name)); }

  async _typed() {
    const text = await this.sql();
    const answer = await (await this._con._send(text, this._params, "typed")).json();
    return answer.columns.map(({ name, type }) => ({ name, type }));
  }

  toString() { return "<pondra Frame>"; }

  // building (frame.py's `_with`, `_as_rel`, `_step`, `_kept`, `_joined`)

  _with(query, ctes = this._ctes, { order = null, rel = null, params = this._params, waits = this._waits } = {}) {
    return new Frame(this._con, query, { ctes, rel, params, order, waits });
  }

  /** A name for this frame in the next step (a new CTE, unless it is one already). */
  _asRel(ctes) {
    if (this._rel) return this._rel;
    const name = `_${nextId()}`;
    ctes.set(name, this._query);
    return name;
  }

  /** The next step: `build(rel)`, then the sort it keeps (`order`: SQL puts it in each step again),
   * then `tail` (a LIMIT). `build` gives text, or a promise of it. */
  _step(build, order = null, tail = "") {
    const ctes = new Map(this._ctes);
    const rel = this._asRel(ctes);
    const q = build(rel);
    const text = later([q, order], ([s, ord]) => `${s}${ord && ord.length ? ` ORDER BY ${ord.map(([o]) => o).join(", ")}` : ""}${tail}`);
    // (a sort alone is its input read in order: the next step reads that input, in that order)
    const keepRel = typeof q === "string" && q === `SELECT * FROM ${rel}` && !tail;
    return this._with(text, ctes, { order, rel: keepRel ? rel : null });
  }

  /** The sort, if every column it names is still there (`names`: the step's columns; undefined: all). */
  _kept(names) {
    return later([this._order, names], ([order, ns]) => (order && (ns === undefined || order.every(([, n]) => ns.has(n))) ? order : null));
  }

  /** Both frames' steps, parameters and promises, for a step that reads both. */
  _joined(other) {
    for (const [k, v] of Object.entries(other._params)) {
      if (k in this._params && this._params[k] !== v) throw new Error(`$${k} has two values`);
    }
    return { ctes: new Map([...this._ctes, ...other._ctes]), params: { ...this._params, ...other._params }, waits: [...this._waits, ...other._waits] };
  }

  // rows and columns

  /** Some columns, as expressions: `select("a", col("b").add(1), { total: "a + b" })`. */
  select(...args) {
    const [items, aliases] = takeNamed(args);
    const es = [...exprs(flat(items)), ...aliases.map(([k, v]) => expr(v, true).alias(k))];
    const keep = this._kept(es.some((e) => e.sql === "*") ? undefined : new Set(es.map((e) => e.name)));
    return this._step((r) => `SELECT ${es.map(named).join(", ")} FROM ${r}`, keep);
  }

  /** New columns, or new values of old ones where they are. SQL snippets need `AS name`, or a name
   * as the object's key: `withColumns(col("qty").mul(col("price")).alias("total"), { total: "qty * price" })`. */
  withColumns(...args) {
    const [items, aliases] = takeNamed(args);
    const es = [...flat(items).map((e) => expr(e, true)), ...aliases.map(([k, v]) => expr(v, true).alias(k))];
    if (es.some((e) => !e.name)) throw new Error("withColumns: each expression needs a name (.alias(…), or AS name in SQL)");
    const have = this._names().then((names) => new Map(names.map((c) => [c.toLowerCase(), c]))); // (Python's `have`)
    return this._step((r) => later([have], ([h]) => {
      const replace = es.filter((e) => h.has(e.name.toLowerCase())).map((e) => `${e.sql} AS ${quote(h.get(e.name.toLowerCase()))}`);
      const added = es.filter((e) => !h.has(e.name.toLowerCase())).map((e) => `${e.sql} AS ${quote(e.name)}`);
      const star = replace.length ? `* REPLACE (${replace.join(", ")})` : "*";
      return `SELECT ${[star, ...added].join(", ")} FROM ${r}`;
    }), this._kept());
  }

  withColumn(...args) { return this.withColumns(...args); }

  filter(...predicates) {
    const conds = flat(predicates).map((p) => expr(p, true).sql);
    return this._step((r) => `SELECT * FROM ${r} WHERE ${conds.join(" AND ")}`, this._kept());
  }

  where(...predicates) { return this.filter(...predicates); }

  drop(...columns) {
    const gone = flat(columns);
    const keep = later([this._order], ([order]) => (order && !order.some(([, n]) => gone.includes(n)) ? order : null));
    return this._step((r) => `SELECT * EXCLUDE (${gone.map((c) => quote(c)).join(", ")}) FROM ${r}`, keep);
  }

  /** Columns renamed: `rename({ qty: "n" })`. */
  rename(mapping) {
    const renamed = new Map(Object.entries(mapping));
    const have = this._names();
    const kept = later([have], ([names]) => new Set(names.filter((c) => !renamed.has(c))));
    return this._step((r) => later([have], ([names]) => {
      const cols = names.map((c) => (renamed.has(c) ? `${quote(c)} AS ${quote(renamed.get(c))}` : quote(c)));
      return `SELECT ${cols.join(", ")} FROM ${r}`;
    }), this._kept(kept));
  }

  /** Columns cast: `cast({ n: "Float64" })`, or SQL types (`{ n: "DOUBLE" }`). */
  cast(dtypes) {
    const casts = Object.entries(dtypes).map(([c, t]) => `CAST(${quote(c)} AS ${sqlType(t)}) AS ${quote(c)}`).join(", ");
    return this._step((r) => `SELECT * REPLACE (${casts}) FROM ${r}`, this._kept());
  }

  /** Sorted: `sort("total", "id", { descending: [true, false], nullsLast: true })`. */
  sort(by, ...more) {
    const options = isPlain(more[more.length - 1]) ? more.pop() : {};
    const { descending = false, nullsLast = false } = options;
    const keys = exprs(flat([by, ...more]));
    const desc = Array.isArray(descending) ? descending : keys.map(() => descending);
    const last = Array.isArray(nullsLast) ? nullsLast : keys.map(() => nullsLast);
    const order = keys.slice(0, Math.min(keys.length, desc.length, last.length)).map((k, i) => [
      `${k.sql} ${desc[i] ? "DESC" : "ASC"} NULLS ${last[i] ? "LAST" : "FIRST"}`, k.name,
    ]);
    return this._step((r) => `SELECT * FROM ${r}`, order);
  }

  limit(n = 5) { return this._step((r) => `SELECT * FROM ${r}`, this._kept(), ` LIMIT ${Math.trunc(n)}`); }

  head(n = 5) { return this.limit(n); }

  /** The distinct rows, or, with `subset`, one row per distinct value of those columns. `keep`: "any" (the
   * engine's pick), "first" or "last" (the row of each value first or last in the sort), or "none" (only the
   * values that occur once). `first`, `last` and `maintainOrder` need a sorted frame: a frame has no first or
   * last row until it is sorted. */
  unique(subset = null, { keep = "any", maintainOrder = false } = {}) {
    if (!["any", "first", "last", "none"].includes(keep)) throw new Error(`unique({ keep: '${keep}' }): any, first, last or none`);
    if (keep === "any" && !maintainOrder) { // (the engine's pick, as it always was)
      if (subset === null || subset === undefined) return this._step((r) => `SELECT DISTINCT * FROM ${r}`);
      const keys = exprs(subset).map((e) => e.sql).join(", ");
      return this._step((r) => `SELECT DISTINCT ON (${keys}) * FROM ${r}`);
    }
    // (after a rename the sort is a promise: the check waits for it, and a frame that isn't sorted refuses then)
    const order = later([this._order], ([o]) => {
      if (!o && (keep === "first" || keep === "last")) throw new Error(`unique({ keep: '${keep}' }): a frame has no row order until it is sorted (sort it first, or keep: 'any')`);
      if (!o && maintainOrder) throw new Error("unique({ maintainOrder: true }): a frame has no row order until it is sorted (sort it first)");
      return o;
    });
    const keys = subset === null || subset === undefined
      ? then(this._names(), (names) => names.map((c) => quote(c)).join(", "))
      : exprs(subset).map((e) => e.sql).join(", ");
    // (a window keeps one row of each value: DISTINCT ON wants its ORDER BY to begin with its keys,
    // which a sort by other columns doesn't, so a sorted frame takes each value's row in its sort)
    return this._step((r) => later([keys, order], ([ks, o]) => {
      if (keep === "none") return `SELECT * EXCLUDE (__pondra_n) FROM (SELECT *, count(*) OVER (PARTITION BY ${ks}) AS __pondra_n FROM ${r}) WHERE __pondra_n = 1`;
      const by = keep === "last" ? reversedOrder(o) : o;
      return `SELECT * EXCLUDE (__pondra_n) FROM (SELECT *, row_number() OVER (PARTITION BY ${ks} ORDER BY ${by.map(([s]) => s).join(", ")}) AS __pondra_n FROM ${r}) WHERE __pondra_n = 1`;
    }), this._kept());
  }

  /** `{ n }` rows, or about `fraction` of them, at random; with a `seed`, the same ones every time
   * (chosen by a hash of the seed and each row's values). */
  sample({ n = null, fraction = null, seed = null } = {}) {
    const names = seed === null ? null : this._names(); // (asked now, as frame.py asks for the columns now)
    const pick = seed === null ? "random()" : later([names], ([cs]) => `md5(concat_ws('|', ${literal(String(seed))}, ${cs.map((c) => `CAST(${quote(c)} AS VARCHAR)`).join(", ")}))`);
    if (n !== null) return this._step((r) => later([pick], ([p]) => `SELECT * FROM ${r} ORDER BY ${p} LIMIT ${Math.trunc(n)}`));
    if (seed === null) return this._step((r) => `SELECT * FROM ${r} WHERE random() < ${pyFloat(fraction)}`, this._kept());
    const cut = Math.min(Math.trunc(fraction * 16 ** 8), 16 ** 8 - 1).toString(16).padStart(8, "0"); // (the hash's first 8 hex digits, below the fraction)
    return this._step((r) => later([pick], ([p]) => `SELECT * FROM ${r} WHERE substr(${p}, 1, 8) < ${literal(cut)}`), this._kept());
  }

  /** Nulls in every column of the value's kind (numbers, text, true/false, dates) become it. */
  fillNull(value) {
    const kind = fillKind(value);
    const types = this.schema();
    return this._step((r) => later([types], ([ts]) => {
      const cols = ts.filter((t) => kindOf(t.type, kind)).map((t) => t.name);
      if (!cols.length) return `SELECT * FROM ${r}`; // (frame.py returns the frame as it is: no column to fill)
      return `SELECT * REPLACE (${cols.map((c) => `coalesce(${quote(c)}, ${literal(value)}) AS ${quote(c)}`).join(", ")}) FROM ${r}`;
    }), this._kept());
  }

  /** The rows without a null in these columns (all of them, if none are named). */
  dropNulls(subset = null) {
    const cols = subset === null || subset === undefined ? this._names() : flat([subset]);
    return this._step((r) => later([cols], ([cs]) => `SELECT * FROM ${r} WHERE ${cs.map((c) => `${quote(c)} IS NOT NULL`).join(" AND ")}`), this._kept());
  }

  /** The groups by these keys. `{ maintainOrder: true }` as the last argument is refused: a GROUP BY promises no order. */
  groupBy(...args) {
    const { maintainOrder = false } = isPlain(args[args.length - 1]) ? args.pop() : {};
    if (maintainOrder) throw new Error("groupBy({ maintainOrder: true }): groups come in no particular order here; sort the result by its keys (.sort(...))");
    return new GroupBy(this, exprs(flat(args)));
  }

  /** Polars' join: the key once (inner, left, right), both kept (full), the right side's other columns
   * with `suffix` where names clash. `how`: inner, left, right, full, cross, semi, anti. */
  join(other, { on = null, how = "inner", leftOn = null, rightOn = null, suffix = "_right" } = {}) {
    const { ctes, params, waits } = this._joined(other);
    const lr = this._asRel(ctes);
    const rr = other._asRel(ctes);
    const lk = flat([leftOn || on || []]);
    const rk = flat([rightOn || on || []]);
    const n = nextId();
    const l = `_l${n}`;
    const r = `_r${n}`;
    const pairs = lk.slice(0, rk.length).map((a, i) => `${l}.${quote(a)} = ${r}.${quote(rk[i])}`);
    const cond = pairs.join(" AND ") || "TRUE";
    if (how === "semi" || how === "anti") {
      return this._with(`SELECT ${l}.* FROM ${lr} AS ${l} LEFT ${how.toUpperCase()} JOIN ${rr} AS ${r} ON ${cond}`, ctes, { params, waits });
    }
    const kinds = new Map(Object.entries({ inner: "JOIN", left: "LEFT JOIN", right: "RIGHT JOIN", full: "FULL JOIN", outer: "FULL JOIN", cross: "CROSS JOIN" }));
    if (!kinds.has(how)) throw new Error(`how: ${how}: inner, left, right, full, cross, semi or anti`);
    const lcols = this._names();
    const rcols = other._names();
    const onText = how === "cross" ? "" : ` ON ${cond}`;
    const text = later([lcols, rcols], ([lc, rc]) => {
      let left;
      let right;
      if (how === "right") { // (the right side's keys, the left's other columns first)
        left = lc.filter((c) => !lk.includes(c)).map((c) => `${l}.${quote(c)}`);
        right = rc.map((c) => `${r}.${quote(c)} AS ${quote(lc.includes(c) && !rk.includes(c) ? c + suffix : c)}`);
      } else {
        left = [`${l}.*`];
        const keep = rc.filter((c) => how === "full" || how === "cross" || !rk.includes(c));
        right = keep.map((c) => `${r}.${quote(c)} AS ${quote(lc.includes(c) ? c + suffix : c)}`);
      }
      return `SELECT ${[...left, ...right].join(", ")} FROM ${lr} AS ${l} ${kinds.get(how)} ${rr} AS ${r}${onText}`;
    });
    return this._with(text, ctes, { params, waits });
  }

  /** The rows of both, each once (SQL's UNION); `{ distinct: false }` keeps repeats (UNION ALL). */
  union(other, { distinct = true } = {}) { return this._set(other, distinct ? "UNION" : "UNION ALL"); }
  intersect(other) { return this._set(other, "INTERSECT"); }
  except(other) { return this._set(other, "EXCEPT"); }

  _set(other, op) {
    const { ctes, params, waits } = this._joined(other);
    const lr = this._asRel(ctes);
    const rr = other._asRel(ctes);
    return this._with(`SELECT * FROM ${lr} ${op} SELECT * FROM ${rr}`, ctes, { params, waits });
  }

  /** Each row with the right side's nearest earlier (backward) or later (forward) row by `on`, within
   * `by` (SQL's ASOF JOIN). */
  joinAsof(other, { on = null, leftOn = null, rightOn = null, by = null, byLeft = null, byRight = null, strategy = "backward", suffix = "_right" } = {}) {
    const { ctes, params, waits } = this._joined(other);
    const lr = this._asRel(ctes);
    const rr = other._asRel(ctes);
    const lo = leftOn || on;
    const ro = rightOn || on;
    const bl = flat([byLeft || by || []]);
    const br = flat([byRight || by || []]);
    const n = nextId();
    const l = `_l${n}`;
    const r = `_r${n}`;
    const ops = new Map([["backward", ">="], ["forward", "<="]]);
    if (!ops.has(strategy)) throw new Error(`strategy: ${strategy}: backward or forward`);
    const lcols = this._names();
    const ocols = other._names();
    const text = later([lcols, ocols], ([lc, oc]) => {
      const right = oc.filter((c) => !br.includes(c) && c !== ro).map((c) => `${r}.${quote(c)} AS ${quote(lc.includes(c) ? c + suffix : c)}`);
      const onText = bl.length ? ` ON ${bl.slice(0, br.length).map((a, i) => `${l}.${quote(a)} = ${r}.${quote(br[i])}`).join(" AND ")}` : "";
      return `SELECT ${[`${l}.*`, ...right].join(", ")} FROM ${lr} AS ${l} ASOF JOIN ${rr} AS ${r} MATCH_CONDITION (${l}.${quote(lo)} ${ops.get(strategy)} ${r}.${quote(ro)})${onText}`;
    });
    return this._with(text, ctes, { params, waits });
  }

  // results

  /** Run it: its rows, as the node's JSON gives them (a null is left out). */
  async rows() {
    return (await this._con._send(await this.sql(), this._params)).json();
  }

  /** Awaiting a frame is its rows. */
  then(resolve, reject) { return this.rows().then(resolve, reject); }
  catch(reject) { return this.rows().catch(reject); }
  finally(fn) { return this.rows().finally(fn); }

  /** The one value of a one-row, one-column answer. */
  async item() {
    const rows = await this.rows();
    if (!rows.length) throw new Error("item(): the answer has no rows");
    return Object.values(rows[0])[0] ?? null;
  }

  /** Run it: an Apache Arrow table (needs the optional `apache-arrow` package). */
  async toArrow() {
    const arrow = await loadArrow();
    const r = await this._con._send(await this.sql(), this._params, "arrow");
    return arrow.tableFromIPC(new Uint8Array(await r.arrayBuffer()));
  }

  collect() { return this.toArrow(); }

  /** Its first `n` rows, as a text table. */
  async show(n = 20) {
    const first = this._first(n); // (its step is made now, as frame.py's is)
    console.log(await (await this._con._send(await first.sql(), first._params, "table")).text());
  }

  /** Pondra's plan for it, and whether it would spread over the nodes. */
  async explain() {
    const plan = this._statement() ? this : new Frame(this._con, `EXPLAIN ${await this.sql()}`, { params: this._params });
    return (await plan.rows()).map((r) => r.plan).join("\n");
  }

  /** A table's new rows as they commit (`db.watch`). */
  watch() {
    if (!this._table) throw new Error("watch() follows a table or a materialized view: db.table(name).watch()");
    return this._con.watch(this._table);
  }

  // back to SQL, and writes

  /** A view others read by name, made now (`CREATE VIEW`; `materialized`: kept up to date; `temporary`:
   * this connection's only, and never materialized). `replace: false` refuses to replace one that exists.
   * The frame returned is the view's table. */
  toView(name, { temporary = false, materialized = false, replace = null, ...options } = {}) {
    if (temporary && materialized) throw new Error("temporary and materialized: a temporary view is this connection's alone, and the lake keeps a materialized one");
    const opts = Object.entries(options);
    if (opts.length && !materialized) throw new Error(`${opts.map(([k]) => k).join(", ")}: options of a materialized view ({ materialized: true })`);
    const withs = opts.length ? ` WITH (${opts.map(([k, v]) => `${k} = ${literal(v)}`).join(", ")})` : "";
    const head = materialized ? `CREATE MATERIALIZED VIEW ${name}${withs} AS`
      : `CREATE ${replace === false ? "" : "OR REPLACE "}${temporary ? "TEMP " : ""}VIEW ${name} AS`;
    const made = this.sql().then((text) => this._con.sql(`${head} ${text}`, this._params));
    return new Frame(this._con, `SELECT * FROM ${name}`, { rel: name, table: name, waits: [handled(made)] });
  }

  /** Its rows into a table: `create` (CREATE TABLE … AS), `append` (INSERT) or `overwrite` (a new
   * table in its place), as Python's `write_table`. */
  async writeTable(name, { mode = "create" } = {}) {
    const text = await this.sql();
    const bodies = new Map([
      ["create", `CREATE TABLE ${name} AS ${text}`],
      ["append", `INSERT INTO ${name} ${text}`],
      ["overwrite", `DROP TABLE IF EXISTS ${name}; CREATE TABLE ${name} AS ${text}`],
    ]);
    if (!bodies.has(mode)) throw new Error(`mode ${mode}: create, append or overwrite`);
    return this._con.sql(bodies.get(mode), this._params);
  }

  writeParquet(path, { partitionBy = null, mode = null } = {}) { return this._sink(path, "parquet", partitionBy, mode); }
  writeCsv(path, { separator = null, includeHeader = true, partitionBy = null, mode = null } = {}) {
    return this._sink(path, "csv", partitionBy, mode, { header: includeHeader, delimiter: separator });
  }
  writeJson(path, { partitionBy = null, mode = null } = {}) { return this._sink(path, "json", partitionBy, mode); }
  writeNdjson(path, options) { return this.writeJson(path, options); }
  writeDelta(target, { mode = "error" } = {}) { return this._tableTo(target, "delta", mode); }
  writeIceberg(target, { mode = "error" } = {}) { return this._tableTo(target, "iceberg", mode); }

  async _tableTo(target, format, mode) {
    if (!["error", "append", "overwrite", "ignore"].includes(mode)) throw new Error(`mode ${mode}: error, append, overwrite or ignore`);
    try {
      return await this._sink(String(target), format, null, mode === "error" || mode === "ignore" ? null : mode);
    } catch (e) {
      if (mode === "ignore" && /is there already/.test(e.message)) return null;
      throw e;
    }
  }

  async _sink(path, format, partitionBy, mode, options = {}) {
    if (mode !== null && !["append", "overwrite"].includes(mode)) throw new Error(`mode ${mode}: append or overwrite (none: a folder must be empty)`);
    const cols = typeof partitionBy === "string" ? [partitionBy] : [...(partitionBy || [])];
    const given = [`FORMAT ${format}`];
    if (cols.length) given.push(`PARTITION_BY (${cols.map((c) => quote(c)).join(", ")})`);
    if (mode) given.push(mode.toUpperCase());
    for (const [k, v] of Object.entries(options)) if (v !== null && v !== undefined) given.push(`${k.toUpperCase()} ${literal(v)}`);
    const text = await this.sql();
    return this._con.sql(`COPY (${text}) TO ${literal(String(path))} (${given.join(", ")})`, this._params);
  }

  _target(what) {
    if (!this._table) throw new Error(`${what} changes a table: db.table(name).${what}(…)`);
    return this._table;
  }

  /** `UPDATE` the table: `update({ qty: 0 }, col("qty").isNull())`. */
  update(set, where = null) {
    const t = this._target("update");
    const assign = Object.entries(set).map(([c, v]) => `${quote(c)} = ${expr(v, true).sql}`).join(", ");
    return this._con.sql(`UPDATE ${t} SET ${assign}${where == null ? "" : ` WHERE ${expr(where, true).sql}`}`);
  }

  delete(where = null) {
    const t = this._target("delete");
    return this._con.sql(`DELETE FROM ${t}${where == null ? "" : ` WHERE ${expr(where, true).sql}`}`);
  }

  /** `MERGE`, with Delta Lake's builder names: `whenMatchedUpdate({…})`, `whenNotMatchedInsert()`,
   * `whenNotMatchedBySourceDelete()`, then `run()`. */
  merge(source, on, { sourceAlias = "s", targetAlias = "t" } = {}) {
    return new Merge(this, source, on, sourceAlias, targetAlias);
  }
}

/** A query's sort, if it ends with one by columns (`ORDER BY u.tier DESC`): SQL drops the ORDER BY of a
 * query that becomes a CTE, so the steps after it put it back. */
export function trailingOrder(sql) {
  const masked = sql.replace(/'(?:[^']|'')*'|--[^\n]*|\/\*[\s\S]*?\*\/|\$(\w*)\$[\s\S]*?\$\1\$/g, (m) => " ".repeat(m.length));
  const pattern = /[()]|\bORDER\s+BY\b/gi;
  let depth = 0;
  let at = null;
  for (const m of masked.matchAll(pattern)) {
    depth += m[0] === "(" ? 1 : m[0] === ")" ? -1 : 0;
    if (depth === 0 && m[0] !== "(" && m[0] !== ")") at = m.index + m[0].length;
  }
  if (at === null) return null;
  const clause = masked.slice(at).split(/\b(?:LIMIT|OFFSET)\b|;/i)[0];
  const out = [];
  for (const item of clause.split(",")) {
    const m = /^\s*(?:\w+\.)?("(?:[^"]|"")+"|\w+)\s*(ASC|DESC)?\s*(NULLS\s+(?:FIRST|LAST))?\s*$/is.exec(item);
    if (!m || /^\d+$/.test(m[1])) return null; // (a position or an expression: not known after the step)
    const name = m[1].startsWith('"') ? m[1].slice(1, -1).replaceAll('""', '"') : m[1].toLowerCase();
    const way = (m[2] || "ASC").toUpperCase();
    const nulls = m[3] ? m[3].toUpperCase().replace(/\s+/g, " ") : way === "ASC" ? "NULLS LAST" : "NULLS FIRST";
    out.push([`${quote(name)} ${way} ${nulls}`, name]);
  }
  return out;
}

/** A sort turned around, for `unique`'s `keep: "last"`: each key's direction and nulls side flipped. */
function reversedOrder(order) {
  return order.map(([sql, name]) => [sql.replace(/(ASC|DESC) NULLS (FIRST|LAST)$/, (_, way, nulls) => `${way === "ASC" ? "DESC" : "ASC"} NULLS ${nulls === "FIRST" ? "LAST" : "FIRST"}`), name]);
}

/** `groupBy(...)`: its `agg(...)` steps, and `len()`. */
class GroupBy {
  constructor(frame, keys) {
    this.frame = frame;
    this.keys = keys;
  }

  agg(...args) {
    const [items, aliases] = takeNamed(args);
    const es = [...flat(items).map((a) => expr(a, true)), ...aliases.map(([k, v]) => expr(v, true).alias(k))];
    const cols = [...this.keys.map(named), ...es.map(named)];
    const group = this.keys.map((_, i) => i + 1).join(", ");
    return this.frame._step((r) => `SELECT ${cols.join(", ")} FROM ${r}${group ? ` GROUP BY ${group}` : ""}`);
  }

  len(name = "len") { return this.agg(len().alias(name)); }
}

/** `MERGE INTO t USING (source) s ON …`, built as delta-rs and Delta on Spark build it. */
export class Merge {
  constructor(target, source, on, s, t) {
    this.target = target;
    this.source = source;
    this.s = s;
    this.t = t;
    this.whens = [];
    if (on instanceof Expr || (typeof on === "string" && !WORD.test(on))) this.on = expr(on, true).sql; // (a condition)
    else this.on = flat([on]).map((k) => `${t}.${quote(k)} = ${s}.${quote(k)}`).join(" AND "); // (key columns)
  }

  _add(clause) {
    this.whens.push(clause);
    return this;
  }

  _set(values) { return Object.entries(values).map(([c, v]) => `${quote(c)} = ${expr(v, true).sql}`).join(", "); }

  whenMatchedUpdate(set, { predicate = null } = {}) {
    return this._add(`WHEN MATCHED${and(predicate)} THEN UPDATE SET ${this._set(set)}`);
  }

  /** The target's columns the source has, in the target's order (a promise). */
  _common() {
    return later([this.target._names(), this._source()._names()], ([tc, sc]) => tc.filter((c) => sc.includes(c)));
  }

  whenMatchedUpdateAll({ predicate = null } = {}) {
    return this._add(later([this._common()], ([cs]) => `WHEN MATCHED${and(predicate)} THEN UPDATE SET ${this._set(Object.fromEntries(cs.map((c) => [c, col(`${this.s}.${c}`)])))}`));
  }

  whenMatchedDelete({ predicate = null } = {}) {
    return this._add(`WHEN MATCHED${and(predicate)} THEN DELETE`);
  }

  /** Insert these values (`{ col: expr }`), or, without them, every column the source has. */
  whenNotMatchedInsert(values = null, { predicate = null } = {}) {
    const insert = (vs) => `WHEN NOT MATCHED${and(predicate)} THEN INSERT (${vs.map(([c]) => quote(c)).join(", ")}) VALUES (${vs.map(([, v]) => expr(v, true).sql).join(", ")})`;
    if (values !== null) return this._add(insert(Object.entries(values)));
    return this._add(later([this._common()], ([cs]) => insert(cs.map((c) => [c, col(`${this.s}.${c}`)]))));
  }

  whenNotMatchedInsertAll({ predicate = null } = {}) { return this.whenNotMatchedInsert(null, { predicate }); }

  whenNotMatchedBySourceDelete({ predicate = null } = {}) {
    return this._add(`WHEN NOT MATCHED BY SOURCE${and(predicate)} THEN DELETE`);
  }

  _source() {
    if (!(this.source instanceof Frame)) throw new Error("merge's source is a frame (db.table, db.sql, a read_* call): write rows with db.writeTable first");
    return this.source;
  }

  async run() {
    const src = this._source();
    const whens = await Promise.all(this.whens);
    const text = await src.sql();
    if (!this.target._table) throw new Error("merge changes a table: db.table(name).merge(…)");
    const sql = `MERGE INTO ${this.target._table} AS ${this.t} USING (${text}) AS ${this.s} ON ${this.on} ${whens.join(" ")}`;
    return this.target._con.sql(sql, src._params);
  }

  execute() { return this.run(); }
}

const and = (predicate) => (predicate === null || predicate === undefined ? "" : ` AND ${expr(predicate, true).sql}`);

/** Frames one after another: by position (`vertical`), or by name, missing columns null (`diagonal`). */
export function concat(frames, { how = "vertical" } = {}) {
  let out = frames[0];
  const ctes = new Map(out._ctes);
  const rels = [out._asRel(ctes)];
  for (const f of frames.slice(1)) {
    const more = out._joined(f);
    for (const [k, v] of more.ctes) ctes.set(k, v);
    rels.push(f._asRel(ctes));
    out = out._with(out._query, ctes, { params: more.params, waits: more.waits });
  }
  const glue = how === "diagonal" ? " UNION ALL BY NAME " : " UNION ALL ";
  return out._with(rels.map((r) => `SELECT * FROM ${r}`).join(glue), ctes);
}

// ---------------------------------------------------------------- files, and a node's Arrow

/** Reader options named as frame.py's read_* take them (camelCase), and SQL's own names: the
 * first are SQL's `read_csv`'s names, and win if both are given (as Polars' do in frame.py). */
const SCAN_NAMES = new Map([["hivePartitioning", "hive_partitioning"], ["separator", "delim"], ["hasHeader", "header"], ["snapshotId", "snapshot_from_id"], ["allowMovedPaths", "allow_moved_paths"]]);

function scanOptions(options) {
  const out = {};
  for (const [k, v] of Object.entries(options)) if (!SCAN_NAMES.has(k)) out[k] = v;
  for (const [k, v] of Object.entries(options)) if (SCAN_NAMES.has(k)) out[SCAN_NAMES.get(k)] = v;
  return out;
}

/** Files as a frame (SQL's `read_*`): a path, a URL, a folder, a glob, or a list of them, read where
 * the node runs each time the frame runs. */
export function scan(con, fn, source, options = {}) {
  const paths = typeof source === "string" ? [source] : [...source].map(String);
  const where = paths.length === 1 ? literal(paths[0]) : `[${paths.map((p) => literal(p)).join(", ")}]`;
  const args = Object.entries(scanOptions(options)).filter(([, v]) => v !== null && v !== undefined).map(([k, v]) => `, ${k} => ${literal(v)}`);
  return new Frame(con, `SELECT * FROM ${fn}(${where}${args.join("")})`);
}

/** Apache Arrow, if the optional package is installed (`npm install apache-arrow`). */
async function loadArrow() {
  try {
    return await import("apache-arrow");
  } catch {
    throw new Error("toArrow() and collect() need the apache-arrow package: npm install apache-arrow (rows() and await work without it)");
  }
}
