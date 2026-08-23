# nu_plugin_df

A Nushell plugin that runs [Apache DataFusion](https://datafusion.apache.org/) inside the plugin process. You get DataFusion's SQL engine and DataFrame API without pulling millions of rows into Nushell `Value`s.

Nushell already has a dataframe plugin: [`nu_plugin_polars`](https://github.com/nushell/nushell/tree/main/crates/nu_plugin_polars). That one taught us a lot. This one is a second take, built around DataFusion instead of Polars, and around staying lazy until you ask for Nushell values.

If you have a 40 million row parquet file, `df open sales.parquet` scans it. Printing the result runs a 20-row `LIMIT`. `df collect` is the only path that copies rows into a native Nu table, and it caps at 10_000 rows unless you pass `--all`.

## What it is

DataFusion is a Rust query engine that uses Apache Arrow in memory. It plans SQL and DataFrame operations as a logical plan, then executes that plan. File readers (parquet in particular) push filters and column projection down into the scan.

This plugin owns one DataFusion `SessionContext` for the life of the plugin process. That session is the catalog, the runtime, and the config. Every `df` command either:

- returns a lazy DataFrame handle (a Nushell custom value of type `dataframe`), or
- runs an action against a handle (`collect`, `count`, `describe`, `save`, `explain`).

The Nushell engine never holds the Arrow data. It holds a UUID. The plugin process holds the real DataFrame. That is the same trick Polars uses. Without it, serializing a DataFusion plan across the plugin protocol would be hopeless.

## Install

The crate path-depends on a local Nushell tree at `../nushell`, same layout as `nu_plugin_exp`.

```nu
cd nu_plugin_df
cargo build --release
plugin add target/release/nu_plugin_df
plugin use df
```

The command prefix is `df`. Unix disk-free is also `df`. While the plugin is loaded, the plugin wins. Use `^df` for disk free.

## How a pipeline works

```nu
df open sales.parquet
    | df filter "amount > 1000"
    | df select region "sum(amount) as total"
```

`df open` does not load the file. It builds a scan plan. `df filter` and `df select` add nodes to that plan. Nothing runs until something forces execution:

- printing the value (preview, 20 rows)
- `df collect`
- `df count`
- `df describe`
- `df sample`
- `df save`
- `df explain --analyze`

Until then you are carrying a handle. `describe` on the custom value reports type `dataframe`.

### Auto-in

Polars makes you write `polars into-df` before any dataframe command. Here, if you pipe a Nu table, list, or record into a `df` command, the plugin converts it to an in-memory Arrow batch and wraps it as a dataframe.

```nu
[[a b]; [1 2] [3 4]] | df filter "a > 1" | df collect
```

That conversion copies every cell through the plugin protocol. Fine for shell-sized tables. Useless for millions of rows, because those rows already exist as Nushell values. Large data has to enter via `df open`.

`df from-nu` is the explicit form of the same conversion, if you want it named in the pipeline.

A record whose fields are equal-length lists is treated as columns:

```nu
{a: [1 3], b: [2 4]} | df from-nu | df collect
```

A list of scalars becomes a one-column frame named `value`. Mixed types in a column are an error. The plugin will not silently stringify them.

### Preview-out

Printing a dataframe does not mean "dump this into the table renderer as a Nu table of every row." The plugin runs `LIMIT 20`, converts those rows, and that is what you see.

`df collect` is the honest conversion to Nushell:

```nu
df open sales.parquet | df collect          # first 10_000 rows
df open sales.parquet | df collect --n 100
df open sales.parquet | df collect --all    # you asked for it
```

If the default cap truncates the result, pipeline metadata includes `truncated: true`. `df save` is uncapped and streams to disk. Prefer save when the result is large.

## SQL

DataFusion's SQL is a real planner, not a hand-rolled subset. `df sql` runs against the session catalog.

A piped dataframe is registered as table `this` for that one call. If the query has no `FROM`, the plugin appends `FROM this`.

```nu
df open sales.parquet | df sql "select region, sum(amount) as total from this group by region"

# same thing, FROM injected:
df open sales.parquet | df sql "select region, sum(amount) as total group by region"
```

`this` is not listed by `df tables` unless you registered that name yourself. After the call, the ephemeral `this` is dropped (a user table named `this` is left alone).

Named tables last for the plugin process:

```nu
df open sales.parquet --as sales
df open returns.parquet --as returns
df sql "select * from sales join returns using (id)" | df save joined.parquet
df tables
df unregister returns
```

`df open path` returns a handle and does not register anything. `--as` is opt-in. `df register name` registers the piped handle. Replacing a name requires `--force`.

The catalog is sticky because the plugin process is sticky. Restarting Nushell, or `plugin stop df`, clears it.

## DataFrame commands

These take SQL fragments and column names. There is no `df col`, no expression custom value, no Nushell closure. That is deliberate. Polars grew an expression language inside the plugin. DataFusion already has SQL.

```nu
df open sales.parquet
    | df filter "amount > 1000 and region <> 'NA'"
    | df with-column "amount * 1.1 as with_tax"
    | df select region with_tax
    | df sort region --reverse
    | df limit 50
```

`df select` mixes names and expressions:

```nu
[[a b]; [1 2] [3 4]] | df select a "a + b as sum" | df collect
```

`df with-column` wants a name. Either `--as` or `expr as name`:

```nu
... | df with-column "a + b" --as sum
... | df with-column "a + b as sum"
```

`df aggregate`:

```nu
[[g v]; [a 1] [a 2] [b 3]]
    | df aggregate --group [g] --agg ["sum(v) as total" "count(*) as n"]
    | df collect
```

`df join` takes a handle or a catalog name:

```nu
let r = ([[id n]; [1 x] [2 y]] | df from-nu)
[[id v]; [1 10] [2 20]] | df join $r --on [id] | df collect

df open left.parquet --as left
df open right.parquet --as right
df sql "select * from left join right using (id)"   # or:
df open left.parquet | df join right --on [id]
```

Join types: `inner` (default), `left`, `right`, `full`, `semi`, `anti`. Use `--left-on` / `--right-on` when the key names differ.

`df union $other` is UNION ALL. `--distinct` drops duplicates.

`df cast a utf8` rewrites a column to an Arrow type (`int64`, `utf8`, `float64`, `boolean`, `timestamp`, and a few others). CSV inference is often wrong. Cast early.

`df rename {old: new}`.

`df distinct` optionally takes column names. `df drop` takes column names. `df sample --n 10` reservoir-samples in one pass over the plan.

Anything awkward is `df sql`. That is the escape hatch, and it is a complete one.

## Files

`df open` / `df save` understand:

- parquet (file, directory, or glob)
- csv / tsv (`--delimiter`, `--no-header`, `--infer-schema`)
- ndjson / jsonl, and json where DataFusion's reader does
- Arrow IPC (`.arrow` / `.ipc` / `.feather`)

Type comes from the extension, or `--type`.

```nu
df open data/*.parquet
df open events/ --type parquet --as events
df open dump.csv --no-header --delimiter "|"
df open sales.parquet | df filter "year = 2024" | df save out.parquet
```

Parquet directories and globs are ListingTable scans. Hive-style partitions work the way DataFusion does. Saving to a path that exists fails unless you pass `--force`.

Arrow IPC open currently reads the file into batches. Parquet is the format that stays lazy at scale.

## Caps and actions

| Command | Runs the plan? | Bound |
|---|---|---|
| print (preview) | yes, `LIMIT 20` | 20 rows |
| `df schema` | no | |
| `df explain` | no, unless `--analyze` | |
| `df collect` | yes | 10_000, or `--n`, or `--all` |
| `df count` | yes | returns an int |
| `df describe` | yes, full scan | |
| `df sample` | yes, one pass | default 10 rows |
| `df save` | yes, streamed | none |

Preview on a huge join can still be slow. The plan has to run far enough to produce 20 rows. `df explain` is there for that.

## Type mapping

Nu to Arrow, on auto-in / `from-nu`:

| Nu | Arrow |
|---|---|
| int, filesize | Int64 |
| float | Float64 |
| bool | Boolean |
| string | Utf8 |
| date | Timestamp(ns) |
| duration | timestamp nanoseconds today; keep dates and durations in separate columns |
| binary | Binary |
| nothing | null |
| list | List |
| record | Struct |

Closures, ranges, globs, and foreign custom values in cells error.

Arrow to Nu, on preview / collect: signed ints become int, unsigned ints that fit i64 become int (u64 that does not fit errors), floats become float, timestamps/dates become date, decimals become strings so they stay exact, structs become records, lists stay lists.

## Commands

Parent `df` only prints help.

**Scan and catalog.** `df open`, `df save`, `df sql`, `df register`, `df unregister`, `df tables`

**Inspect and materialize.** `df schema`, `df collect`, `df count`, `df explain`, `df describe`, `df sample`, `df from-nu`

**Shape the plan.** `df select`, `df filter`, `df with-column`, `df drop`, `df sort`, `df limit`, `df join`, `df aggregate`, `df distinct`, `df union`, `df rename`, `df cast`

## Compared with `nu_plugin_polars`

Both plugins exist because Nushell tables are a bad store for tens of millions of rows. Both run as a separate process, stash the real frame behind a UUID custom value, disable plugin GC while that cache has entries, and open parquet/csv without collecting first.

They are not interchangeable.

**Engine.** Polars is Polars. This is DataFusion 55 on Arrow. Predicate pushdown, SQL planning, and file listing come from DataFusion. If you already live in the Arrow/parquet ecosystem, this plugin talks that dialect. If you already have Polars-shaped muscle memory (`col`, `lit`, `over`, selectors), stay on `nu_plugin_polars`.

**Conversion.** Polars is explicit: `polars into-df` / `polars into-nu`. Easy to explain, easy to forget, and `into-nu` on a large frame hurts. Here, Nu tables convert on the way in, print is a preview, and `df collect` is capped. That is the whole point of this plugin's UX.

**SQL.** Polars in Nushell implements a slice of SQL with `sqlparser` over LazyFrames. Joins and catalog tables are limited. DataFusion SQL is the product. Multi-table queries against registered scans are a normal thing to do here.

**Expressions.** Polars ships `polars col`, `polars lit`, `when`/`otherwise`, dtype selectors, and expression custom values. That is why it has on the order of 150 commands. This plugin has 25 verbs plus a help stub. Filters look like `"amount > 1000"`. If you need more, write SQL.

**Lazy vs eager.** Polars has both `NuDataFrame` and `NuLazyFrame`, plus `polars into-lazy` / `collect` to move between them. DataFusion DataFrames are always a plan. There is no eager twin. Execution is an action, not a type.

**Catalog.** Polars dataframes are self-contained. DataFusion dataframes belong to a session. This plugin exposes that as `df register` / `df tables` / table `this`. The session dies with the plugin process.

**Command prefix.** `polars ...` vs `df ...`. `df` is shorter and collides with Unix disk-free.

**Where it lives.** Polars is in the Nushell tree and tracks workspace versions. This crate is standalone, path-dep'd on `../nushell` for development.

**What Polars still does better.** The command set is far wider: rolling aggregations, time-series helpers, string kernels as first-class commands, cloud object-store options, more file formats (Avro), and an expression system that composes without dropping into SQL. If your workflow is "Polars in the shell," use that plugin.

**What this plugin is for.** Open a large parquet (or a directory of them), filter and aggregate with SQL or a short DataFrame pipeline, write the result back to parquet, and only `collect` when you need a small Nu table. That loop is the design.

You can load both plugins. They do not share handles. A Polars dataframe is not a DataFusion dataframe.

## Build notes

- Rust 1.95, edition 2024, matching current Nushell.
- `datafusion` 55 with parquet and nested expressions.
- Tokio runtime on the plugin struct. DataFusion calls are `block_on`'d.
- MIT license. See `LICENSE`.

`cargo test --lib` runs conversion, SQL `this` injection, and a parquet roundtrip. Full `PluginTest` examples need `nu-plugin-test-support`; that currently fails to compile against some Nushell trees because of an unmatched `ServerCommunicationIo::LocalSocket` arm in `nu-plugin-engine`.

## License

MIT. See [LICENSE](LICENSE).
