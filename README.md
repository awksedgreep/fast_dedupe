# FastDedupe

`FastDedupe` is a SQLite-backed duplicate finder optimized around staged filtering:

1. Scan directories concurrently and store every file path plus exact byte size.
2. Compute a partial MD5 only for files that collide on size.
3. Compute a full MD5 only for files that still collide on size and partial MD5.

## Status

This is an initial working implementation. It uses:

- one concurrent task per directory walk step
- one SQLite writer process to avoid write contention
- SQLite indexes on size, partial hash, and full hash

## Usage

```elixir
{:ok, result} =
  FastDedupe.run(
    ["/path/to/scan"],
    db_path: "/path/to/fast_dedupe.sqlite3",
    partial_bytes: 4_096
  )
```

The result shape is:

```elixir
%{
  scanned_files: 123,
  same_size_groups: 10,
  partial_hash_groups: 3,
  duplicate_groups: [
    ["/path/a/file.bin", "/path/b/file.bin"]
  ]
}
```

## CLI

During development:

```sh
mix run -e 'FastDedupe.CLI.main(System.argv())' -- /path/to/scan
mix run -e 'FastDedupe.CLI.main(System.argv())' --
mix run -e 'FastDedupe.CLI.main(System.argv())' -- --search invoice
```

You can also build an `escript` entry point:

```sh
mix escript.build
./fast_dedupe /path/to/scan
./fast_dedupe
./fast_dedupe --search invoice
```

## Build

### macOS

If you are developing on macOS and just want to run the tool locally:

```sh
mix deps.get
mix escript.build
./fast_dedupe
./fast_dedupe --search invoice
```

This produces an `escript`, which still requires Erlang on the machine where you run it.

### Linux

For a Linux server, the recommended path is to build on that Linux machine instead of cross-building elsewhere:

```sh
mix deps.get
mix escript.build
./fast_dedupe
./fast_dedupe --search invoice
```

For your Debian fileserver, this is the simplest and lowest-risk deployment model. Install Erlang/Elixir on the server, clone the repo there, and build the `escript` locally.

### Debian 13

On Debian 13 (`trixie`), the package names are `elixir` and `erlang`. A practical setup looks like:

```sh
sudo apt update
sudo apt install -y git build-essential erlang elixir
git clone https://github.com/awksedgreep/fast_dedupe.git
cd fast_dedupe
mix deps.get
mix escript.build
./fast_dedupe
```

If you want to keep the built executable around system-wide, you can copy it somewhere on your `PATH`, for example:

```sh
sudo install -m 0755 fast_dedupe /usr/local/bin/fast_dedupe
fast_dedupe --search invoice
```

### Notes

- The generated `./fast_dedupe` file is an `escript`, not a fully static native binary.
- The target machine must have a compatible Erlang runtime available.
- The default database path is `./fast_dedupe.sqlite3`.
- If no scan path is provided, the CLI scans the current working directory (`.`).

Useful flags:

- `--db-path /tmp/fast_dedupe.sqlite3`
- `--partial-bytes 4096`
- `--search invoice`
- `--limit 50`
- `--keep newest`
- `--output json`
- `--delete --dry-run`
- `--delete --yes`
- `--help`

For scripting:

```sh
./fast_dedupe --output json /path/to/scan
./fast_dedupe --delete --dry-run --output json /path/to/scan
./fast_dedupe --search invoice --output json
```

If no scan path is provided, the CLI defaults to the current working directory (`.`).

If you do not pass `--db-path`:

- scan mode stores `fast_dedupe.sqlite3` in the first scan path
- search mode looks for `fast_dedupe.sqlite3` in the current working directory

Paths are stored in SQLite as raw `BLOB` values, not `TEXT`, so Unicode filenames are preserved cleanly and the storage layer does not assume UTF-8 text semantics.

## Current schema

The `files` table stores:

- `path` as a raw SQLite `BLOB`
- `size_bytes`
- `partial_md5`
- `full_md5`

## Next steps

- batch SQLite writes in transactions
- add richer keep policies based on directory preferences
- add machine-readable output for scripting
