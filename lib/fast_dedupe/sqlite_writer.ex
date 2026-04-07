defmodule FastDedupe.SQLiteWriter do
  @moduledoc false

  use GenServer

  alias Exqlite.Sqlite3

  @type file_row :: %{id: integer(), path: binary()}

  def start_link(db_path) do
    GenServer.start_link(__MODULE__, db_path)
  end

  def stop(pid) do
    GenServer.stop(pid, :normal)
  end

  def insert_file(pid, path, size, mtime_unix) do
    GenServer.call(pid, {:insert_file, path, size, mtime_unix}, :infinity)
  end

  def insert_files(pid, rows) do
    GenServer.call(pid, {:insert_files, rows}, :infinity)
  end

  def update_partial_hash(pid, file_id, hash) do
    GenServer.call(pid, {:update_partial_hash, file_id, hash}, :infinity)
  end

  def update_partial_hashes(pid, rows) do
    GenServer.call(pid, {:update_partial_hashes, rows}, :infinity)
  end

  def update_full_hash(pid, file_id, hash) do
    GenServer.call(pid, {:update_full_hash, file_id, hash}, :infinity)
  end

  def update_full_hashes(pid, rows) do
    GenServer.call(pid, {:update_full_hashes, rows}, :infinity)
  end

  def file_count(pid) do
    GenServer.call(pid, :file_count, :infinity)
  end

  def size_collision_groups(pid) do
    GenServer.call(pid, :size_collision_groups, :infinity)
  end

  def partial_collision_groups(pid) do
    GenServer.call(pid, :partial_collision_groups, :infinity)
  end

  def duplicate_groups(pid) do
    GenServer.call(pid, :duplicate_groups, :infinity)
  end

  def search_files(pid, term, limit \\ 100) do
    GenServer.call(pid, {:search_files, term, limit}, :infinity)
  end

  @impl true
  def init(db_path) do
    with {:ok, conn} <- Sqlite3.open(db_path),
         :ok <- configure(conn),
         :ok <- create_schema(conn),
         :ok <- ensure_search_columns(conn),
         :ok <- backfill_search_columns(conn) do
      {:ok, %{conn: conn}}
    end
  end

  @impl true
  def terminate(_reason, %{conn: conn}) do
    Sqlite3.close(conn)
    :ok
  end

  @impl true
  def handle_call({:insert_file, path, size, mtime_unix}, _from, state) do
    %{path_text: path_text, basename_text: basename_text} = path_search_fields(path)

    sql = """
    INSERT INTO files(path, path_text, basename_text, size_bytes, mtime_unix)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(path) DO UPDATE SET
      path_text = excluded.path_text,
      basename_text = excluded.basename_text,
      size_bytes = excluded.size_bytes,
      mtime_unix = excluded.mtime_unix,
      partial_md5 = CASE
        WHEN files.size_bytes = excluded.size_bytes AND IFNULL(files.mtime_unix, -1) = IFNULL(excluded.mtime_unix, -1)
        THEN files.partial_md5
        ELSE NULL
      END,
      full_md5 = CASE
        WHEN files.size_bytes = excluded.size_bytes AND IFNULL(files.mtime_unix, -1) = IFNULL(excluded.mtime_unix, -1)
        THEN files.full_md5
        ELSE NULL
      END
    """

    {:reply, exec(state.conn, sql, [path, path_text, basename_text, size, mtime_unix]), state}
  end

  def handle_call({:insert_files, rows}, _from, state) do
    sql = """
    INSERT INTO files(path, path_text, basename_text, size_bytes, mtime_unix)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(path) DO UPDATE SET
      path_text = excluded.path_text,
      basename_text = excluded.basename_text,
      size_bytes = excluded.size_bytes,
      mtime_unix = excluded.mtime_unix,
      partial_md5 = CASE
        WHEN files.size_bytes = excluded.size_bytes AND IFNULL(files.mtime_unix, -1) = IFNULL(excluded.mtime_unix, -1)
        THEN files.partial_md5
        ELSE NULL
      END,
      full_md5 = CASE
        WHEN files.size_bytes = excluded.size_bytes AND IFNULL(files.mtime_unix, -1) = IFNULL(excluded.mtime_unix, -1)
        THEN files.full_md5
        ELSE NULL
      END
    """

    params =
      Enum.map(rows, fn {path, size, mtime_unix} ->
        %{path_text: path_text, basename_text: basename_text} = path_search_fields(path)
        [path, path_text, basename_text, size, mtime_unix]
      end)

    {:reply, exec_many(state.conn, sql, params), state}
  end

  def handle_call({:update_partial_hash, file_id, hash}, _from, state) do
    sql = """
    UPDATE files
    SET partial_md5 = ?
    WHERE id = ?
    """

    {:reply, exec(state.conn, sql, [hash, file_id]), state}
  end

  def handle_call({:update_partial_hashes, rows}, _from, state) do
    sql = """
    UPDATE files
    SET partial_md5 = ?
    WHERE id = ?
    """

    params = Enum.map(rows, fn {file_id, hash} -> [hash, file_id] end)
    {:reply, exec_many(state.conn, sql, params), state}
  end

  def handle_call({:update_full_hash, file_id, hash}, _from, state) do
    sql = """
    UPDATE files
    SET full_md5 = ?
    WHERE id = ?
    """

    {:reply, exec(state.conn, sql, [hash, file_id]), state}
  end

  def handle_call({:update_full_hashes, rows}, _from, state) do
    sql = """
    UPDATE files
    SET full_md5 = ?
    WHERE id = ?
    """

    params = Enum.map(rows, fn {file_id, hash} -> [hash, file_id] end)
    {:reply, exec_many(state.conn, sql, params), state}
  end

  def handle_call(:file_count, _from, state) do
    sql = "SELECT COUNT(*) FROM files"

    reply =
      with {:ok, [[count]]} <- fetch_rows(state.conn, sql, []) do
        {:ok, count}
      end

    {:reply, reply, state}
  end

  def handle_call(:size_collision_groups, _from, state) do
    group_sql = """
    SELECT size_bytes
    FROM files
    GROUP BY size_bytes
    HAVING COUNT(*) > 1
    ORDER BY size_bytes
    """

    reply =
      with {:ok, rows} <- fetch_rows(state.conn, group_sql, []) do
        groups =
          Enum.map(rows, fn [size] ->
            {:ok, files} = files_for_size(state.conn, size)
            {size, files}
          end)

        {:ok, groups}
      end

    {:reply, reply, state}
  end

  def handle_call(:partial_collision_groups, _from, state) do
    group_sql = """
    SELECT size_bytes, partial_md5
    FROM files
    WHERE partial_md5 IS NOT NULL
    GROUP BY size_bytes, partial_md5
    HAVING COUNT(*) > 1
    ORDER BY size_bytes, partial_md5
    """

    reply =
      with {:ok, rows} <- fetch_rows(state.conn, group_sql, []) do
        groups =
          Enum.map(rows, fn [size, partial_md5] ->
            {:ok, files} = files_for_partial_hash(state.conn, size, partial_md5)
            {size, partial_md5, files}
          end)

        {:ok, groups}
      end

    {:reply, reply, state}
  end

  def handle_call(:duplicate_groups, _from, state) do
    group_sql = """
    SELECT full_md5
    FROM files
    WHERE full_md5 IS NOT NULL
    GROUP BY size_bytes, full_md5
    HAVING COUNT(*) > 1
    ORDER BY full_md5
    """

    reply =
      with {:ok, rows} <- fetch_rows(state.conn, group_sql, []) do
        groups =
          Enum.map(rows, fn [full_md5] ->
            {:ok, files} = files_for_full_hash(state.conn, full_md5)
            Enum.map(files, & &1.path)
          end)

        {:ok, groups}
      end

    {:reply, reply, state}
  end

  def handle_call({:search_files, term, limit}, _from, state) do
    sql = """
    SELECT path
    FROM files
    WHERE basename_text LIKE ? OR path_text LIKE ?
    ORDER BY basename_text, path_text
    LIMIT ?
    """

    like_term = "%" <> term <> "%"

    reply =
      with {:ok, rows} <- fetch_rows(state.conn, sql, [like_term, like_term, limit]) do
        {:ok, Enum.map(rows, fn [path] -> path end)}
      end

    {:reply, reply, state}
  end

  defp configure(conn) do
    with :ok <- Sqlite3.execute(conn, "PRAGMA journal_mode=WAL;"),
         :ok <- Sqlite3.execute(conn, "PRAGMA synchronous=NORMAL;"),
         :ok <- Sqlite3.execute(conn, "PRAGMA temp_store=MEMORY;") do
      :ok
    end
  end

  defp create_schema(conn) do
    sql = """
    CREATE TABLE IF NOT EXISTS files (
      id INTEGER PRIMARY KEY,
      path BLOB NOT NULL UNIQUE,
      path_text TEXT,
      basename_text TEXT,
      size_bytes INTEGER NOT NULL,
      mtime_unix INTEGER,
      partial_md5 TEXT,
      full_md5 TEXT
    );

    CREATE INDEX IF NOT EXISTS files_size_bytes_idx ON files (size_bytes);
    CREATE INDEX IF NOT EXISTS files_size_partial_idx ON files (size_bytes, partial_md5);
    CREATE INDEX IF NOT EXISTS files_size_full_idx ON files (size_bytes, full_md5);
    CREATE INDEX IF NOT EXISTS files_basename_text_idx ON files (basename_text);
    CREATE INDEX IF NOT EXISTS files_path_text_idx ON files (path_text);
    """

    Sqlite3.execute(conn, sql)
  end

  defp ensure_search_columns(conn) do
    with {:ok, columns} <- table_columns(conn),
         :ok <-
           maybe_add_column(
             conn,
             columns,
             "path_text",
             "ALTER TABLE files ADD COLUMN path_text TEXT"
           ),
         :ok <-
           maybe_add_column(
             conn,
             columns,
             "basename_text",
             "ALTER TABLE files ADD COLUMN basename_text TEXT"
           ),
         :ok <-
           maybe_add_column(
             conn,
             columns,
             "mtime_unix",
             "ALTER TABLE files ADD COLUMN mtime_unix INTEGER"
           ),
         :ok <-
           Sqlite3.execute(
             conn,
             "CREATE INDEX IF NOT EXISTS files_basename_text_idx ON files (basename_text)"
           ),
         :ok <-
           Sqlite3.execute(
             conn,
             "CREATE INDEX IF NOT EXISTS files_path_text_idx ON files (path_text)"
           ) do
      :ok
    end
  end

  defp backfill_search_columns(conn) do
    sql = """
    SELECT id, path
    FROM files
    WHERE path_text IS NULL OR basename_text IS NULL
    """

    with {:ok, rows} <- fetch_rows(conn, sql, []) do
      rows
      |> Enum.map(fn [id, path] -> {id, path_search_fields(path)} end)
      |> Enum.reject(fn {_id, %{path_text: path_text, basename_text: basename_text}} ->
        is_nil(path_text) and is_nil(basename_text)
      end)
      |> backfill_search_rows(conn)
    end
  end

  defp backfill_search_rows([], _conn), do: :ok

  defp backfill_search_rows(rows, conn) do
    sql = """
    UPDATE files
    SET path_text = ?, basename_text = ?
    WHERE id = ?
    """

    params =
      Enum.map(rows, fn {id, %{path_text: path_text, basename_text: basename_text}} ->
        [path_text, basename_text, id]
      end)

    exec_many(conn, sql, params)
  end

  defp table_columns(conn) do
    with {:ok, rows} <- fetch_rows(conn, "PRAGMA table_info(files)", []) do
      {:ok, MapSet.new(Enum.map(rows, fn [_cid, name | _rest] -> name end))}
    end
  end

  defp maybe_add_column(conn, columns, column_name, sql) do
    if MapSet.member?(columns, column_name) do
      :ok
    else
      Sqlite3.execute(conn, sql)
    end
  end

  defp path_search_fields(path) when is_binary(path) do
    case :unicode.characters_to_binary(path) do
      binary when is_binary(binary) ->
        %{
          path_text: binary,
          basename_text: Path.basename(binary)
        }

      _ ->
        %{
          path_text: nil,
          basename_text: nil
        }
    end
  rescue
    ArgumentError ->
      %{
        path_text: nil,
        basename_text: nil
      }
  end

  defp files_for_size(conn, size) do
    sql = """
    SELECT id, path
    FROM files
    WHERE size_bytes = ?
    ORDER BY path
    """

    with {:ok, rows} <- fetch_rows(conn, sql, [size]) do
      {:ok, Enum.map(rows, &row_to_file/1)}
    end
  end

  defp files_for_partial_hash(conn, size, partial_md5) do
    sql = """
    SELECT id, path
    FROM files
    WHERE size_bytes = ? AND partial_md5 = ?
    ORDER BY path
    """

    with {:ok, rows} <- fetch_rows(conn, sql, [size, partial_md5]) do
      {:ok, Enum.map(rows, &row_to_file/1)}
    end
  end

  defp files_for_full_hash(conn, full_md5) do
    sql = """
    SELECT id, path
    FROM files
    WHERE full_md5 = ?
    ORDER BY path
    """

    with {:ok, rows} <- fetch_rows(conn, sql, [full_md5]) do
      {:ok, Enum.map(rows, &row_to_file/1)}
    end
  end

  defp row_to_file([id, path]), do: %{id: id, path: path}

  defp exec(conn, sql, params) do
    with {:ok, statement} <- Sqlite3.prepare(conn, sql),
         :ok <- Sqlite3.bind(statement, params),
         :done <- Sqlite3.step(conn, statement) do
      Sqlite3.release(conn, statement)
      :ok
    else
      {:error, _reason} = error ->
        error

      other ->
        {:error, other}
    end
  end

  defp exec_many(_conn, _sql, []), do: :ok

  defp exec_many(conn, sql, rows) do
    with :ok <- Sqlite3.execute(conn, "BEGIN IMMEDIATE"),
         {:ok, statement} <- Sqlite3.prepare(conn, sql),
         :ok <- exec_many_rows(conn, statement, rows),
         :ok <- Sqlite3.release(conn, statement),
         :ok <- Sqlite3.execute(conn, "COMMIT") do
      :ok
    else
      {:error, _reason} = error ->
        _ = Sqlite3.execute(conn, "ROLLBACK")
        error

      other ->
        _ = Sqlite3.execute(conn, "ROLLBACK")
        {:error, other}
    end
  end

  defp exec_many_rows(_conn, _statement, []), do: :ok

  defp exec_many_rows(conn, statement, [params | rest]) do
    with :ok <- Sqlite3.bind(statement, params),
         :done <- Sqlite3.step(conn, statement),
         :ok <- Sqlite3.reset(statement) do
      exec_many_rows(conn, statement, rest)
    else
      {:error, _reason} = error -> error
      other -> {:error, other}
    end
  end

  defp fetch_rows(conn, sql, params) do
    with {:ok, statement} <- Sqlite3.prepare(conn, sql),
         :ok <- Sqlite3.bind(statement, params) do
      do_fetch_rows(conn, statement, [])
    end
  end

  defp do_fetch_rows(conn, statement, rows) do
    case Sqlite3.step(conn, statement) do
      {:row, row} ->
        do_fetch_rows(conn, statement, [row | rows])

      :done ->
        :ok = Sqlite3.release(conn, statement)
        {:ok, Enum.reverse(rows)}

      {:error, _reason} = error ->
        error
    end
  end
end
