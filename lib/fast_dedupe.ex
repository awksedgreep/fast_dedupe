defmodule FastDedupe do
  @moduledoc """
  Fast duplicate-file candidate detection backed by SQLite.

  The pipeline is intentionally staged:

  1. Walk directories concurrently and store every file path and byte size.
  2. Compute a partial MD5 for files that collide on size.
  3. Compute a full MD5 for files that still collide on size and partial MD5.
  """

  alias FastDedupe.SQLiteWriter

  @default_partial_bytes 4 * 1024
  @hash_chunk_bytes 1_048_576
  @db_batch_size 500

  @type run_result :: %{
          scanned_files: non_neg_integer(),
          same_size_groups: non_neg_integer(),
          partial_hash_groups: non_neg_integer(),
          duplicate_groups: [[binary()]]
        }

  @type search_result :: [binary()]

  @spec run(Path.t() | [Path.t()], keyword()) :: {:ok, run_result()} | {:error, term()}
  def run(paths, opts \\ []) do
    paths = List.wrap(paths)
    partial_bytes = Keyword.get(opts, :partial_bytes, @default_partial_bytes)
    db_path = Keyword.get(opts, :db_path, "fast_dedupe.sqlite3")
    ignore_paths = ignored_paths(db_path)
    progress_fun = Keyword.get(opts, :progress_fun, fn _event -> :ok end)

    emit_progress(progress_fun, {:starting, %{paths: paths, db_path: db_path}})

    with {:ok, writer} <- SQLiteWriter.start_link(db_path),
         {:ok, scanned_files} <- scan(paths, writer, ignore_paths),
         {:ok, same_size_groups} <- persist_partial_hashes(writer, partial_bytes, progress_fun),
         {:ok, partial_hash_groups} <- persist_full_hashes(writer, progress_fun),
         {:ok, duplicate_groups} <- SQLiteWriter.duplicate_groups(writer),
         {:ok, confirmed_duplicate_groups} <- confirm_duplicate_groups(duplicate_groups) do
      :ok = SQLiteWriter.stop(writer)
      emit_progress(progress_fun, {:finished, %{scanned_files: scanned_files}})

      {:ok,
       %{
         scanned_files: scanned_files,
         same_size_groups: length(same_size_groups),
         partial_hash_groups: length(partial_hash_groups),
         duplicate_groups: confirmed_duplicate_groups
       }}
    else
      {:error, _reason} = error ->
        error
    end
  end

  @spec search(Path.t(), binary(), keyword()) :: {:ok, search_result()} | {:error, term()}
  def search(db_path, term, opts \\ []) when is_binary(term) do
    limit = Keyword.get(opts, :limit, 100)

    with {:ok, writer} <- SQLiteWriter.start_link(db_path),
         {:ok, results} <- SQLiteWriter.search_files(writer, term, limit) do
      :ok = SQLiteWriter.stop(writer)
      {:ok, results}
    else
      {:error, _reason} = error ->
        error
    end
  end

  defp scan(paths, writer, ignore_paths) do
    paths
    |> Task.async_stream(
      fn path -> scan_directory(path, writer, ignore_paths) end,
      ordered: false,
      timeout: :infinity
    )
    |> Enum.reduce_while({:ok, 0}, fn
      {:ok, {:ok, count}}, {:ok, acc} -> {:cont, {:ok, acc + count}}
      {:ok, {:error, _reason} = error}, _acc -> {:halt, error}
      {:exit, reason}, _acc -> {:halt, {:error, reason}}
    end)
  end

  defp scan_directory(path, writer, ignore_paths) do
    expanded_path = Path.expand(path)

    cond do
      MapSet.member?(ignore_paths, expanded_path) ->
        {:ok, 0}

      File.regular?(expanded_path) ->
        insert_files_batch([{expanded_path, writer}])

      File.dir?(expanded_path) ->
        with {:ok, entries} <- File.ls(expanded_path) do
          {files, directories} =
            entries
            |> Enum.map(&Path.join(expanded_path, &1))
            |> Enum.reject(&MapSet.member?(ignore_paths, Path.expand(&1)))
            |> Enum.split_with(&File.regular?/1)

          with {:ok, file_count} <- insert_files_batch(Enum.map(files, &{&1, writer})),
               {:ok, dir_count} <- scan_subdirectories(directories, writer, ignore_paths) do
            {:ok, file_count + dir_count}
          end
        end

      true ->
        {:ok, 0}
    end
  end

  defp ignored_paths(db_path) do
    expanded = Path.expand(db_path)

    [expanded, expanded <> "-wal", expanded <> "-shm"]
    |> MapSet.new()
  end

  defp scan_subdirectories(directories, writer, ignore_paths) do
    directories
    |> Task.async_stream(
      fn entry -> scan_directory(entry, writer, ignore_paths) end,
      ordered: false,
      timeout: :infinity
    )
    |> Enum.reduce_while({:ok, 0}, fn
      {:ok, {:ok, count}}, {:ok, acc} -> {:cont, {:ok, acc + count}}
      {:ok, {:error, _reason} = error}, _acc -> {:halt, error}
      {:exit, reason}, _acc -> {:halt, {:error, reason}}
    end)
  end

  defp insert_files_batch([]), do: {:ok, 0}

  defp insert_files_batch(files_with_writer) do
    writer = files_with_writer |> List.first() |> elem(1)

    files_with_writer
    |> Enum.chunk_every(@db_batch_size)
    |> Enum.reduce_while({:ok, 0}, fn chunk, {:ok, acc} ->
      case stat_and_insert_chunk(writer, chunk) do
        {:ok, count} -> {:cont, {:ok, acc + count}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp stat_and_insert_chunk(writer, files_with_writer) do
    with {:ok, rows} <- stat_rows(files_with_writer),
         :ok <- SQLiteWriter.insert_files(writer, rows) do
      {:ok, length(rows)}
    end
  end

  defp stat_rows(files_with_writer) do
    Enum.reduce_while(files_with_writer, {:ok, []}, fn {path, _writer}, {:ok, acc} ->
      case File.stat(path, time: :posix) do
        {:ok, stat} -> {:cont, {:ok, [{path, stat.size, stat.mtime} | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, rows} -> {:ok, Enum.reverse(rows)}
      {:error, _reason} = error -> error
    end
  end

  defp persist_partial_hashes(writer, partial_bytes, progress_fun) do
    with {:ok, groups} <- SQLiteWriter.size_collision_groups(writer) do
      emit_progress(progress_fun, {:partial_phase_started, %{groups: length(groups)}})

      Enum.reduce_while(Enum.with_index(groups, 1), {:ok, groups}, fn {{size, files}, index},
                                                                      {:ok, _groups} = acc ->
        emit_progress(
          progress_fun,
          {:partial_group_started,
           %{index: index, total: length(groups), size: size, files: length(files)}}
        )

        case update_group_hashes(
               writer,
               files,
               partial_bytes,
               :partial,
               progress_fun,
               index,
               length(groups)
             ) do
          :ok -> {:cont, acc}
          {:error, _reason} = error -> {:halt, error}
        end
      end)
    end
  end

  defp persist_full_hashes(writer, progress_fun) do
    with {:ok, groups} <- SQLiteWriter.partial_collision_groups(writer) do
      emit_progress(progress_fun, {:full_phase_started, %{groups: length(groups)}})

      Enum.reduce_while(Enum.with_index(groups, 1), {:ok, groups}, fn {{size, partial_hash,
                                                                        files}, index},
                                                                      {:ok, _groups} = acc ->
        emit_progress(
          progress_fun,
          {:full_group_started,
           %{
             index: index,
             total: length(groups),
             size: size,
             partial_hash: partial_hash,
             files: length(files)
           }}
        )

        case update_group_hashes(writer, files, :all, :full, progress_fun, index, length(groups)) do
          :ok -> {:cont, acc}
          {:error, _reason} = error -> {:halt, error}
        end
      end)
    end
  end

  defp update_group_hashes(
         writer,
         files,
         bytes_to_read,
         hash_kind,
         progress_fun,
         group_index,
         group_total
       ) do
    files
    |> Enum.chunk_every(@db_batch_size)
    |> Enum.with_index(1)
    |> Enum.reduce_while(:ok, fn {chunk, batch_index}, :ok ->
      emit_progress(
        progress_fun,
        {:hash_batch_started,
         %{
           kind: hash_kind,
           group_index: group_index,
           group_total: group_total,
           batch_index: batch_index,
           batch_files: length(chunk)
         }}
      )

      with {:ok, rows} <- hash_rows(chunk, bytes_to_read),
           :ok <- persist_hash_batch(writer, rows, hash_kind) do
        {:cont, :ok}
      else
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp persist_hash_batch(writer, rows, :partial) do
    SQLiteWriter.update_partial_hashes(writer, rows)
  end

  defp persist_hash_batch(writer, rows, :full) do
    SQLiteWriter.update_full_hashes(writer, rows)
  end

  defp hash_rows(files, bytes_to_read) do
    Enum.reduce_while(files, {:ok, []}, fn %{id: id, path: path}, {:ok, acc} ->
      case hash_file(path, bytes_to_read) do
        {:ok, hash} -> {:cont, {:ok, [{id, hash} | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, rows} -> {:ok, Enum.reverse(rows)}
      {:error, _reason} = error -> error
    end
  end

  defp hash_file(path, :all) do
    case File.open(path, [:read, :binary]) do
      {:ok, io_device} ->
        try do
          stream_md5(io_device)
        after
          File.close(io_device)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp hash_file(path, bytes) when is_integer(bytes) and bytes > 0 do
    case File.open(path, [:read, :binary]) do
      {:ok, io_device} ->
        try do
          case IO.binread(io_device, bytes) do
            :eof ->
              {:ok, md5(<<>>)}

            data when is_binary(data) ->
              {:ok, md5(data)}

            {:error, reason} ->
              {:error, reason}
          end
        after
          File.close(io_device)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp stream_md5(io_device) do
    io_device
    |> do_stream_md5(:crypto.hash_init(:md5))
    |> case do
      {:ok, context} ->
        {:ok, context |> :crypto.hash_final() |> Base.encode16(case: :lower)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp md5(data) when is_binary(data) do
    :crypto.hash(:md5, data)
    |> Base.encode16(case: :lower)
  end

  defp emit_progress(progress_fun, event) when is_function(progress_fun, 1) do
    progress_fun.(event)
  end

  defp confirm_duplicate_groups(groups) do
    groups
    |> Enum.reduce_while({:ok, []}, fn group, {:ok, acc} ->
      case split_confirmed_group(group) do
        {:ok, confirmed} -> {:cont, {:ok, confirmed ++ acc}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, groups} -> {:ok, Enum.reverse(groups)}
      {:error, _reason} = error -> error
    end
  end

  defp split_confirmed_group(paths) when length(paths) < 2, do: {:ok, []}

  defp split_confirmed_group([path | rest]) do
    case Enum.reduce_while(rest, {:ok, {[path], []}}, fn candidate, {:ok, {same, different}} ->
           case same_file_contents?(path, candidate) do
             {:ok, true} -> {:cont, {:ok, {[candidate | same], different}}}
             {:ok, false} -> {:cont, {:ok, {same, [candidate | different]}}}
             {:error, _reason} = error -> {:halt, error}
           end
         end) do
      {:error, _reason} = error ->
        error

      {:ok, {same, different}} ->
        with {:ok, remaining_groups} <- split_remaining_groups(different) do
          confirmed =
            case same do
              [_single] -> remaining_groups
              multiple -> [Enum.sort(multiple) | remaining_groups]
            end

          {:ok, confirmed}
        end
    end
  end

  defp split_remaining_groups([]), do: {:ok, []}
  defp split_remaining_groups(paths), do: split_confirmed_group(paths)

  defp same_file_contents?(left_path, right_path) do
    case File.open(left_path, [:read, :binary]) do
      {:ok, left} ->
        try do
          case File.open(right_path, [:read, :binary]) do
            {:ok, right} ->
              try do
                compare_streams(left, right)
              after
                File.close(right)
              end

            {:error, reason} ->
              {:error, reason}
          end
        after
          File.close(left)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp compare_streams(left, right) do
    case {IO.binread(left, @hash_chunk_bytes), IO.binread(right, @hash_chunk_bytes)} do
      {:eof, :eof} ->
        {:ok, true}

      {left_chunk, right_chunk} when is_binary(left_chunk) and is_binary(right_chunk) ->
        if left_chunk == right_chunk do
          compare_streams(left, right)
        else
          {:ok, false}
        end

      {{:error, reason}, _} ->
        {:error, reason}

      {_, {:error, reason}} ->
        {:error, reason}

      _ ->
        {:ok, false}
    end
  end

  defp do_stream_md5(io_device, context) do
    case IO.binread(io_device, @hash_chunk_bytes) do
      data when is_binary(data) and byte_size(data) > 0 ->
        do_stream_md5(io_device, :crypto.hash_update(context, data))

      :eof ->
        {:ok, context}

      {:error, reason} ->
        {:error, reason}
    end
  end
end
