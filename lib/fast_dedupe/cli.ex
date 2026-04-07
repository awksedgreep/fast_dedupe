defmodule FastDedupe.CLI do
  @moduledoc false

  @default_db_path "fast_dedupe.sqlite3"
  @default_partial_bytes 4 * 1024
  @keep_policies ~w(first newest oldest shortest-path longest-path)a
  @output_formats ~w(text json)a

  def main(argv) do
    requested_output = requested_output_format(argv)

    case parse_args(argv) do
      {:ok, opts, paths} ->
        run_command(opts, paths)

      {:help, help_text} ->
        print_help(help_text, requested_output)
        System.halt(0)

      {:error, message} ->
        print_error(message, requested_output, "invalid_arguments")
        System.halt(1)
    end
  end

  defp parse_args(argv) do
    {opts, paths, invalid} =
      OptionParser.parse(
        argv,
        strict: [
          db_path: :string,
          partial_bytes: :integer,
          keep: :string,
          search: :string,
          limit: :integer,
          output: :string,
          dry_run: :boolean,
          delete: :boolean,
          yes: :boolean,
          help: :boolean
        ],
        aliases: [d: :db_path, p: :partial_bytes, h: :help]
      )

    cond do
      invalid != [] ->
        {:error, "invalid arguments: #{format_invalid(invalid)}"}

      opts[:help] ->
        {:help, help_text()}

      opts[:partial_bytes] && opts[:partial_bytes] <= 0 ->
        {:error, "--partial-bytes must be a positive integer"}

      opts[:limit] && opts[:limit] <= 0 ->
        {:error, "--limit must be a positive integer"}

      opts[:yes] && !opts[:delete] ->
        {:error, "--yes only makes sense together with --delete"}

      opts[:dry_run] && !opts[:delete] ->
        {:error, "--dry-run only makes sense together with --delete"}

      opts[:keep] && parse_keep_policy(opts[:keep]) == :error ->
        {:error, "--keep must be one of: #{Enum.join(@keep_policies, ", ")}"}

      opts[:output] && parse_output_format(opts[:output]) == :error ->
        {:error, "--output must be one of: #{Enum.join(@output_formats, ", ")}"}

      true ->
        {:ok, opts, default_paths(paths, opts)}
    end
  end

  defp run_command(opts, paths) do
    if search_term = opts[:search] do
      run_search(search_term, opts)
    else
      case run_find(paths, opts) do
        {:ok, result, run_opts} ->
          if opts[:delete] do
            run_delete(result, run_opts, opts)
          else
            System.halt(0)
          end

        :error ->
          System.halt(1)
      end
    end
  end

  defp run_search(term, opts) do
    run_opts = [
      db_path: opts[:db_path] || default_search_db_path(),
      output: output_format(opts),
      limit: opts[:limit] || 100
    ]

    case FastDedupe.search(run_opts[:db_path], term, limit: run_opts[:limit]) do
      {:ok, results} ->
        print_search_report(term, run_opts, results)
        System.halt(0)

      {:error, reason} ->
        print_error("search failed: #{inspect(reason)}", run_opts[:output], "search_failed")
        System.halt(1)
    end
  end

  defp run_find(paths, opts) do
    run_opts = [
      db_path: opts[:db_path] || default_scan_db_path(paths),
      partial_bytes: opts[:partial_bytes] || @default_partial_bytes,
      output: output_format(opts)
    ]

    case FastDedupe.run(paths, run_opts) do
      {:ok, result} ->
        print_report(paths, run_opts, result)
        {:ok, result, run_opts}

      {:error, reason} ->
        print_error("scan failed: #{inspect(reason)}", run_opts[:output], "scan_failed")
        :error
    end
  end

  defp run_delete(result, _run_opts, opts) do
    keep_policy = keep_policy(opts)
    output = output_format(opts)
    planned_actions = planned_actions(result.duplicate_groups, keep_policy)
    delete_targets = Enum.map(planned_actions, & &1.delete)

    cond do
      delete_targets == [] ->
        print_delete_empty(output)
        System.halt(0)

      opts[:dry_run] ->
        print_delete_plan(planned_actions, keep_policy, true, output)
        System.halt(0)

      opts[:yes] || confirm_delete?(delete_targets) ->
        if output == :text do
          print_delete_plan(planned_actions, keep_policy, false, output)
        end

        case delete_files(delete_targets) do
          :ok ->
            print_delete_success(planned_actions, keep_policy, output)
            System.halt(0)

          {:error, failures} ->
            print_delete_failure(failures, keep_policy, output)

            System.halt(1)
        end

      true ->
        print_delete_cancelled(output)
        System.halt(1)
    end
  end

  defp print_report(paths, run_opts, result) do
    case run_opts[:output] do
      :json ->
        print_json(%{
          mode: "find",
          paths: Enum.map(paths, &path_json/1),
          db_path: path_json(run_opts[:db_path]),
          partial_bytes: run_opts[:partial_bytes],
          scanned_files: result.scanned_files,
          same_size_groups: result.same_size_groups,
          partial_hash_groups: result.partial_hash_groups,
          confirmed_duplicate_groups:
            Enum.map(result.duplicate_groups, fn group -> Enum.map(group, &path_json/1) end)
        })

      :text ->
        IO.puts("FastDedupe report")
        IO.puts("paths: #{Enum.map_join(paths, ", ", &format_path/1)}")
        IO.puts("db: #{format_path(run_opts[:db_path])}")
        IO.puts("partial bytes: #{run_opts[:partial_bytes]}")
        IO.puts("scanned files: #{result.scanned_files}")
        IO.puts("same-size groups: #{result.same_size_groups}")
        IO.puts("partial-hash groups: #{result.partial_hash_groups}")
        IO.puts("confirmed duplicate groups: #{length(result.duplicate_groups)}")

        case result.duplicate_groups do
          [] ->
            IO.puts("no duplicates found")

          groups ->
            Enum.each(groups, fn group ->
              IO.puts("")
              IO.puts("duplicate group:")
              Enum.each(group, fn path -> IO.puts("  #{format_path(path)}") end)
            end)
        end
    end
  end

  defp print_search_report(term, run_opts, results) do
    case run_opts[:output] do
      :json ->
        print_json(%{
          mode: "search",
          term: term,
          db_path: path_json(run_opts[:db_path]),
          limit: run_opts[:limit],
          results: Enum.map(results, &path_json/1)
        })

      :text ->
        IO.puts("FastDedupe search")
        IO.puts("term: #{inspect(term)}")
        IO.puts("db: #{format_path(run_opts[:db_path])}")
        IO.puts("limit: #{run_opts[:limit]}")
        IO.puts("matches: #{length(results)}")

        case results do
          [] ->
            IO.puts("no matches found")

          _ ->
            Enum.each(results, fn path -> IO.puts("  #{format_path(path)}") end)
        end
    end
  end

  defp planned_actions(groups, keep_policy) do
    groups
    |> Enum.flat_map(fn group ->
      case choose_keep(group, keep_policy) do
        nil ->
          []

        keep ->
          group
          |> Enum.reject(&(&1 == keep))
          |> Enum.map(fn delete -> %{keep: keep, delete: delete} end)
      end
    end)
  end

  defp confirm_delete?(delete_targets) do
    IO.puts("")

    IO.puts(
      "delete mode will remove #{length(delete_targets)} files and keep the first path in each duplicate group"
    )

    case IO.gets("continue? [y/N] ") do
      response when is_binary(response) ->
        String.trim(response) in ["y", "Y", "yes", "YES"]

      _ ->
        false
    end
  end

  defp print_delete_plan(actions, keep_policy, dry_run?, :json) do
    print_json(%{
      mode: "delete",
      dry_run: dry_run?,
      keep_policy: Atom.to_string(keep_policy),
      actions:
        Enum.map(actions, fn %{keep: keep, delete: delete} ->
          %{keep: path_json(keep), delete: path_json(delete)}
        end)
    })
  end

  defp print_delete_plan(actions, keep_policy, dry_run?, :text) do
    verb = if dry_run?, do: "would delete", else: "deleting"
    IO.puts("")
    IO.puts("#{verb} #{length(actions)} files using keep policy #{inspect(keep_policy)}")

    Enum.each(actions, fn %{keep: keep, delete: delete} ->
      IO.puts("  keep   #{format_path(keep)}")
      IO.puts("  delete #{format_path(delete)}")
    end)
  end

  defp print_delete_empty(:json),
    do: print_json(%{mode: "delete", dry_run: false, actions: [], deleted: 0})

  defp print_delete_empty(:text), do: IO.puts("nothing to delete")

  defp print_delete_success(actions, keep_policy, :json) do
    print_json(%{
      mode: "delete",
      dry_run: false,
      keep_policy: Atom.to_string(keep_policy),
      deleted: length(actions),
      actions:
        Enum.map(actions, fn %{keep: keep, delete: delete} ->
          %{keep: path_json(keep), delete: path_json(delete)}
        end)
    })
  end

  defp print_delete_success(actions, keep_policy, :text) do
    _ = keep_policy
    IO.puts("deleted #{length(actions)} duplicate files")
  end

  defp print_delete_failure(failures, keep_policy, :json) do
    print_json(%{
      mode: "delete",
      status: "error",
      error_code: "delete_failed",
      keep_policy: Atom.to_string(keep_policy),
      failures:
        Enum.map(failures, fn {path, reason} ->
          %{path: path_json(path), reason: inspect(reason)}
        end)
    })
  end

  defp print_delete_failure(failures, _keep_policy, :text) do
    Enum.each(failures, fn {path, reason} ->
      IO.puts(:stderr, "failed to delete #{format_path(path)}: #{inspect(reason)}")
    end)
  end

  defp print_delete_cancelled(:json) do
    print_json(%{mode: "delete", status: "cancelled", error_code: "delete_cancelled"})
  end

  defp print_delete_cancelled(:text), do: IO.puts("delete cancelled")

  defp delete_files(paths) do
    failures =
      Enum.reduce(paths, [], fn path, acc ->
        case File.rm(path) do
          :ok -> acc
          {:error, reason} -> [{path, reason} | acc]
        end
      end)

    case failures do
      [] -> :ok
      _ -> {:error, Enum.reverse(failures)}
    end
  end

  defp format_path(path) when is_binary(path) do
    inspect(path, binaries: :as_binaries)
  end

  defp path_json(path) when is_binary(path) do
    %{
      display: format_path(path),
      bytes_b64: Base.encode64(path)
    }
  end

  defp keep_policy(opts) do
    case Keyword.get(opts, :keep) do
      nil -> :first
      value -> parse_keep_policy(value)
    end
  end

  defp parse_keep_policy(value) when is_binary(value) do
    case value do
      "first" -> :first
      "newest" -> :newest
      "oldest" -> :oldest
      "shortest-path" -> :"shortest-path"
      "longest-path" -> :"longest-path"
      _ -> :error
    end
  end

  defp output_format(opts) do
    case Keyword.get(opts, :output) do
      nil -> :text
      value -> parse_output_format(value)
    end
  end

  defp default_paths(paths, opts) do
    if opts[:search] do
      paths
    else
      case paths do
        [] -> ["."]
        _ -> paths
      end
    end
  end

  defp default_scan_db_path([first_path | _rest]) do
    expanded = Path.expand(first_path)

    base_dir =
      cond do
        File.dir?(expanded) -> expanded
        File.regular?(expanded) -> Path.dirname(expanded)
        true -> expanded
      end

    Path.join(base_dir, @default_db_path)
  end

  defp default_search_db_path do
    Path.expand(@default_db_path)
  end

  defp parse_output_format(value) when is_binary(value) do
    case value do
      "text" -> :text
      "json" -> :json
      _ -> :error
    end
  end

  defp choose_keep([], _keep_policy), do: nil

  defp choose_keep(group, :first), do: List.first(group)

  defp choose_keep(group, :newest) do
    Enum.max_by(group, &mtime_or_zero/1)
  end

  defp choose_keep(group, :oldest) do
    Enum.min_by(group, &mtime_or_zero/1)
  end

  defp choose_keep(group, :"shortest-path") do
    Enum.min_by(group, &{byte_size(&1), &1})
  end

  defp choose_keep(group, :"longest-path") do
    Enum.max_by(group, &{byte_size(&1), &1})
  end

  defp mtime_or_zero(path) do
    case File.stat(path, time: :posix) do
      {:ok, stat} -> stat.mtime
      {:error, _reason} -> 0
    end
  end

  defp format_invalid(invalid) do
    invalid
    |> Enum.map(fn {key, value} -> "--#{key}=#{value}" end)
    |> Enum.join(", ")
  end

  defp print_json(data) do
    IO.puts(Jason.encode!(data))
  end

  defp print_error(message, :json, error_code) do
    print_json(%{status: "error", error_code: error_code, message: message})
  end

  defp print_error(message, :text, _error_code) do
    IO.puts(:stderr, message)
  end

  defp print_help(help_text, :json) do
    print_json(%{status: "ok", mode: "help", help: help_text})
  end

  defp print_help(help_text, :text) do
    IO.puts(help_text)
  end

  defp requested_output_format(argv) do
    case Enum.find_index(argv, &(&1 == "--output")) do
      nil ->
        :text

      index ->
        case Enum.at(argv, index + 1) do
          "json" -> :json
          _ -> :text
        end
    end
  end

  defp help_text do
    """
    Usage:
      mix run -e 'FastDedupe.CLI.main(System.argv())' -- [options] PATH...
      mix escript.build && ./fast_dedupe [options] PATH...

    If PATH is omitted, FastDedupe scans the current working directory (`.`).

    Options:
      -d, --db-path PATH          SQLite database path
      -p, --partial-bytes N       Bytes to hash for the partial pass (default: #{@default_partial_bytes})
      --search TERM           Search the existing database for matching basenames or paths
      --limit N               Maximum search results to return (default: 100)
      --keep POLICY           Keep policy: first, newest, oldest, shortest-path, longest-path
          --output FORMAT         Output format: text or json
          --dry-run               Show what delete mode would remove without deleting
          --delete                Delete confirmed duplicates, keeping the first path in each group
          --yes                   Non-interactive delete mode; only valid with --delete
      -h, --help                  Show this help text
    """
  end
end
