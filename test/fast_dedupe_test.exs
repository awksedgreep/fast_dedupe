defmodule FastDedupeTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureIO

  test "finds duplicate files through size, partial hash, and full hash stages" do
    root = Path.join(System.tmp_dir!(), "fast_dedupe_test_#{System.unique_integer([:positive])}")
    db_path = Path.join(root, "dedupe.sqlite3")
    left = Path.join(root, "left")
    right = Path.join(root, "right")
    third = Path.join(root, "third")

    File.mkdir_p!(left)
    File.mkdir_p!(right)
    File.mkdir_p!(third)
    on_exit(fn -> File.rm_rf(root) end)

    duplicate_body = String.duplicate("a", 8_192)
    same_prefix = String.duplicate("a", 4_096)
    variant_body = same_prefix <> String.duplicate("b", 4_096)
    unique_body = String.duplicate("z", 3_000)

    duplicate_one = Path.join(left, "duplicate-one.bin")
    duplicate_two = Path.join(right, "duplicate-two.bin")
    same_prefix_only = Path.join(third, "same-prefix-only.bin")
    unique_file = Path.join(left, "unique.bin")

    File.write!(duplicate_one, duplicate_body)
    File.write!(duplicate_two, duplicate_body)
    File.write!(same_prefix_only, variant_body)
    File.write!(unique_file, unique_body)

    assert {:ok, result} = FastDedupe.run(root, db_path: db_path, partial_bytes: 4_096)

    assert result.scanned_files == 4
    assert result.same_size_groups == 1
    assert result.partial_hash_groups == 1

    assert result.duplicate_groups == [
             Enum.sort([duplicate_one, duplicate_two])
           ]
  end

  test "handles duplicate empty files" do
    root =
      Path.join(System.tmp_dir!(), "fast_dedupe_empty_test_#{System.unique_integer([:positive])}")

    db_path = Path.join(root, "dedupe.sqlite3")
    left = Path.join(root, "left")
    right = Path.join(root, "right")

    File.mkdir_p!(left)
    File.mkdir_p!(right)
    on_exit(fn -> File.rm_rf(root) end)

    empty_one = Path.join(left, "empty-one.txt")
    empty_two = Path.join(right, "empty-two.txt")

    File.write!(empty_one, "")
    File.write!(empty_two, "")

    assert {:ok, result} = FastDedupe.run(root, db_path: db_path, partial_bytes: 4_096)
    assert result.duplicate_groups == [Enum.sort([empty_one, empty_two])]
  end

  test "rerunning against the same database reuses existing rows" do
    root =
      Path.join(
        System.tmp_dir!(),
        "fast_dedupe_resume_test_#{System.unique_integer([:positive])}"
      )

    db_path = Path.join(root, "dedupe.sqlite3")
    left = Path.join(root, "left")
    right = Path.join(root, "right")

    File.mkdir_p!(left)
    File.mkdir_p!(right)
    on_exit(fn -> File.rm_rf(root) end)

    duplicate_one = Path.join(left, "resume-one.txt")
    duplicate_two = Path.join(right, "resume-two.txt")

    File.write!(duplicate_one, "same")
    File.write!(duplicate_two, "same")

    assert {:ok, first_result} = FastDedupe.run(root, db_path: db_path, partial_bytes: 4_096)
    assert {:ok, second_result} = FastDedupe.run(root, db_path: db_path, partial_bytes: 4_096)

    assert first_result.duplicate_groups == [Enum.sort([duplicate_one, duplicate_two])]
    assert second_result.duplicate_groups == [Enum.sort([duplicate_one, duplicate_two])]
  end

  test "rerunning after changing one file updates only the changed candidate" do
    root =
      Path.join(
        System.tmp_dir!(),
        "fast_dedupe_resume_change_test_#{System.unique_integer([:positive])}"
      )

    db_path = Path.join(root, "dedupe.sqlite3")
    left = Path.join(root, "left")
    right = Path.join(root, "right")

    File.mkdir_p!(left)
    File.mkdir_p!(right)
    on_exit(fn -> File.rm_rf(root) end)

    duplicate_one = Path.join(left, "resume-one.txt")
    duplicate_two = Path.join(right, "resume-two.txt")

    File.write!(duplicate_one, "same")
    File.write!(duplicate_two, "same")

    assert {:ok, initial_result} = FastDedupe.run(root, db_path: db_path, partial_bytes: 4_096)
    assert initial_result.duplicate_groups == [Enum.sort([duplicate_one, duplicate_two])]

    File.write!(duplicate_two, "same-but-different")

    assert {:ok, changed_result} = FastDedupe.run(root, db_path: db_path, partial_bytes: 4_096)
    assert changed_result.duplicate_groups == []
  end

  test "cli prints a duplicate report in find mode" do
    root =
      Path.join(System.tmp_dir!(), "fast_dedupe_cli_test_#{System.unique_integer([:positive])}")

    db_path = Path.join(root, "dedupe.sqlite3")
    left = Path.join(root, "left")
    right = Path.join(root, "right")

    File.mkdir_p!(left)
    File.mkdir_p!(right)
    on_exit(fn -> File.rm_rf(root) end)

    duplicate_body = String.duplicate("x", 5_000)
    duplicate_one = Path.join(left, "duplicate-one.bin")
    duplicate_two = Path.join(right, "duplicate-two.bin")

    File.write!(duplicate_one, duplicate_body)
    File.write!(duplicate_two, duplicate_body)

    output =
      capture_io(fn ->
        assert catch_exit(FastDedupe.CLI.main(["--db-path", db_path, root])) == {:shutdown, 0}
      end)

    assert output =~ "FastDedupe report"
    assert output =~ "confirmed duplicate groups: 1"
    assert output =~ inspect(duplicate_one, binaries: :as_binaries)
    assert output =~ inspect(duplicate_two, binaries: :as_binaries)
  end

  test "cli defaults the database path into the scan root" do
    root =
      Path.join(
        System.tmp_dir!(),
        "fast_dedupe_default_db_path_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf(root) end)

    output =
      capture_io(fn ->
        assert catch_exit(FastDedupe.CLI.main([root])) == {:shutdown, 0}
      end)

    assert output =~ inspect(Path.join(root, "fast_dedupe.sqlite3"), binaries: :as_binaries)
    assert File.exists?(Path.join(root, "fast_dedupe.sqlite3"))
  end

  test "cli emits json in find mode" do
    root =
      Path.join(System.tmp_dir!(), "fast_dedupe_json_find_#{System.unique_integer([:positive])}")

    db_path = Path.join(root, "dedupe.sqlite3")
    left = Path.join(root, "left")
    right = Path.join(root, "right")

    File.mkdir_p!(left)
    File.mkdir_p!(right)
    on_exit(fn -> File.rm_rf(root) end)

    duplicate_body = String.duplicate("j", 5_000)
    duplicate_one = Path.join(left, "duplicate-one.bin")
    duplicate_two = Path.join(right, "duplicate-two.bin")

    File.write!(duplicate_one, duplicate_body)
    File.write!(duplicate_two, duplicate_body)

    output =
      capture_io(fn ->
        assert catch_exit(FastDedupe.CLI.main(["--db-path", db_path, "--output", "json", root])) ==
                 {:shutdown, 0}
      end)

    json = Jason.decode!(output)

    assert json["mode"] == "find"
    assert json["scanned_files"] == 2
    assert json["confirmed_duplicate_groups"] |> length() == 1

    [group] = json["confirmed_duplicate_groups"]

    assert Enum.map(group, & &1["bytes_b64"]) |> Enum.sort() ==
             Enum.map([duplicate_one, duplicate_two], &Base.encode64/1) |> Enum.sort()
  end

  test "cli emits json for invalid arguments" do
    output =
      capture_io(:stderr, fn ->
        assert catch_exit(FastDedupe.CLI.main(["--output", "json", "--yes"])) == {:shutdown, 1}
      end)

    json = Jason.decode!(output)
    assert json["status"] == "error"
    assert json["error_code"] == "invalid_arguments"
  end

  test "supports complex utf8 filenames end-to-end" do
    root =
      Path.join(
        System.tmp_dir!(),
        "fast_dedupe_utf8_name_#{System.unique_integer([:positive])}"
      )

    db_path = Path.join(root, "dedupe.sqlite3")
    left = Path.join(root, "left")
    right = Path.join(root, "right")

    File.mkdir_p!(left)
    File.mkdir_p!(right)
    on_exit(fn -> File.rm_rf(root) end)

    duplicate_body = String.duplicate("q", 6_000)
    duplicate_one = Path.join(left, "café-☕-東京-😀.bin")
    duplicate_two = Path.join(right, "mañana-ग्रंथ-🍃.bin")

    File.write!(duplicate_one, duplicate_body)
    File.write!(duplicate_two, duplicate_body)

    assert {:ok, result} = FastDedupe.run(root, db_path: db_path, partial_bytes: 4_096)
    assert [group] = result.duplicate_groups
    assert length(group) == 2

    normalized_group =
      Enum.map(group, &String.normalize(&1, :nfc))
      |> Enum.sort()

    expected_group =
      [duplicate_one, duplicate_two]
      |> Enum.map(&String.normalize(&1, :nfc))
      |> Enum.sort()

    assert normalized_group == expected_group
  end

  test "cli deletes confirmed duplicates with --yes" do
    root =
      Path.join(
        System.tmp_dir!(),
        "fast_dedupe_delete_test_#{System.unique_integer([:positive])}"
      )

    db_path = Path.join(root, "dedupe.sqlite3")
    left = Path.join(root, "left")
    right = Path.join(root, "right")

    File.mkdir_p!(left)
    File.mkdir_p!(right)
    on_exit(fn -> File.rm_rf(root) end)

    duplicate_body = String.duplicate("d", 7_000)
    keep_path = Path.join(left, "duplicate-one.bin")
    delete_path = Path.join(right, "duplicate-two.bin")

    File.write!(keep_path, duplicate_body)
    File.write!(delete_path, duplicate_body)

    output =
      capture_io(fn ->
        assert catch_exit(FastDedupe.CLI.main(["--db-path", db_path, "--delete", "--yes", root])) ==
                 {:shutdown, 0}
      end)

    assert output =~ "deleted 1 duplicate files"
    assert File.exists?(keep_path)
    refute File.exists?(delete_path)
  end

  test "cli dry-run prints delete plan without deleting files" do
    root =
      Path.join(
        System.tmp_dir!(),
        "fast_dedupe_dry_run_test_#{System.unique_integer([:positive])}"
      )

    db_path = Path.join(root, "dedupe.sqlite3")
    left = Path.join(root, "left")
    right = Path.join(root, "right")

    File.mkdir_p!(left)
    File.mkdir_p!(right)
    on_exit(fn -> File.rm_rf(root) end)

    duplicate_body = String.duplicate("r", 7_000)
    keep_path = Path.join(left, "duplicate-one.bin")
    delete_path = Path.join(right, "duplicate-two.bin")

    File.write!(keep_path, duplicate_body)
    File.write!(delete_path, duplicate_body)

    output =
      capture_io(fn ->
        assert catch_exit(
                 FastDedupe.CLI.main(["--db-path", db_path, "--delete", "--dry-run", root])
               ) == {:shutdown, 0}
      end)

    assert output =~ "would delete 1 files"
    assert output =~ inspect(keep_path, binaries: :as_binaries)
    assert output =~ inspect(delete_path, binaries: :as_binaries)
    assert File.exists?(keep_path)
    assert File.exists?(delete_path)
  end

  test "cli dry-run emits json delete plan" do
    root =
      Path.join(
        System.tmp_dir!(),
        "fast_dedupe_json_delete_#{System.unique_integer([:positive])}"
      )

    db_path = Path.join(root, "dedupe.sqlite3")
    left = Path.join(root, "left")
    right = Path.join(root, "right")

    File.mkdir_p!(left)
    File.mkdir_p!(right)
    on_exit(fn -> File.rm_rf(root) end)

    duplicate_body = String.duplicate("s", 7_000)
    keep_path = Path.join(left, "duplicate-one.bin")
    delete_path = Path.join(right, "duplicate-two.bin")

    File.write!(keep_path, duplicate_body)
    File.write!(delete_path, duplicate_body)

    output =
      capture_io(fn ->
        assert catch_exit(
                 FastDedupe.CLI.main([
                   "--db-path",
                   db_path,
                   "--delete",
                   "--dry-run",
                   "--output",
                   "json",
                   root
                 ])
               ) == {:shutdown, 0}
      end)

    json = Jason.decode!(output)

    assert json["mode"] == "delete"
    assert json["dry_run"] == true
    assert json["keep_policy"] == "first"
    assert length(json["actions"]) == 1

    [action] = json["actions"]
    assert action["keep"]["bytes_b64"] == Base.encode64(keep_path)
    assert action["delete"]["bytes_b64"] == Base.encode64(delete_path)
    assert File.exists?(keep_path)
    assert File.exists?(delete_path)
  end

  test "cli emits json when delete is cancelled" do
    root =
      Path.join(
        System.tmp_dir!(),
        "fast_dedupe_json_cancel_#{System.unique_integer([:positive])}"
      )

    db_path = Path.join(root, "dedupe.sqlite3")
    left = Path.join(root, "left")
    right = Path.join(root, "right")

    File.mkdir_p!(left)
    File.mkdir_p!(right)
    on_exit(fn -> File.rm_rf(root) end)

    duplicate_body = String.duplicate("c", 7_000)
    keep_path = Path.join(left, "duplicate-one.bin")
    delete_path = Path.join(right, "duplicate-two.bin")

    File.write!(keep_path, duplicate_body)
    File.write!(delete_path, duplicate_body)

    output =
      capture_io([input: "n\n"], fn ->
        assert catch_exit(
                 FastDedupe.CLI.main([
                   "--db-path",
                   db_path,
                   "--delete",
                   "--output",
                   "json",
                   root
                 ])
               ) == {:shutdown, 1}
      end)

    json = Jason.decode!(output)
    assert json["status"] == "cancelled"
    assert json["error_code"] == "delete_cancelled"
    assert File.exists?(keep_path)
    assert File.exists?(delete_path)
  end

  test "cli searches an existing database in text mode" do
    root =
      Path.join(
        System.tmp_dir!(),
        "fast_dedupe_search_text_#{System.unique_integer([:positive])}"
      )

    db_path = Path.join(root, "dedupe.sqlite3")
    left = Path.join(root, "left")
    right = Path.join(root, "right")

    File.mkdir_p!(left)
    File.mkdir_p!(right)
    on_exit(fn -> File.rm_rf(root) end)

    match_path = Path.join(left, "invoice-2024-report.txt")
    other_path = Path.join(right, "notes.txt")

    File.write!(match_path, "alpha")
    File.write!(other_path, "beta")

    assert {:ok, _result} = FastDedupe.run(root, db_path: db_path)

    output =
      capture_io(fn ->
        assert catch_exit(FastDedupe.CLI.main(["--db-path", db_path, "--search", "invoice"])) ==
                 {:shutdown, 0}
      end)

    assert output =~ "FastDedupe search"
    assert output =~ inspect(match_path, binaries: :as_binaries)
    refute output =~ inspect(other_path, binaries: :as_binaries)
  end

  test "cli searches an existing database in json mode" do
    root =
      Path.join(
        System.tmp_dir!(),
        "fast_dedupe_search_json_#{System.unique_integer([:positive])}"
      )

    db_path = Path.join(root, "dedupe.sqlite3")
    left = Path.join(root, "left")
    right = Path.join(root, "right")

    File.mkdir_p!(left)
    File.mkdir_p!(right)
    on_exit(fn -> File.rm_rf(root) end)

    match_path = Path.join(left, "holiday-photo.jpg")
    other_path = Path.join(right, "work-document.pdf")

    File.write!(match_path, "aaa")
    File.write!(other_path, "bbb")

    assert {:ok, _result} = FastDedupe.run(root, db_path: db_path)

    output =
      capture_io(fn ->
        assert catch_exit(
                 FastDedupe.CLI.main([
                   "--db-path",
                   db_path,
                   "--search",
                   "holiday",
                   "--output",
                   "json"
                 ])
               ) == {:shutdown, 0}
      end)

    json = Jason.decode!(output)
    assert json["mode"] == "search"
    assert json["term"] == "holiday"
    assert json["limit"] == 100
    assert Enum.map(json["results"], & &1["bytes_b64"]) == [Base.encode64(match_path)]
  end

  test "cli keep policy newest preserves the newest duplicate" do
    root =
      Path.join(
        System.tmp_dir!(),
        "fast_dedupe_keep_policy_test_#{System.unique_integer([:positive])}"
      )

    db_path = Path.join(root, "dedupe.sqlite3")
    left = Path.join(root, "left")
    right = Path.join(root, "right")

    File.mkdir_p!(left)
    File.mkdir_p!(right)
    on_exit(fn -> File.rm_rf(root) end)

    duplicate_body = String.duplicate("n", 7_000)
    older_path = Path.join(left, "older.bin")
    newer_path = Path.join(right, "newer.bin")

    File.write!(older_path, duplicate_body)
    File.write!(newer_path, duplicate_body)
    File.touch!(older_path, {{2020, 1, 1}, {0, 0, 0}})
    File.touch!(newer_path, {{2024, 1, 1}, {0, 0, 0}})

    output =
      capture_io(fn ->
        assert catch_exit(
                 FastDedupe.CLI.main([
                   "--db-path",
                   db_path,
                   "--delete",
                   "--yes",
                   "--keep",
                   "newest",
                   root
                 ])
               ) == {:shutdown, 0}
      end)

    assert output =~ "using keep policy :newest"
    refute File.exists?(older_path)
    assert File.exists?(newer_path)
  end
end
