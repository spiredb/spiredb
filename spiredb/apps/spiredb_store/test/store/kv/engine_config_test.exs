defmodule Store.KV.EngineConfigTest do
  use ExUnit.Case, async: true

  describe "RocksDB configuration defaults" do
    test "compression defaults to lz4" do
      default = Application.get_env(:spiredb_store, :rocksdb_compression, :lz4)
      assert default == :lz4
    end

    test "bottommost compression defaults to zstd" do
      default = Application.get_env(:spiredb_store, :rocksdb_bottommost_compression, :zstd)
      assert default == :zstd
    end

    test "block cache size is configurable" do
      default = Application.get_env(:spiredb_store, :rocksdb_block_cache_size, 512 * 1024 * 1024)
      assert is_integer(default)
      assert default > 0
    end

    test "bloom bits per key defaults to 10" do
      default = Application.get_env(:spiredb_store, :rocksdb_bloom_bits_per_key, 10)
      assert default == 10
    end

    test "write buffer size is configurable" do
      default = Application.get_env(:spiredb_store, :rocksdb_write_buffer_size, 128 * 1024 * 1024)
      assert is_integer(default)
      assert default > 0
    end

    test "max write buffer number defaults to 4" do
      default = Application.get_env(:spiredb_store, :rocksdb_max_write_buffer_number, 4)
      assert default == 4
    end

    test "max background jobs is configurable" do
      default = Application.get_env(:spiredb_store, :rocksdb_max_background_jobs, 4)
      assert is_integer(default)
      assert default > 0
    end

    test "rate limiter defaults to 100MB/s" do
      default =
        Application.get_env(:spiredb_store, :rocksdb_rate_limit_bytes_per_sec, 100 * 1024 * 1024)

      assert default == 100 * 1024 * 1024
    end

    test "max total wal size is configurable" do
      default =
        Application.get_env(:spiredb_store, :rocksdb_max_total_wal_size, 512 * 1024 * 1024)

      assert is_integer(default)
      assert default > 0
    end

    test "recycle log file num defaults to 4" do
      default = Application.get_env(:spiredb_store, :rocksdb_recycle_log_file_num, 4)
      assert default == 4
    end

    test "wal bytes per sync is configurable" do
      default = Application.get_env(:spiredb_store, :rocksdb_wal_bytes_per_sync, 512 * 1024)
      assert is_integer(default)
      assert default > 0
    end
  end

  describe "configuration override" do
    test "compression can be overridden" do
      # Store original
      original = Application.get_env(:spiredb_store, :rocksdb_compression)

      # Override
      Application.put_env(:spiredb_store, :rocksdb_compression, :snappy)
      assert Application.get_env(:spiredb_store, :rocksdb_compression) == :snappy

      # Restore
      if original do
        Application.put_env(:spiredb_store, :rocksdb_compression, original)
      else
        Application.delete_env(:spiredb_store, :rocksdb_compression)
      end
    end
  end
end
