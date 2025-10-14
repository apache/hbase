# frozen_string_literal: true

#
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

require 'hbase_shell'
require 'stringio'
require 'hbase_constants'
require 'hbase/hbase'
require 'hbase/table'
require 'tempfile'
require 'fileutils'

java_import org.apache.hadoop.conf.Configuration
java_import org.apache.hadoop.fs.FSDataInputStream
java_import org.apache.hadoop.hbase.CellUtil
java_import org.apache.hadoop.hbase.HConstants
java_import org.apache.hadoop.hbase.TableName
java_import org.apache.hadoop.hbase.client.Get
java_import org.apache.hadoop.hbase.client.Scan
java_import org.apache.hadoop.hbase.io.crypto.Encryption
java_import org.apache.hadoop.hbase.io.crypto.KeyStoreKeyProvider
java_import org.apache.hadoop.hbase.io.crypto.ManagedKeyProvider
java_import org.apache.hadoop.hbase.io.crypto.ManagedKeyStoreKeyProvider
java_import org.apache.hadoop.hbase.io.hfile.FixedFileTrailer
java_import org.apache.hadoop.hbase.io.hfile.HFile
java_import org.apache.hadoop.hbase.io.hfile.CacheConfig
java_import org.apache.hadoop.hbase.util.Bytes
java_import org.apache.hadoop.hbase.keymeta.KeymetaServiceEndpoint
java_import org.apache.hadoop.hbase.keymeta.KeymetaTableAccessor
java_import org.apache.hadoop.hbase.security.EncryptionUtil
java_import java.security.KeyStore
java_import java.security.MessageDigest
java_import javax.crypto.spec.SecretKeySpec
java_import java.io.FileOutputStream
java_import java.net.URLEncoder
java_import java.util.Base64

module Hbase
  # Test class for key provider migration functionality
  class KeyProviderKeymetaMigrationTest < Test::Unit::TestCase
    include TestHelpers

    def setup
      @test_timestamp = Time.now.to_i.to_s
      @master_key_alias = 'masterkey'
      @shared_key_alias = 'sharedkey'
      @table_key_alias = 'tablelevelkey'
      @cf_key1_alias = 'cfkey1'
      @cf_key2_alias = 'cfkey2'
      @keystore_password = 'password'

      # Test table names
      @table_no_encryption = "no_enc_#{@test_timestamp}"
      @table_random_key = "random_key_#{@test_timestamp}"
      @table_table_key = "table_key_#{@test_timestamp}"
      @table_shared_key1 = "shared1_#{@test_timestamp}"
      @table_shared_key2 = "shared2_#{@test_timestamp}"
      @table_cf_keys = "cf_keys_#{@test_timestamp}"

      # Unified table metadata with CFs and expected namespaces
      @tables_metadata = {
        @table_no_encryption => {
          cfs: ['f'],
          expected_namespace: { 'f' => nil },
          no_encryption: true
        },
        @table_random_key => {
          cfs: ['f'],
          expected_namespace: { 'f' => nil }
        },
        @table_table_key => {
          cfs: ['f'],
          expected_namespace: { 'f' => @table_table_key }
        },
        @table_shared_key1 => {
          cfs: ['f'],
          expected_namespace: { 'f' => 'shared-global-key' }
        },
        @table_shared_key2 => {
          cfs: ['f'],
          expected_namespace: { 'f' => 'shared-global-key' }
        },
        @table_cf_keys => {
          cfs: ['cf1', 'cf2'],
          expected_namespace: {
            'cf1' => "#{@table_cf_keys}/cf1",
            'cf2' => "#{@table_cf_keys}/cf2"
          }
        }
      }


      # Setup initial KeyStoreKeyProvider
      setup_old_key_provider
      puts "  >> Starting Cluster"
      $TEST.startMiniCluster()
      puts "  >> Cluster started"

      setup_hbase
    end

    define_test 'Test complete key provider migration' do
      puts "\n=== Starting Key Provider Migration Test ==="

      # Step 1-3: Setup old provider and create tables
      create_test_tables
      puts "\n--- Validating initial table operations ---"
      validate_pre_migration_operations(false)

      # Step 4: Setup new provider and restart
      setup_new_key_provider
      restart_cluster_and_validate

      # Step 5: Perform migration
      migrate_tables_step_by_step

      # Step 6: Cleanup and final validation
      cleanup_old_provider_and_validate

      puts "\n=== Migration Test Completed Successfully ==="
    end

    private

    def setup_old_key_provider
      puts "\n--- Setting up old KeyStoreKeyProvider ---"

      # Use proper test directory (similar to KeymetaTestUtils.setupTestKeyStore)
      test_data_dir = $TEST_CLUSTER.getDataTestDir("old_keystore_#{@test_timestamp}").toString
      FileUtils.mkdir_p(test_data_dir)
      @old_keystore_file = File.join(test_data_dir, 'keystore.jceks')
      puts "  >> Old keystore file: #{@old_keystore_file}"

      # Create keystore with only the master key
      # ENCRYPTION_KEY attributes generate their own keys and don't use keystore entries
      create_keystore(@old_keystore_file, {
        @master_key_alias => generate_key(@master_key_alias)
      })

      # Configure old KeyStoreKeyProvider
      provider_uri = "jceks://#{File.expand_path(@old_keystore_file)}?password=#{@keystore_password}"
      $TEST_CLUSTER.getConfiguration.set(HConstants::CRYPTO_KEYPROVIDER_CONF_KEY,
                                        KeyStoreKeyProvider.java_class.name)
      $TEST_CLUSTER.getConfiguration.set(HConstants::CRYPTO_KEYPROVIDER_PARAMETERS_KEY, provider_uri)
      $TEST_CLUSTER.getConfiguration.set(HConstants::CRYPTO_MASTERKEY_NAME_CONF_KEY, @master_key_alias)

      puts "  >> Old KeyStoreKeyProvider configured with keystore: #{@old_keystore_file}"
    end

    def create_test_tables
      puts "\n--- Creating test tables ---"

      # 1. Table without encryption
      command(:create, @table_no_encryption, { 'NAME' => 'f' })
      puts "  >> Created table #{@table_no_encryption} without encryption"

      # 2. Table with random key (no explicit key set)
      command(:create, @table_random_key, { 'NAME' => 'f', 'ENCRYPTION' => 'AES' })
      puts "  >> Created table #{@table_random_key} with random key"

      # 3. Table with table-level key
      command(:create, @table_table_key, { 'NAME' => 'f', 'ENCRYPTION' => 'AES',
                                          'ENCRYPTION_KEY' => @table_key_alias })
      puts "  >> Created table #{@table_table_key} with table-level key"

      # 4. First table with shared key
      command(:create, @table_shared_key1, { 'NAME' => 'f', 'ENCRYPTION' => 'AES',
                                            'ENCRYPTION_KEY' => @shared_key_alias })
      puts "  >> Created table #{@table_shared_key1} with shared key"

      # 5. Second table with shared key
      command(:create, @table_shared_key2, { 'NAME' => 'f', 'ENCRYPTION' => 'AES',
                                            'ENCRYPTION_KEY' => @shared_key_alias })
      puts "  >> Created table #{@table_shared_key2} with shared key"

      # 6. Table with column family specific keys
      command(:create, @table_cf_keys,
              { 'NAME' => 'cf1', 'ENCRYPTION' => 'AES', 'ENCRYPTION_KEY' => @cf_key1_alias },
              { 'NAME' => 'cf2', 'ENCRYPTION' => 'AES', 'ENCRYPTION_KEY' => @cf_key2_alias })
      puts "  >> Created table #{@table_cf_keys} with CF-specific keys"
    end

    def validate_pre_migration_operations(is_key_management_enabled)
      @tables_metadata.each do |table_name, metadata|
        puts "  >> test_table_operations on table: #{table_name} with CFs: #{metadata[:cfs].join(', ')}"
        if metadata[:no_encryption]
          next
        end
        test_table_operations(table_name, metadata[:cfs])
        check_hfile_trailers_pre_migration(table_name, metadata[:cfs], is_key_management_enabled)
      end
    end

    def test_table_operations(table_name, column_families)
      puts "    >> Testing operations on table #{table_name}"

      test_table = table(table_name)

      column_families.each do |cf|
        puts "    >> Running put operations on CF: #{cf} in table: #{table_name}"
        # Put data
        test_table.put('row1', "#{cf}:col1", 'value1')
        test_table.put('row2', "#{cf}:col2", 'value2')
      end

      # Flush table
      puts "    >> Flushing table: #{table_name}"
      $TEST_CLUSTER.flush(TableName.valueOf(table_name))

      # Get data and validate
      column_families.each do |cf|
        puts "    >> Validating data in CF: #{cf} in table: #{table_name}"
        get_result = test_table.table.get(Get.new(Bytes.toBytes('row1')))
        assert_false(get_result.isEmpty)
        assert_equal('value1',
                    Bytes.toString(get_result.getValue(Bytes.toBytes(cf), Bytes.toBytes('col1'))))
      end

      puts "    >> Operations validated for #{table_name}"
    end

    def setup_new_key_provider
      puts "\n--- Setting up new ManagedKeyStoreKeyProvider ---"

      # Use proper test directory (similar to KeymetaTestUtils.setupTestKeyStore)
      test_data_dir = $TEST_CLUSTER.getDataTestDir("new_keystore_#{@test_timestamp}").toString
      FileUtils.mkdir_p(test_data_dir)
      @new_keystore_file = File.join(test_data_dir, 'managed_keystore.jceks')
      puts "  >> New keystore file: #{@new_keystore_file}"

      # Extract wrapped keys from encrypted tables and unwrap them
      migrated_keys = extract_and_unwrap_keys_from_tables

      # Create new keystore with migrated keys
      create_keystore(@new_keystore_file, migrated_keys)

      # Configure ManagedKeyStoreKeyProvider
      provider_uri = "jceks://#{File.expand_path(@new_keystore_file)}?password=#{@keystore_password}"
      $TEST_CLUSTER.getConfiguration.set(HConstants::CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, 'true')
      $TEST_CLUSTER.getConfiguration.set(HConstants::CRYPTO_MANAGED_KEYPROVIDER_CONF_KEY,
                                        ManagedKeyStoreKeyProvider.java_class.name)
      $TEST_CLUSTER.getConfiguration.set(HConstants::CRYPTO_MANAGED_KEYPROVIDER_PARAMETERS_KEY, provider_uri)
      $TEST_CLUSTER.getConfiguration.set(HConstants::CRYPTO_MANAGED_KEY_STORE_SYSTEM_KEY_NAME_CONF_KEY,
                                        'system_key')

      # Setup key configurations for ManagedKeyStoreKeyProvider
      # Shared key configuration
      $TEST_CLUSTER.getConfiguration.set(
        "hbase.crypto.managed_key_store.cust.#{$GLOB_CUST_ENCODED}.shared-global-key.alias",
        'shared_global_key')
      $TEST_CLUSTER.getConfiguration.setBoolean(
        "hbase.crypto.managed_key_store.cust.#{$GLOB_CUST_ENCODED}.shared-global-key.active", true)

      # Table-level key configuration - let system determine namespace automatically
      $TEST_CLUSTER.getConfiguration.set(
        "hbase.crypto.managed_key_store.cust.#{$GLOB_CUST_ENCODED}.#{@table_table_key}.alias",
        "#{@table_table_key}_key")
      $TEST_CLUSTER.getConfiguration.setBoolean(
        "hbase.crypto.managed_key_store.cust.#{$GLOB_CUST_ENCODED}.#{@table_table_key}.active", true)

      # CF-level key configurations - let system determine namespace automatically
      $TEST_CLUSTER.getConfiguration.set(
        "hbase.crypto.managed_key_store.cust.#{$GLOB_CUST_ENCODED}.#{@table_cf_keys}/cf1.alias",
        "#{@table_cf_keys}_cf1_key")
      $TEST_CLUSTER.getConfiguration.setBoolean(
        "hbase.crypto.managed_key_store.cust.#{$GLOB_CUST_ENCODED}.#{@table_cf_keys}/cf1.active", true)

      $TEST_CLUSTER.getConfiguration.set(
        "hbase.crypto.managed_key_store.cust.#{$GLOB_CUST_ENCODED}.#{@table_cf_keys}/cf2.alias",
        "#{@table_cf_keys}_cf2_key")
      $TEST_CLUSTER.getConfiguration.setBoolean(
        "hbase.crypto.managed_key_store.cust.#{$GLOB_CUST_ENCODED}.#{@table_cf_keys}/cf2.active", true)

      # Enable KeyMeta coprocessor
      $TEST_CLUSTER.getConfiguration.set('hbase.coprocessor.master.classes',
                                        KeymetaServiceEndpoint.java_class.name)

      puts "  >> New ManagedKeyStoreKeyProvider configured"
    end

    def restart_cluster_and_validate
      puts "\n--- Restarting cluster with managed key store key provider ---"

      $TEST.restartMiniCluster(KeymetaTableAccessor::KEY_META_TABLE_NAME)
      puts "  >> Cluster restarted with ManagedKeyStoreKeyProvider"
      setup_hbase

      # Validate key management service is functional
      output = capture_stdout { command(:show_key_status, "#{$GLOB_CUST_ENCODED}:*") }
      assert(output.include?('0 row(s)'), "Expected 0 rows from show_key_status, got: #{output}")
      #assert(output.include?(' FAILED '), "Expected FAILED status for show_key_status, got: #{output}")
      puts "  >> Key management service is functional"

      # Test operations still work and check HFile trailers
      puts "\n--- Validating operations after restart ---"
      validate_pre_migration_operations(true)
    end

    def check_hfile_trailers_pre_migration(table_name, column_families, is_key_management_enabled)
      puts "    >> Checking HFile trailers for #{table_name} with CFs: #{column_families.join(', ')}"

      column_families.each do |cf_name|
        validate_hfile_trailer(table_name, cf_name, false, is_key_management_enabled, false)
      end
    end

    def migrate_tables_step_by_step
      puts "\n--- Performing step-by-step table migration ---"

      # Migrate shared key tables first
      migrate_shared_key_tables

      # Migrate table-level key
      migrate_table_level_key

      # Migrate CF-level keys
      migrate_cf_level_keys
    end

    def migrate_shared_key_tables
      puts "\n--- Migrating shared key tables ---"

      # Enable key management for shared global key
      cust_and_namespace = "#{$GLOB_CUST_ENCODED}:shared-global-key"
      output = capture_stdout { command(:enable_key_management, cust_and_namespace) }
      assert(output.include?("#{$GLOB_CUST_ENCODED} shared-global-key ACTIVE"),
            "Expected ACTIVE status for shared key, got: #{output}")
      puts "  >> Enabled key management for shared global key"

      # Migrate first shared key table
      migrate_table_to_managed_key(@table_shared_key1, 'f', 'shared-global-key', true)

      # Migrate second shared key table
      migrate_table_to_managed_key(@table_shared_key2, 'f', 'shared-global-key', true)
    end

    def migrate_table_level_key
      puts "\n--- Migrating table-level key ---"

      # Enable key management for table namespace
      cust_and_namespace = "#{$GLOB_CUST_ENCODED}:#{@table_table_key}"
      output = capture_stdout { command(:enable_key_management, cust_and_namespace) }
      assert(output.include?("#{$GLOB_CUST_ENCODED} #{@table_table_key} ACTIVE"),
            "Expected ACTIVE status for table key, got: #{output}")
      puts "  >> Enabled key management for table-level key"

      # Migrate the table - no namespace attribute, let system auto-determine
      migrate_table_to_managed_key(@table_table_key, 'f', @table_table_key, false)
    end

    def migrate_cf_level_keys
      puts "\n--- Migrating CF-level keys ---"

      # Enable key management for CF1
      cf1_namespace = "#{@table_cf_keys}/cf1"
      cust_and_namespace = "#{$GLOB_CUST_ENCODED}:#{cf1_namespace}"
      output = capture_stdout { command(:enable_key_management, cust_and_namespace) }
      assert(output.include?("#{$GLOB_CUST_ENCODED} #{cf1_namespace} ACTIVE"),
            "Expected ACTIVE status for CF1 key, got: #{output}")
      puts "  >> Enabled key management for CF1"

      # Enable key management for CF2
      cf2_namespace = "#{@table_cf_keys}/cf2"
      cust_and_namespace = "#{$GLOB_CUST_ENCODED}:#{cf2_namespace}"
      output = capture_stdout { command(:enable_key_management, cust_and_namespace) }
      assert(output.include?("#{$GLOB_CUST_ENCODED} #{cf2_namespace} ACTIVE"),
            "Expected ACTIVE status for CF2 key, got: #{output}")
      puts "  >> Enabled key management for CF2"

      # Migrate CF1
      migrate_table_to_managed_key(@table_cf_keys, 'cf1', cf1_namespace, false)

      # Migrate CF2
      migrate_table_to_managed_key(@table_cf_keys, 'cf2', cf2_namespace, false)
    end

    def migrate_table_to_managed_key(table_name, cf_name, namespace, use_namespace_attribute = false)
      puts "    >> Migrating table #{table_name}, CF #{cf_name} to namespace #{namespace}"

      # Use atomic alter operation to remove ENCRYPTION_KEY and optionally add ENCRYPTION_KEY_NAMESPACE
      if use_namespace_attribute
        # For shared key tables: remove ENCRYPTION_KEY and add ENCRYPTION_KEY_NAMESPACE atomically
        command(:alter, table_name,
                { 'NAME' => cf_name, 'CONFIGURATION' => {'ENCRYPTION_KEY' => '', 'ENCRYPTION_KEY_NAMESPACE' => namespace }})
      else
        # For table/CF level keys: just remove ENCRYPTION_KEY, let system auto-determine namespace
        command(:alter, table_name,
                { 'NAME' => cf_name, 'CONFIGURATION' => {'ENCRYPTION_KEY' => '' }})
      end

      puts "      >> Altered #{table_name} CF #{cf_name} to use namespace #{namespace}"

      # The new encryption attribute won't be used unless HStore is reinitialized.
      # To force reinitialization, disable and enable the table.
      command(:disable, table_name)
      command(:enable, table_name)
      # sleep for 5s to ensure region is reopened and store is reinitialized
      sleep(5)

      # Scan all existing data to verify accessibility
      scan_and_validate_table(table_name, [cf_name])

      # Add new data
      test_table = table(table_name)
      test_table.put('new_row', "#{cf_name}:new_col", 'new_value')

      # Flush and validate trailer
      $TEST_CLUSTER.flush(TableName.valueOf(table_name))
      validate_hfile_trailer(table_name, cf_name, true, true, false, namespace)

      puts "      >> Migration completed for #{table_name} CF #{cf_name}"
    end


    def scan_and_validate_table(table_name, column_families)
      puts "      >> Scanning and validating existing data in #{table_name}"

      test_table = table(table_name)
      scan = Scan.new
      scanner = test_table.table.getScanner(scan)

      row_count = 0
      while (result = scanner.next)
        row_count += 1
        assert_false(result.isEmpty)
      end
      scanner.close

      assert(row_count > 0, "Expected to find existing data in #{table_name}")
      puts "        >> Found #{row_count} rows, all accessible"
    end

    def validate_hfile_trailer(table_name, cf_name, is_post_migration, is_key_management_enabled,
                               is_compacted, expected_namespace = nil)
      context = is_post_migration ? 'migrated' : 'pre-migration'
      puts "      >> Validating HFile trailer for #{context} table #{table_name}, CF: #{cf_name}"

      table_name_obj = TableName.valueOf(table_name)
      region_servers = $TEST_CLUSTER.getRSForFirstRegionInTable(table_name_obj)
      regions = region_servers.getRegions(table_name_obj)

      regions.each do |region|
        region.getStores.each do |store|
          next unless store.getColumnFamilyName == cf_name
          puts "      >> store file count for CF: #{cf_name} in table: #{table_name} is #{store.getStorefiles.size}"
          if is_compacted
            assert_equal(1, store.getStorefiles.size)
          else
            assert_true(store.getStorefiles.size > 0)
          end
          store.getStorefiles.each do |storefile|
            puts "      >> Checking HFile trailer for storefile: #{storefile.getPath.getName} with sequence id: #{storefile.getMaxSequenceId} against max sequence id of store: #{store.getMaxSequenceId.getAsLong}"
            # The flush would have created new HFiles, but the old would still be there
            # so we need to make sure to check the latest store only.
            next unless storefile.getMaxSequenceId == store.getMaxSequenceId.getAsLong
            store_file_info = storefile.getFileInfo
            next unless store_file_info

            hfile_info = store_file_info.getHFileInfo
            next unless hfile_info

            trailer = hfile_info.getTrailer

            assert_not_nil(trailer.getEncryptionKey)

            if is_key_management_enabled
              assert_not_nil(trailer.getKEKMetadata)
              assert_not_equal(0, trailer.getKEKChecksum)
            else
              assert_nil(trailer.getKEKMetadata)
              assert_equal(0, trailer.getKEKChecksum)
            end

            if is_post_migration
              assert_equal(expected_namespace, trailer.getKeyNamespace)
              puts "        >> Trailer validation passed - namespace: #{trailer.getKeyNamespace}"
            else
              assert_nil(trailer.getKeyNamespace)
              puts "        >> Trailer validation passed - using legacy key format"
            end
          end
        end
      end
    end


    def cleanup_old_provider_and_validate
      puts "\n--- Cleaning up old key provider and final validation ---"

      # Remove old KeyProvider configurations
      $TEST_CLUSTER.getConfiguration.unset(HConstants::CRYPTO_KEYPROVIDER_CONF_KEY)
      $TEST_CLUSTER.getConfiguration.unset(HConstants::CRYPTO_KEYPROVIDER_PARAMETERS_KEY)
      $TEST_CLUSTER.getConfiguration.unset(HConstants::CRYPTO_MASTERKEY_NAME_CONF_KEY)

      # Remove old keystore
      FileUtils.rm_rf(@old_keystore_file) if File.directory?(@old_keystore_file)
      puts "  >> Removed old keystore and configuration"

      # Restart cluster
      $TEST.restartMiniCluster(KeymetaTableAccessor::KEY_META_TABLE_NAME)
      puts "  >> Cluster restarted without old key provider"
      setup_hbase

      # Validate all data is still accessible
      validate_all_tables_final

      # Perform major compaction and validate
      perform_major_compaction_and_validate
    end

    def validate_all_tables_final
      puts "\n--- Final validation - scanning all tables ---"

      @tables_metadata.each do |table_name, metadata|
        if metadata[:no_encryption]
          next
        end
        puts "  >> Final validation for table: #{table_name} with CFs: #{metadata[:cfs].join(', ')}"
        scan_and_validate_table(table_name, metadata[:cfs])
        puts "    >> #{table_name} - all data accessible"
      end
    end

    def perform_major_compaction_and_validate
      puts "\n--- Performing major compaction and final validation ---"

      $TEST_CLUSTER.compact(true)

      @tables_metadata.each do |table_name, metadata|
        if metadata[:no_encryption]
          next
        end
        puts "  >> Validating post-compaction HFiles for table: #{table_name} with CFs: #{metadata[:cfs].join(', ')}"
        metadata[:cfs].each do |cf_name|
          # When using random key from system key, there is no namespace
          #next if metadata[:expected_namespace][cf_name] == '*'
          validate_hfile_trailer(table_name, cf_name, true, true, true, metadata[:expected_namespace][cf_name])
        end
      end
    end

    # Utility methods

    def extract_and_unwrap_keys_from_tables
      puts "    >> Extracting and unwrapping keys from encrypted tables"

      keys = {}

      # Reuse existing master key from old keystore as system key
      old_key_provider = Encryption.getKeyProvider($TEST_CLUSTER.getConfiguration)
      master_key_bytes = old_key_provider.getKey(@master_key_alias).getEncoded
      keys['system_key'] = master_key_bytes

      # Extract wrapped keys from table descriptors and unwrap them
      # Only call extract_key_from_table for tables that have ENCRYPTION_KEY attribute

      # For shared key tables (both use same key)
      shared_key = extract_key_from_table(@table_shared_key1, 'f')
      keys['shared_global_key'] = shared_key

      # For table-level key
      table_key = extract_key_from_table(@table_table_key, 'f')
      keys["#{@table_table_key}_key"] = table_key

      # For CF-level keys
      cf1_key = extract_key_from_table(@table_cf_keys, 'cf1')
      keys["#{@table_cf_keys}_cf1_key"] = cf1_key

      cf2_key = extract_key_from_table(@table_cf_keys, 'cf2')
      keys["#{@table_cf_keys}_cf2_key"] = cf2_key

      puts "    >> Extracted #{keys.size} keys for migration"
      keys
    end

    def extract_key_from_table(table_name, cf_name)
      # Get table descriptor
      admin = $TEST_CLUSTER.getAdmin
      table_descriptor = admin.getDescriptor(TableName.valueOf(table_name))
      cf_descriptor = table_descriptor.getColumnFamily(Bytes.toBytes(cf_name))

      # Get the wrapped key bytes from ENCRYPTION_KEY attribute
      wrapped_key_bytes = cf_descriptor.getEncryptionKey

      # Use EncryptionUtil.unwrapKey with master key alias as subject
      unwrapped_key = EncryptionUtil.unwrapKey($TEST_CLUSTER.getConfiguration,
                                              @master_key_alias, wrapped_key_bytes)

      return unwrapped_key.getEncoded
    end

    def generate_key(alias_name)
      MessageDigest.getInstance('SHA-256').digest(Bytes.toBytes(alias_name))
    end

    def create_keystore(keystore_path, key_entries)
      store = KeyStore.getInstance('JCEKS')
      password_chars = @keystore_password.to_java.toCharArray
      store.load(nil, password_chars)

      key_entries.each do |alias_name, key_bytes|
        secret_key = SecretKeySpec.new(key_bytes, 'AES')
        store.setEntry(alias_name, KeyStore::SecretKeyEntry.new(secret_key),
                      KeyStore::PasswordProtection.new(password_chars))
      end

      fos = FileOutputStream.new(keystore_path)
      begin
        store.store(fos, password_chars)
      ensure
        fos.close
      end
    end


    def teardown
      # Cleanup temporary test directories (keystore files will be cleaned up with the directories)
      test_base_dir = $TEST_CLUSTER.getDataTestDir().toString
      Dir.glob(File.join(test_base_dir, "*keystore_#{@test_timestamp}*")).each do |dir|
        FileUtils.rm_rf(dir) if File.directory?(dir)
      end
    end
  end
end
