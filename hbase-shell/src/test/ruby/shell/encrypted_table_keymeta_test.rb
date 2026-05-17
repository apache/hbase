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

java_import org.apache.hadoop.conf.Configuration
java_import org.apache.hadoop.fs.FSDataInputStream
java_import org.apache.hadoop.hbase.CellUtil
java_import org.apache.hadoop.hbase.HConstants
java_import org.apache.hadoop.hbase.client.Get
java_import org.apache.hadoop.hbase.io.crypto.Encryption
java_import org.apache.hadoop.hbase.io.crypto.ManagedKeyProvider
java_import org.apache.hadoop.hbase.io.crypto.MockManagedKeyProvider
java_import org.apache.hadoop.hbase.keymeta.KeyIdentityPrefixBytesBacked
java_import org.apache.hadoop.hbase.io.hfile.CorruptHFileException
java_import org.apache.hadoop.hbase.io.hfile.FixedFileTrailer
java_import org.apache.hadoop.hbase.io.hfile.HFile
java_import org.apache.hadoop.hbase.io.hfile.CacheConfig
java_import org.apache.hadoop.hbase.keymeta.KeyIdentitySingleArrayBacked
java_import org.apache.hadoop.hbase.keymeta.KeymetaTableAccessor
java_import org.apache.hadoop.hbase.shaded.protobuf.generated.EncryptionProtos
java_import org.apache.hadoop.hbase.util.Bytes
java_import java.io.ByteArrayInputStream


module Hbase
  # Test class for encrypted table keymeta functionality
  class EncryptedTableKeymetaTest < Test::Unit::TestCase
    include TestHelpers

    def setup
      setup_hbase
      @test_table = "enctest#{Time.now.to_i}"
      @connection = $TEST_CLUSTER.connection
    end

    define_test 'Test table put/get with encryption (scenario 2a)' do
      # Custodian is currently not supported, so this will end up falling back to local key
      # generation.
      run_table_put_get_with_encryption(
                                         { 'NAME' => 'f', 'ENCRYPTION' => 'AES' },
                                         false)
    end

    define_test 'Test table with custom namespace attribute in Column Family' do
      custom_namespace = 'test_global_namespace'
      run_table_put_get_with_encryption(
        { 'NAME' => 'f', 'ENCRYPTION' => 'AES', 'ENCRYPTION_KEY_NAMESPACE' => custom_namespace },
        false
      )
    end

    define_test 'Test table put/get with encryption and local key gen per file (scenario 2b)' do
      # Enable local key gen per file so SecurityUtil uses scenario 2b: active key (DEK) as KEK,
      # locally generated key as CEK per file.
      conf_key = HConstants::CRYPTO_MANAGED_KEYS_LOCAL_KEY_GEN_PER_FILE_ENABLED_CONF_KEY
      $TEST_CLUSTER.getConfiguration.setBoolean(conf_key, true)
      $TEST.restartMiniCluster(KeymetaTableAccessor::KEY_META_TABLE_NAME)
      setup_hbase
      begin
        custom_namespace = 'test_global_namespace'
        run_table_put_get_with_encryption(
          { 'NAME' => 'f', 'ENCRYPTION' => 'AES', 'ENCRYPTION_KEY_NAMESPACE' => custom_namespace },
          true
        )
      ensure
        # Restore default so other tests or future runs are not affected
        $TEST_CLUSTER.getConfiguration.setBoolean(conf_key, false)
        $TEST.restartMiniCluster(KeymetaTableAccessor::KEY_META_TABLE_NAME)
        setup_hbase
      end
    end

    def run_table_put_get_with_encryption(table_attrs, local_key_gen_scenario)
      cust = $GLOB_CUST_ENCODED
      has_namespace = table_attrs.has_key?('ENCRYPTION_KEY_NAMESPACE')
      if has_namespace
        expected_ns = table_attrs['ENCRYPTION_KEY_NAMESPACE']
        cust_and_namespace = "#{cust}:#{expected_ns}"

        output = capture_stdout { @shell.command('enable_key_management', cust_and_namespace) }
        assert(output.include?("#{cust} #{expected_ns} ACTIVE"), "Expected cust #{cust} and namespace #{expected_ns} to be ACTIVE, got: #{output}")
      else
        expected_ns = '*'
      end
      @shell.command(:create, @test_table, table_attrs)
      test_table = table(@test_table)
      test_table.put('1', 'f:a', '2')
      puts "Added a row, now flushing table #{@test_table}"
      command(:flush, @test_table)

      table_name = TableName.valueOf(@test_table)
      store_file_info = nil
      $TEST_CLUSTER.getRSForFirstRegionInTable(table_name).getRegions(table_name).each do |region|
        region.getStores.each do |store|
          store.getStorefiles.each do |storefile|
            store_file_info = storefile.getFileInfo
          end
        end
      end
      assert_not_nil(store_file_info)
      hfile_info = store_file_info.getHFileInfo
      assert_not_nil(hfile_info)
      live_trailer = hfile_info.getTrailer
      assert_trailer(live_trailer)
      # When we have an active key (2a or 2b), KEK identity in trailer should reflect our namespace

      encryption_context = hfile_info.getHFileContext.getEncryptionContext
      assert_not_nil(encryption_context)
      assert_not_nil(encryption_context.getKeyBytes)
      key_provider = Encryption.getManagedKeyProvider($TEST_CLUSTER.getConfiguration)
      cluster_id = $TEST_CLUSTER.getMiniHBaseCluster().getMaster().getClusterId();
      system_key = key_provider.getSystemKey(cluster_id.bytes)
      dek_data = key_provider.getManagedKey(KeyIdentityPrefixBytesBacked.new(
        Bytes.new(ManagedKeyProvider.decodeToBytes($GLOB_CUST_ENCODED)),
        Bytes.new(Bytes.toBytes(expected_ns))))
      assert_not_nil(dek_data)
      live_key = EncryptionProtos::WrappedKey::parser()
        .parseDelimitedFrom(ByteArrayInputStream.new(live_trailer.getEncryptionKey));
      parsed_namespace = parse_namespace_from_kek_identity(live_key.getKekIdentity)
      # When active key is the CEK (scenario 2a), validate key bytes in context match
      # provider. For scenario 2b (local key gen), CEK is generated per file so key bytes differ.
      if local_key_gen_scenario
        # Scenario 2b: CEK is locally generated per file, so it must not equal the provider key
        # (which is used as KEK).
        assert_not_equal(dek_data.getTheKey.getEncoded, encryption_context.getKeyBytes)
        assert_not_equal(system_key.getTheKey.getEncoded, encryption_context.getKeyBytes)
        assert_equal(encryption_context.getKEKData().getKeyNamespaceBytes(), parsed_namespace)
        assert_equal(has_namespace ? dek_data : system_key, encryption_context.getKEKData())
      else
        # Scenario 2a: active key is used as CEK directly
        assert_equal(dek_data.getTheKey.getEncoded, encryption_context.getKeyBytes)
        assert_equal(system_key, encryption_context.getKEKData())
        assert_equal(system_key.getKeyNamespaceBytes(), parsed_namespace)
      end

      ## Disable table to ensure that the stores are not cached.
      command(:disable, @test_table)
      assert(!command(:is_enabled, @test_table))

      # Open FSDataInputStream to the path pointed to by the store_file_info
      fs = store_file_info.getFileSystem
      fio = fs.open(store_file_info.getPath)
      assert_not_nil(fio)
      # Read trailer using FiledFileTrailer
      offline_trailer = FixedFileTrailer.readFromStream(
        fio, fs.getFileStatus(store_file_info.getPath).getLen
      )
      fio.close
      assert_trailer(offline_trailer, live_trailer)

      # Test for the ability to read HFile with encryption in an offline offline
      reader = HFile.createReader(fs, store_file_info.getPath, CacheConfig::DISABLED, true,
                                  $TEST_CLUSTER.getConfiguration)
      assert_not_nil(reader)
      offline_trailer = reader.getTrailer
      assert_trailer(offline_trailer, live_trailer)
      scanner = reader.getScanner($TEST_CLUSTER.getConfiguration, false, false)
      assert_true(scanner.seekTo)
      cell = scanner.getCell
      assert_equal('1', Bytes.toString(CellUtil.cloneRow(cell)))
      assert_equal('2', Bytes.toString(CellUtil.cloneValue(cell)))
      assert_false(scanner.next)

      # Confirm that the offline reading will fail with no config related to encryption
      Encryption.clearKeyProviderCache
      conf = Configuration.new($TEST_CLUSTER.getConfiguration)
      conf.set(HConstants::CRYPTO_MANAGED_KEYPROVIDER_CONF_KEY,
               MockManagedKeyProvider.java_class.getName)
      # This is expected to fail with CorruptHFileException.
      e = assert_raises(CorruptHFileException) do
        reader = HFile.createReader(fs, store_file_info.getPath, CacheConfig::DISABLED, true, conf)
      end
      assert_true(e.message.include?(
                    "Problem reading HFile Trailer from file #{store_file_info.getPath}"
                  ))
      Encryption.clearKeyProviderCache

      ## Enable back the table to be able to query.
      command(:enable, @test_table)
      assert(command(:is_enabled, @test_table))

      get = Get.new(Bytes.toBytes('1'))
      res = test_table.table.get(get)
      puts "res for row '1' and column f:a: #{res}"
      assert_false(res.isEmpty)
      assert_equal('2', Bytes.toString(res.getValue(Bytes.toBytes('f'), Bytes.toBytes('a'))))
    end

    def assert_trailer(offline_trailer, live_trailer = nil)
      assert_not_nil(offline_trailer)
      assert_not_nil(offline_trailer.getEncryptionKey)
      offline_key = EncryptionProtos::WrappedKey::parser()
        .parseDelimitedFrom(ByteArrayInputStream.new(offline_trailer.getEncryptionKey));
      assert_not_nil(offline_key.getKekMetadata)
      assert_not_nil(offline_key.getKekIdentity)
      assert_true(offline_key.getKekIdentity.size > 0)
      parsed_namespace = parse_namespace_from_kek_identity(offline_key.getKekIdentity)
      assert_not_nil(parsed_namespace)

      return unless live_trailer

      assert_equal(live_trailer.getEncryptionKey, offline_trailer.getEncryptionKey)
      live_key = EncryptionProtos::WrappedKey::parser()
        .parseDelimitedFrom(ByteArrayInputStream.new(live_trailer.getEncryptionKey));
      assert_equal(live_key.getKekMetadata, offline_key.getKekMetadata)
      assert_equal(live_key.getKekIdentity.toByteArray, offline_key.getKekIdentity.toByteArray)
      assert_equal(parse_namespace_from_kek_identity(live_key.getKekIdentity),
                  parse_namespace_from_kek_identity(offline_key.getKekIdentity))
    end

    # Returns the key namespace string parsed from KEK identity bytes, or nil if not present.
    def parse_namespace_from_kek_identity(kek_identity)
      return nil if kek_identity.nil? || kek_identity.size == 0

      KeyIdentitySingleArrayBacked.new(kek_identity.toByteArray).getNamespaceView().copyBytes()
    end
  end
end
