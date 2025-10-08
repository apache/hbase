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
java_import org.apache.hadoop.hbase.io.hfile.CorruptHFileException
java_import org.apache.hadoop.hbase.io.hfile.FixedFileTrailer
java_import org.apache.hadoop.hbase.io.hfile.HFile
java_import org.apache.hadoop.hbase.io.hfile.CacheConfig
java_import org.apache.hadoop.hbase.util.Bytes

module Hbase
  # Test class for encrypted table keymeta functionality
  class EncryptedTableKeymetaTest < Test::Unit::TestCase
    include TestHelpers

    def setup
      setup_hbase
      @test_table = 'enctest'+Time.now.to_i.to_s
      @connection = $TEST_CLUSTER.connection
    end

    define_test 'Test table put/get with encryption' do
      # Custodian is currently not supported, so this will end up falling back to local key
      # generation.
      test_table_put_get_with_encryption($CUST1_ENCODED, '*',
        { 'NAME' => 'f', 'ENCRYPTION' => 'AES' }, true)
    end

    define_test 'Test table with custom namespace attribute in Column Family' do
      custom_namespace = "test_global_namespace"
      test_table_put_get_with_encryption($GLOB_CUST_ENCODED, custom_namespace,
        { 'NAME' => 'f', 'ENCRYPTION' => 'AES', 'ENCRYPTION_KEY_NAMESPACE' => custom_namespace },
        false)
    end

    def test_table_put_get_with_encryption(cust, namespace, table_attrs, fallback_scenario)
      cust_and_namespace = "#{cust}:#{namespace}"
      output = capture_stdout { @shell.command('enable_key_management', cust_and_namespace) }
      assert(output.include?("#{cust} #{namespace} ACTIVE"))
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
      assert_equal(namespace, live_trailer.getKeyNamespace())

      # When active key is supposed to be used, we can valiate the key bytes in the context against
      # the actual key from provider.
      if !fallback_scenario
        encryption_context = hfile_info.getHFileContext().getEncryptionContext()
        assert_not_nil(encryption_context)
        assert_not_nil(encryption_context.getKeyBytes())
        key_provider = Encryption.getManagedKeyProvider($TEST_CLUSTER.getConfiguration)
        key_data = key_provider.getManagedKey(ManagedKeyProvider.decodeToBytes(cust), namespace)
        assert_not_nil(key_data)
        assert_equal(namespace, key_data.getKeyNamespace())
        assert_equal(key_data.getTheKey().getEncoded(), encryption_context.getKeyBytes())
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
      assert_not_nil(offline_trailer.getKEKMetadata)
      assert_not_nil(offline_trailer.getKEKChecksum)
      assert_not_nil(offline_trailer.getKeyNamespace)

      return unless live_trailer

      assert_equal(live_trailer.getEncryptionKey, offline_trailer.getEncryptionKey)
      assert_equal(live_trailer.getKEKMetadata, offline_trailer.getKEKMetadata)
      assert_equal(live_trailer.getKEKChecksum, offline_trailer.getKEKChecksum)
      assert_equal(live_trailer.getKeyNamespace, offline_trailer.getKeyNamespace)
    end
  end
end
