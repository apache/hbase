/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.trace;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.semconv.SemanticAttributes;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The constants in this class correspond with the guidance outlined by the OpenTelemetry <a href=
 * "https://github.com/open-telemetry/opentelemetry-specification/tree/main/specification/trace/semantic_conventions">Semantic
 * Conventions</a>.
 */
@InterfaceAudience.Private
public final class HBaseSemanticAttributes {
  public static final AttributeKey<String> DB_SYSTEM = SemanticAttributes.DB_SYSTEM;
  public static final String DB_SYSTEM_VALUE = SemanticAttributes.DbSystemValues.HBASE;
  public static final AttributeKey<String> DB_CONNECTION_STRING =
    SemanticAttributes.DB_CONNECTION_STRING;
  public static final AttributeKey<String> DB_USER = SemanticAttributes.DB_USER;
  public static final AttributeKey<String> DB_NAME = SemanticAttributes.DB_NAME;
  public static final AttributeKey<String> DB_OPERATION = SemanticAttributes.DB_OPERATION;
  public static final AttributeKey<String> TABLE_KEY = AttributeKey.stringKey("db.hbase.table");
  /**
   * For operations that themselves ship one or more operations, such as {@link Operation#BATCH} and
   * {@link Operation#CHECK_AND_MUTATE}.
   */
  public static final AttributeKey<List<String>> CONTAINER_DB_OPERATIONS_KEY =
    AttributeKey.stringArrayKey("db.hbase.container_operations");
  public static final AttributeKey<List<String>> REGION_NAMES_KEY =
    AttributeKey.stringArrayKey("db.hbase.regions");
  public static final AttributeKey<String> RPC_SYSTEM = SemanticAttributes.RPC_SYSTEM;
  public static final AttributeKey<String> RPC_SERVICE = SemanticAttributes.RPC_SERVICE;
  public static final AttributeKey<String> RPC_METHOD = SemanticAttributes.RPC_METHOD;
  public static final AttributeKey<String> SERVER_NAME_KEY =
    AttributeKey.stringKey("db.hbase.server.name");
  public static final AttributeKey<String> NET_PEER_NAME = SemanticAttributes.NET_PEER_NAME;
  public static final AttributeKey<Long> NET_PEER_PORT = SemanticAttributes.NET_PEER_PORT;
  public static final AttributeKey<Boolean> ROW_LOCK_READ_LOCK_KEY =
    AttributeKey.booleanKey("db.hbase.rowlock.readlock");
  public static final AttributeKey<String> WAL_IMPL = AttributeKey.stringKey("db.hbase.wal.impl");

  public static final AttributeKey<String> EXCEPTION_TYPE = SemanticAttributes.EXCEPTION_TYPE;
  public static final AttributeKey<String> EXCEPTION_MESSAGE = SemanticAttributes.EXCEPTION_MESSAGE;
  public static final String EXCEPTION_EVENT_NAME = SemanticAttributes.EXCEPTION_EVENT_NAME;

  /**
   * Indicates the amount of data was read into a {@link ByteBuffer} of type
   * {@link ByteBuffer#isDirect() direct}.
   */
  public static final AttributeKey<Long> DIRECT_BYTES_READ_KEY =
    AttributeKey.longKey("db.hbase.io.direct_bytes_read");
  /**
   * Indicates the amount of data was read into a {@link ByteBuffer} not of type
   * {@link ByteBuffer#isDirect() direct}.
   */
  public static final AttributeKey<Long> HEAP_BYTES_READ_KEY =
    AttributeKey.longKey("db.hbase.io.heap_bytes_read");
  /**
   * Indicates the {@link org.apache.hadoop.hbase.io.compress.Compression.Algorithm} used to encode
   * an HFile.
   */
  public static final AttributeKey<String> COMPRESSION_ALGORITHM_KEY =
    AttributeKey.stringKey("db.hbase.io.hfile.data_block_encoding");
  /**
   * Indicates the {@link org.apache.hadoop.hbase.io.encoding.DataBlockEncoding} algorithm used to
   * encode this HFile.
   */
  public static final AttributeKey<String> DATA_BLOCK_ENCODING_KEY =
    AttributeKey.stringKey("db.hbase.io.hfile.data_block_encoding");
  /**
   * Indicates the {@link org.apache.hadoop.hbase.io.crypto.Cipher} used to encrypt this HFile.
   */
  public static final AttributeKey<String> ENCRYPTION_CIPHER_KEY =
    AttributeKey.stringKey("db.hbase.io.hfile.encryption_cipher");
  /**
   * Indicates the {@link org.apache.hadoop.hbase.util.ChecksumType} used to encode this HFile.
   */
  public static final AttributeKey<String> CHECKSUM_KEY =
    AttributeKey.stringKey("db.hbase.io.hfile.checksum_type");
  /**
   * Indicates the name of the HFile accessed.
   */
  public static final AttributeKey<String> HFILE_NAME_KEY =
    AttributeKey.stringKey("db.hbase.io.hfile.file_name");
  /**
   * Indicated the type of read.
   */
  public static final AttributeKey<String> READ_TYPE_KEY =
    AttributeKey.stringKey("db.hbase.io.hfile.read_type");
  /**
   * Identifies an entry in the Block Cache.
   */
  public static final AttributeKey<String> BLOCK_CACHE_KEY_KEY =
    AttributeKey.stringKey("db.hbase.io.hfile.block_cache_key");

  /**
   * These values represent the different IO read strategies HBase may employ for accessing
   * filesystem data.
   */
  public enum ReadType {
    // TODO: promote this to the FSReader#readBlockData API. Or somehow instead use Scan.ReadType.
    POSITIONAL_READ,
    SEEK_PLUS_READ,
  }

  /**
   * These are values used with {@link #DB_OPERATION}. They correspond with the implementations of
   * {@code org.apache.hadoop.hbase.client.Operation}, as well as
   * {@code org.apache.hadoop.hbase.client.CheckAndMutate}, and "MULTI", meaning a batch of multiple
   * operations.
   */
  public enum Operation {
    APPEND,
    BATCH,
    CHECK_AND_MUTATE,
    COPROC_EXEC,
    DELETE,
    GET,
    INCREMENT,
    PUT,
    SCAN,
  }

  /**
   * These are values used with {@link #RPC_SYSTEM}. Only a single value for now; more to come as we
   * add tracing over our gateway components.
   */
  public enum RpcSystem {
    HBASE_RPC,
  }

  private HBaseSemanticAttributes() {
  }
}
