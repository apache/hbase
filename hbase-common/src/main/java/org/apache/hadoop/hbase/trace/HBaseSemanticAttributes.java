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
import io.opentelemetry.semconv.DbAttributes;
import io.opentelemetry.semconv.ExceptionAttributes;
import io.opentelemetry.semconv.NetworkAttributes;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The constants in this class correspond with the guidance outlined by the OpenTelemetry <a href=
 * "https://github.com/open-telemetry/semantic-conventions">Semantic Conventions</a>.
 *
 * <p>This class is in a migration phase from experimental (1.29.0-alpha) to stable (1.42.0)
 * semantic conventions. Deprecated attributes are aliased to their stable equivalents and will be
 * removed in HBase 5.x. Use the non-deprecated attribute names for new code.
 */
@InterfaceAudience.Private
public final class HBaseSemanticAttributes {
  // ========== DATABASE ATTRIBUTES ==========

  /**
   * Stable database system attribute from OpenTelemetry semconv 1.42.0.
   * @see DbAttributes#DB_SYSTEM_NAME
   */
  public static final AttributeKey<String> DB_SYSTEM_NAME = DbAttributes.DB_SYSTEM_NAME;

  /**
   * HBase system identifier. HBase is not in the stable enum, so we use a custom value.
   */
  public static final String DB_SYSTEM_NAME_VALUE = "hbase";

  /**
   * Stable database namespace attribute from OpenTelemetry semconv 1.42.0.
   * @see DbAttributes#DB_NAMESPACE
   */
  public static final AttributeKey<String> DB_NAMESPACE = DbAttributes.DB_NAMESPACE;

  /**
   * Stable database operation attribute from OpenTelemetry semconv 1.42.0.
   * @see DbAttributes#DB_OPERATION_NAME
   */
  public static final AttributeKey<String> DB_OPERATION_NAME = DbAttributes.DB_OPERATION_NAME;

  /**
   * @deprecated Use {@link #DB_SYSTEM_NAME} instead. Will be removed in HBase 5.x.
   */
  @Deprecated
  public static final AttributeKey<String> DB_SYSTEM = DB_SYSTEM_NAME;

  /**
   * @deprecated Use {@link #DB_SYSTEM_NAME_VALUE} instead. Will be removed in HBase 5.x.
   */
  @Deprecated
  public static final String DB_SYSTEM_VALUE = DB_SYSTEM_NAME_VALUE;

  /**
   * @deprecated This attribute was removed from stable OpenTelemetry semantic conventions for
   *             security/PII concerns. HBase continues emitting it as a custom attribute for
   *             backward compatibility. Will be removed in HBase 5.x.
   */
  @Deprecated
  public static final AttributeKey<String> DB_CONNECTION_STRING =
    AttributeKey.stringKey("db.connection_string");

  /**
   * @deprecated This attribute was removed from stable OpenTelemetry semantic conventions for PII
   *             concerns. HBase continues emitting it as a custom attribute for backward
   *             compatibility. Will be removed in HBase 5.x.
   */
  @Deprecated
  public static final AttributeKey<String> DB_USER = AttributeKey.stringKey("db.user");

  /**
   * @deprecated Use {@link #DB_NAMESPACE} instead. Will be removed in HBase 5.x.
   */
  @Deprecated
  public static final AttributeKey<String> DB_NAME = DB_NAMESPACE;

  /**
   * @deprecated Use {@link #DB_OPERATION_NAME} instead. Will be removed in HBase 5.x.
   */
  @Deprecated
  public static final AttributeKey<String> DB_OPERATION = DB_OPERATION_NAME;

  /**
   * HBase-specific attribute for table names.
   */
  public static final AttributeKey<String> TABLE_KEY = AttributeKey.stringKey("db.hbase.table");

  /**
   * For operations that themselves ship one or more operations, such as {@link Operation#BATCH} and
   * {@link Operation#CHECK_AND_MUTATE}.
   */
  public static final AttributeKey<List<String>> CONTAINER_DB_OPERATIONS_KEY =
    AttributeKey.stringArrayKey("db.hbase.container_operations");

  /**
   * HBase-specific attribute for region names.
   */
  public static final AttributeKey<List<String>> REGION_NAMES_KEY =
    AttributeKey.stringArrayKey("db.hbase.regions");

  // ========== RPC ATTRIBUTES ==========

  /**
   * RPC system attribute. RPC conventions are stable in the OpenTelemetry specification but not yet
   * present in the semconv 1.42.0 JAR, so we define this directly.
   */
  public static final AttributeKey<String> RPC_SYSTEM = AttributeKey.stringKey("rpc.system");

  /**
   * @deprecated RPC service attribute was removed from stable OpenTelemetry semantic conventions
   *             (merged into rpc.method). HBase continues emitting it for backward compatibility.
   *             Will be removed in HBase 5.x.
   */
  @Deprecated
  public static final AttributeKey<String> RPC_SERVICE = AttributeKey.stringKey("rpc.service");

  /**
   * RPC method attribute. RPC conventions are stable in the OpenTelemetry specification but not yet
   * present in the semconv 1.42.0 JAR, so we define this directly.
   */
  public static final AttributeKey<String> RPC_METHOD = AttributeKey.stringKey("rpc.method");

  /**
   * HBase-specific attribute for server names.
   */
  public static final AttributeKey<String> SERVER_NAME_KEY =
    AttributeKey.stringKey("db.hbase.server.name");

  // ========== NETWORK ATTRIBUTES ==========

  /**
   * Stable network peer address attribute from OpenTelemetry semconv 1.42.0.
   * @see NetworkAttributes#NETWORK_PEER_ADDRESS
   */
  public static final AttributeKey<String> NETWORK_PEER_ADDRESS =
    NetworkAttributes.NETWORK_PEER_ADDRESS;

  /**
   * Stable network peer port attribute from OpenTelemetry semconv 1.42.0.
   * @see NetworkAttributes#NETWORK_PEER_PORT
   */
  public static final AttributeKey<Long> NETWORK_PEER_PORT = NetworkAttributes.NETWORK_PEER_PORT;

  /**
   * @deprecated Use {@link #NETWORK_PEER_ADDRESS} instead. Will be removed in HBase 5.x.
   */
  @Deprecated
  public static final AttributeKey<String> NET_PEER_NAME = NETWORK_PEER_ADDRESS;

  /**
   * @deprecated Use {@link #NETWORK_PEER_PORT} instead. Will be removed in HBase 5.x.
   */
  @Deprecated
  public static final AttributeKey<Long> NET_PEER_PORT = NETWORK_PEER_PORT;

  /**
   * HBase-specific attribute for row lock types.
   */
  public static final AttributeKey<Boolean> ROW_LOCK_READ_LOCK_KEY =
    AttributeKey.booleanKey("db.hbase.rowlock.readlock");

  /**
   * HBase-specific attribute for WAL implementation.
   */
  public static final AttributeKey<String> WAL_IMPL = AttributeKey.stringKey("db.hbase.wal.impl");

  // ========== EXCEPTION ATTRIBUTES ==========

  /**
   * Stable exception type attribute from OpenTelemetry semconv 1.42.0.
   * @see ExceptionAttributes#EXCEPTION_TYPE
   */
  public static final AttributeKey<String> EXCEPTION_TYPE = ExceptionAttributes.EXCEPTION_TYPE;

  /**
   * Stable exception message attribute from OpenTelemetry semconv 1.42.0.
   * @see ExceptionAttributes#EXCEPTION_MESSAGE
   */
  public static final AttributeKey<String> EXCEPTION_MESSAGE =
    ExceptionAttributes.EXCEPTION_MESSAGE;

  /**
   * The name to use for exception events. This is a string literal, not an AttributeKey.
   */
  public static final String EXCEPTION_EVENT_NAME = "exception";

  // ========== HFILE / IO ATTRIBUTES ==========

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
   * Indicates the type of read.
   */
  public static final AttributeKey<String> READ_TYPE_KEY =
    AttributeKey.stringKey("db.hbase.io.hfile.read_type");
  /**
   * Identifies an entry in the Block Cache.
   */
  public static final AttributeKey<String> BLOCK_CACHE_KEY_KEY =
    AttributeKey.stringKey("db.hbase.io.hfile.block_cache_key");

  // ========== ENUMS ==========

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
   * These are values used with {@link #DB_OPERATION_NAME}. They correspond with the implementations
   * of {@code org.apache.hadoop.hbase.client.Operation}, as well as
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
