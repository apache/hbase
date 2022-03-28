/**
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

package org.apache.hadoop.hbase.rest;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Common constants for org.apache.hadoop.hbase.rest
 */
@InterfaceAudience.Public
public interface Constants {
  // All constants in a public interface are 'public static final'

  String VERSION_STRING = "0.0.3";

  int DEFAULT_MAX_AGE = 60 * 60 * 4;  // 4 hours

  int DEFAULT_LISTEN_PORT = 8080;

  String MIMETYPE_TEXT = "text/plain";
  String MIMETYPE_HTML = "text/html";
  String MIMETYPE_XML = "text/xml";
  String MIMETYPE_BINARY = "application/octet-stream";
  String MIMETYPE_PROTOBUF = "application/x-protobuf";
  String MIMETYPE_PROTOBUF_IETF = "application/protobuf";
  String MIMETYPE_JSON = "application/json";

  String CRLF = "\r\n";

  String REST_KEYTAB_FILE = "hbase.rest.keytab.file";
  String REST_KERBEROS_PRINCIPAL = "hbase.rest.kerberos.principal";
  String REST_AUTHENTICATION_TYPE = "hbase.rest.authentication.type";
  String REST_AUTHENTICATION_PRINCIPAL = "hbase.rest.authentication.kerberos.principal";

  String REST_SSL_ENABLED = "hbase.rest.ssl.enabled";
  String REST_SSL_KEYSTORE_STORE = "hbase.rest.ssl.keystore.store";
  String REST_SSL_KEYSTORE_PASSWORD = "hbase.rest.ssl.keystore.password";
  String REST_SSL_KEYSTORE_TYPE = "hbase.rest.ssl.keystore.type";
  String REST_SSL_TRUSTSTORE_STORE = "hbase.rest.ssl.truststore.store";
  String REST_SSL_TRUSTSTORE_PASSWORD = "hbase.rest.ssl.truststore.password";
  String REST_SSL_TRUSTSTORE_TYPE = "hbase.rest.ssl.truststore.type";
  String REST_SSL_KEYSTORE_KEYPASSWORD = "hbase.rest.ssl.keystore.keypassword";
  String REST_SSL_EXCLUDE_CIPHER_SUITES = "hbase.rest.ssl.exclude.cipher.suites";
  String REST_SSL_INCLUDE_CIPHER_SUITES = "hbase.rest.ssl.include.cipher.suites";
  String REST_SSL_EXCLUDE_PROTOCOLS = "hbase.rest.ssl.exclude.protocols";
  String REST_SSL_INCLUDE_PROTOCOLS = "hbase.rest.ssl.include.protocols";

  String REST_THREAD_POOL_THREADS_MAX = "hbase.rest.threads.max";
  String REST_THREAD_POOL_THREADS_MIN = "hbase.rest.threads.min";
  String REST_THREAD_POOL_TASK_QUEUE_SIZE = "hbase.rest.task.queue.size";
  String REST_THREAD_POOL_THREAD_IDLE_TIMEOUT = "hbase.rest.thread.idle.timeout";
  String REST_CONNECTOR_ACCEPT_QUEUE_SIZE = "hbase.rest.connector.accept.queue.size";

  String REST_DNS_NAMESERVER = "hbase.rest.dns.nameserver";
  String REST_DNS_INTERFACE = "hbase.rest.dns.interface";

  String FILTER_CLASSES = "hbase.rest.filter.classes";
  String SCAN_START_ROW = "startrow";
  String SCAN_END_ROW = "endrow";
  String SCAN_COLUMN = "column";
  String SCAN_START_TIME = "starttime";
  String SCAN_END_TIME = "endtime";
  String SCAN_MAX_VERSIONS = "maxversions";
  String SCAN_BATCH_SIZE = "batchsize";
  String SCAN_LIMIT = "limit";
  String SCAN_FETCH_SIZE = "hbase.rest.scan.fetchsize";
  String SCAN_FILTER = "filter";
  String SCAN_REVERSED = "reversed";
  String SCAN_CACHE_BLOCKS = "cacheblocks";
  String CUSTOM_FILTERS = "hbase.rest.custom.filters"; 

  String ROW_KEYS_PARAM_NAME = "row";
  /** If this query parameter is present when processing row or scanner resources,
      it disables server side block caching */
  String NOCACHE_PARAM_NAME = "nocache";

  /** Configuration parameter to set rest client connection timeout */
  String REST_CLIENT_CONN_TIMEOUT = "hbase.rest.client.conn.timeout";
  int DEFAULT_REST_CLIENT_CONN_TIMEOUT = 2 * 1000;

  /** Configuration parameter to set rest client socket timeout */
  String REST_CLIENT_SOCKET_TIMEOUT = "hbase.rest.client.socket.timeout";
  int DEFAULT_REST_CLIENT_SOCKET_TIMEOUT = 30 * 1000;
}
