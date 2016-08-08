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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * Common constants for org.apache.hadoop.hbase.rest
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
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
  String REST_SSL_KEYSTORE_KEYPASSWORD = "hbase.rest.ssl.keystore.keypassword";

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
  String CUSTOM_FILTERS = "hbase.rest.custom.filters"; 

  String ROW_KEYS_PARAM_NAME = "row";
  /** If this query parameter is present when processing row or scanner resources,
      it disables server side block caching */
  String NOCACHE_PARAM_NAME = "nocache";
}
