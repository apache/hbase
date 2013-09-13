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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Common constants for org.apache.hadoop.hbase.rest
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface Constants {
  String VERSION_STRING = "0.0.2";

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

  static final String REST_KEYTAB_FILE = "hbase.rest.keytab.file";
  static final String REST_KERBEROS_PRINCIPAL = "hbase.rest.kerberos.principal";
  static final String REST_AUTHENTICATION_TYPE = "hbase.rest.authentication.type";
  static final String REST_AUTHENTICATION_PRINCIPAL =
    "hbase.rest.authentication.kerberos.principal";

  static final String REST_SSL_ENABLED = "hbase.rest.ssl.enabled";
  static final String REST_SSL_KEYSTORE_STORE = "hbase.rest.ssl.keystore.store";
  static final String REST_SSL_KEYSTORE_PASSWORD = "hbase.rest.ssl.keystore.password";
  static final String REST_SSL_KEYSTORE_KEYPASSWORD =
    "hbase.rest.ssl.keystore.keypassword";

  static final String REST_DNS_NAMESERVER = "hbase.rest.dns.nameserver";
  static final String REST_DNS_INTERFACE = "hbase.rest.dns.interface";
  public static final String FILTER_CLASSES = "hbase.rest.filter.classes";
}
