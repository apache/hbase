/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
#ifndef HBASE_COMMON_MSGS_H_
#define HBASE_COMMON_MSGS_H_

namespace hbase {

class Msgs {
public:
  static const char *ZK_PORT_INVALID;

  static const char *ERR_CONN_NULL;
  static const char *ERR_CONNPTR_NULL;

  static const char *ERR_ADMIN_NULL;
  static const char *ERR_ADMIN_PTR_NULL;

  static const char *ERR_CLIENT_NULL;
  static const char *ERR_CLIENTPTR_NULL;

  static const char *ERR_TBL_NULL;
  static const char *ERR_TBL_LEN;
  static const char *ERR_TBL_NAME_NULL;

  static const char *ERR_START_ROW_NULL;
  static const char *ERR_START_ROWL_LEN;

  static const char *ERR_END_ROW_NULL;
  static const char *ERR_END_ROWL_LEN;

  static const char *ERR_TBLPTR_NULL;
  static const char *ERR_TBL_LENPTR_NULL;

  static const char *ERR_KEYPTR_NULL;
  static const char *ERR_KEY_LENPTR;

  static const char *ERR_HTBL_NULL;
  static const char *ERR_HTBLPTR_NULL;

  static const char *ERR_FAMILY_NULL;
  static const char *ERR_FAMILY_LEN;
  static const char *ERR_FAMILIES_NULL;
  static const char *ERR_NUM_FAMILIES;
  static const char *ERR_COLDESC_NULL;

  static const char *ERR_MAX_VERSIONS;
  static const char *ERR_MIN_VERSIONS;
  static const char *ERR_TTL;

  static const char *ERR_CELL_NULL;
  static const char *ERR_CELLPTR_NULL;
  static const char *ERR_CELL_COUNTPTR_NULL;

  static const char *ERR_KEY_NULL;
  static const char *ERR_KEY_LEN;

  static const char *ERR_DEL_NULL;
  static const char *ERR_DELPTR_NULL;

  static const char *ERR_GET_NULL;
  static const char *ERR_GETPTR_NULL;

  static const char *ERR_RESULT_NULL;
  static const char *ERR_RESULTPTR_NULL;

  static const char *ERR_PUT_NULL;
  static const char *ERR_PUTPTR_NULL;

  static const char *ERR_PUTS_NULL;
  static const char *ERR_NUM_PUTS;

  static const char *ERR_MUTATION_NULL;
  static const char *ERR_MUTATIONPTR_NULL;

  static const char *ERR_MUTATIONS_NULL;
  static const char *ERR_NUM_MUTATIONS;

  static const char *ERR_VALUE_NULL;
  static const char *ERR_VALUE_LEN;

  static const char *ERR_QUAL_NULL;
  static const char *ERR_QUAL_LEN;

  static const char *ERR_TIMESTAMP1;
  static const char *ERR_TIMESTAMP2;

  static const char *ERR_NUM_VERSIONS;

  static const char *ERR_TABLE_ALREADY_DISABLED;
  static const char *ERR_TABLE_ALREADY_ENABLED;

  static const char *ERR_SCANNER_NULL;
  static const char *ERR_SCANNERPTR_NULL;
  static const char *ERR_CACHE_SIZE;
  static const char *ERR_SCANNER_OPEN;
};

} // namespace hbase

#endif /* HBASE_COMMON_MSGS_H_ */
