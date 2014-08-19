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
#include <inttypes.h>

#include "hbase_msgs.h"

namespace hbase {

const char *Msgs::ZK_PORT_INVALID =
    "Invalid port number '%s' specified in zookeeper ensemble '%s'.";

const char *Msgs::ERR_CONN_NULL = "'hb_connection_t' is NULL.";

const char *Msgs::ERR_CONNPTR_NULL = "'hb_connection_t*' is NULL.";

const char *Msgs::ERR_ADMIN_NULL = "'hb_admin_t' is NULL.";

const char *Msgs::ERR_ADMIN_PTR_NULL = "'hb_admin_t*' is NULL.";

const char *Msgs::ERR_CLIENT_NULL = "'hb_client_t' is NULL.";

const char *Msgs::ERR_CLIENTPTR_NULL = "'hb_client_t*' is NULL.";

const char *Msgs::ERR_TBL_NAME_NULL = "'tableName' is NULL.";

const char *Msgs::ERR_HTBL_NULL = "'hb_table_t' is NULL.";

const char *Msgs::ERR_HTBLPTR_NULL = "'hb_table_t*' is NULL.";

const char *Msgs::ERR_TBLPTR_NULL = "'table_ptr' is NULL.";

const char *Msgs::ERR_TBL_LENPTR_NULL = "'table_length_ptr' is NULL.";

const char *Msgs::ERR_TBL_NULL = "'table' is NULL.";

const char *Msgs::ERR_TBL_LEN = "'table_len' %d is <= 0.";

const char *Msgs::ERR_START_ROW_NULL = "'start_row' is NULL.";

const char *Msgs::ERR_START_ROWL_LEN = "'start_row_len' %d is <= 0.";

const char *Msgs::ERR_END_ROW_NULL = "'end_row' is NULL.";

const char *Msgs::ERR_END_ROWL_LEN = "'end_row_len' %d is <= 0.";

const char *Msgs::ERR_FAMILIES_NULL = "'families[]' is NULL.";

const char *Msgs::ERR_FAMILY_NULL = "'family' is NULL.";

const char *Msgs::ERR_FAMILY_LEN = "'family_len' %d is <= 0.";

const char *Msgs::ERR_COLDESC_NULL = "'hb_columndesc' is NULL.";

const char *Msgs::ERR_NUM_FAMILIES = "'numFamilies' %d is <= 0.";

const char *Msgs::ERR_MAX_VERSIONS = "'max_versions' is <= 0.";

const char *Msgs::ERR_MIN_VERSIONS = "'min_versions' is < 0.";

const char *Msgs::ERR_TTL = "'ttl' is < 0.";

const char *Msgs::ERR_CELL_NULL = "'hb_cell_t' is NULL.";

const char *Msgs::ERR_CELLPTR_NULL = "'hb_cell_t*' is NULL.";

const char *Msgs::ERR_CELL_COUNTPTR_NULL = "'cell_count_ptr' is NULL.";

const char *Msgs::ERR_KEY_NULL = "'rowkey' is NULL.";

const char *Msgs::ERR_KEY_LEN = "'rowkey_len' %d is <= 0.";

const char *Msgs::ERR_KEYPTR_NULL = "'key_ptr' is NULL.";

const char *Msgs::ERR_KEY_LENPTR = "'key_length_ptr' is NULL.";

const char *Msgs::ERR_QUAL_NULL = "'qualifier' is NULL.";

const char *Msgs::ERR_QUAL_LEN = "'qualifier_len' %d is <= 0.";

const char *Msgs::ERR_VALUE_NULL = "'value' is NULL.";

const char *Msgs::ERR_VALUE_LEN = "'value_len' %d is <= 0.";

const char *Msgs::ERR_DEL_NULL = "'hb_delete_t' is NULL.";

const char *Msgs::ERR_DELPTR_NULL = "'hb_delete_t*' is NULL.";

const char *Msgs::ERR_GET_NULL = "'hb_get_t' is NULL.";

const char *Msgs::ERR_GETPTR_NULL = "'hb_get_t*' is NULL.";

const char *Msgs::ERR_RESULT_NULL = "'hb_result_t' is NULL.";

const char *Msgs::ERR_RESULTPTR_NULL = "'hb_result_t*' is NULL.";

const char *Msgs::ERR_PUT_NULL = "'hb_put_t' is NULL.";

const char *Msgs::ERR_PUTPTR_NULL = "'hb_put_t*' is NULL.";

const char *Msgs::ERR_MUTATION_NULL = "'hb_mutation_t' is NULL.";

const char *Msgs::ERR_MUTATIONPTR_NULL = "'hb_mutation_t*' is NULL.";

const char *Msgs::ERR_MUTATIONS_NULL = "'hb_mutation_t[]' is NULL.";

const char *Msgs::ERR_NUM_MUTATIONS = "'num_mutations' %d is <= 0.";

const char *Msgs::ERR_PUTS_NULL = "'hb_put_t[]' is NULL.";

const char *Msgs::ERR_NUM_PUTS = "'num_puts' %d is <= 0.";

const char *Msgs::ERR_TIMESTAMP1 = "'timestamp' "PRIu64" is <= 0.";

const char *Msgs::ERR_TIMESTAMP2 = "'timestamp' "PRIu64" is <= 0 and != -1.";

const char *Msgs::ERR_NUM_VERSIONS = "'num_versions' is <= 0.";

const char *Msgs::ERR_TABLE_ALREADY_DISABLED =
    "DisableTable() called for '%s' which is already disabled.";

const char *Msgs::ERR_TABLE_ALREADY_ENABLED =
    "EnableTable() called for '%s' which is already enabled.";

const char *Msgs::ERR_SCANNER_NULL = "'hb_scanner_t' is NULL.";

const char *Msgs::ERR_SCANNERPTR_NULL = "'hb_scanner_t*' is NULL.";

const char *Msgs::ERR_CACHE_SIZE = "'cache_size' %d is <= 0.";

const char *Msgs::ERR_SCANNER_OPEN = "Scanner is already open.";

} // namespace hbase
