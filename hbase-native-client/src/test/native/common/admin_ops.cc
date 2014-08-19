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
#line 19 "admin_ops.cc" // ensures short filename in logs.

#include <errno.h>

#include <hbase/hbase.h>

#include "test_types.h"

namespace hbase {
namespace test {

#define COULD_NOT_CREATE_TABLE "Could not create HBase admin : errorCode = %d."
#define TABLE_EXISTS_DELETING "Table '%s' exists, deleting..."
#define COULD_NOT_DELETE_TABLE "Could not delete table %s[%d]."
#define TABLE_EXISTS "Table '%s' exists, will use it for test."
#define ERROR_WHILE_TABLE_EXISTS "Error while checking if the table exists: errorCode = %d."
#define CREATING_TABLE "Creating table '%s'..."
#define TABLE_IS_ENABLED_OR "Table '%s' is %senabled, result %d."
#define TABLE_CREATED_VERIFYING "Table '%s' created, verifying if enabled."

int32_t
ensureTable(
    hb_connection_t connection,
    bool createNew,
    const char* table_name,
    bytebuffer families[],
    size_t numFamilies) {
  int32_t retCode = 0;
  hb_admin_t admin = NULL;
  hb_columndesc *hcds = NULL;

  if ((retCode = hb_admin_create(connection, &admin)) != 0) {
    HBASE_LOG_ERROR(COULD_NOT_CREATE_TABLE, retCode);
    goto cleanup;
  }

  if ((retCode = hb_admin_table_exists(admin, NULL, table_name)) == 0) {
    if (createNew) {
      HBASE_LOG_INFO(TABLE_EXISTS_DELETING, table_name);
      if ((retCode = hb_admin_table_delete(admin, NULL, table_name)) != 0) {
        HBASE_LOG_ERROR(COULD_NOT_DELETE_TABLE, table_name, retCode);
        goto cleanup;
      }
    } else {
      HBASE_LOG_INFO(TABLE_EXISTS, table_name);
      goto cleanup;
    }
  } else if (retCode != ENOENT) {
    HBASE_LOG_ERROR(ERROR_WHILE_TABLE_EXISTS, retCode);
    goto cleanup;
  }

  hcds = new hb_columndesc[numFamilies];
  for (size_t i = 0; i < numFamilies; ++i) {
    hb_coldesc_create(families[i]->buffer, families[i]->length, &hcds[i]);
  }

  HBASE_LOG_INFO(CREATING_TABLE, table_name);
  if ((retCode = hb_admin_table_create(
      admin, NULL, table_name, hcds, numFamilies)) == 0) {
    HBASE_LOG_INFO(TABLE_CREATED_VERIFYING, table_name);
    retCode = hb_admin_table_enabled(admin, NULL, table_name);
    HBASE_LOG_MSG((retCode ? HBASE_LOG_LEVEL_ERROR : HBASE_LOG_LEVEL_INFO),
        TABLE_IS_ENABLED_OR, table_name, (retCode?"not ":""));
  }

  for (size_t i = 0; i < numFamilies; ++i) {
    hb_coldesc_destroy(hcds[i]);
  }

cleanup:
  if (hcds) {
    delete[] hcds;
  }
  if (admin) {
    hb_admin_destroy(admin, NULL, NULL);
  }
  return retCode;
}

} /* namespace test */
} /* namespace hbase */
