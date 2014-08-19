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
#ifndef HBASE_TEST_UTIL_H
#define HBASE_TEST_UTIL_H

#include <string>
#include <vector>

#include <sys/errno.h>
#include <sys/types.h>

#include <hbase/hbase.h>

#include "byte_buffer.h"

#define CELL_NOT_FOUND_ERR 0xFF12
#define MISMATCH_RECEIVED_CELL_COUNT 0xFF13
#define MISMATCH_RECEIVED_ROW_COUNT 0xFF14
#define MISMATCH_MAX_ROW_COUNT 0xFF15

#define HBASE_MSG_LEN 1024

namespace hbase {
namespace test {

int32_t
InitLogger(std::string);

int32_t
CloseLogger();

std::string
GetConfigParameter(std::string);

std::vector<std::string>
SplitString(const std::string &strLine, char delim);

typedef struct ut_cell_data_t_ {
  bytebuffer value_;
  bytebuffer columnFamily_;
  bytebuffer columnName_;
  bytebuffer ts_;
  hb_cell_t *hb_cell;
  struct ut_cell_data_t_ *next_cell;
} ut_cell_data_t;

typedef struct row_data_t_ {
  bytebuffer key_;
  struct ut_cell_data_t_ *first_cell;
} row_data_t;

typedef struct scan_data_t_ {
  bytebuffer table_name_;
  bytebuffer start_row_key_;
  bytebuffer end_row_key_;
  bytebuffer name_space;
} scan_data_t;

extern hb_connection_t connection;
extern hb_client_t client;
extern hb_admin_t admin;
extern uint64_t expectedNumberOfCellCount;
extern uint64_t expectedNumberOfRowCount;
extern uint64_t maxNumberOfRows;

int32_t connectionCreate(const char *zk_ensemble, const char *zk_root_znode);
int32_t clientCreate();
int32_t adminCreate();
int32_t connectionDestroy();
int32_t adminDestroy();
int32_t clientDestroy();
int32_t deleteTableIfExists(const char *table_name);
int32_t createTable(const char *table_name, std::vector<std::string> columnFamilies,
    int maxVersions = 3, int minVersions = 3, int32_t ttl = 99999999/*some max value*/);
int32_t disableTable(const char *table_name);
int32_t enableTable(const char *table_name);
int32_t deleteTable(const char *table_name);
int32_t isTableEnabled(const char *table_name);
void setLogLevel(HBaseLogLevel level);
HBaseLogLevel getLogLevel();
int32_t clientFlush();
int32_t putRow(std::string table_name, std::string row_key, std::vector<std::string> row_data,
    uint64_t ts = HBASE_LATEST_TIMESTAMP, hb_durability_t durability = DURABILITY_USE_DEFAULT);
int32_t putRowAndWait(std::string table_name, std::string row_key, std::vector<std::string> data,
    uint64_t ts = HBASE_LATEST_TIMESTAMP);
int32_t getVerifyRow(std::string table_name, std::string row_key, std::vector<std::string> row_data,
bool isEntireRow = false, bool isEntireCF = false, int versions = 3, uint64_t min_ts = 0,
    uint64_t max_ts = 0);
int32_t deleteRow(std::string table_name, std::string row_key, std::vector<std::string> row_data,
    uint64_t ts = HBASE_LATEST_TIMESTAMP);
int32_t scanTable(std::string table_name, int32_t num_versions = 1, uint64_t max_num_rows = 0,
    std::string start_row_key = "", std::string end_row_key = "", std::string name_space = "");
bool waitForPutsToComplete();

} /* namespace test */
} /* namespace hbase */

#endif /* HBASE_TEST_UTIL_H*/
