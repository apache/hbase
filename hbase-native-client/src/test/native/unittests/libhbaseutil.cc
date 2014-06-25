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
#line 19 "libhbaseutil.cc" // ensures short filename in logs.

#include <fstream>
#include <map>

#include <pthread.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/unistd.h>

#include "common_utils.h"
#include "libhbaseutil.h"

namespace hbase {
namespace test {

static int32_t retCode_delete_row = 0;
static int32_t ret_get_row = 0;
static int32_t ret_put_row = 0;
static int32_t ret_client_flush = 0;
static int32_t ret_scan = 0;
static uint64_t scan_num_rows = 0;
static uint64_t scan_cell_count = 0;

void client_disconnection_callback(int32_t err, hb_client_t client, void *extra);
void wait_client_disconnection();
void waitForGetsToComplete();
void waitForDeletesToComplete();
void waitForScanToComplete();

void delete_callback(int32_t err, hb_client_t client, hb_mutation_t del, hb_result_t result,
    void *extra);
void put_callback(int32_t retCode, hb_client_t client, hb_mutation_t mutation, hb_result_t result,
    void *extra);
void get_callback(int32_t status, hb_client_t client, hb_get_t get, hb_result_t result,
    void *extra);
void scan_callback(int32_t err, hb_scanner_t scanner, hb_result_t results[], size_t num_results,
    void *extra);
void release_row_data(row_data_t *row_data);
void release_scan_data(scan_data_t *scan_data);
char HBASE_MSG_BUFF[HBASE_MSG_LEN] = "";
volatile int32_t client_destroyed = false;
pthread_cond_t client_destroyed_cv;
pthread_mutex_t client_destroyed_mutex;

static volatile int32_t get_done = false;
static pthread_cond_t get_cv = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t get_mutex = PTHREAD_MUTEX_INITIALIZER;

static volatile int32_t outstanding_puts_count;
static pthread_cond_t puts_cv = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t puts_mutex = PTHREAD_MUTEX_INITIALIZER;

static volatile int32_t delete_done = false;
static pthread_cond_t del_cv = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t del_mutex = PTHREAD_MUTEX_INITIALIZER;

static volatile bool flush_done = false;
static pthread_cond_t flush_cv = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t flush_mutex = PTHREAD_MUTEX_INITIALIZER;

static volatile bool scan_done = false;
static pthread_cond_t scan_cv = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t scan_mutex = PTHREAD_MUTEX_INITIALIZER;

hb_cell_t *cell[64] = { NULL };
ut_cell_data_t *cell_data[64] = { NULL };

hb_connection_t connection = NULL;
hb_client_t client = NULL;
hb_admin_t admin = NULL;

ut_cell_data_t*
new_ut_cell_data() {
  ut_cell_data_t *cell_data = (ut_cell_data_t*) calloc(1, sizeof(ut_cell_data_t));
  cell_data->next_cell = NULL;
  return cell_data;
}

int32_t
connectionCreate(
    const char* zk_quorum,
    const char *zk_root_node) {
  int retCode = 0;
  HBASE_LOG_INFO("creating connection with zk ensemble :%s root znode:%s ", zk_quorum,
      zk_root_node);
  if ((retCode = hb_connection_create(zk_quorum, zk_root_node, &connection)) != 0) {

    HBASE_LOG_FATAL("Could not create HBase connection returncode:%ld", retCode);
    return retCode;
  }

  else
  HBASE_LOG_INFO("connection crate is successful");
  return retCode;

}

int32_t
clientCreate() {
  int retCode = 0;
  if (!connection) {
    HBASE_LOG_INFO("not a valid connection");
    return retCode;
  }
  HBASE_LOG_INFO("creating clinet");
  if ((retCode = hb_client_create(connection, &client)) != 0) {

    HBASE_LOG_INFO("could not create client return code:%ld", retCode);
    return retCode;
  } else
  HBASE_LOG_INFO("client creation is successful");

  return retCode;

}

int32_t
adminCreate() {
  int retCode = 0;
  if ((retCode = hb_admin_create(connection, &admin)) != 0) {

    HBASE_LOG_INFO("could not create admin return code:%ld", retCode);
  } else
  HBASE_LOG_INFO("admin crate is successful");
  return retCode;

}

int32_t
connectionDestroy() {

  return hb_connection_destroy(connection);
}

int32_t
clientDestroy() {
  HBASE_LOG_INFO("client destory called");
  if (client) {
    hb_client_destroy(client, client_disconnection_callback, NULL);
    wait_client_disconnection();
    return 0;
  } else {
    HBASE_LOG_INFO("no valid client object :");
    return 1;
  }

}

int32_t
adminDestroy() {
  if (admin) {
    return hb_admin_destroy(admin, NULL, NULL);
  }
  return 1;
}

void
client_disconnection_callback(
    int32_t err,
    hb_client_t client, void *extra) {
  HBASE_LOG_INFO("Received client disconnection callback.");
  pthread_mutex_lock(&client_destroyed_mutex);
  client_destroyed = true;
  pthread_cond_signal(&client_destroyed_cv);
  pthread_mutex_unlock(&client_destroyed_mutex);
}

void
wait_client_disconnection() {
  pthread_mutex_lock(&client_destroyed_mutex);
  while (!client_destroyed) {
    pthread_cond_wait(&client_destroyed_cv, &client_destroyed_mutex);
  }
  pthread_mutex_unlock(&client_destroyed_mutex);
}

static void
client_flush_callback(
    int32_t err,
    hb_client_t client,
    void *extra) {
  ret_client_flush = err;
  HBASE_LOG_INFO("Received client flush callback.retcode:%ld", err);
  pthread_mutex_lock(&flush_mutex);
  flush_done = true;
  pthread_cond_signal(&flush_cv);
  pthread_mutex_unlock(&flush_mutex);
}

static void
waitForFlush() {
  HBASE_LOG_INFO("Waiting for flush to complete.");
  pthread_mutex_lock(&flush_mutex);
  while (!flush_done) {
    pthread_cond_wait(&flush_cv, &flush_mutex);
  }
  pthread_mutex_unlock(&flush_mutex);
  HBASE_LOG_INFO("Flush completed.");
}

int32_t
clientFlush() {
  int32_t retCode;
  if ((retCode = hb_client_flush(client, client_flush_callback, NULL)) != 0) {
    HBASE_LOG_INFO("hb_client_flush failed with retCode:%ld", retCode);
    return retCode;
  }
  waitForFlush();
  flush_done = false; //reset
  return ret_client_flush;
}

int32_t
deleteTableIfExists(const char *table_name) {
  int32_t retCode = 0;
  if ((retCode = hb_admin_table_exists(admin, NULL, table_name)) == 0) {

    HBASE_LOG_INFO("table :%s already exists deleting the table", table_name);
    if ((retCode = hb_admin_table_delete(admin, NULL, table_name)) != 0) {
      HBASE_LOG_FATAL("Could not delete table: %s return code:", table_name, retCode);
      return retCode;
    }
    HBASE_LOG_INFO(" existing table:%s deleted", table_name);
  } else
  HBASE_LOG_INFO("table already exists returncode : %ld", retCode);
  if (retCode == 2)
    return 0;
  else
    return retCode;
}

int32_t
deleteTable(const char *table_name) {
  int32_t retCode = 0;

  if ((retCode = hb_admin_table_delete(admin, NULL, table_name)) != 0) {
    HBASE_LOG_FATAL("Could not delete table:%s return code:%ld ", table_name, retCode);
  }
  return retCode;
}

void
setLogLevel(HBaseLogLevel level) {
  hb_log_set_level(level);
}

HBaseLogLevel
getLogLevel() {
  return hb_log_get_level();
}

int32_t
disableTable(const char *table_name) {
  int32_t retCode;
  retCode = hb_admin_table_disable(admin, NULL, table_name);
  HBASE_LOG_INFO("disable table: %s return code:%ld", table_name, retCode);
  return retCode;
}

int32_t
enableTable(const char *table_name) {
  int32_t retCode = 0;
  retCode = hb_admin_table_enable(admin, NULL, table_name);
  HBASE_LOG_INFO("enable table: %s return code:%ld", table_name, retCode);
  return retCode;
}

int32_t
isTableEnabled(const char *table_name) {
  int32_t retCode = 0;
  retCode = hb_admin_table_enabled(admin, NULL, table_name);
  HBASE_LOG_INFO("enabled table:%s return code:%ld ", table_name, retCode);
  return retCode;
}

int32_t
createTable(
    const char *table_name,
    std::vector<std::string> columnFamilies,
    int maxVersions,
    int minVersions,
    int32_t ttl) {
  hb_columndesc HCD[100] = { NULL };
  int32_t retCode = 0;

  for (uint32_t t = 0; t < columnFamilies.size(); ++t) {
    byte_t *buffer = (byte_t *) columnFamilies.at(t).c_str();
    HBASE_LOG_DEBUG("column Family:%s ", buffer);

    retCode = hb_coldesc_create(buffer, columnFamilies.at(t).size(), &HCD[t]);
    if (retCode != 0) {
      HBASE_LOG_FATAL("Could not create column descriptor:  returncode: %ld", retCode);
      return retCode;
    }
    hb_coldesc_set_maxversions(HCD[t], maxVersions);
    hb_coldesc_set_minversions(HCD[t], minVersions);
    hb_coldesc_set_ttl(HCD[t], ttl);
    hb_coldesc_set_inmemory(HCD[t], 1);
  }

  HBASE_LOG_INFO("Creating table :%s column size:%d", table_name, columnFamilies.size());
  retCode = hb_admin_table_create(admin, NULL, table_name, HCD, columnFamilies.size());
  if (retCode != 0) {
    HBASE_LOG_FATAL("Could not create table:%s return code:%ld ", table_name, retCode);

  } else {
    HBASE_LOG_INFO("table :%s created successfully ", table_name);
  }

  for (int t = 0; t < (int) columnFamilies.size(); ++t) {
    hb_coldesc_destroy(HCD[t]);
  }

  return retCode;
}

int32_t
putRow(
    std::string table_name,
    std::string row_key,
    std::vector<std::string> data,
    uint64_t ts,
    hb_durability_t durability) {
  srand(time(NULL));
  hb_put_t put = NULL;
  int32_t retCode = 0;

  row_data_t *row_data = (row_data_t *) calloc(1, sizeof(row_data_t));
  row_data->key_ = bytebuffer_printf("%s", row_key.c_str());

  HBASE_LOG_DEBUG("row_data key:%s", row_data->key_);
  hb_put_create(row_data->key_->buffer, row_data->key_->length, &put);
  HBASE_LOG_DEBUG("table name for put:%s", table_name.c_str());
  hb_mutation_set_table(put, (char*) table_name.c_str(), table_name.size());
  hb_mutation_set_durability(put, durability);

  for (int t = 0; t < (int) data.size(); ++t) {
    HBASE_LOG_DEBUG("current data:%s", data.at(t).c_str());
    std::vector<std::string> data_qualifier = SplitString(data.at(t), ':');

    cell_data[t] = new_ut_cell_data();
    if (t == 0)
      row_data->first_cell = cell_data[t];
    else
      cell_data[t - 1]->next_cell = cell_data[t];

    cell_data[t]->columnFamily_ = bytebuffer_printf("%s", data_qualifier[0].c_str());
    cell_data[t]->columnName_ = bytebuffer_printf("%s", data_qualifier[1].c_str());
    cell_data[t]->value_ = bytebuffer_printf("%s", data_qualifier[2].c_str());
    cell[t] = (hb_cell_t*) calloc(1, sizeof(hb_cell_t));
    cell_data[t]->hb_cell = cell[t];

    cell[t] = (hb_cell_t*) calloc(1, sizeof(hb_cell_t));

    cell[t]->row = row_data->key_->buffer;
    cell[t]->row_len = row_data->key_->length;

    cell[t]->family = cell_data[t]->columnFamily_->buffer;
    cell[t]->family_len = cell_data[t]->columnFamily_->length;
    HBASE_LOG_DEBUG("column family :%s length:%d ", cell[t]->family, cell[t]->family_len);
    cell[t]->qualifier = cell_data[t]->columnName_->buffer;
    cell[t]->qualifier_len = cell_data[t]->columnName_->length;
    HBASE_LOG_DEBUG("cell qualifer :%s length:%ld", cell[t]->qualifier, cell[t]->qualifier_len);
    cell[t]->value = cell_data[t]->value_->buffer;
    cell[t]->value_len = cell_data[t]->value_->length;

    cell[t]->ts = ts;
    retCode = hb_put_add_cell(put, cell[t]);
    if (retCode != 0) {

      HBASE_LOG_FATAL("Could not add cel to put:%s return code :%ld ", table_name.c_str(), retCode);
      return retCode;
    }
  }
  outstanding_puts_count++;
  retCode = hb_mutation_send(client, put, put_callback, row_data);
  if (retCode) {
    HBASE_LOG_FATAL("mutation send is failed for table:%s return code:%ld ", table_name.c_str(),
        retCode);
    return retCode;
  } else
  HBASE_LOG_DEBUG("hb_mutation_send for put is success");

  return retCode;
}

int32_t
putRowAndWait(
    std::string table_name,
    std::string row_key_,
    std::vector<std::string> data,
    uint64_t ts) {
  int32_t retCode = 0;
  retCode = putRow(table_name, row_key_, data, ts);
  if (retCode) {
    return retCode;
  }

  HBASE_LOG_INFO("Waiting for put operation to complete.");
  waitForPutsToComplete();

  return ret_put_row;
}

void
put_callback(
    int32_t retCode,
    hb_client_t client,
    hb_mutation_t mutation,
    hb_result_t result,
    void *extra) {
  ret_put_row = retCode;
  row_data_t* row_data = (row_data_t *) extra;
  HBASE_LOG_DEBUG("Received  callback for row key:%s retCode:%ld ", row_data->key_->buffer,
      retCode);
  if (retCode == EAGAIN || retCode == ENOBUFS) {
    HBASE_LOG_FATAL("received again/nobufs error retrying to send the put again for row key:%s",
        row_data->key_->buffer);
    sleep(1);
    hb_mutation_send(client, mutation, put_callback, extra);
    return;
  }

  if (retCode != 0) {
    HBASE_LOG_FATAL("fail to put row:%s return code:%d ", row_data->key_->buffer, retCode);
  }

  HBASE_LOG_DEBUG("Received  callback for row key:%s return code:%d ",
      row_data->key_->buffer, retCode);
  release_row_data(row_data);
  hb_mutation_destroy(mutation);

  pthread_mutex_lock(&puts_mutex);
  outstanding_puts_count--;
  if (outstanding_puts_count == 0) {
    pthread_cond_signal(&puts_cv);
  }
  pthread_mutex_unlock(&puts_mutex);
}

bool
waitForPutsToComplete() {
  HBASE_LOG_INFO("in wait for puts:");
  pthread_mutex_lock(&puts_mutex);
  while (outstanding_puts_count > 0) {
    pthread_cond_wait(&puts_cv, &puts_mutex);
  }
  pthread_mutex_unlock(&puts_mutex);
  HBASE_LOG_INFO(" wait for puts done");
  return true;
}

void
release_row_data(row_data_t *row_data) {
  if (row_data != NULL) {
    ut_cell_data_t *cell = row_data->first_cell;
    while (cell) {
      if (cell->value_)
        bytebuffer_free(cell->value_);

      if (cell->columnFamily_)
        bytebuffer_free(cell->columnFamily_);
      if (cell->columnName_)
        bytebuffer_free(cell->columnName_);
      free(cell->hb_cell);
      ut_cell_data_t *cur_cell = cell;
      cell = cell->next_cell;
      free(cur_cell);
    }
    bytebuffer_free(row_data->key_);
    free(row_data);
  }
}

int32_t
getVerifyRow(
    std::string table_name,
    std::string row_key,
    std::vector<std::string> data,
    bool isEntireRow,
    bool isEntireCF,
    int versions,
    uint64_t min_ts,
    uint64_t max_ts) {
  int32_t retCode;
  row_data_t *row_data = (row_data_t *) calloc(1, sizeof(row_data_t));
  row_data->key_ = bytebuffer_printf("%s", row_key.c_str());

  hb_get_t get = NULL;
  hb_get_create(row_data->key_->buffer, row_data->key_->length, &get);
  // time range is not yet supported
  /*if(min_ts || max_ts)
   {
   HBASE_LOG_INFO("time range for get is specified min:",min_ts,"max:"<<max_ts);
   hb_get_set_timerange(get,min_ts,max_ts);
   isEntireRow=true;
   }*/

  for (int t = 0; t < (int) data.size(); ++t) {
    std::vector<std::string> data_qualifier = SplitString(data.at(t), ':');

    cell_data[t] = new_ut_cell_data();
    if (t == 0) {
      row_data->first_cell = cell_data[t];
    }
    else {
      cell_data[t - 1]->next_cell = cell_data[t];
    }

    cell_data[t]->columnFamily_ = bytebuffer_printf("%s", data_qualifier[0].c_str());
    if (data_qualifier.size() > 1) { // column qualifer
      cell_data[t]->columnName_ = bytebuffer_printf("%s", data_qualifier[1].c_str());
    }

    // even for getting entire CF need to fill cell_data with cols,
    // so that in get_callback we can validate the cells
    if (!isEntireRow) {
      HBASE_LOG_INFO("adding columns");
      if (!isEntireCF) {
        hb_get_add_column(get, cell_data[t]->columnFamily_->buffer,
            cell_data[t]->columnFamily_->length, cell_data[t]->columnName_->buffer,
            cell_data[t]->columnName_->length);
      } else {
        hb_get_add_column(get, cell_data[t]->columnFamily_->buffer,
            cell_data[t]->columnFamily_->length, NULL, 0);
      }
    }
  }
  hb_get_set_table(get, (char*) table_name.c_str(), table_name.size());
  hb_get_set_num_versions(get, versions);

  ret_get_row = 0;  //reset retCode_get_row
  get_done = false;  //reset  flag

  if ((retCode = hb_get_send(client, get, get_callback, row_data)) != 0) {
    HBASE_LOG_INFO("hb_get_send failed with return code:%d", retCode);
    return retCode;
  }
  waitForGetsToComplete();

  return ret_get_row;
}

void
waitForGetsToComplete() {
  HBASE_LOG_INFO("in wait for get");
  pthread_mutex_lock(&get_mutex);
  while (!get_done) {
    pthread_cond_wait(&get_cv, &get_mutex);
  }
  pthread_mutex_unlock(&get_mutex);
  HBASE_LOG_INFO("wait for get done.");
}

void
get_callback(
    int32_t status,
    hb_client_t client,
    hb_get_t get,
    hb_result_t result,
    void *extra) {
  const char *table_name;
  size_t table_name_len;
  size_t cell_count;
  HBASE_LOG_INFO("get_callback return code:%ld", status);
  ret_get_row = status;
  row_data_t *row_data = (row_data_t *) extra;
  ut_cell_data_t *cell = row_data->first_cell;
  if (status == 0) {
    hb_result_get_cell_count(result, &cell_count);
    hb_result_get_table(result, &table_name, &table_name_len);
    const hb_cell_t **cells;
    hb_result_get_cells(result, &cells, &cell_count);
    HBASE_LOG_INFO("cell count:%d", cell_count);
    if (expectedNumberOfCellCount != 0 && cell_count != expectedNumberOfCellCount) {
      HBASE_LOG_INFO("expected cell count:%ld actual result count:%ld",
          expectedNumberOfCellCount, cell_count);
      ret_get_row = MISMATCH_RECEIVED_CELL_COUNT;
    }

    const hb_cell_t *mycell;
    while (cell) {
      HBASE_LOG_INFO("getting result for cf:%s", cell->columnFamily_->buffer);
      if (hb_result_get_cell(result, cell->columnFamily_->buffer, cell->columnFamily_->length,
          cell->columnName_->buffer, cell->columnName_->length, &mycell) == 0) {
        HBASE_LOG_INFO("Cell found value :%s", mycell->value);
      } else {
        HBASE_LOG_INFO("Cell not found.");
        // This error is specific to Testcode, for cell not found
        ret_get_row = CELL_NOT_FOUND_ERR;
      }
      cell = cell->next_cell;
    }
  }
  hb_get_destroy(get);
  hb_result_destroy(result);
  release_row_data(row_data);
  pthread_mutex_lock(&get_mutex);
  get_done = true;
  pthread_cond_signal(&get_cv);
  pthread_mutex_unlock(&get_mutex);
}

int32_t
deleteRow(
    std::string table_name,
    std::string row_key,
    std::vector<std::string> data) {
  return deleteRow(table_name, row_key, data, HBASE_LATEST_TIMESTAMP);
}

// Input: data is in format "CF:Column"
//        If there is only "CF", then delete all the columns of that CF
//  If data is NULL, then delete the entire row
int32_t
deleteRow(
    std::string table_name,
    std::string row_key,
    std::vector<std::string> data,
    uint64_t ts) {
  int32_t retCode = 0;

  hb_delete_t del = NULL;
  row_data_t *row_data = (row_data_t *) calloc(1, sizeof(row_data_t));
  row_data->key_ = bytebuffer_printf("%s", row_key.c_str());

  HBASE_LOG_INFO("row_data key:%s", row_data->key_);

  hb_delete_create(row_data->key_->buffer, row_data->key_->length, &del);

  // If data is NULL, then delete the entire row
  for (int t = 0; t < (int) data.size(); t++) {
    std::vector<std::string> data_qualifier = SplitString(data.at(t), ':');
    if (data_qualifier[0].size() == 0) {
      HBASE_LOG_INFO("Deleting the entire row.");
      break;
    }

    if (data_qualifier.size() == 1) {
      HBASE_LOG_INFO("Deleting all the columns of CF:%s", data_qualifier[0].c_str());
      hb_delete_add_column(del, (byte_t *) data_qualifier[0].c_str(), data_qualifier[0].size(),
          NULL, 0, ts);
    } else {
      HBASE_LOG_INFO("Deleting cf:%s column:%s", data_qualifier[0].c_str(),
          data_qualifier[1].c_str());
      hb_delete_add_column(del, (byte_t *) data_qualifier[0].c_str(), data_qualifier[0].size(),
          (byte_t *) data_qualifier[1].c_str(), data_qualifier[1].size(), ts);
    }
  }

  hb_mutation_set_table(del, (char*) table_name.c_str(), table_name.size());
  retCode_delete_row = 0;  //reset retCode_delete_row
  delete_done = false;  //reset delete_done flag
  if ((retCode = hb_mutation_send(client, del, delete_callback, row_data)) != 0) {
    HBASE_LOG_ERROR("hb_mutation_send for delete failed:%d", retCode);
    return retCode;
  }

  HBASE_LOG_INFO("hb_mutation_send for delete is success");
  HBASE_LOG_INFO("Waiting for delete operation to complete.");
  waitForDeletesToComplete();
  HBASE_LOG_INFO("Delete operation completed.");
  return retCode_delete_row;
}

void
delete_callback(
    int32_t retCode,
    hb_client_t client,
    hb_mutation_t del,
    hb_result_t result,
    void *extra) {
  row_data_t* row_data = (row_data_t *) extra;
  HBASE_LOG_INFO("Received delete_callback for row key_:%s retcode:%d ",
      row_data->key_->buffer, retCode);
  if (retCode != 0) {
    HBASE_LOG_FATAL("fail to delete row:%s retcode:%d ", row_data->key_->buffer, retCode);
  }

  retCode_delete_row = retCode;
  hb_mutation_destroy(del);
  pthread_mutex_lock(&del_mutex);
  delete_done = true;
  pthread_cond_signal(&del_cv);
  pthread_mutex_unlock(&del_mutex);
}

void
waitForDeletesToComplete() {
  HBASE_LOG_INFO("in wait for delete");
  pthread_mutex_lock(&del_mutex);
  while (!delete_done) {
    pthread_cond_wait(&del_cv, &del_mutex);
  }
  pthread_mutex_unlock(&del_mutex);
  HBASE_LOG_INFO("wait for delete done.");
}

int32_t
scanTable(
    std::string table_name,
    int32_t num_versions,
    uint64_t max_num_rows,
    std::string start_row_key,
    std::string end_row_key,
    std::string name_space) {
  hb_scanner_t scanner = NULL;
  hb_scanner_create(client, &scanner);
  int32_t retCode;
  scan_num_rows = 0;
  scan_cell_count = 0;
  scan_done = false;

  scan_data_t *scan_data = (scan_data_t *) calloc(1, sizeof(scan_data_t));
  scan_data->table_name_ = bytebuffer_printf("%s", table_name.c_str());

  if ((retCode = hb_scanner_set_table(scanner, (char *) scan_data->table_name_->buffer,
      scan_data->table_name_->length)) != 0) {
    HBASE_LOG_INFO("scanner set table failed with return code:%d", retCode);
    return retCode;
  }

  if (!start_row_key.empty()) {
    scan_data->start_row_key_ = bytebuffer_printf("%s", start_row_key.c_str());
    hb_scanner_set_start_row(scanner, scan_data->start_row_key_->buffer,
        scan_data->start_row_key_->length);
  }

  if (!end_row_key.empty()) {
    scan_data->end_row_key_ = bytebuffer_printf("%s", end_row_key.c_str());
    hb_scanner_set_end_row(scanner, scan_data->end_row_key_->buffer,
        scan_data->end_row_key_->length);
  }

  hb_scanner_set_num_versions(scanner, num_versions);

  if (max_num_rows != 0) {
    HBASE_LOG_INFO("setting max num rows for scan :%ld", max_num_rows);
    hb_scanner_set_num_max_rows(scanner, max_num_rows);
  }

  retCode = hb_scanner_next(scanner, scan_callback, scan_data);
  if (retCode == 0) {
    waitForScanToComplete();
    return ret_scan;
  } else {
    HBASE_LOG_INFO(" scanner failed with retcode:%ld", retCode);
  }

  return retCode;
}

void
scan_callback(
    int32_t err,
    hb_scanner_t scanner,
    hb_result_t results[],
    size_t num_results,
    void *extra) {
  HBASE_LOG_INFO("scan_callback return code:%ld", err);
  ret_scan = err;
  scan_data_t* scan_data = (scan_data_t *) extra;

  if (num_results) {
    if (maxNumberOfRows != 0 && maxNumberOfRows < num_results) {
      HBASE_LOG_INFO("max number of rows:%ld results in scan:%ld", maxNumberOfRows, num_results);
      err = MISMATCH_MAX_ROW_COUNT;
    }

    scan_num_rows += num_results;
    const char *table_name;
    size_t table_name_len;
    hb_result_get_table(results[0], &table_name, &table_name_len);
    HBASE_LOG_INFO("Received scan_next callback for table:%s results:%ld", table_name, num_results);

    for (uint32_t i = 0; i < num_results; ++i) {
      size_t cell_count = 0;
      hb_result_get_cell_count(results[i], &cell_count);
      const byte_t *key = NULL;
      size_t key_len = 0;
      hb_result_get_key(results[i], &key, &key_len);
      HBASE_LOG_DEBUG("scanned key:%s", key);

      scan_cell_count += cell_count;
      hb_result_destroy(results[i]);
    }
    hb_scanner_next(scanner, scan_callback, extra);
  } else {
    HBASE_LOG_INFO("scan completed");
    if (scan_num_rows != expectedNumberOfRowCount) {
      HBASE_LOG_INFO("expected row count:%ld actual scan count:%ld",
          expectedNumberOfRowCount, scan_num_rows);
      ret_scan = MISMATCH_RECEIVED_ROW_COUNT;
    }

    if (expectedNumberOfCellCount && scan_cell_count != expectedNumberOfCellCount) {
      HBASE_LOG_INFO("expected cell count:%ld actual scan count:%ld",
          expectedNumberOfCellCount, scan_cell_count);
      ret_scan = MISMATCH_RECEIVED_CELL_COUNT;
    }

    release_scan_data(scan_data);
    hb_scanner_destroy(scanner, NULL, NULL);
    pthread_mutex_lock(&scan_mutex);
    scan_done = true;
    pthread_cond_signal(&scan_cv);
    pthread_mutex_unlock(&scan_mutex);
  }
}

void
release_scan_data(scan_data_t *scan_data) {
  if (scan_data != NULL) {
    if (scan_data->table_name_)
      bytebuffer_free(scan_data->table_name_);
    if (scan_data->start_row_key_)
      bytebuffer_free(scan_data->start_row_key_);
    if (scan_data->end_row_key_)
      bytebuffer_free(scan_data->end_row_key_);
  }
}

void
waitForScanToComplete() {
  HBASE_LOG_INFO("Waiting for scan to complete.");
  pthread_mutex_lock(&scan_mutex);
  while (!scan_done) {
    pthread_cond_wait(&scan_cv, &scan_mutex);
  }
  pthread_mutex_unlock(&scan_mutex);
  HBASE_LOG_INFO("Scan completed.");
}

/**
 * Other utility functions
 */
static std::map<std::string, std::string> s_ConfigOptions;

static const char *CONFIG_FILE_PATH = "src/test/resources/config.properties";

static int32_t
InitConfig() {
  std::string line, eq, val;
  std::ifstream myfile(CONFIG_FILE_PATH);
  if (myfile.is_open()) {
    while (getline(myfile, line)) {
      if (line.size() == 0 || line[0] == '#') continue;  // skip empty lines and comments
      std::vector<std::string> data = SplitString(line, '=');
      if(data.size() < 2) {
        fprintf(stdout, "Parse error for line '%s'.\n", line.c_str());
        continue;
      }
      const char *env = getenv(data[0].c_str());
      s_ConfigOptions[data[0]] = env ? env : data[1];
    }
    return 0;
  } else {
    fprintf(stdout, "No config file : %s\n",CONFIG_FILE_PATH);
    return 1;
  }
}

std::string
GetConfigParameter(std::string key) {
  if (s_ConfigOptions.empty()) {
    InitConfig();
  }
  return s_ConfigOptions.find(key)->second;
}

std::vector<std::string>
SplitString(const std::string &strLine, char delim) {
  std::string strTempString;
  std::vector<int> splitIndices;
  std::vector<std::string> splitLine;
  int nCharIndex = 0;
  int nLineSize = strLine.size();

  // find indices
  for(int i = 0; i < nLineSize; i++) {
    if(strLine[i] == delim)
      splitIndices.push_back(i);
  }
  splitIndices.push_back(nLineSize); // end index

  // fill split lines
  for(int i = 0; i < (int)splitIndices.size(); i++) {
    strTempString = strLine.substr(nCharIndex, (splitIndices[i] - nCharIndex));
    splitLine.push_back(strTempString);
    nCharIndex = splitIndices[i] + 1;
  }
  return splitLine;
}

static int32_t
EnsureParentDir(const char *filePath) {
  if (!filePath) {
    fprintf(stdout, "filePath is NULL.""\n");
    return EINVAL;
  }

  char *dup_path = strdup(filePath);
  if (dup_path) {
    size_t path_len = strlen(dup_path);
    for (size_t i = 1; i < path_len; ++i) { // skip first '/', if exists.
      if (dup_path[i] == '/' || dup_path[i] == '\\') {
        dup_path[i] = '\0';
        if (mkdir(dup_path, 0) && errno != EEXIST) {
          fprintf(stdout, "Failed to create folder %s.""\n", dup_path);
          perror(NULL);
          free(dup_path);
          return errno;
        }
        dup_path[i] = '/';
      }
    }
    free(dup_path);
  }
  return 0;
}

static FILE *logFile = NULL;

int32_t
InitLogger(std::string logFilePath) {
  const char *log_file_path = logFilePath.c_str();
  if (EnsureParentDir(log_file_path)) {
    return errno;
  }

  fprintf(stdout, "Initializing HBase logger with file: %s\n", log_file_path);
  logFile = fopen(log_file_path, "a");
  if (!logFile) {
    fprintf(stdout, "Unable to open log file \"%s\"", log_file_path);
    perror(NULL);
    return errno;
  }
  hb_log_set_stream(logFile); // defaults to stderr
  return 0;
}

int32_t
CloseLogger() {
  if (logFile) {
    fclose(logFile);
  }
  return 0;
}

} /* namespace test */
} /* namespace hbase */
