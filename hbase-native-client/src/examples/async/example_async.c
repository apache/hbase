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
#line 19 "example_async.c" // ensures short filename in logs.

#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <time.h>
#include <unistd.h>

#include <hbase/hbase.h>

/* Found under /libhbase/src/test/native/common */
#include "byte_buffer.h"

/*
 * Sample code to illustrate usage of libhbase APIs
 */

#ifdef __cplusplus
extern  "C" {
#endif

#define CHECK_API_ERROR(retCode, ...) \
    HBASE_LOG_MSG((retCode ? HBASE_LOG_LEVEL_ERROR : HBASE_LOG_LEVEL_INFO), \
        __VA_ARGS__, retCode);

static byte_t *FAMILIES[] = { (byte_t *)"f", (byte_t *)"g" };
static hb_columndesc HCD[2] = { NULL };

typedef struct cell_data_t_ {
  bytebuffer value;
  hb_cell_t  *hb_cell;
  struct cell_data_t_ *next_cell;
} cell_data_t;

cell_data_t*
new_cell_data() {
  cell_data_t *cell_data = (cell_data_t*) calloc(1, sizeof(cell_data_t));
  cell_data->next_cell = NULL;
  return cell_data;
}

typedef struct row_data_t_ {
  bytebuffer key;
  struct cell_data_t_ *first_cell;
} row_data_t;

static void
release_row_data(row_data_t *row_data) {
  if (row_data != NULL) {
    cell_data_t *cell = row_data->first_cell;
    while (cell) {
      bytebuffer_free(cell->value);
      free(cell->hb_cell);
      cell_data_t *cur_cell = cell;
      cell = cell->next_cell;
      free(cur_cell);
    }
    bytebuffer_free(row_data->key);
    free(row_data);
  }
}

/**
 * Put synchronizer and callback
 */
static volatile int32_t outstanding_puts_count;
static pthread_cond_t puts_cv = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t puts_mutex = PTHREAD_MUTEX_INITIALIZER;

static void
put_callback(int32_t err, hb_client_t client,
    hb_mutation_t mutation, hb_result_t result, void *extra) {
  row_data_t* row_data = (row_data_t *)extra;
  HBASE_LOG_INFO("Received put callback for row \'%.*s\', result = %d.",
      row_data->key->length, row_data->key->buffer, err);
  release_row_data(row_data);
  hb_mutation_destroy(mutation);

  pthread_mutex_lock(&puts_mutex);
  outstanding_puts_count--;
  if (outstanding_puts_count == 0) {
    pthread_cond_signal(&puts_cv);
  }
  pthread_mutex_unlock(&puts_mutex);
}

static void
wait_for_puts() {
  HBASE_LOG_INFO("Waiting for outstanding puts to complete.");
  pthread_mutex_lock(&puts_mutex);
  while (outstanding_puts_count > 0) {
    pthread_cond_wait(&puts_cv, &puts_mutex);
  }
  pthread_mutex_unlock(&puts_mutex);
  HBASE_LOG_INFO("Put operations completed.");
}

/**
 * Flush synchronizer and callback
 */
static volatile bool flush_done = false;
static pthread_cond_t flush_cv = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t flush_mutex = PTHREAD_MUTEX_INITIALIZER;

static void
client_flush_callback(int32_t err,
    hb_client_t client, void *extra) {
  HBASE_LOG_INFO("Received client flush callback.");
  pthread_mutex_lock(&flush_mutex);
  flush_done = true;
  pthread_cond_signal(&flush_cv);
  pthread_mutex_unlock(&flush_mutex);
}

static void
wait_for_flush() {
  HBASE_LOG_INFO("Waiting for flush to complete.");
  pthread_mutex_lock(&flush_mutex);
  while (!flush_done) {
    pthread_cond_wait(&flush_cv, &flush_mutex);
  }
  pthread_mutex_unlock(&flush_mutex);
  HBASE_LOG_INFO("Flush completed.");
}

static void printRow(const hb_result_t result) {
  const byte_t *key = NULL;
  size_t key_len = 0;
  hb_result_get_key(result, &key, &key_len);
  size_t cell_count = 0;
  hb_result_get_cell_count(result, &cell_count);
  HBASE_LOG_INFO("Row=\'%.*s\', cell count=%d", key_len, key, cell_count);
  const hb_cell_t **cells;
  hb_result_get_cells(result, &cells, &cell_count);
  for (size_t i = 0; i < cell_count; ++i) {
    HBASE_LOG_INFO(
        "Cell %d: family=\'%.*s\', qualifier=\'%.*s\', "
        "value=\'%.*s\', timestamp=%lld.", i,
        cells[i]->family_len, cells[i]->family,
        cells[i]->qualifier_len, cells[i]->qualifier,
        cells[i]->value_len, cells[i]->value, cells[i]->ts);
  }
}

/**
 * Get synchronizer and callback
 */
static volatile bool get_done = false;
static pthread_cond_t get_cv = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t get_mutex = PTHREAD_MUTEX_INITIALIZER;

static void
get_callback(int32_t err, hb_client_t client,
    hb_get_t get, hb_result_t result, void *extra) {
  bytebuffer rowKey = (bytebuffer)extra;
  if (err == 0) {
    const char *table_name;
    size_t table_name_len;
    hb_result_get_table(result, &table_name, &table_name_len);
    HBASE_LOG_INFO("Received get callback for table=\'%.*s\'.",
        table_name_len, table_name);

    printRow(result);

    const hb_cell_t *mycell;
    bytebuffer qualifier = bytebuffer_strcpy("column-a");
    HBASE_LOG_INFO("Looking up cell for family=\'%s\', qualifier=\'%.*s\'.",
        FAMILIES[0], qualifier->length, qualifier->buffer);
    if (hb_result_get_cell(result, FAMILIES[0], 1, qualifier->buffer,
        qualifier->length, &mycell) == 0) {
      HBASE_LOG_INFO("Cell found, value=\'%.*s\', timestamp=%lld.",
          mycell->value_len, mycell->value, mycell->ts);
    } else {
      HBASE_LOG_ERROR("Cell not found.");
    }
    bytebuffer_free(qualifier);
    hb_result_destroy(result);
  } else {
    HBASE_LOG_ERROR("Get failed with error code: %d.", err);
  }

  bytebuffer_free(rowKey);
  hb_get_destroy(get);

  pthread_mutex_lock(&get_mutex);
  get_done = true;
  pthread_cond_signal(&get_cv);
  pthread_mutex_unlock(&get_mutex);
}

static void
wait_for_get() {
  HBASE_LOG_INFO("Waiting for get operation to complete.");
  pthread_mutex_lock(&get_mutex);
  while (!get_done) {
    pthread_cond_wait(&get_cv, &get_mutex);
  }
  pthread_mutex_unlock(&get_mutex);
  HBASE_LOG_INFO("Get operation completed.");
}

/**
 * Delete synchronizer and callbacks
 */
static volatile bool delete_done = false;
static pthread_cond_t del_cv = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t del_mutex = PTHREAD_MUTEX_INITIALIZER;

static void
delete_callback(int32_t err, hb_client_t client,
    hb_mutation_t delete, hb_result_t result, void *extra) {
  bytebuffer rowKey = (bytebuffer)extra;
  HBASE_LOG_INFO("Received delete callback for row \'%.*s\', "
      "result = %d.", rowKey->length, rowKey->buffer, err);

  hb_mutation_destroy(delete);
  pthread_mutex_lock(&del_mutex);
  delete_done = true;
  pthread_cond_signal(&del_cv);
  pthread_mutex_unlock(&del_mutex);
}

static void
wait_for_delete() {
  HBASE_LOG_INFO("Waiting for delete operation to complete.");
  pthread_mutex_lock(&del_mutex);
  while (!delete_done) {
    pthread_cond_wait(&del_cv, &del_mutex);
  }
  pthread_mutex_unlock(&del_mutex);
  HBASE_LOG_INFO("Delete operation completed.");
}

/**
 * Scan synchronizer and callbacks
 */
static volatile bool scan_done = false;
static pthread_cond_t scan_cv = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t scan_mutex = PTHREAD_MUTEX_INITIALIZER;

void scan_callback(int32_t err, hb_scanner_t scanner,
      hb_result_t results[], size_t num_results, void *extra) {
  if (num_results) {
    const char *table_name;
    size_t table_name_len;
    hb_result_get_table(results[0], &table_name, &table_name_len);
    HBASE_LOG_INFO("Received scan_next callback for table=\'%.*s\', row count=%d.",
        table_name_len, table_name, num_results);

    for (int i = 0; i < num_results; ++i) {
      printRow(results[i]);
      hb_result_destroy(results[i]);
    }
    hb_scanner_next(scanner, scan_callback, NULL);
  } else {
    hb_scanner_destroy(scanner, NULL, NULL);
    pthread_mutex_lock(&scan_mutex);
    scan_done = true;
    pthread_cond_signal(&scan_cv);
    pthread_mutex_unlock(&scan_mutex);
  }
}

static void
wait_for_scan() {
  HBASE_LOG_INFO("Waiting for scan to complete.");
  pthread_mutex_lock(&scan_mutex);
  while (!scan_done) {
    pthread_cond_wait(&scan_cv, &scan_mutex);
  }
  pthread_mutex_unlock(&scan_mutex);
  HBASE_LOG_INFO("Scan completed.");
}

/**
 * Client destroy synchronizer and callbacks
 */
static volatile bool client_destroyed = false;
static pthread_cond_t client_destroyed_cv = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t client_destroyed_mutex = PTHREAD_MUTEX_INITIALIZER;

static void
client_disconnection_callback(int32_t err,
    hb_client_t client, void *extra) {
  HBASE_LOG_INFO("Received client disconnection callback.");
  pthread_mutex_lock(&client_destroyed_mutex);
  client_destroyed = true;
  pthread_cond_signal(&client_destroyed_cv);
  pthread_mutex_unlock(&client_destroyed_mutex);
}

static void
wait_client_disconnection() {
  HBASE_LOG_INFO("Waiting for client to disconnect.");
  pthread_mutex_lock(&client_destroyed_mutex);
  while (!client_destroyed) {
    pthread_cond_wait(&client_destroyed_cv, &client_destroyed_mutex);
  }
  pthread_mutex_unlock(&client_destroyed_mutex);
  HBASE_LOG_INFO("Client disconnected.");
}

static int
ensureTable(hb_connection_t connection, const char *table_name) {
  int32_t retCode = 0;
  hb_admin_t admin = NULL;

  if ((retCode = hb_admin_create(connection, &admin)) != 0) {
    HBASE_LOG_ERROR("Could not create HBase admin : errorCode = %d.", retCode);
    goto cleanup;
  }

  if ((retCode = hb_admin_table_exists(admin, NULL, table_name)) == 0) {
    HBASE_LOG_INFO("Table '%s' exists, deleting...", table_name);
    if ((retCode = hb_admin_table_delete(admin, NULL, table_name)) != 0) {
      HBASE_LOG_ERROR("Could not delete table %s[%d].", table_name, retCode);
      goto cleanup;
    }
  } else if (retCode != ENOENT) {
    HBASE_LOG_ERROR("Error while checking if the table exists: errorCode = %d.", retCode);
    goto cleanup;
  }

  hb_coldesc_create(FAMILIES[0], 1, &HCD[0]);
  hb_coldesc_set_maxversions(HCD[0], 2);
  hb_coldesc_set_minversions(HCD[0], 1);
  hb_coldesc_set_ttl(HCD[0], 2147480000);
  hb_coldesc_set_inmemory(HCD[0], 1);

  hb_coldesc_create(FAMILIES[1], 1, &HCD[1]);

  HBASE_LOG_INFO("Creating table '%s'...", table_name);
  if ((retCode = hb_admin_table_create(admin, NULL, table_name, HCD, 2)) == 0) {
    HBASE_LOG_INFO("Table '%s' created, verifying if enabled.", table_name);
    retCode = hb_admin_table_enabled(admin, NULL, table_name);
    CHECK_API_ERROR(retCode,
        "Table '%s' is %senabled, result %d.", table_name, retCode?"not ":"");
    retCode = hb_admin_table_disable(admin, NULL, table_name);
    CHECK_API_ERROR(retCode,
        "Attempted to disable table '%s', result %d.", table_name);
    retCode = hb_admin_table_disable(admin, NULL, table_name);
    CHECK_API_ERROR(retCode,
        "Attempted to disable table '%s' again, result %d.", table_name);
    retCode = hb_admin_table_enable(admin, NULL, table_name);
    CHECK_API_ERROR(retCode,
        "Attempted to enable table '%s', result %d.", table_name);
    retCode = hb_admin_table_enable(admin, NULL, table_name);
    CHECK_API_ERROR(retCode,
        "Attempted to enable table '%s' again, result %d.", table_name);
  }
  hb_coldesc_destroy(HCD[0]);
  hb_coldesc_destroy(HCD[1]);

cleanup:
  if (admin) {
    hb_admin_destroy(admin, NULL, NULL);
  }
  return retCode;
}

/**
 * Program entry point
 */
int
main(int argc, char **argv) {
  int32_t retCode = 0;
  FILE* logFile = NULL;
  hb_connection_t connection = NULL;
  hb_client_t client = NULL;
  const char *rowkey_prefix = "row";
  const char *value_prefix = "test value";
  bytebuffer column_a = bytebuffer_strcpy("column-a");
  bytebuffer column_b = bytebuffer_strcpy("column-b");

  const char *table_name      = (argc > 1) ? argv[1] : "TempTable";
  const char *zk_ensemble     = (argc > 2) ? argv[2] : "localhost:2181";
  const char *zk_root_znode   = (argc > 3) ? argv[3] : NULL;
  const size_t table_name_len = strlen(table_name);

  const int num_puts = 10;
  hb_put_t put = NULL;

  srand(time(NULL));
  hb_log_set_level(HBASE_LOG_LEVEL_DEBUG); // defaults to INFO
  const char *logFilePath = getenv("HBASE_LOG_FILE");
  if (logFilePath != NULL) {
    FILE* logFile = fopen(logFilePath, "a");
    if (!logFile) {
      retCode = errno;
      fprintf(stderr, "Unable to open log file \"%s\"", logFilePath);
      perror(NULL);
      goto cleanup;
    }
    hb_log_set_stream(logFile); // defaults to stderr
  }

  if ((retCode = hb_connection_create(zk_ensemble,
                                      zk_root_znode,
                                      &connection)) != 0) {
    HBASE_LOG_ERROR("Could not create HBase connection : errorCode = %d.", retCode);
    goto cleanup;
  }

  if ((retCode = ensureTable(connection, table_name)) != 0) {
    HBASE_LOG_ERROR("Failed to ensure table %s : errorCode = %d", table_name, retCode);
    goto cleanup;
  }

  HBASE_LOG_INFO("Connecting to HBase cluster using Zookeeper ensemble '%s'.",
                 zk_ensemble);
  if ((retCode = hb_client_create(connection, &client)) != 0) {
    HBASE_LOG_ERROR("Could not connect to HBase cluster : errorCode = %d.", retCode);
    goto cleanup;
  }

  // let's send a batch of 10 puts with single cell asynchronously
  outstanding_puts_count += num_puts;
  for (int i = 0; i < num_puts; ++i) {
    row_data_t *row_data = (row_data_t *) calloc(1, sizeof(row_data_t));
    row_data->key   = bytebuffer_printf("%s%02d", rowkey_prefix, i);
    hb_put_create(row_data->key->buffer, row_data->key->length, &put);
    hb_mutation_set_table(put, table_name, table_name_len);
    hb_mutation_set_durability(put, DURABILITY_SKIP_WAL);
    hb_mutation_set_bufferable(put, false);

    cell_data_t *cell_data = new_cell_data();
    row_data->first_cell = cell_data;
    cell_data->value = bytebuffer_printf("%s%02d", value_prefix, i);

    hb_cell_t *cell = (hb_cell_t*) calloc(1, sizeof(hb_cell_t));
    cell_data->hb_cell = cell;

    cell->row = row_data->key->buffer;
    cell->row_len = row_data->key->length;
    cell->family = FAMILIES[rand() % 2];
    cell->family_len = 1;
    cell->qualifier = column_a->buffer;
    cell->qualifier_len = column_a->length;
    cell->value = cell_data->value->buffer;
    cell->value_len = cell_data->value->length;
    cell->ts = HBASE_LATEST_TIMESTAMP;

    hb_put_add_cell(put, cell);
    HBASE_LOG_INFO("Sending row with row key : '%.*s'.",
                   cell->row_len, cell->row);
    hb_mutation_send(client, put, put_callback, row_data);
  }
  hb_client_flush(client, client_flush_callback, NULL);
  wait_for_flush();

  wait_for_puts(); // outside the loop, wait for 10 puts to complete

  // now, let's put two cells in a single row
  outstanding_puts_count++;
  {
    row_data_t *row_data = (row_data_t *) calloc(1, sizeof(row_data_t));
    row_data->key = bytebuffer_printf("row_with_two_cells");
    hb_put_create(row_data->key->buffer, row_data->key->length, &put);
    hb_mutation_set_table(put, table_name, table_name_len);
    hb_mutation_set_durability(put, DURABILITY_SYNC_WAL);

    // first cell
    cell_data_t *cell1_data = new_cell_data();
    row_data->first_cell = cell1_data;
    cell1_data->value = bytebuffer_printf("cell1_value_v1");

    hb_cell_t *cell1 = (hb_cell_t*) calloc(1, sizeof(hb_cell_t));
    cell1_data->hb_cell = cell1;

    cell1->row = row_data->key->buffer;
    cell1->row_len = row_data->key->length;
    cell1->family = FAMILIES[0];
    cell1->family_len = 1;
    cell1->qualifier = column_a->buffer;
    cell1->qualifier_len = column_a->length;
    cell1->value = cell1_data->value->buffer;
    cell1->value_len = cell1_data->value->length;
    cell1->ts = 1391111111111L;
    hb_put_add_cell(put, cell1);

    // second cell
    cell_data_t *cell2_data = new_cell_data();
    cell1_data->next_cell = cell2_data;
    cell2_data->value = bytebuffer_printf("cell2_value_v1");

    hb_cell_t *cell2 = (hb_cell_t*) calloc(1, sizeof(hb_cell_t));
    cell2_data->hb_cell = cell2;

    cell2->row = row_data->key->buffer;
    cell2->row_len = row_data->key->length;
    cell2->family = FAMILIES[1];
    cell2->family_len = 1;
    cell2->qualifier = column_b->buffer;
    cell2->qualifier_len = column_b->length;
    cell2->value = cell2_data->value->buffer;
    cell2->value_len = cell2_data->value->length;
    cell2->ts = 1391111111111L;
    hb_put_add_cell(put, cell2);

    HBASE_LOG_INFO("Sending row with row key : '%.*s'.",
                   cell1->row_len, cell1->row);
    hb_mutation_send(client, put, put_callback, row_data);
    wait_for_puts();
  }

  // now, let's put second version in one column
  outstanding_puts_count++;
  {
    row_data_t *row_data = (row_data_t *) calloc(1, sizeof(row_data_t));
    row_data->key = bytebuffer_printf("row_with_two_cells");
    hb_put_create(row_data->key->buffer, row_data->key->length, &put);
    hb_mutation_set_table(put, table_name, table_name_len);
    hb_mutation_set_durability(put, DURABILITY_SYNC_WAL);

    // first cell
    cell_data_t *cell1_data = new_cell_data();
    row_data->first_cell = cell1_data;
    cell1_data->value = bytebuffer_printf("cell1_value_v2");

    hb_cell_t *cell1 = (hb_cell_t*) calloc(1, sizeof(hb_cell_t));
    cell1_data->hb_cell = cell1;

    cell1->row = row_data->key->buffer;
    cell1->row_len = row_data->key->length;
    cell1->family = FAMILIES[0];
    cell1->family_len = 1;
    cell1->qualifier = column_a->buffer;
    cell1->qualifier_len = column_a->length;
    cell1->value = cell1_data->value->buffer;
    cell1->value_len = cell1_data->value->length;
    cell1->ts = 1392222222222L;
    hb_put_add_cell(put, cell1);

    HBASE_LOG_INFO("Sending row with row key : '%.*s'.",
                   cell1->row_len, cell1->row);
    hb_mutation_send(client, put, put_callback, row_data);
    wait_for_puts();
  }

  // now, scan the entire table
  {
    hb_scanner_t scanner = NULL;
    hb_scanner_create(client, &scanner);
    hb_scanner_set_table(scanner, table_name, table_name_len);
    hb_scanner_set_num_max_rows(scanner, 3);  // maximum 3 rows at a time
    hb_scanner_set_num_versions(scanner, 10); // up to 10 versions of the cell
    hb_scanner_next(scanner, scan_callback, NULL); // dispatch the call
    wait_for_scan();
  }

  // fetch a row with row-key="row_with_two_cells"
  {
    bytebuffer rowKey = bytebuffer_strcpy("row_with_two_cells");
    hb_get_t get = NULL;
    hb_get_create(rowKey->buffer, rowKey->length, &get);
    hb_get_add_column(get, FAMILIES[0], 1, NULL, 0);
    hb_get_add_column(get, FAMILIES[1], 1, NULL, 0);
    hb_get_set_table(get, table_name, table_name_len);
    hb_get_set_num_versions(get, 10); // up to ten versions of each column

    get_done = false;
    hb_get_send(client, get, get_callback, rowKey);
    wait_for_get();
  }

  // delete a specific version of a column
  {
    bytebuffer rowKey = bytebuffer_strcpy("row_with_two_cells");
    hb_delete_t del = NULL;
    hb_delete_create(rowKey->buffer, rowKey->length, &del);
    hb_delete_add_column(del, FAMILIES[0], 1,
        column_a->buffer, column_a->length, 1391111111112L);
    hb_mutation_set_table(del, table_name, table_name_len);

    delete_done = false;
    hb_mutation_send(client, del, delete_callback, rowKey);
    wait_for_delete();
  }

  // fetch a row with row-key="row_with_two_cells"
  {
    bytebuffer rowKey = bytebuffer_strcpy("row_with_two_cells");
    hb_get_t get = NULL;
    hb_get_create(rowKey->buffer, rowKey->length, &get);
    hb_get_add_column(get, FAMILIES[0], 1, NULL, 0);
    hb_get_add_column(get, FAMILIES[1], 1, NULL, 0);
    hb_get_set_table(get, table_name, table_name_len);
    hb_get_set_num_versions(get, 10); // up to ten versions of each column

    get_done = false;
    hb_get_send(client, get, get_callback, rowKey);
    wait_for_get();
  }

cleanup:
  if (client) {
    HBASE_LOG_INFO("Disconnecting client.");
    hb_client_destroy(client, client_disconnection_callback, NULL);
    wait_client_disconnection();
  }

  if (connection) {
    hb_connection_destroy(connection);
  }

  if (column_a) {
    bytebuffer_free(column_a);  // do not need 'column' anymore
  }
  if (column_b) {
    bytebuffer_free(column_b);
  }

  if (logFile) {
    fclose(logFile);
  }

  pthread_cond_destroy(&puts_cv);
  pthread_mutex_destroy(&puts_mutex);

  pthread_cond_destroy(&get_cv);
  pthread_mutex_destroy(&get_mutex);

  pthread_cond_destroy(&del_cv);
  pthread_mutex_destroy(&del_mutex);

  pthread_cond_destroy(&client_destroyed_cv);
  pthread_mutex_destroy(&client_destroyed_mutex);

  return retCode;
}

#ifdef __cplusplus
}
#endif
