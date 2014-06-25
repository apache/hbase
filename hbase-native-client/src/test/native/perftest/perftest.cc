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
#line 19 "perftest.cc" // ensures short filename in logs.

#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include <hbase/hbase.h>

#include "admin_ops.h"
#include "byte_buffer.h"
#include "common_utils.h"
#include "flusher.h"
#include "ops_runner.h"
#include "stats_keeper.h"
#include "test_types.h"

namespace hbase {
namespace test {

static bool argCreateTable = false;
static bool argCheckRead   = false;
static bool argHashKeys    = true;
static bool argBufferPuts  = true;
static bool argWriteToWAL  = true;

static char *argZkQuorum    = (char*) "localhost:2181";
static char *argZkRootNode  = NULL;
static char *argTableName   = (char*) "test_table";
static char *argFamilyName  = (char*) "f";
static char *argLogFilePath = NULL;
static char *argKeyPrefix   = (char*) "user";

static uint64_t argStartRow       = 1;
static uint64_t argNumOps         = 1000000;
static uint32_t argValueSize      = 1024;
static uint32_t argFlushBatchSize = 0;
static uint32_t argMaxPendingRPCs = 100000;
static uint32_t argNumThreads     = 1;
static uint32_t argPutPercent     = 100;

static
void usage() {
  fprintf(stderr, "Usage: perftest [options]...\n"
      "Available options: (default values in [])\n"
      "  -zkQuorum <zk_quorum> [localhost:2181]\n"
      "  -zkRootNode <zk_root_node> [/hbase]\n"
      "  -table <table> [test_table]\n"
      "  -family <family> [f]\n"
      "  -startRow <start_row> [1]\n"
      "  -valueSize <value_size> [1024]\n"
      "  -numOps <numops> [1000000]\n"
      "  -keyPrefix <key_prefix> [user]\n"
      "  -hashKeys true|false [true]\n"
      "  -bufferPuts true|false [true]\n"
      "  -writeToWAL true|false [true]\n"
      "  -flushBatchSize <flush_batch_size> [0(disabled)]\n"
      "  -maxPendingRPCs <max_pending_rpcs> [100000]\n"
      "  -numThreads <num_threads> [1]\n"
      "  -putPercent <put_percent> [100]\n"
      "  -createTable true|false [false]\n"
      "  -logFilePath <log_file_path> [stderr]\n");
  exit(1);
}

static void
parseArgs(int argc,
          char *argv[]) {
  // skip program name
  argc--; argv++;
#define ArgEQ(a) (argc > 1 && (strcmp(argv[0], a) == 0))
  while (argc >= 1) {
    if (ArgEQ("-valueSize")) {
      argValueSize = atoi(argv[1]);
    } else if (ArgEQ("-numOps")) {
      argNumOps = atol(argv[1]);
    } else if (ArgEQ("-startRow")) {
      argStartRow = atol(argv[1]);
    } else if (ArgEQ("-table")) {
      argTableName = argv[1];
    } else if (ArgEQ("-family")) {
      argFamilyName = argv[1];
    } else if (ArgEQ("-keyPrefix")) {
      argKeyPrefix = argv[1];
    } else if (ArgEQ("-createTable")) {
      argCreateTable = !(strcmp(argv[1], "false") == 0);
    } else if (ArgEQ("-checkRead")) {
      argCheckRead = !(strcmp(argv[1], "false") == 0);
    } else if (ArgEQ("-hashKeys")) {
      argHashKeys = !(strcmp(argv[1], "false") == 0);
    } else if (ArgEQ("-bufferPuts")) {
      argBufferPuts = !(strcmp(argv[1], "false") == 0);
    } else if (ArgEQ("-writeToWAL")) {
      argWriteToWAL = !(strcmp(argv[1], "false") == 0);
    } else if (ArgEQ("-zkQuorum")) {
      argZkQuorum = argv[1];
    } else if (ArgEQ("-zkRootNode")) {
      argZkRootNode = argv[1];
    } else if (ArgEQ("-logFilePath")) {
      argLogFilePath = argv[1];
    } else if (ArgEQ("-flushBatchSize")) {
      // if not set to 0, starts a thread to
      // flush after every 'flushBatchSize' RPCs
      argFlushBatchSize = atoi(argv[1]);
    } else if (ArgEQ("-maxPendingRPCs")) {
      argMaxPendingRPCs = atoi(argv[1]);
    } else if (ArgEQ("-numThreads")) {
      argNumThreads = atoi(argv[1]);
    } else if (ArgEQ("-putPercent")) {
      argPutPercent = atoi(argv[1]);
    } else {
      usage();
    }
    argv += 2; argc -= 2;
  }
#undef ArgEQ

  if (!argBufferPuts && argFlushBatchSize > 0) {
    fprintf(stderr, "'-flushBatchSize' can not be specified if '-bufferPuts' is false");
  } else if (argPutPercent < 0 || argPutPercent > 100) {
    fprintf(stderr, "'-putPercent' must be between 0 and 100.");
  } else {
    // everything okay
    return;
  }
  exit(1);
}

/**
 * Program entry point
 */
extern "C" int
main(int argc,
    char *argv[]) {
  if (argc == 1) usage();

  int32_t retCode = 0;
  FILE *logFile = NULL;
  hb_connection_t connection = NULL;
  hb_client_t client = NULL;
  bytebuffer table = NULL, column = NULL;
  bytebuffer families[1];

  parseArgs(argc, argv);

  uint64_t opsPerThread = (argNumOps/argNumThreads);
  int32_t maxPendingRPCsPerThread = argMaxPendingRPCs/argNumThreads;
  if (maxPendingRPCsPerThread > SEM_VALUE_MAX) {
    fprintf(stderr, "Can not have more than %d pending RPCs per thread.",
            SEM_VALUE_MAX);
    exit(1);
  }

  table = bytebuffer_strcpy(argTableName);
  families[0] = bytebuffer_strcpy(argFamilyName);
  column = bytebuffer_strcpy("a");

  hb_log_set_level(HBASE_LOG_LEVEL_DEBUG); // defaults to INFO
  if (argLogFilePath != NULL) {
    logFile = fopen(argLogFilePath, "a");
    if (!logFile) {
      retCode = errno;
      fprintf(stderr, "Unable to open log file \"%s\"", argLogFilePath);
      perror(NULL);
      goto cleanup;
    }
    hb_log_set_stream(logFile); // defaults to stderr
  }

  if ((retCode = hb_connection_create(argZkQuorum, argZkRootNode, &connection))) {
    HBASE_LOG_ERROR("Could not create HBase connection : errorCode = %d.", retCode);
    goto cleanup;
  }

  if ((retCode = ensureTable(connection,
      argCreateTable, argTableName, families, 1)) != 0) {
    HBASE_LOG_ERROR("Failed to ensure table %s : errorCode = %d",
        argTableName, retCode);
    goto cleanup;
  }

  HBASE_LOG_INFO("Connecting to HBase cluster using Zookeeper ensemble '%s'.",
                 argZkQuorum);
  if ((retCode = hb_client_create(connection, &client)) != 0) {
    HBASE_LOG_ERROR("Could not connect to HBase cluster : errorCode = %d.", retCode);
    goto cleanup;
  }

  // launch threads
  {
    StatKeeper *statKeeper = new StatKeeper;
    Flusher *flushRunner = NULL;
    OpsRunner *runner[argNumThreads];

    srand(time(NULL));

    for (size_t i = 0; i < argNumThreads; ++i) {
      runner[i] = new OpsRunner(client, table, argPutPercent,
          (argStartRow + (i*opsPerThread)), opsPerThread,
          families[0], column, argKeyPrefix, argValueSize,
          argHashKeys, argBufferPuts, argWriteToWAL,
          maxPendingRPCsPerThread, argCheckRead, statKeeper);
      runner[i]->Start();
    }

    statKeeper->Start();

    if (argFlushBatchSize > 0) {
      flushRunner = new Flusher(client, argFlushBatchSize, statKeeper);
      flushRunner->Start();
    }

    for (size_t i = 0; i < argNumThreads; ++i) {
      runner[i]->Stop();
      delete runner[i];
    }

    if (flushRunner != NULL) {
      flushRunner->Stop();
      delete flushRunner;
    }

    statKeeper->Stop();
    statKeeper->PrintSummary();
    delete statKeeper;
  }

cleanup:
  if (client) {
    disconnect_client_and_wait(client);
  }

  if (connection) {
    hb_connection_destroy(connection);
  }

  if (column) {
    bytebuffer_free(column);
  }

  if (families[0]) {
    bytebuffer_free(families[0]);
  }

  if (table) {
    bytebuffer_free(table);
  }

  if (logFile) {
    fclose(logFile);
  }

  return retCode;
}

} /* namespace test */
} /* namespace hbase */
