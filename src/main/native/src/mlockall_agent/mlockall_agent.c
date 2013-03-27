/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * mlockall_agent is a simple VM Agent that allows to lock the address space of
 * the process. This avoids the process' memory eviction under pressure.
 *
 * One example is when on the same machine you run the Region Server and
 * some map-reduce tasks, some unused data in the region server may get swapped
 * and this affects the region server performance.
 *
 * You can load the agent by adding it as a jvm option:
 * export HBASE_REGIONSERVER_OPTS="-agentpath:./libmlockall_agent.so=user=hbase"
 */

#include <libgen.h>
#include <grp.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include "jvmti.h"

typedef struct opts {
  char *setuid_user;
} opts_t;

#define PREFIX "mlockall_agent: "
#define LOG(fmt, ...) { fprintf(stderr, PREFIX fmt, #__VA_ARGS__); }

static int parse_options (const char *options, opts_t *parsed) {
  char *optr, *opts_dup;
  char *save2 = NULL;
  char *save = NULL;
  char *key, *val;
  int ret = 0;
  char *tok;

  memset(parsed, 0, sizeof(opts_t));
  if ((opts_dup = strdup(options)) == NULL)
    return(-1);

  optr = opts_dup;
  while ((tok = strtok_r(optr, ",", &save)) != NULL) {
    optr = NULL;
    save2 = NULL;

    key = strtok_r(tok, "=", &save2);
    val = strtok_r(NULL, "=", &save2);
    if (!strcmp(key, "user")) {
      parsed->setuid_user = strdup(val);
    } else {
      LOG("Unknown agent parameter '%s'\n", key);
      ret = 1;
    }
  }

  free(opts_dup);
  return(ret);
}

static void warn_unless_root() {
  if (geteuid() != 0) {
    LOG("(this may be because java was not run as root!)\n");
  }
}

JNIEXPORT jint JNICALL Agent_OnLoad(JavaVM *vm, char *init_str, void *reserved) {
  struct passwd *pwd = NULL;
  opts_t opts;

  if (parse_options(init_str, &opts)) {
    return(1);
  }

  // Check that the target user for setuid is specified if current user is root
  if (opts.setuid_user == NULL) {
    LOG("Unable to setuid: specify a target username as the agent option user=<username>\n");
    return(1);
  }

  // Check that this user exists
  if ((pwd = getpwnam(opts.setuid_user)) == NULL) {
    LOG("Unable to setuid: could not find user '%s'\n", opts.setuid_user);
    return(1);
  }

  // Boost the mlock limit up to infinity
  struct rlimit lim;
  lim.rlim_max = RLIM_INFINITY;
  lim.rlim_cur = lim.rlim_max;
  if (setrlimit(RLIMIT_MEMLOCK, &lim)) {
    perror(PREFIX "Unable to boost memlock resource limit");
    warn_unless_root();
    return(1);
  }

  // Actually lock our memory, including future allocations.
  if (mlockall(MCL_CURRENT | MCL_FUTURE)) {
    perror(PREFIX "Unable to lock memory");
    warn_unless_root();
    return(1);
  }

  // Drop down to the user's supplemental group list
  if (initgroups(opts.setuid_user, pwd->pw_gid)) {
    perror(PREFIX "Unable to initgroups");
    warn_unless_root();
    return(1);
  }

  // And primary group ID
  if (setgid(pwd->pw_gid)) {
    perror(PREFIX "Unable to setgid");
    warn_unless_root();
    return(1);
  }

  // And user ID
  if (setuid(pwd->pw_uid)) {
    perror(PREFIX "Unable to setuid");
    warn_unless_root();
    return(1);
  }

  LOG("Successfully locked memory and setuid to %s\n", opts.setuid_user);
  return(0);
}

