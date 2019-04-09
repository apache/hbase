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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.gson.JsonArray;
import org.apache.hbase.thirdparty.com.google.gson.JsonElement;
import org.apache.hbase.thirdparty.com.google.gson.JsonObject;
import org.apache.hbase.thirdparty.com.google.gson.JsonParser;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState;

public final class ProcedureTestUtil {

  private static final Logger LOG = LoggerFactory.getLogger(ProcedureTestUtil.class);

  private ProcedureTestUtil() {
  }

  private static Optional<JsonObject> getProcedure(HBaseTestingUtility util,
      Class<? extends Procedure<?>> clazz, JsonParser parser) throws IOException {
    JsonArray array = parser.parse(util.getAdmin().getProcedures()).getAsJsonArray();
    Iterator<JsonElement> iterator = array.iterator();
    while (iterator.hasNext()) {
      JsonElement element = iterator.next();
      JsonObject obj = element.getAsJsonObject();
      String className = obj.get("className").getAsString();
      if (className.equals(clazz.getName())) {
        return Optional.of(obj);
      }
    }
    return Optional.empty();
  }

  public static void waitUntilProcedureWaitingTimeout(HBaseTestingUtility util,
      Class<? extends Procedure<?>> clazz, long timeout) throws IOException {
    JsonParser parser = new JsonParser();
    util.waitFor(timeout,
      () -> getProcedure(util, clazz, parser)
        .filter(o -> ProcedureState.WAITING_TIMEOUT.name().equals(o.get("state").getAsString()))
        .isPresent());
  }

  public static void waitUntilProcedureTimeoutIncrease(HBaseTestingUtility util,
      Class<? extends Procedure<?>> clazz, int times) throws IOException, InterruptedException {
    JsonParser parser = new JsonParser();
    long oldTimeout = 0;
    int timeoutIncrements = 0;
    for (;;) {
      long timeout = getProcedure(util, clazz, parser).filter(o -> o.has("timeout"))
        .map(o -> o.get("timeout").getAsLong()).orElse(-1L);
      if (timeout > oldTimeout) {
        LOG.info("Timeout incremented, was {}, now is {}, increments={}", timeout, oldTimeout,
          timeoutIncrements);
        oldTimeout = timeout;
        timeoutIncrements++;
        if (timeoutIncrements > times) {
          break;
        }
      }
      Thread.sleep(1000);
    }
  }
}
