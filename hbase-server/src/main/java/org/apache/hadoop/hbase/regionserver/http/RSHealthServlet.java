/*
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
package org.apache.hadoop.hbase.regionserver.http;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.monitoring.HealthCheckServlet;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class RSHealthServlet extends HealthCheckServlet<HRegionServer> {

  private final Map<String, Instant> regionUnavailableSince = new ConcurrentHashMap<>();

  public RSHealthServlet() {
    super(HRegionServer.REGIONSERVER);
  }

  @Override
  protected Optional<String> check(HRegionServer regionServer, HttpServletRequest req,
    Connection conn) throws IOException {
    long maxUnavailableMillis = Optional.ofNullable(req.getParameter("maxUnavailableMillis"))
      .filter(StringUtils::isNumeric).map(Long::parseLong).orElse(Long.MAX_VALUE);

    Instant oldestUnavailableSince = Instant.MAX;
    String longestUnavailableRegion = null;
    int unavailableCount = 0;

    synchronized (regionUnavailableSince) {
      Set<String> regionsPreviouslyUnavailable = new HashSet<>(regionUnavailableSince.keySet());

      for (HRegion region : regionServer.getOnlineRegionsLocalContext()) {
        regionsPreviouslyUnavailable.remove(region.getRegionInfo().getEncodedName());
        if (!region.isAvailable()) {
          unavailableCount++;
          Instant unavailableSince = regionUnavailableSince
            .computeIfAbsent(region.getRegionInfo().getEncodedName(), k -> Instant.now());

          if (unavailableSince.isBefore(oldestUnavailableSince)) {
            oldestUnavailableSince = unavailableSince;
            longestUnavailableRegion = region.getRegionInfo().getEncodedName();
          }

        } else {
          regionUnavailableSince.remove(region.getRegionInfo().getEncodedName());
        }
      }

      regionUnavailableSince.keySet().removeAll(regionsPreviouslyUnavailable);
    }

    String message = "ok";

    if (unavailableCount > 0) {
      Duration longestUnavailableRegionTime =
        Duration.between(oldestUnavailableSince, Instant.now());
      if (longestUnavailableRegionTime.toMillis() > maxUnavailableMillis) {
        throw new IOException("Region " + longestUnavailableRegion
          + " has been unavailable too long, since " + oldestUnavailableSince);
      }

      message += " - unavailableRegions: " + unavailableCount + ", longestUnavailableDuration: "
        + longestUnavailableRegionTime + ", longestUnavailableRegion: " + longestUnavailableRegion;
    }

    return Optional.of(message);

  }
}
