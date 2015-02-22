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
package org.apache.hadoop.hbase.regionserver.compactions;

import java.util.Calendar;
import java.util.GregorianCalendar;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

@InterfaceAudience.Private
public class CurrentHourProvider {
  private CurrentHourProvider() { throw new AssertionError(); }

  private static final class Tick {
    final int currentHour;
    final long expirationTimeInMillis;

    Tick(int currentHour, long expirationTimeInMillis) {
      this.currentHour = currentHour;
      this.expirationTimeInMillis = expirationTimeInMillis;
    }
  }

  private static Tick nextTick() {
    Calendar calendar = new GregorianCalendar();
    int currentHour = calendar.get(Calendar.HOUR_OF_DAY);
    moveToNextHour(calendar);
    return new Tick(currentHour, calendar.getTimeInMillis());
  }

  private static void moveToNextHour(Calendar calendar) {
    calendar.add(Calendar.HOUR_OF_DAY, 1);
    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);
  }

  private static volatile Tick tick = nextTick();

  public static int getCurrentHour() {
    Tick tick = CurrentHourProvider.tick;
    if(System.currentTimeMillis() < tick.expirationTimeInMillis) {
      return tick.currentHour;
    }

    CurrentHourProvider.tick = tick = nextTick();
    return tick.currentHour;
  }
}
