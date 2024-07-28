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
package org.apache.hadoop.hbase.wal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A helper class so we can call some package private methods of {@link WALEdit} from other
 * packages. Since {@link WALEdit} has been exposed to coprocessor and replication implementations,
 * we do not want to make all the methods in it public.
 */
@InterfaceAudience.Private
public final class WALEditInternalHelper {

  private WALEditInternalHelper() {
  }

  public static WALEdit addExtendedCell(WALEdit edit, ExtendedCell cell) {
    return edit.add(cell);
  }

  public static void addExtendedCell(WALEdit edit, List<ExtendedCell> cells) {
    edit.add(cells);
  }

  public static void addMap(WALEdit edit, Map<byte[], List<ExtendedCell>> familyMap) {
    edit.addMap(familyMap);
  }

  public static void setExtendedCells(WALEdit edit, ArrayList<ExtendedCell> cells) {
    edit.setExtendedCells(cells);
  }

  public static List<ExtendedCell> getExtendedCells(WALEdit edit) {
    return edit.getExtendedCells();
  }
}
