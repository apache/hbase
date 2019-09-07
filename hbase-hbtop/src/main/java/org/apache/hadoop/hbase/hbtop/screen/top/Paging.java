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
package org.apache.hadoop.hbase.hbtop.screen.top;

import org.apache.yetus.audience.InterfaceAudience;


/**
 * Utility class for paging for the metrics.
 */
@InterfaceAudience.Private
public class Paging {
  private int currentPosition;
  private int pageStartPosition;
  private int pageEndPosition;

  private int pageSize;
  private int recordsSize;

  public void init() {
    currentPosition = 0;
    pageStartPosition = 0;
    pageEndPosition = Math.min(pageSize, recordsSize);
  }

  public void updatePageSize(int pageSize) {
    this.pageSize = pageSize;

    if (pageSize == 0) {
      pageStartPosition = 0;
      pageEndPosition = 0;
    } else {
      pageEndPosition = pageStartPosition + pageSize;
      keepConsistent();
    }
  }

  public void updateRecordsSize(int recordsSize) {
    if (this.recordsSize == 0) {
      currentPosition = 0;
      pageStartPosition = 0;
      pageEndPosition = Math.min(pageSize, recordsSize);
      this.recordsSize = recordsSize;
    } else if (recordsSize == 0) {
      currentPosition = 0;
      pageStartPosition = 0;
      pageEndPosition = 0;
      this.recordsSize = recordsSize;
    } else {
      this.recordsSize = recordsSize;
      if (pageSize > 0) {
        pageEndPosition = pageStartPosition + pageSize;
        keepConsistent();
      }
    }
  }

  public void arrowUp() {
    if (currentPosition > 0) {
      currentPosition -= 1;
      if (pageSize > 0) {
        keepConsistent();
      }
    }
  }

  public void arrowDown() {
    if (currentPosition < recordsSize - 1) {
      currentPosition += 1;
      if (pageSize > 0) {
        keepConsistent();
      }
    }
  }

  public void pageUp() {
    if (pageSize > 0 && currentPosition > 0) {
      currentPosition -= pageSize;
      if (currentPosition < 0) {
        currentPosition = 0;
      }
      keepConsistent();
    }
  }

  public void pageDown() {
    if (pageSize > 0 && currentPosition < recordsSize - 1) {

      currentPosition = currentPosition + pageSize;
      if (currentPosition >= recordsSize) {
        currentPosition = recordsSize - 1;
      }

      pageStartPosition = currentPosition;
      pageEndPosition = pageStartPosition + pageSize;
      keepConsistent();
    }
  }

  private void keepConsistent() {
    if (currentPosition < pageStartPosition) {
      pageStartPosition = currentPosition;
      pageEndPosition = pageStartPosition + pageSize;
    } else if (currentPosition > recordsSize - 1) {
      currentPosition = recordsSize - 1;
      pageEndPosition = recordsSize;
      pageStartPosition = pageEndPosition - pageSize;
    } else if (currentPosition > pageEndPosition - 1) {
      pageEndPosition = currentPosition + 1;
      pageStartPosition = pageEndPosition - pageSize;
    }

    if (pageStartPosition < 0) {
      pageStartPosition = 0;
    }

    if (pageEndPosition > recordsSize) {
      pageEndPosition = recordsSize;
      pageStartPosition = pageEndPosition - pageSize;
      if (pageStartPosition < 0) {
        pageStartPosition = 0;
      }
    }
  }

  public int getCurrentPosition() {
    return currentPosition;
  }

  public int getPageStartPosition() {
    return pageStartPosition;
  }

  public int getPageEndPosition() {
    return pageEndPosition;
  }
}
