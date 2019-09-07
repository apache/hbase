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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(SmallTests.class)
public class PagingTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(PagingTest.class);

  @Test
  public void testArrowUpAndArrowDown() {
    Paging paging = new Paging();
    paging.updatePageSize(3);
    paging.updateRecordsSize(5);

    assertPaging(paging, 0, 0, 3);

    paging.arrowDown();
    assertPaging(paging, 1, 0, 3);

    paging.arrowDown();
    assertPaging(paging, 2, 0, 3);

    paging.arrowDown();
    assertPaging(paging, 3, 1, 4);

    paging.arrowDown();
    assertPaging(paging, 4, 2, 5);

    paging.arrowDown();
    assertPaging(paging, 4, 2, 5);

    paging.arrowUp();
    assertPaging(paging, 3, 2, 5);

    paging.arrowUp();
    assertPaging(paging, 2, 2, 5);

    paging.arrowUp();
    assertPaging(paging, 1, 1, 4);

    paging.arrowUp();
    assertPaging(paging, 0, 0, 3);

    paging.arrowUp();
    assertPaging(paging, 0, 0, 3);
  }

  @Test
  public void testPageUpAndPageDown() {
    Paging paging = new Paging();
    paging.updatePageSize(3);
    paging.updateRecordsSize(8);

    assertPaging(paging, 0, 0, 3);

    paging.pageDown();
    assertPaging(paging, 3, 3, 6);

    paging.pageDown();
    assertPaging(paging, 6, 5, 8);

    paging.pageDown();
    assertPaging(paging, 7, 5, 8);

    paging.pageDown();
    assertPaging(paging, 7, 5, 8);

    paging.pageUp();
    assertPaging(paging, 4, 4, 7);

    paging.pageUp();
    assertPaging(paging, 1, 1, 4);

    paging.pageUp();
    assertPaging(paging, 0, 0, 3);

    paging.pageUp();
    assertPaging(paging, 0, 0, 3);
  }

  @Test
  public void testInit() {
    Paging paging = new Paging();
    paging.updatePageSize(3);
    paging.updateRecordsSize(5);

    assertPaging(paging, 0, 0, 3);

    paging.pageDown();
    paging.pageDown();
    paging.pageDown();
    paging.pageDown();
    paging.init();

    assertPaging(paging, 0, 0, 3);
  }

  @Test
  public void testWhenPageSizeGraterThanRecordsSize() {
    Paging paging = new Paging();
    paging.updatePageSize(5);
    paging.updateRecordsSize(3);

    assertPaging(paging, 0, 0, 3);

    paging.arrowDown();
    assertPaging(paging, 1, 0, 3);

    paging.arrowDown();
    assertPaging(paging, 2, 0, 3);

    paging.arrowDown();
    assertPaging(paging, 2, 0, 3);

    paging.arrowUp();
    assertPaging(paging, 1, 0, 3);

    paging.arrowUp();
    assertPaging(paging, 0, 0, 3);

    paging.arrowUp();
    assertPaging(paging, 0, 0, 3);

    paging.pageDown();
    assertPaging(paging, 2, 0, 3);

    paging.pageDown();
    assertPaging(paging, 2, 0, 3);

    paging.pageUp();
    assertPaging(paging, 0, 0, 3);

    paging.pageUp();
    assertPaging(paging, 0, 0, 3);
  }

  @Test
  public void testWhenPageSizeIsZero() {
    Paging paging = new Paging();
    paging.updatePageSize(0);
    paging.updateRecordsSize(5);

    assertPaging(paging, 0, 0, 0);

    paging.arrowDown();
    assertPaging(paging, 1, 0, 0);

    paging.arrowUp();
    assertPaging(paging, 0, 0, 0);

    paging.pageDown();
    assertPaging(paging, 0, 0, 0);

    paging.pageUp();
    assertPaging(paging, 0, 0, 0);
  }

  @Test
  public void testWhenRecordsSizeIsZero() {
    Paging paging = new Paging();
    paging.updatePageSize(3);
    paging.updateRecordsSize(0);

    assertPaging(paging, 0, 0, 0);

    paging.arrowDown();
    assertPaging(paging, 0, 0, 0);

    paging.arrowUp();
    assertPaging(paging, 0, 0, 0);

    paging.pageDown();
    assertPaging(paging, 0, 0, 0);

    paging.pageUp();
    assertPaging(paging, 0, 0, 0);
  }

  @Test
  public void testWhenChangingPageSizeDynamically() {
    Paging paging = new Paging();
    paging.updatePageSize(3);
    paging.updateRecordsSize(5);

    assertPaging(paging, 0, 0, 3);

    paging.arrowDown();
    assertPaging(paging, 1, 0, 3);

    paging.updatePageSize(2);
    assertPaging(paging, 1, 0, 2);

    paging.arrowDown();
    assertPaging(paging, 2, 1, 3);

    paging.arrowDown();
    assertPaging(paging, 3, 2, 4);

    paging.updatePageSize(4);
    assertPaging(paging, 3, 1, 5);

    paging.updatePageSize(5);
    assertPaging(paging, 3, 0, 5);

    paging.updatePageSize(0);
    assertPaging(paging, 3, 0, 0);

    paging.arrowDown();
    assertPaging(paging, 4, 0, 0);

    paging.arrowUp();
    assertPaging(paging, 3, 0, 0);

    paging.pageDown();
    assertPaging(paging, 3, 0, 0);

    paging.pageUp();
    assertPaging(paging, 3, 0, 0);

    paging.updatePageSize(1);
    assertPaging(paging, 3, 3, 4);
  }

  @Test
  public void testWhenChangingRecordsSizeDynamically() {
    Paging paging = new Paging();
    paging.updatePageSize(3);
    paging.updateRecordsSize(5);

    assertPaging(paging, 0, 0, 3);

    paging.updateRecordsSize(2);
    assertPaging(paging, 0, 0, 2);
    assertThat(paging.getCurrentPosition(), is(0));
    assertThat(paging.getPageStartPosition(), is(0));
    assertThat(paging.getPageEndPosition(), is(2));

    paging.arrowDown();
    assertPaging(paging, 1, 0, 2);

    paging.updateRecordsSize(3);
    assertPaging(paging, 1, 0, 3);

    paging.arrowDown();
    assertPaging(paging, 2, 0, 3);

    paging.updateRecordsSize(1);
    assertPaging(paging, 0, 0, 1);

    paging.updateRecordsSize(0);
    assertPaging(paging, 0, 0, 0);

    paging.arrowDown();
    assertPaging(paging, 0, 0, 0);

    paging.arrowUp();
    assertPaging(paging, 0, 0, 0);

    paging.pageDown();
    assertPaging(paging, 0, 0, 0);

    paging.pageUp();
    assertPaging(paging, 0, 0, 0);
  }

  private void assertPaging(Paging paging, int currentPosition, int pageStartPosition,
    int pageEndPosition) {
    assertThat(paging.getCurrentPosition(), is(currentPosition));
    assertThat(paging.getPageStartPosition(), is(pageStartPosition));
    assertThat(paging.getPageEndPosition(), is(pageEndPosition));
  }
}
