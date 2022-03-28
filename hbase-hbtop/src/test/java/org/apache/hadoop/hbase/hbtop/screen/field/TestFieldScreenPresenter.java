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
package org.apache.hadoop.hbase.hbtop.screen.field;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;

import java.util.EnumMap;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.hbtop.field.Field;
import org.apache.hadoop.hbase.hbtop.field.FieldInfo;
import org.apache.hadoop.hbase.hbtop.mode.Mode;
import org.apache.hadoop.hbase.hbtop.screen.top.TopScreenView;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;


@Category(SmallTests.class)
@RunWith(MockitoJUnitRunner.class)
public class TestFieldScreenPresenter {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestFieldScreenPresenter.class);

  @Mock
  private FieldScreenView fieldScreenView;

  private int sortFieldPosition = -1;
  private List<Field> fields;
  private EnumMap<Field, Boolean> fieldDisplayMap;

  @Mock
  private FieldScreenPresenter.ResultListener resultListener;

  @Mock
  private TopScreenView topScreenView;

  private FieldScreenPresenter fieldScreenPresenter;

  @Before
  public void setup() {
    Field sortField = Mode.REGION.getDefaultSortField();
    fields = Mode.REGION.getFieldInfos().stream()
      .map(FieldInfo::getField)
      .collect(Collectors.toList());

    fieldDisplayMap = Mode.REGION.getFieldInfos().stream()
      .collect(() -> new EnumMap<>(Field.class),
        (r, fi) -> r.put(fi.getField(), fi.isDisplayByDefault()), (r1, r2) -> {});

    fieldScreenPresenter =
      new FieldScreenPresenter(fieldScreenView, sortField, fields, fieldDisplayMap, resultListener,
        topScreenView);

    for (int i = 0; i < fields.size(); i++) {
      Field field = fields.get(i);
      if (field == sortField) {
        sortFieldPosition = i;
        break;
      }
    }
  }

  @Test
  public void testInit() {
    fieldScreenPresenter.init();

    int modeHeaderMaxLength = "#COMPingCell".length();
    int modeDescriptionMaxLength = "Filtered Read Request Count per second".length();

    verify(fieldScreenView).showFieldScreen(eq("#REQ/S"), eq(fields), eq(fieldDisplayMap),
      eq(sortFieldPosition), eq(modeHeaderMaxLength), eq(modeDescriptionMaxLength), eq(false));
  }

  @Test
  public void testChangeSortField() {
    fieldScreenPresenter.arrowUp();
    fieldScreenPresenter.setSortField();

    fieldScreenPresenter.arrowDown();
    fieldScreenPresenter.arrowDown();
    fieldScreenPresenter.setSortField();

    fieldScreenPresenter.pageUp();
    fieldScreenPresenter.setSortField();

    fieldScreenPresenter.pageDown();
    fieldScreenPresenter.setSortField();

    InOrder inOrder = inOrder(fieldScreenView);
    inOrder.verify(fieldScreenView).showScreenDescription(eq("LRS"));
    inOrder.verify(fieldScreenView).showScreenDescription(eq("#READ/S"));
    inOrder.verify(fieldScreenView).showScreenDescription(eq(fields.get(0).getHeader()));
    inOrder.verify(fieldScreenView).showScreenDescription(
      eq(fields.get(fields.size() - 1).getHeader()));
  }

  @Test
  public void testSwitchFieldDisplay() {
    fieldScreenPresenter.switchFieldDisplay();
    fieldScreenPresenter.switchFieldDisplay();

    InOrder inOrder = inOrder(fieldScreenView);
    inOrder.verify(fieldScreenView).showField(anyInt(), any(), eq(false), anyBoolean(), anyInt(),
      anyInt(), anyBoolean());
    inOrder.verify(fieldScreenView).showField(anyInt(), any(), eq(true), anyBoolean(), anyInt(),
      anyInt(), anyBoolean());
  }

  @Test
  public void testChangeFieldsOrder() {
    fieldScreenPresenter.turnOnMoveMode();
    fieldScreenPresenter.arrowUp();
    fieldScreenPresenter.turnOffMoveMode();

    Field removed = fields.remove(sortFieldPosition);
    fields.add(sortFieldPosition - 1, removed);

    assertThat(fieldScreenPresenter.transitionToNextScreen(), is(topScreenView));
    verify(resultListener).accept(any(), eq(fields), any());
  }
}
