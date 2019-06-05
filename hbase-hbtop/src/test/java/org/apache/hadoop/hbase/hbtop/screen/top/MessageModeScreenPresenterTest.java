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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;


@Category(SmallTests.class)
@RunWith(MockitoJUnitRunner.class)
public class MessageModeScreenPresenterTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(MessageModeScreenPresenterTest.class);

  private static final String TEST_MESSAGE = "test message";

  @Mock
  private MessageModeScreenView messageModeScreenView;

  @Mock
  private TopScreenView topScreenView;

  private MessageModeScreenPresenter messageModeScreenPresenter;

  @Before
  public void setup() {
    messageModeScreenPresenter = new MessageModeScreenPresenter(messageModeScreenView,
      TEST_MESSAGE, topScreenView);
  }

  @Test
  public void testInit() {
    messageModeScreenPresenter.init();

    verify(messageModeScreenView).showMessage(eq(TEST_MESSAGE));
  }

  @Test
  public void testReturnToTopScreen() {
    assertThat(messageModeScreenPresenter.returnToNextScreen(), is(topScreenView));
  }
}
