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
package org.apache.hadoop.hbase.hbtop.screen.help;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.hbtop.screen.top.TopScreenView;
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
public class HelpScreenPresenterTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(HelpScreenPresenterTest.class);

  private static final long TEST_REFRESH_DELAY = 5;

  @Mock
  private HelpScreenView helpScreenView;

  @Mock
  private TopScreenView topScreenView;

  private HelpScreenPresenter helpScreenPresenter;

  @Before
  public void setup() {
    helpScreenPresenter = new HelpScreenPresenter(helpScreenView, TEST_REFRESH_DELAY,
      topScreenView);
  }

  @Test
  public void testInit() {
    helpScreenPresenter.init();
    verify(helpScreenView).showHelpScreen(eq(TEST_REFRESH_DELAY), argThat(cds -> cds.length == 14));
  }

  @Test
  public void testTransitionToTopScreen() {
    assertThat(helpScreenPresenter.transitionToNextScreen(), is(topScreenView));
  }
}
