/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.livy.rsc;

import io.netty.util.concurrent.Promise;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.cloudera.livy.JobHandle;

@RunWith(MockitoJUnitRunner.class)
public class TestJobHandle {

  @Mock private RSCClient client;
  @Mock private Promise<Object> promise;
  @Mock private JobHandle.Listener<Object> listener;
  @Mock private JobHandle.Listener<Object> listener2;

  @Test
  public void testStateChanges() throws Exception {
    JobHandleImpl<Object> handle = new JobHandleImpl<Object>(client, promise, "job");
    handle.addListener(listener);

    assertTrue(handle.changeState(JobHandle.State.QUEUED));
    verify(listener).onJobQueued(handle);

    assertTrue(handle.changeState(JobHandle.State.STARTED));
    verify(listener).onJobStarted(handle);

    assertTrue(handle.changeState(JobHandle.State.CANCELLED));
    verify(listener).onJobCancelled(handle);

    assertFalse(handle.changeState(JobHandle.State.STARTED));
    assertFalse(handle.changeState(JobHandle.State.FAILED));
    assertFalse(handle.changeState(JobHandle.State.SUCCEEDED));
  }

  @Test
  public void testFailedJob() throws Exception {
    JobHandleImpl<Object> handle = new JobHandleImpl<Object>(client, promise, "job");
    handle.addListener(listener);

    Throwable cause = new Exception();
    when(promise.cause()).thenReturn(cause);

    assertTrue(handle.changeState(JobHandle.State.FAILED));
    verify(promise).cause();
    verify(listener).onJobFailed(handle, cause);
  }

  @Test
  public void testSucceededJob() throws Exception {
    JobHandleImpl<Object> handle = new JobHandleImpl<Object>(client, promise, "job");
    handle.addListener(listener);

    Object result = new Exception();
    when(promise.getNow()).thenReturn(result);

    assertTrue(handle.changeState(JobHandle.State.SUCCEEDED));
    verify(promise).getNow();
    verify(listener).onJobSucceeded(handle, result);
  }

  @Test
  public void testImmediateCallback() throws Exception {
    JobHandleImpl<Object> handle = new JobHandleImpl<Object>(client, promise, "job");
    assertTrue(handle.changeState(JobHandle.State.QUEUED));
    handle.addListener(listener);
    verify(listener).onJobQueued(handle);

    handle.changeState(JobHandle.State.STARTED);
    handle.changeState(JobHandle.State.CANCELLED);

    handle.addListener(listener2);
    verify(listener2).onJobCancelled(same(handle));
  }

}
