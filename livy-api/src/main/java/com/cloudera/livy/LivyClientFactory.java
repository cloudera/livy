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

package com.cloudera.livy;

import java.net.URI;
import java.util.Properties;

import com.cloudera.livy.annotations.Private;

/**
 * A factory for Livy clients. Client implementations can register themselves by using the
 * Java services mechanism, providing implementations of this interface.
 * <p>
 * Client applications do not need to use this interface directly. Instead, use
 * {@link LivyClientBuilder}.
 *
 * @see java.util.ServiceLoader
 */
@Private
public interface LivyClientFactory {

  /**
   * Instantiates a new client if the given URI is supported by the implementation.
   *
   * @param uri URI pointing at the livy backend to use.
   */
  LivyClient createClient(URI uri, Properties config);

}
