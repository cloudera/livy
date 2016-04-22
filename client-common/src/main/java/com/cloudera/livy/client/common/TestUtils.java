/*
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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

package com.cloudera.livy.client.common;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.cloudera.livy.annotations.Private;

/**
 * Utility methods used by Livy tests.
 */
@Private
public class TestUtils {

  /**
   * Returns JVM arguments that enable jacoco on a process to be run. The returned arguments
   * create a new, unique output file in the same directory referenced by the "jacoco.args"
   * system property.
   *
   * @return JVM arguments, or null.
   */
  public static String getJacocoArgs() {
    String jacocoArgs = System.getProperty("jacoco.args");
    if (jacocoArgs == null) {
      return null;
    }

    Pattern p = Pattern.compile("(.+?destfile=)(.+?)(,.+)?");
    Matcher m = p.matcher(jacocoArgs);
    if (!m.matches()) {
      return null;
    }

    String fileName = new File(m.group(2)).getName();
    File outputDir = new File(m.group(2)).getParentFile();

    File newFile;
    while (true) {
      int newId = outputDir.list().length;
      newFile = new File(outputDir, "jacoco-" + newId + ".exec");
      try {
        Files.newOutputStream(newFile.toPath(), StandardOpenOption.CREATE_NEW).close();
        break;
      } catch (IOException ioe) {
        // Try again.
      }
    }

    StringBuilder newArgs = new StringBuilder();
    newArgs.append(m.group(1));
    newArgs.append(newFile.getAbsolutePath());
    if (m.group(3) != null) {
      newArgs.append(m.group(3));
    }

    return newArgs.toString();
  }

}
