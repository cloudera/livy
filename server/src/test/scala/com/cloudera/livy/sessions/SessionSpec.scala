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

package com.cloudera.livy.sessions

import java.net.URI

import org.scalatest.FunSuite

import com.cloudera.livy.LivyConf

class SessionSpec extends FunSuite {

  test("use default fs in paths") {
    val conf = new LivyConf(false)
    conf.hadoopConf.set("fs.defaultFS", "dummy:///")

    val session = new MockSession(0, null, conf)

    val uris = Seq("http://example.com/foo", "hdfs:/bar", "/baz")
    val expected = Seq(uris(0), uris(1), "dummy:///baz")
    assert(session.resolveURIs(uris) === expected)

    intercept[IllegalArgumentException] {
      session.resolveURI(new URI("relative_path"))
    }
  }

  test("local fs whitelist") {
    val conf = new LivyConf(false)
    conf.set(LivyConf.LOCAL_FS_WHITELIST, "/allowed/,/also_allowed")

    val session = new MockSession(0, null, conf)

    Seq("/allowed/file", "/also_allowed/file").foreach { path =>
      assert(session.resolveURI(new URI(path)) === new URI("file://" + path))
    }

    Seq("/not_allowed", "/allowed_not_really").foreach { path =>
      intercept[IllegalArgumentException] {
        session.resolveURI(new URI(path))
      }
    }
  }

  test("conf validation and preparation") {
    val conf = new LivyConf(false)
    conf.hadoopConf.set("fs.defaultFS", "dummy:///")
    conf.set(LivyConf.LOCAL_FS_WHITELIST, "/allowed")

    val session = new MockSession(0, null, conf)

    // Test baseline.
    assert(session.prepareConf(Map(), Nil, Nil, Nil, Nil) === Map())

    // Test validations.
    intercept[IllegalArgumentException] {
      session.prepareConf(Map("spark.do_not_set" -> "1"), Nil, Nil, Nil, Nil)
    }
    conf.sparkFileLists.foreach { key =>
      intercept[IllegalArgumentException] {
        session.prepareConf(Map(key -> "file:/not_allowed"), Nil, Nil, Nil, Nil)
      }
    }
    intercept[IllegalArgumentException] {
      session.prepareConf(Map(), Seq("file:/not_allowed"), Nil, Nil, Nil)
    }
    intercept[IllegalArgumentException] {
      session.prepareConf(Map(), Nil, Seq("file:/not_allowed"), Nil, Nil)
    }
    intercept[IllegalArgumentException] {
      session.prepareConf(Map(), Nil, Nil, Seq("file:/not_allowed"), Nil)
    }
    intercept[IllegalArgumentException] {
      session.prepareConf(Map(), Nil, Nil, Nil, Seq("file:/not_allowed"))
    }

    // Test that file lists are merged and resolved.
    val base = "/file1.txt"
    val other = Seq("/file2.txt")
    val expected = Some(Seq("dummy://" + other(0), "dummy://" + base).mkString(","))

    val userLists = Seq(LivyConf.SPARK_JARS, LivyConf.SPARK_FILES, LivyConf.SPARK_ARCHIVES,
      LivyConf.SPARK_PY_FILES)
    val baseConf = userLists.map { key => (key -> base) }.toMap
    val result = session.prepareConf(baseConf, other, other, other, other)
    userLists.foreach { key => assert(result.get(key) ===  expected) }
  }

}
