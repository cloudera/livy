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
package com.cloudera.livy.test

import java.io.{File, FileOutputStream, InputStream}
import java.nio.charset.StandardCharsets._
import java.nio.file.Files

import scala.collection.JavaConverters._

import com.cloudera.livy.test.framework.BaseIntegrationTestSuite

class PythonJobApiIT extends BaseIntegrationTestSuite {

  private var process: Process = _

  private def createPyTestsForPythonAPI(): File = {
    val source: InputStream = getClass.getClassLoader.getResourceAsStream("test_python_api.py")

    val file = Files.createTempFile("", "").toFile
    file.deleteOnExit()

    val sink = new FileOutputStream(file)
    val buf = new Array[Byte](1024)
    var n = source.read(buf)

    while (n > 0) {
      sink.write(buf, 0, n)
      n = source.read(buf)
    }

    source.close()
    sink.close()

    file
  }

  private def createTempFilesForTest(fileName: String,
                                     fileExtension: String,
                                     fileContent: String,
                                     uploadFileToHdfs: Boolean): String = {
    val path = Files.createTempFile(fileName, fileExtension)
    Files.write(path, fileContent.getBytes(UTF_8))
    if (uploadFileToHdfs) {
      return uploadToHdfs(path.toFile())
    }
    path.toString
  }

  test("validate Python-API requests") {
    val addFileContent = "hello from addfile"
    val addFilePath = createTempFilesForTest("add_file", ".txt", addFileContent, true)
    val addPyFileContent = "def test_add_pyfile(): return \"hello from addpyfile\""
    val addPyFilePath = createTempFilesForTest("add_pyfile", ".py", addPyFileContent, true)
    val uploadFileContent = "hello from uploadfile"
    val uploadFilePath = createTempFilesForTest("upload_pyfile", ".py", uploadFileContent, false)
    val uploadPyFileContent = "def test_upload_pyfile(): return \"hello from uploadpyfile\""
    val uploadPyFilePath = createTempFilesForTest("upload_pyfile", ".py",
      uploadPyFileContent, false)

    val builder = new ProcessBuilder(Seq("python", createPyTestsForPythonAPI().toString).asJava)

    val env = builder.environment()
    env.put("LIVY_END_POINT", livyEndpoint)
    env.put("ADD_FILE_URL", addFilePath)
    env.put("ADD_PYFILE_URL", addPyFilePath)
    env.put("UPLOAD_FILE_URL", uploadFilePath)
    env.put("UPLOAD_PYFILE_URL", uploadPyFilePath)

    builder.redirectOutput(new File("src/test/resources/pytest_results.txt"))

    process = builder.start()

    process.waitFor()

    if (process.exitValue() == 1) {
      fail("One or more pytest have failed. Check pytest_results.txt for test results")
    }
  }
}
