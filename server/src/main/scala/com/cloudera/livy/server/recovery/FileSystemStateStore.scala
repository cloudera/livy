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
package com.cloudera.livy.server.recovery

import java.io.{ByteArrayOutputStream, FileNotFoundException}
import java.net.URI
import java.util

import org.apache.hadoop.fs.{CreateFlag, FileContext, FSDataInputStream, Path}
import org.apache.hadoop.fs.Options.{CreateOpts, Rename}

import com.cloudera.livy.{LivyConf, Logging}

object FileSystemStateStore extends StateStoreCompanion {
  override def create(livyConf: LivyConf): StateStore = new FileSystemStateStore(livyConf)
}

class FileSystemStateStore(livyConf: LivyConf) extends StateStore with Logging {
  private val fsUri = {
    val fsPath = livyConf.get(LivyConf.RECOVERY_STATE_STORE_URL_CONF)
    require(!fsPath.isEmpty, s"Please config ${LivyConf.RECOVERY_STATE_STORE_URL_CONF.key}.")
    new URI(fsPath)
  }

  private val fileContext: FileContext = FileContext.getFileContext(fsUri)

  override def set(key: String, value: Object): Unit = {
    // Write to a temp file then rename to avoid file corruption if livy-server crashes
    // in the middle of the write.
    val tmpPath = absPath(s"$key.tmp")
    val tmpFileOutput = fileContext.create(tmpPath,
      util.EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE),
      CreateOpts.createParent())

    try {
      tmpFileOutput.write(serializeToBytes(value))
    } finally {
      tmpFileOutput.close()
      // Assume rename is atomic.
      fileContext.rename(tmpPath, absPath(key), Rename.OVERWRITE)
    }
  }

  override def get[T](key: String, valueType: Class[T]): Option[T] = {
    try {
      val is = fileContext.open(absPath(key))
      try {
        val b = readAllBytes(is)
        Option(deserialize(b, valueType))
      } finally {
        is.close()
      }
    } catch {
      case _: FileNotFoundException => None
    }
  }

  override def getChildren(key: String): Seq[String] = {
    try {
      fileContext.util.listStatus(absPath(key)).map(_.getPath.getName)
    } catch {
      case _: FileNotFoundException => Seq.empty
    }
  }

  override def remove(key: String): Unit = {
    fileContext.delete(absPath(key), false)
  }

  private def absPath(key: String): Path = new Path(s"${fsUri.getPath}/$key")

  private def readAllBytes(is: FSDataInputStream): Array[Byte] = {
    val wholeBuf = new ByteArrayOutputStream()

    val buf = Array.ofDim[Byte](1024)
    while(is.available() > 0) {
      val readByteCount = is.read(buf)
      wholeBuf.write(buf, 0, readByteCount)
    }

    wholeBuf.toByteArray
  }
}
