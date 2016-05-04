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

package com.cloudera.livy.test.jobs;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.cloudera.livy.Job;
import com.cloudera.livy.JobContext;

public class SQLGetTweets implements Job<List<String>> {

  private final boolean useHiveContext;

  public SQLGetTweets(boolean useHiveContext) {
    this.useHiveContext = useHiveContext;
  }

  @Override
  public List<String> call(JobContext jc) throws Exception {
    InputStream source = getClass().getResourceAsStream("/testweet.json");

    // Save the resource as a file in the temp directory.
    File target = File.createTempFile("tweets", ".json", jc.getLocalTmpDir());
    Files.copy(source, target.toPath(), StandardCopyOption.REPLACE_EXISTING);

    SQLContext sqlctx = useHiveContext ? jc.hivectx() : jc.sqlctx();
    DataFrame input = sqlctx.jsonFile(target.toURI().toString());
    input.registerTempTable("tweets");

    DataFrame topTweets = sqlctx.sql(
      "SELECT text, retweetCount FROM tweets ORDER BY retweetCount LIMIT 10");
    List<String> tweetList = new ArrayList<>();
    for (Row r : topTweets.collect()) {
       tweetList.add(r.toString());
    }
    return tweetList;
  }

}
