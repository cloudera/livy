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

package com.cloudera.livy.repl

import org.apache.spark.{JobExecutionStatus, SparkContext, SparkStatusTracker}

/**
 * [[StatementProgressTracker]] is used to track the progress of submitted statement, this class
 * leverages Spark's SparkStatusTrack to build a mapping relation between statement, jobs, stages
 * and tasks, and uses the finished task number to calculate the statement progress.
 *
 * This statement progress can only reflect the statement in which has Spark jobs, if
 * the statement submitted doesn't generate any Spark job, the progress will always return 0.0.
 *
 * Also if the statement includes several Spark jobs, the progress will be flipped because we
 * don't know the actual number of Spark jobs/tasks generated before the statement executed.
 */
private[repl] class StatementProgressTracker(sc: SparkContext) {

  private val _statusTracker: SparkStatusTracker = sc.statusTracker

  /**
   * Set job group of current statement.
   */
  def setJobGroup(stmtId: Int): Unit = {
    sc.setJobGroup(statementIdToJobGroup(stmtId), s"Job group for statement $stmtId")
  }

  /**
   * Get the current progress of given statement id.
   */
  def progressOfStatement(stmtId: Int): Double = {
    val jobGroup = statementIdToJobGroup(stmtId)

    val jobIds = _statusTracker.getJobIdsForGroup(jobGroup)
    val jobs = jobIds.flatMap { id => _statusTracker.getJobInfo(id) }
    val stages = jobs.flatMap { job =>
      job.stageIds().flatMap(_statusTracker.getStageInfo)
    }

    val taskCount = stages.map(_.numTasks).sum
    val completedTaskCount = stages.map(_.numCompletedTasks).sum
    if (taskCount == 0) {
      0.0
    } else {
      completedTaskCount.toDouble / taskCount
    }
  }

  /**
   * Get the active job ids of the given statement id.
   */
  def activeJobsOfStatement(stmtId: Int): Seq[Int] = {
    _statusTracker.getJobIdsForGroup(statementIdToJobGroup(stmtId)).flatMap { id =>
      _statusTracker.getJobInfo(id)
    }.filter { info => info.status() == JobExecutionStatus.RUNNING }
      .map(_.jobId())
  }

  private def statementIdToJobGroup(stmtId: Int): String = stmtId.toString
}
