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

import scala.collection.mutable

import com.google.common.annotations.VisibleForTesting
import org.apache.spark.scheduler._

import com.cloudera.livy.rsc.RSCConf

case class TaskCount(var currFinishedTasks: Int, totalTasks: Int)
case class JobState(jobId: Int, var isCompleted: Boolean)

/**
 * [[StatementProgressListener]] is an implementation of SparkListener, used to track the progress
 * of submitted statement, this class builds a mapping relation between statement, jobs, stages
 * and tasks, and uses the finished task number to calculate the statement progress.
 *
 * By default 100 latest statement progresses will be kept, users could also configure
 * livy.rsc.retained_statements to change the cached number.
 *
 * This statement progress can only reflect the statement in which has Spark jobs, if
 * the statement submitted doesn't generate any Spark job, the progress will always return 0.0
 * until completed.
 */
class StatementProgressListener(conf: RSCConf) extends SparkListener {

  private val retainedStatements = conf.getInt(RSCConf.Entry.RETAINED_STATEMENT_NUMBER)

  /** Statement id to list of jobs map */
  @VisibleForTesting
  private[repl] val statementToJobs = new mutable.LinkedHashMap[Int, Seq[JobState]]()
  @VisibleForTesting
  private[repl] val jobIdToStatement = new mutable.HashMap[Int, Int]()
  /** Job id to list of stage ids map */
  @VisibleForTesting
  private[repl] val jobIdToStages = new mutable.HashMap[Int, Seq[Int]]()
  /** Stage id to number of finished/total tasks map */
  @VisibleForTesting
  private[repl] val stageIdToTaskCount = new mutable.HashMap[Int, TaskCount]()

  private var currentStatementId: Int = _

  /**
   * Set current statement id, onJobStart() will use current statement id to build the mapping
   * relations.
   */
  def setCurrentStatementId(stmtId: Int): Unit = {
    currentStatementId = stmtId
  }

  /**
   * Get the current progress of given statement id.
   */
  def progressOfStatement(stmtId: Int): Double = synchronized {
    var finishedTasks = 0
    var totalTasks = 0

    for {
      job <- statementToJobs.getOrElse(stmtId, Seq.empty)
      stageId <- jobIdToStages.getOrElse(job.jobId, Seq.empty)
      taskCount <- stageIdToTaskCount.get(stageId)
    } yield {
      finishedTasks += taskCount.currFinishedTasks
      totalTasks += taskCount.totalTasks
    }

    if (totalTasks == 0) {
      0.0
    } else {
      finishedTasks.toDouble / totalTasks
    }
  }

  /**
   * Get the active job ids of the given statement id.
   */
  def activeJobsOfStatement(stmtId: Int): Seq[Int] = synchronized {
    statementToJobs.getOrElse(stmtId, Seq.empty).filter(!_.isCompleted).map(_.jobId)
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = synchronized {
    val jobs = statementToJobs.getOrElseUpdate(currentStatementId, Seq.empty) :+
      JobState(jobStart.jobId, isCompleted = false)
    statementToJobs.put(currentStatementId, jobs)
    jobIdToStatement(jobStart.jobId) = currentStatementId

    statementToJobs.foreach(println)

    jobIdToStages(jobStart.jobId) = jobStart.stageInfos.map(_.stageId)
    jobStart.stageInfos.foreach { s => stageIdToTaskCount(s.stageId) = TaskCount(0, s.numTasks) }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
    stageIdToTaskCount.get(taskEnd.stageId).foreach { t => t.currFinishedTasks += 1 }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = synchronized {
    stageIdToTaskCount.get(stageCompleted.stageInfo.stageId).foreach { t =>
      t.currFinishedTasks = t.totalTasks
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = synchronized {
    jobIdToStatement.get(jobEnd.jobId).foreach { stmtId =>
      statementToJobs.get(stmtId).foreach { jobs =>
        jobs.filter(_.jobId == jobEnd.jobId).foreach(_.isCompleted = true)
      }
    }

    // Try to clean the old data when job is finished. This will trigger data cleaning in LRU
    // policy.
    cleanOldMetadata()
  }

  private def cleanOldMetadata(): Unit = {
    if (statementToJobs.size > retainedStatements) {
      val toRemove = statementToJobs.size - retainedStatements
      statementToJobs.take(toRemove).foreach { case (_, jobs) =>
        jobs.foreach { job =>
          jobIdToStatement.remove(job.jobId)
          jobIdToStages.remove(job.jobId).foreach { stages =>
            stages.foreach(s => stageIdToTaskCount.remove(s))
          }
        }
      }
      (0 until toRemove).foreach(_ => statementToJobs.remove(statementToJobs.head._1))
    }
  }
}
