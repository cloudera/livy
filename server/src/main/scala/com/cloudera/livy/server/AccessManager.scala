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

package com.cloudera.livy.server

import com.cloudera.livy.{LivyConf, Logging}

private[livy] class AccessManager(conf: LivyConf) extends Logging {
  private val aclsOn = conf.getBoolean(LivyConf.ACCESS_CONTROL_ENABLED)

  private val WILDCARD_ACL = "*"

  private val superUsers = conf.configToSeq(LivyConf.SUPERUSERS)
  private val modifyUsers = conf.configToSeq(LivyConf.ACCESS_CONTROL_MODIFY_USERS)
  private val viewUsers = conf.configToSeq(LivyConf.ACCESS_CONTROL_VIEW_USERS)
  private val allowedUsers = conf.configToSeq(LivyConf.ACCESS_CONTROL_ALLOWED_USERS).toSet

  private val viewAcls = (superUsers ++ modifyUsers ++ viewUsers).toSet
  private val modifyAcls = (superUsers ++ modifyUsers).toSet
  private val superAcls = superUsers.toSet

  {
    // Livy will load AccessFilter if acls is on. In this case if configured users (view users,
    // modify users, super users) are not in the allowed user list, then AccessFilter check will
    // be failed, so all these configured users should be included in the allowed users.
    val notAllowedSuperUsers = superUsers.filter(!isUserAllowed(_))
    if (notAllowedSuperUsers.nonEmpty && aclsOn) {
      throw new IllegalArgumentException(
        s"Users ${notAllowedSuperUsers.mkString(", ")} configured in " +
        s"${LivyConf.SUPERUSERS.key} are not fully included in " +
        s"${LivyConf.ACCESS_CONTROL_ALLOWED_USERS.key}, you should added them to the allowed user")
    }

    val notAllowedViewUsers = viewUsers.filter(!isUserAllowed(_))
    if (notAllowedViewUsers.nonEmpty && aclsOn) {
      throw new IllegalArgumentException(
        s"Users ${notAllowedViewUsers.mkString(", ")} configured in " +
        s"${LivyConf.ACCESS_CONTROL_VIEW_USERS.key} are not fully included in " +
        s"${LivyConf.ACCESS_CONTROL_ALLOWED_USERS.key}, you should added them to the allowed user")
    }

   val notAllowedModifyUsers = modifyUsers.filter(!isUserAllowed(_))
    if (notAllowedModifyUsers.nonEmpty && aclsOn) {
      throw new IllegalArgumentException(
        s"Users ${notAllowedViewUsers.mkString(", ")} configured in " +
        s"${LivyConf.ACCESS_CONTROL_MODIFY_USERS.key} are not fully included in " +
        s"${LivyConf.ACCESS_CONTROL_ALLOWED_USERS.key}, you should added them to the allowed user")
    }
  }

  info(s"AccessControlManager acls ${if (aclsOn) "enabled" else "disabled"};" +
    s"users with view permission: ${viewUsers.mkString(", ")};" +
    s"users with modify permission: ${modifyUsers.mkString(", ")};" +
    s"users with super permission: ${superUsers.mkString(", ")}")

  /**
   * Check whether the given user has view access to the REST APIs.
   */
  def checkViewPermissions(user: String): Boolean = {
    debug(s"user=$user aclsOn=$aclsOn viewAcls=${viewAcls.mkString(", ")}")
    if (!aclsOn || user == null || viewAcls.contains(user) || viewAcls.contains(WILDCARD_ACL)) {
      true
    } else {
      false
    }
  }

  /**
   * Check whether the give user has modification access to the REST APIs.
   */
  def checkModifyPermissions(user: String): Boolean = {
    debug(s"user=$user aclsOn=$aclsOn modifyAcls=${modifyAcls.mkString(", ")}")
    if (!aclsOn || user == null || modifyAcls.contains(user) || modifyAcls.contains(WILDCARD_ACL)) {
      true
    } else {
      false
    }
  }

  /**
   * Check whether the give user has super access to the REST APIs. This will always be checked
   * no matter acls is on or off.
   */
  def checkSuperUser(user: String): Boolean = {
    debug(s"user=$user aclsOn=$aclsOn superAcls=${superAcls.mkString(", ")}")
    if (user == null || superUsers.contains(user) || superUsers.contains(WILDCARD_ACL)) {
      true
    } else {
      false
    }
  }

  /**
   * Check whether the given user has the permission to access REST APIs.
   */
  def isUserAllowed(user: String): Boolean = {
    debug(s"user=$user aclsOn=$aclsOn, allowedUsers=${allowedUsers.mkString(", ")}")
    if (!aclsOn || allowedUsers.contains(user) || allowedUsers.contains(WILDCARD_ACL)) {
      true
    } else {
      false
    }
  }

  /**
   * Check whether access control is enabled or not.
   */
  def isAccessControlOn: Boolean = aclsOn
}
