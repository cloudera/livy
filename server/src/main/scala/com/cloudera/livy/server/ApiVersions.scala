package com.cloudera.livy.server

/**
  * This enum defines Livy's API versions.
  * [[com.cloudera.livy.server.AbstractApiVersioningSupport]] uses this enum to perform API version checking.
  *
  * Version is defined as <major version>.<minor version>.
  * When a backward compatible change is made to the API (e.g. adding a new method/field), minor version is bumped.
  * When a backward incompatible change is made (e.g. renaming or removing a method/field), major version is bumped.
  * This scheme ensures our user can safely migrate to a newer version if there’s no change in major version.
  */
object ApiVersions extends Enumeration {
  type ApiVersions = Value
  // ApiVersions are ordered and the ordering is defined by the order of Value() calls.
  // Please make sure API version is defined in ascending order (Older API before newer). AbstractApiVersioningSupport relies on the ordering.
  val v0_1 = Value("0.1")
}
