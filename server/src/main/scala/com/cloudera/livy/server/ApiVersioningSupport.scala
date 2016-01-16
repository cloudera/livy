package com.cloudera.livy.server

import javax.servlet.http.HttpServletRequest

import org.scalatra.{NotAcceptable, ScalatraBase}

/**
  * Livy's servlets can mix-in this trait to get API version support.
  *
  * Example: {{{
  * import ApiVersions._
  * class FooServlet
  *   ...
  *   with ApiVersioningSupport
  *   ...
  * {
  *   get("/test") {
  *     ...
  *   }
  *   get("/test", getApiVersion() <= v0_2) {
  *     ...
  *   }
  *   get("/test", getApiVersion() <= v0_1) {
  *     ...
  *   }
  * }
  * }}}
  */
trait ApiVersioningSupport extends AbstractApiVersioningSupport {
  this: ScalatraBase =>
  // Link the abstract trait to Livy's version enum.
  override val apiVersions = ApiVersions
  override type ApiVersionType = ApiVersions.Value
}

trait AbstractApiVersioningSupport {
  this: ScalatraBase =>
  protected val apiVersions: Enumeration
  protected type ApiVersionType

  /**
    * Before proceeding with routing, validate the specified API version in the request.
    * If validation passes, cache the parsed API version as a per-request attribute.
    */
  before() {
    try {
      request(AbstractApiVersioningSupport.ApiVersionKey) = parseForApiVersion(request)
    } catch {
      case e: Exception =>
        halt(NotAcceptable(e.getMessage))
    }
  }

  /**
    * @return The specified API version in the request.
    */
  def getApiVersion(): ApiVersionType = {
    request(AbstractApiVersioningSupport.ApiVersionKey).asInstanceOf[ApiVersionType]
  }

  private def extractVersionFromAcceptHeader(acceptHeader: String): Option[String] = {
    // Get every character after "application/vnd.livy.v" until hitting a + sign.
    val acceptHeaderRegex = """application/vnd\.livy\.v([^\+]*).*""".r

    acceptHeader match {
      case acceptHeaderRegex(v) => Option(v)
      case _ => None
    }
  }

  private def parseForApiVersion(request: HttpServletRequest): ApiVersionType = {
    val latestVersion = apiVersions.apply(apiVersions.maxId - 1).asInstanceOf[ApiVersionType]
    val acceptHeader = request.getHeader("Accept")
    if (acceptHeader == null) {
      // Accept header is missing. Use the latest API.
      latestVersion
    } else {
      val version = extractVersionFromAcceptHeader(acceptHeader)
      if (version.isDefined) {
        try {
          apiVersions.withName(version.get).asInstanceOf[ApiVersionType]
        } catch {
          case e: NoSuchElementException =>
            throw new Exception(s"Unknown API version: $acceptHeader", e)
        }
      } else {
        // Version is missing in the Accept header. Use the latest API.
        latestVersion
      }
    }
  }
}

object AbstractApiVersioningSupport {
  // AbstractApiVersioningSupport uses a per-request attribute to store the parsed API version. This is the key name for the attribute.
  private final val ApiVersionKey = "apiVersion"
}
