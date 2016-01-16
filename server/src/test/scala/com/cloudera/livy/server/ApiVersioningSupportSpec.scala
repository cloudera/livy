package com.cloudera.livy.server

import org.scalatest.FunSpecLike
import org.scalatra.ScalatraServlet
import org.scalatra.test.scalatest.ScalatraSuite

class ApiVersioningSupportSpec extends ScalatraSuite with FunSpecLike {
  val LatestVersionOutput = "latest"

  object FakeApiVersions extends Enumeration {
    type FakeApiVersions = Value
    val v0_1 = Value("0.1")
    val v0_2 = Value("0.2")
    val v1_0 = Value("1.0")
  }

  import FakeApiVersions._

  class MockServlet extends ScalatraServlet with AbstractApiVersioningSupport {
    override val apiVersions = FakeApiVersions
    override type ApiVersionType = FakeApiVersions.Value

    get("/test") {
      response.writer.write(LatestVersionOutput)
    }

    get("/test", getApiVersion() <= v0_2) {
      response.writer.write(v0_2.toString)
    }

    get("/test", getApiVersion() <= v0_1) {
      response.writer.write(v0_1.toString)
    }

    get("/droppedApi", getApiVersion() <= v0_2) {
    }

    get("/newApi", getApiVersion() >= v0_2) {
    }
  }

  var mockServlet: MockServlet = new MockServlet
  addServlet(mockServlet, "/*")

  def generateHeader(acceptHeader: String): Map[String, String] = {
    if (acceptHeader != null) Map("Accept" -> acceptHeader) else Map.empty
  }

  def shouldReturn(url: String, acceptHeader: String, expectedVersion: String = null) = {
    get(url, headers = generateHeader(acceptHeader)) {
      status should equal(200)
      if (expectedVersion != null) {
        body should equal(expectedVersion)
      }
    }
  }

  def shouldFail(url: String, acceptHeader: String, expectedErrorCode: Int) = {
    get(url, headers = generateHeader(acceptHeader)) {
      status should equal(expectedErrorCode)
    }
  }

  it("should pick the latest API version if Accept header is unspecified") {
    shouldReturn("/test", null, LatestVersionOutput)
  }

  it("should pick the latest API version if Accept header does not specify any version") {
    shouldReturn("/test", "foo", LatestVersionOutput)
    shouldReturn("/test", "application/vnd.random.v1.1", LatestVersionOutput)
    shouldReturn("/test", "application/vnd.livy.+json", LatestVersionOutput)
  }

  it("should pick the correct API version") {
    shouldReturn("/test", "application/vnd.livy.v0.1", v0_1.toString)
    shouldReturn("/test", "application/vnd.livy.v0.2+", v0_2.toString)
    shouldReturn("/test", "application/vnd.livy.v0.1+bar", v0_1.toString)
    shouldReturn("/test", "application/vnd.livy.v0.2+foo", v0_2.toString)
    shouldReturn("/test", "application/vnd.livy.v0.1+vnd.livy.v0.2", v0_1.toString)
    shouldReturn("/test", "application/vnd.livy.v0.2++++++++++++++++", v0_2.toString)
    shouldReturn("/test", "application/vnd.livy.v1.0", LatestVersionOutput)
  }

  it("should return error when the specified API version does not exist") {
    shouldFail("/test", "application/vnd.livy.v", 406)
    shouldFail("/test", "application/vnd.livy.v+json", 406)
    shouldFail("/test", "application/vnd.livy.v666.666", 406)
    shouldFail("/test", "application/vnd.livy.v666.666+json", 406)
    shouldFail("/test", "application/vnd.livy.v1.1+json", 406)
  }

  it("should not see a dropped API") {
    shouldReturn("/droppedApi", "application/vnd.livy.v0.1+json")
    shouldReturn("/droppedApi", "application/vnd.livy.v0.2+json")
    shouldFail("/droppedApi", "application/vnd.livy.v1.0+json", 404)
  }

  it("should not see a new API at an older version") {
    shouldFail("/newApi", "application/vnd.livy.v0.1+json", 404)
    shouldReturn("/newApi", "application/vnd.livy.v0.2+json")
    shouldReturn("/newApi", "application/vnd.livy.v1.0+json")
  }
}
