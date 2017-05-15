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
package com.cloudera.livy.server.auth;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Properties;

import javax.naming.NamingException;
import javax.naming.directory.InitialDirContext;
import javax.naming.ldap.Control;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.StartTlsRequest;
import javax.naming.ldap.StartTlsResponse;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.server.AuthenticationHandler;
import org.apache.hadoop.security.authentication.server.AuthenticationHandlerUtil;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Private
@Evolving
public class LdapAuthenticationHandlerImpl implements AuthenticationHandler {
  private static Logger logger = LoggerFactory.getLogger(LdapAuthenticationHandlerImpl.class);
  public static final String TYPE = "com.cloudera.livy.server.auth.LdapAuthenticationHandlerImpl";
  public static final String SECURITY_AUTHENTICATION = "simple";
  public static final String PROVIDER_URL = "ldap.providerurl";
  public static final String BASE_DN = "ldap.basedn";
  public static final String LDAP_BIND_DOMAIN = "ldap.binddomain";
  public static final String ENABLE_START_TLS = "ldap.enablestarttls";
  private String ldapDomain;
  private String baseDN;
  private String providerUrl;
  private Boolean enableStartTls;
  private Boolean disableHostNameVerification;

  public LdapAuthenticationHandlerImpl() {
  }

  @VisibleForTesting
  public void setEnableStartTls(Boolean enableStartTls) {
    this.enableStartTls = enableStartTls;
  }

  @VisibleForTesting
  public void setDisableHostNameVerification(Boolean disableHostNameVerification) {
    this.disableHostNameVerification = disableHostNameVerification;
  }

  public String getType() {
    return "ldap";
  }

  public void init(Properties config)
      throws ServletException {
    this.baseDN = config.getProperty(BASE_DN);
    this.providerUrl = config.getProperty(PROVIDER_URL);
    this.ldapDomain = config.getProperty(LDAP_BIND_DOMAIN);
    this.enableStartTls = Boolean.valueOf(config.getProperty(ENABLE_START_TLS, "false"));
    Preconditions.checkNotNull(this.providerUrl, "The LDAP URI can not be null");
    if (this.enableStartTls.booleanValue()) {
      String tmp = this.providerUrl.toLowerCase();
      Preconditions.checkArgument(!tmp.startsWith("ldaps"), "Can not use ldaps and StartTLS option at the same time");
    }
  }

  public void destroy() {
  }

  public boolean managementOperation(AuthenticationToken token, HttpServletRequest request,
      HttpServletResponse response)
      throws IOException, AuthenticationException {
    return true;
  }

  public AuthenticationToken authenticate(HttpServletRequest request, HttpServletResponse response)
      throws IOException, AuthenticationException {
    logger.debug("[authenticate] started");
    logger.debug("[authenticate] Cookies: " + Arrays.toString(request.getCookies()));
    AuthenticationToken token = null;
    String authorization = request.getHeader("Authorization");
    if (authorization != null && AuthenticationHandlerUtil.matchAuthScheme("Basic", authorization)) {
      authorization = authorization.substring("Basic".length()).trim();
      Base64 base64 = new Base64(0);
      String[] credentials = (new String(base64.decode(authorization), StandardCharsets.UTF_8)).split(":", 2);
      if (credentials.length == 2) {
        token = this.authenticateUser(credentials[0], credentials[1]);
        response.setStatus(200);
      }
    } else {
      response.setHeader("WWW-Authenticate", "Basic");
      response.setStatus(401);
      if (authorization == null) {
        logger.trace("Basic auth starting");
      } else {
        logger.warn("\'Authorization\' does not start with \'Basic\' :  {}", authorization);
      }
    }
    logger.debug("[authenticate] ended");
    return token;
  }

  private AuthenticationToken authenticateUser(String userName, String password)
      throws AuthenticationException {
    logger.debug("[authenticateUser] started");
    if (userName != null && !userName.isEmpty()) {
      if (!hasDomain(userName) && this.ldapDomain != null) {
        userName = userName + "@" + this.ldapDomain;
      }

      if (password != null && !password.isEmpty() && password.getBytes(StandardCharsets.UTF_8)[0] != 0) {
        String bindDN;
        if (this.baseDN == null) {
          bindDN = userName;
        } else {
          bindDN = "uid=" + userName + "," + this.baseDN;
        }

        if (this.enableStartTls.booleanValue()) {
          this.authenticateWithTlsExtension(bindDN, password);
        } else {
          this.authenticateWithoutTlsExtension(bindDN, password);
        }
        logger.debug("[authenticateUser] ended");
        return new AuthenticationToken(userName, userName, "ldap");
      } else {
        throw new AuthenticationException("Error validating LDAP user: a null or blank password has been provided");
      }
    } else {
      throw new AuthenticationException("Error validating LDAP user: a null or blank username has been provided");
    }
  }

  private void authenticateWithTlsExtension(String userDN, String password)
      throws AuthenticationException {
    InitialLdapContext ctx = null;
    Hashtable env = new Hashtable();
    env.put("java.naming.factory.initial", "com.sun.jndi.ldap.LdapCtxFactory");
    env.put("java.naming.provider.url", this.providerUrl);

    try {
      ctx = new InitialLdapContext(env, (Control[]) null);
      StartTlsResponse ex = (StartTlsResponse) ctx.extendedOperation(new StartTlsRequest());
      if (this.disableHostNameVerification.booleanValue()) {
        ex.setHostnameVerifier(new HostnameVerifier() {
          public boolean verify(String hostname, SSLSession session) {
            return true;
          }
        });
      }

      ex.negotiate();
      ctx.addToEnvironment("java.naming.security.authentication", SECURITY_AUTHENTICATION);
      ctx.addToEnvironment("java.naming.security.principal", userDN);
      ctx.addToEnvironment("java.naming.security.credentials", password);
      ctx.lookup(userDN);
      logger.debug("Authentication successful for {}", userDN);
    } catch (IOException | NamingException var13) {
      throw new AuthenticationException("Error validating LDAP user", var13);
    } finally {
      if (ctx != null) {
        try {
          ctx.close();
        } catch (NamingException var12) {
          ;
        }
      }
    }
  }

  private void authenticateWithoutTlsExtension(String userDN, String password)
      throws AuthenticationException {
    Hashtable env = new Hashtable();
    env.put("java.naming.factory.initial", "com.sun.jndi.ldap.LdapCtxFactory");
    env.put("java.naming.provider.url", this.providerUrl);
    env.put("java.naming.security.authentication", SECURITY_AUTHENTICATION);
    env.put("java.naming.security.principal", userDN);
    env.put("java.naming.security.credentials", password);

    try {
      InitialDirContext e = new InitialDirContext(env);
      e.close();
      logger.debug("Authentication successful for {}", userDN);
    } catch (NamingException var5) {
      throw new AuthenticationException("Error validating LDAP user", var5);
    }
  }

  private static boolean hasDomain(String userName) {
    return indexOfDomainMatch(userName) > 0;
  }

  private static int indexOfDomainMatch(String userName) {
    if (userName == null) {
      return -1;
    } else {
      int idx = userName.indexOf(47);
      int idx2 = userName.indexOf(64);
      int endIdx = Math.min(idx, idx2);
      if (endIdx == -1) {
        endIdx = Math.max(idx, idx2);
      }

      return endIdx;
    }
  }
}