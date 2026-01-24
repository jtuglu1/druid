/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.iceberg.input;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.auth.TaskAuthContext;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.iceberg.guice.HiveConf;
import org.apache.druid.utils.DynamicConfigProviderUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.rest.RESTSessionCatalog;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Catalog implementation for Iceberg REST catalogs.
 * Uses {@link RESTSessionCatalog} to support session-based authentication where
 * credentials can be passed per-request via {@link SessionCatalog.SessionContext}.
 *
 * <p>The session context is built from {@link TaskAuthContext} if available, allowing
 * user credentials (e.g., OAuth tokens) to be passed to the REST catalog for
 * authentication and credential vending.
 */
public class RestIcebergCatalog extends IcebergCatalog
{
  public static final String TYPE_KEY = "rest";

  @JsonProperty
  private final String catalogUri;

  @JsonProperty
  private final Map<String, String> catalogProperties;

  private final Configuration configuration;

  /**
   * Task auth context for accessing the REST catalog with user credentials.
   * Transient - not serialized, set programmatically during task execution.
   */
  @JsonIgnore
  private transient TaskAuthContext taskAuthContext;

  private RESTSessionCatalog restSessionCatalog;

  @JsonCreator
  public RestIcebergCatalog(
      @JsonProperty("catalogUri") String catalogUri,
      @JsonProperty("catalogProperties") @Nullable
          Map<String, Object> catalogProperties,
      @JacksonInject @Json ObjectMapper mapper,
      @JacksonInject @HiveConf Configuration configuration
  )
  {
    if (catalogUri == null) {
      throw InvalidInput.exception("catalogUri cannot be null");
    }
    this.catalogUri = catalogUri;
    this.catalogProperties = DynamicConfigProviderUtils.extraConfigAndSetStringMap(
        catalogProperties,
        DRUID_DYNAMIC_CONFIG_PROVIDER_KEY,
        mapper
    );
    this.configuration = configuration;
  }

  /**
   * Returns a Catalog instance that uses the current session context for all operations.
   * The returned Catalog wraps the underlying RESTSessionCatalog and passes session
   * credentials to all catalog operations (listTables, loadTable, etc.).
   */
  @Override
  public Catalog retrieveCatalog()
  {
    return getOrCreateSessionCatalog().asCatalog(buildSessionContext());
  }

  public String getCatalogUri()
  {
    return catalogUri;
  }

  public Map<String, String> getCatalogProperties()
  {
    return catalogProperties;
  }

  /**
   * Sets the task auth context for accessing the REST catalog with user credentials.
   * When set, the credentials will be used to create a SessionContext for authentication.
   *
   * @param taskAuthContext the auth context containing credentials, may be null
   */
  public void setTaskAuthContext(@Nullable TaskAuthContext taskAuthContext)
  {
    this.taskAuthContext = taskAuthContext;
  }

  private RESTSessionCatalog getOrCreateSessionCatalog()
  {
    if (restSessionCatalog == null) {
      restSessionCatalog = setupCatalog();
    }
    return restSessionCatalog;
  }

  private RESTSessionCatalog setupCatalog()
  {
    RESTSessionCatalog catalog = new RESTSessionCatalog();
    catalog.setConf(configuration);
    Map<String, String> props = new HashMap<>(catalogProperties);
    props.put(CatalogProperties.URI, catalogUri);
    catalog.initialize("rest", props);
    return catalog;
  }

  /**
   * Builds a SessionContext for the REST catalog. If a TaskAuthContext is available with credentials,
   * they will be included in the session context for authentication with the REST catalog server.
   *
   * @return a SessionContext, either with credentials or empty
   */
  private SessionCatalog.SessionContext buildSessionContext()
  {
    if (taskAuthContext != null && taskAuthContext.getCredentials() != null && !taskAuthContext.getCredentials().isEmpty()) {
      // Create session context with credentials
      // Constructor: (sessionId, identity, credentials, properties)
      return new SessionCatalog.SessionContext(
          UUID.randomUUID().toString(),
          taskAuthContext.getIdentity(),
          taskAuthContext.getCredentials(),
          Collections.emptyMap()
      );
    }
    return SessionCatalog.SessionContext.createEmpty();
  }
}
