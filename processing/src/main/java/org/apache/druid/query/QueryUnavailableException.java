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

package org.apache.druid.query;

/**
 * Thrown when one or more segments required by a query are in the broker's timeline
 * but have no available servers serving them. This indicates a temporary outage
 * of data nodes rather than a missing segment.
 *
 * <p>Returns HTTP 503 Service Unavailable to allow clients to retry.
 */
public class QueryUnavailableException extends QueryException
{
  public static final String ERROR_CODE = "Unavailable segments";

  public QueryUnavailableException(String errorMessage)
  {
    super(
        null,
        ERROR_CODE,
        errorMessage,
        QueryUnavailableException.class.getName(),
        resolveHostname()
    );
  }
}
