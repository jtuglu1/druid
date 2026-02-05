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

package org.apache.druid.segment.virtual;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Assert;
import org.junit.Test;

public class ListFilteredVirtualColumnTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();

  @Test
  public void testSerdeAllowList() throws JsonProcessingException
  {
    ListFilteredVirtualColumn virtualColumn = new ListFilteredVirtualColumn(
        "hello",
        new DefaultDimensionSpec("column", "output", ColumnType.STRING),
        ImmutableSet.of("foo", "bar"),
        true
    );
    ListFilteredVirtualColumn roundTrip = MAPPER.readValue(MAPPER.writeValueAsString(virtualColumn), ListFilteredVirtualColumn.class);
    Assert.assertEquals(virtualColumn, roundTrip);
    Assert.assertArrayEquals(virtualColumn.getCacheKey(), roundTrip.getCacheKey());
  }

  @Test
  public void testSerdeDenyList() throws JsonProcessingException
  {
    ListFilteredVirtualColumn virtualColumn = new ListFilteredVirtualColumn(
        "hello",
        new DefaultDimensionSpec("column", "output", ColumnType.STRING),
        ImmutableSet.of("foo", "bar"),
        false
    );
    ListFilteredVirtualColumn roundTrip = MAPPER.readValue(MAPPER.writeValueAsString(virtualColumn), ListFilteredVirtualColumn.class);
    Assert.assertEquals(virtualColumn, roundTrip);
    Assert.assertArrayEquals(virtualColumn.getCacheKey(), roundTrip.getCacheKey());
  }

  @Test
  public void testEqualsAndHashcode()
  {
    EqualsVerifier.forClass(ListFilteredVirtualColumn.class).usingGetClass().verify();
  }

  @Test
  public void testCapabilitiesWithoutInspector()
  {
    ListFilteredVirtualColumn virtualColumn = new ListFilteredVirtualColumn(
        "filtered",
        new DefaultDimensionSpec("column", "output", ColumnType.STRING),
        ImmutableSet.of("a", "b"),
        true
    );

    ColumnCapabilities capabilities = virtualColumn.capabilities("filtered");
    Assert.assertNotNull(capabilities);
    Assert.assertEquals(ColumnType.STRING, capabilities.toColumnType());
    Assert.assertTrue(capabilities.isDictionaryEncoded().isTrue());
    Assert.assertTrue(capabilities.hasBitmapIndexes());
    // After list filtering, the output is single-valued for grouping purposes.
    Assert.assertTrue(capabilities.hasMultipleValues().isFalse());
  }

  @Test
  public void testCapabilitiesWithInspector()
  {
    ListFilteredVirtualColumn virtualColumn = new ListFilteredVirtualColumn(
        "filtered",
        new DefaultDimensionSpec("column", "output", ColumnType.STRING),
        ImmutableSet.of("a", "b"),
        true
    );

    // Create an inspector that returns multi-valued capabilities for the underlying column
    ColumnInspector inspector = columnName -> {
      if ("column".equals(columnName)) {
        return new ColumnCapabilitiesImpl()
            .setType(ColumnType.STRING)
            .setDictionaryEncoded(true)
            .setHasBitmapIndexes(true)
            .setHasMultipleValues(true);
      }
      return null;
    };

    ColumnCapabilities capabilities = virtualColumn.capabilities(inspector, "filtered");
    Assert.assertNotNull(capabilities);
    Assert.assertEquals(ColumnType.STRING, capabilities.toColumnType());
    Assert.assertTrue(capabilities.isDictionaryEncoded().isTrue());
    Assert.assertTrue(capabilities.hasBitmapIndexes());
    // Even though the underlying column has multiple values, after filtering
    // the output is single-valued for grouping purposes.
    Assert.assertTrue(capabilities.hasMultipleValues().isFalse());
  }

  @Test
  public void testCapabilitiesWithInspectorNullUnderlying()
  {
    ListFilteredVirtualColumn virtualColumn = new ListFilteredVirtualColumn(
        "filtered",
        new DefaultDimensionSpec("nonexistent", "output", ColumnType.STRING),
        ImmutableSet.of("a", "b"),
        true
    );

    // Inspector returns null for non-existent column
    ColumnInspector inspector = columnName -> null;

    ColumnCapabilities capabilities = virtualColumn.capabilities(inspector, "filtered");
    Assert.assertNull(capabilities);
  }
}
