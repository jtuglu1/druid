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

package org.apache.druid.server.router;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.selector.Server;
import org.apache.druid.discovery.BrokerNodeService;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordinator.rules.IntervalLoadRule;
import org.apache.druid.server.coordinator.rules.Rule;
import org.apache.druid.sql.http.SqlQuery;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class TieredBrokerHostSelectorTest
{
  private DruidNodeDiscoveryProvider druidNodeDiscoveryProvider;
  private DruidNodeDiscovery druidNodeDiscovery;
  private TieredBrokerHostSelector brokerSelector;

  private DiscoveryDruidNode node1;
  private DiscoveryDruidNode node2;
  private DiscoveryDruidNode node3;

  @Before
  public void setUp()
  {
    druidNodeDiscoveryProvider = EasyMock.createStrictMock(DruidNodeDiscoveryProvider.class);

    node1 = new DiscoveryDruidNode(
        new DruidNode("hotBroker", "hotHost", false, 8080, null, true, false),
        NodeRole.BROKER,
        ImmutableMap.of(
            BrokerNodeService.DISCOVERY_SERVICE_KEY,
            new BrokerNodeService("hot-v1")
        )
    );

    node2 = new DiscoveryDruidNode(
        new DruidNode("coldBroker", "coldHost1", false, 8080, null, true, false),
        NodeRole.BROKER,
        ImmutableMap.of(
            BrokerNodeService.DISCOVERY_SERVICE_KEY,
            new BrokerNodeService("cold-v1")
        )
    );

    node3 = new DiscoveryDruidNode(
        new DruidNode("coldBroker", "coldHost2", false, 8080, null, true, false),
        NodeRole.BROKER,
        ImmutableMap.of(
            BrokerNodeService.DISCOVERY_SERVICE_KEY,
            new BrokerNodeService("cold-v1")
        )
    );

    druidNodeDiscovery = new DruidNodeDiscovery()
    {
      @Override
      public Collection<DiscoveryDruidNode> getAllNodes()
      {
        return ImmutableSet.of(node1, node2, node3);
      }

      @Override
      public void registerListener(Listener listener)
      {
        listener.nodesAdded(ImmutableList.of(node1, node2, node3));
        listener.nodeViewInitialized();
      }
    };

    EasyMock.expect(druidNodeDiscoveryProvider.getForNodeRole(NodeRole.BROKER))
            .andReturn(druidNodeDiscovery);

    EasyMock.replay(druidNodeDiscoveryProvider);

    brokerSelector = new TieredBrokerHostSelector(
        new TestRuleManager(null),
        new TieredBrokerConfig()
        {
          @Override
          public LinkedHashMap<String, String> getTierToBrokerMap()
          {
            return new LinkedHashMap<>(
                ImmutableMap.of(
                    "hot", "hotBroker",
                    "medium", "mediumBroker",
                    DruidServer.DEFAULT_TIER, "coldBroker"
                )
            );
          }

          @Override
          public String getDefaultBrokerServiceName()
          {
            return "hotBroker";
          }
        },
        druidNodeDiscoveryProvider,
        Arrays.asList(
            new ManualTieredBrokerSelectorStrategy(null),
            new TimeBoundaryTieredBrokerSelectorStrategy(),
            new PriorityTieredBrokerSelectorStrategy(0, 1)
        )
    );

    brokerSelector.start();
  }

  @After
  public void tearDown()
  {
    brokerSelector.stop();

    EasyMock.verify(druidNodeDiscoveryProvider);
  }

  @Test
  public void testBasicSelect()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("test")
                                  .granularity("all")
                                  .aggregators(
                                      Collections.singletonList(new CountAggregatorFactory("rows")))
                                  .intervals(Collections.singletonList(Intervals.of("2011-08-31/2011-09-01")))
                                  .build();

    Pair<String, Server> p = brokerSelector.select(query);
    Assert.assertEquals("coldBroker", p.lhs);
    Assert.assertEquals("coldHost1:8080", p.rhs.getHost());

    p = brokerSelector.select(query);
    Assert.assertEquals("coldBroker", p.lhs);
    Assert.assertEquals("coldHost2:8080", p.rhs.getHost());

    p = brokerSelector.select(query);
    Assert.assertEquals("coldBroker", p.lhs);
    Assert.assertEquals("coldHost1:8080", p.rhs.getHost());
  }


  @Test
  public void testBasicSelect2()
  {
    Pair<String, Server> p = brokerSelector.select(
        Druids.newTimeseriesQueryBuilder()
              .dataSource("test")
              .granularity("all")
              .aggregators(Collections.singletonList(new CountAggregatorFactory("rows")))
              .intervals(Collections.singletonList(Intervals.of("2013-08-31/2013-09-01")))
              .build()
    );

    Assert.assertEquals("hotBroker", p.lhs);
    Assert.assertEquals("hotHost:8080", p.rhs.getHost());
  }

  @Test
  public void testSelectMatchesNothing()
  {
    String brokerName = (String) brokerSelector.select(
        Druids.newTimeseriesQueryBuilder()
              .dataSource("test")
              .granularity("all")
              .aggregators(Collections.singletonList(new CountAggregatorFactory("rows")))
              .intervals(Collections.singletonList(Intervals.of("2010-08-31/2010-09-01")))
              .build()
    ).lhs;

    Assert.assertEquals("hotBroker", brokerName);
  }

  @Test
  public void testSelectMultiInterval()
  {
    String brokerName = (String) brokerSelector.select(
        Druids.newTimeseriesQueryBuilder()
              .dataSource("test")
              .aggregators(Collections.singletonList(new CountAggregatorFactory("count")))
              .intervals(
                  new MultipleIntervalSegmentSpec(
                      Arrays.asList(
                          Intervals.of("2013-08-31/2013-09-01"),
                          Intervals.of("2012-08-31/2012-09-01"),
                          Intervals.of("2011-08-31/2011-09-01")
                      )
                  )
              ).build()
    ).lhs;

    Assert.assertEquals("coldBroker", brokerName);
  }

  @Test
  public void testSelectMultiInterval2()
  {
    String brokerName = (String) brokerSelector.select(
        Druids.newTimeseriesQueryBuilder()
              .dataSource("test")
              .aggregators(Collections.singletonList(new CountAggregatorFactory("count")))
              .intervals(
                  new MultipleIntervalSegmentSpec(
                      Arrays.asList(
                          Intervals.of("2011-08-31/2011-09-01"),
                          Intervals.of("2012-08-31/2012-09-01"),
                          Intervals.of("2013-08-31/2013-09-01")
                      )
                  )
              ).build()
    ).lhs;

    Assert.assertEquals("coldBroker", brokerName);
  }

  @Test
  public void testPrioritySelect()
  {
    String brokerName = (String) brokerSelector.select(
        Druids.newTimeseriesQueryBuilder()
              .dataSource("test")
              .aggregators(Collections.singletonList(new CountAggregatorFactory("count")))
              .intervals(
                  new MultipleIntervalSegmentSpec(
                      Arrays.asList(
                          Intervals.of("2011-08-31/2011-09-01"),
                          Intervals.of("2012-08-31/2012-09-01"),
                          Intervals.of("2013-08-31/2013-09-01")
                      )
                  )
              )
              .context(ImmutableMap.of("priority", -1))
              .build()
    ).lhs;

    Assert.assertEquals("hotBroker", brokerName);
  }

  @Test
  public void testPrioritySelect2()
  {
    String brokerName = (String) brokerSelector.select(
        Druids.newTimeseriesQueryBuilder()
              .dataSource("test")
              .aggregators(Collections.singletonList(new CountAggregatorFactory("count")))
              .intervals(
                  new MultipleIntervalSegmentSpec(
                      Arrays.asList(
                          Intervals.of("2011-08-31/2011-09-01"),
                          Intervals.of("2012-08-31/2012-09-01"),
                          Intervals.of("2013-08-31/2013-09-01")
                      )
                  )
              )
              .context(ImmutableMap.of("priority", 5))
              .build()
    ).lhs;

    Assert.assertEquals("hotBroker", brokerName);
  }

  @Test
  public void testSelectBasedOnQueryContext()
  {
    final Druids.TimeseriesQueryBuilder queryBuilder =
        Druids.newTimeseriesQueryBuilder()
              .dataSource("test")
              .aggregators(Collections.singletonList(new CountAggregatorFactory("count")))
              .intervals(
                  new MultipleIntervalSegmentSpec(
                      Collections.singletonList(Intervals.of("2009/2010"))
                  )
              );

    Assert.assertEquals(
        brokerSelector.getDefaultServiceName(),
        brokerSelector.select(queryBuilder.build()).lhs
    );
    Assert.assertEquals(
        "hotBroker",
        brokerSelector.select(
            queryBuilder
                .context(ImmutableMap.of(QueryContexts.BROKER_SERVICE_NAME, "hotBroker"))
                .build()
        ).lhs
    );
    Assert.assertEquals(
        "coldBroker",
        brokerSelector.select(
            queryBuilder
                .context(ImmutableMap.of(QueryContexts.BROKER_SERVICE_NAME, "coldBroker"))
                .build()
        ).lhs
    );
  }

  @Test
  public void testSelectForSql()
  {
    Assert.assertEquals(
        brokerSelector.getDefaultServiceName(),
        brokerSelector.selectForSql(
            createSqlQueryWithContext(null)
        ).lhs
    );
    Assert.assertEquals(
        "hotBroker",
        brokerSelector.selectForSql(
            createSqlQueryWithContext(
                ImmutableMap.of(QueryContexts.BROKER_SERVICE_NAME, "hotBroker")
            )
        ).lhs
    );
    Assert.assertEquals(
        "coldBroker",
        brokerSelector.selectForSql(
            createSqlQueryWithContext(
                ImmutableMap.of(QueryContexts.BROKER_SERVICE_NAME, "coldBroker")
            )
        ).lhs
    );
  }

  @Test
  public void testGetAllBrokers()
  {
    Assert.assertEquals(
        ImmutableMap.of(
            "mediumBroker", ImmutableList.of(),
            "coldBroker", ImmutableList.of("coldHost1:8080", "coldHost2:8080"),
            "hotBroker", ImmutableList.of("hotHost:8080")
        ),
        Maps.transformValues(
            brokerSelector.getAllBrokers(),
            new Function<List<Server>, List<String>>()
            {
              @Override
              public List<String> apply(@Nullable List<Server> servers)
              {
                return Lists.transform(servers, server -> server.getHost());
              }
            }
        )
    );
  }

  @Test
  public void testGetAllBrokersWithRoutableTiers()
  {
    final DiscoveryDruidNode hotV1Node = newBrokerNode("hotBroker", "hotV1Host", "hot-v1");
    final DiscoveryDruidNode hotV2Node = newBrokerNode("hotBroker", "hotV2Host", "hot-v2");
    final DiscoveryDruidNode nodeWithoutTier = new DiscoveryDruidNode(
        new DruidNode("hotBroker", "hotNoTierHost", false, 8080, null, true, false),
        NodeRole.BROKER,
        ImmutableMap.of()
    );

    final DruidNodeDiscoveryProvider provider = EasyMock.createStrictMock(DruidNodeDiscoveryProvider.class);
    EasyMock.expect(provider.getForNodeRole(NodeRole.BROKER))
            .andReturn(newDiscovery(ImmutableList.of(hotV1Node, hotV2Node, nodeWithoutTier)));
    EasyMock.replay(provider);

    final TieredBrokerHostSelector selector = new TieredBrokerHostSelector(
        new TestRuleManager(null),
        new TestTieredBrokerConfig(ImmutableSet.of("hot-v1")),
        provider,
        Collections.emptyList()
    );
    selector.start();
    try {
      Assert.assertEquals(
          ImmutableMap.of(
              "mediumBroker", ImmutableList.of(),
              "coldBroker", ImmutableList.of(),
              "hotBroker", ImmutableList.of("hotV1Host:8080")
          ),
          Maps.transformValues(
              selector.getAllBrokers(),
              new Function<List<Server>, List<String>>()
              {
                @Override
                public List<String> apply(@Nullable List<Server> servers)
                {
                  return Lists.transform(servers, server -> server.getHost());
                }
              }
          )
      );
    }
    finally {
      selector.stop();
      EasyMock.verify(provider);
    }
  }

  private static DiscoveryDruidNode newBrokerNode(final String serviceName, final String host, final String tier)
  {
    return new DiscoveryDruidNode(
        new DruidNode(serviceName, host, false, 8080, null, true, false),
        NodeRole.BROKER,
        ImmutableMap.of(
            BrokerNodeService.DISCOVERY_SERVICE_KEY,
            new BrokerNodeService(tier)
        )
    );
  }

  private static DruidNodeDiscovery newDiscovery(final List<DiscoveryDruidNode> nodes)
  {
    return new DruidNodeDiscovery()
    {
      @Override
      public Collection<DiscoveryDruidNode> getAllNodes()
      {
        return nodes;
      }

      @Override
      public void registerListener(Listener listener)
      {
        listener.nodesAdded(nodes);
        listener.nodeViewInitialized();
      }
    };
  }

  private SqlQuery createSqlQueryWithContext(Map<String, Object> queryContext)
  {
    return new SqlQuery(
        "SELECT * FROM test",
        null,
        false,
        false,
        false,
        queryContext,
        null
    );
  }

  private static class TestRuleManager extends CoordinatorRuleManager
  {
    public TestRuleManager(
        Supplier<TieredBrokerConfig> config
    )
    {
      super(config, null);
    }

    @Override
    public boolean isStarted()
    {
      return true;
    }

    @Override
    public List<Rule> getRulesWithDefault(String dataSource)
    {
      return Arrays.asList(
          new IntervalLoadRule(Intervals.of("2013/2014"), ImmutableMap.of("hot", 1), null),
          new IntervalLoadRule(Intervals.of("2012/2013"), ImmutableMap.of("medium", 1), null),
          new IntervalLoadRule(
              Intervals.of("2011/2012"),
              ImmutableMap.of(DruidServer.DEFAULT_TIER, 1),
              null
          )
      );
    }
  }

  private static class TestTieredBrokerConfig extends TieredBrokerConfig
  {
    @Nullable
    private final Set<String> routableTiers;

    private TestTieredBrokerConfig(@Nullable Set<String> routableTiers)
    {
      this.routableTiers = routableTiers;
    }

    @Override
    public LinkedHashMap<String, String> getTierToBrokerMap()
    {
      return new LinkedHashMap<>(
          ImmutableMap.of(
              "hot", "hotBroker",
              "medium", "mediumBroker",
              DruidServer.DEFAULT_TIER, "coldBroker"
          )
      );
    }

    @Override
    public String getDefaultBrokerServiceName()
    {
      return "hotBroker";
    }

    @Nullable
    @Override
    public Set<String> getRoutableTiers()
    {
      return routableTiers;
    }
  }
}
