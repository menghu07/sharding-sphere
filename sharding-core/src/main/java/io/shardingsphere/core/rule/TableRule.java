/*
 * Copyright 2016-2018 shardingsphere.io.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package io.shardingsphere.core.rule;

import com.google.common.base.Preconditions;
import io.shardingsphere.api.config.TableRuleConfiguration;
import io.shardingsphere.api.config.strategy.ShardingStrategyConfiguration;
import io.shardingsphere.core.exception.ShardingException;
import io.shardingsphere.core.keygen.KeyGenerator;
import io.shardingsphere.core.routing.strategy.ShardingStrategy;
import io.shardingsphere.core.routing.strategy.ShardingStrategyFactory;
import io.shardingsphere.core.util.InlineExpressionParser;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.ToString;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Table rule configuration.
 *
 * @author zhangliang
 */
@Getter
@ToString(exclude = "dataNodeIndexMap")
public final class TableRule {

    //分表正则表达式
    private static final String SPLITTABLE_PATTERN = "^\\w+\\d+$";

    private final String logicTable;

    private final int perMonthTables;

    /**
     * 子分表策略配置
     */
    private final List<ShardingStrategy> subTableShardingStrategies;

    private final List<DataNode> actualDataNodes;

    @Getter(AccessLevel.NONE)
    private final Map<DataNode, Integer> dataNodeIndexMap;
    
    private final ShardingStrategy databaseShardingStrategy;
    
    private final ShardingStrategy tableShardingStrategy;
    
    private final String generateKeyColumn;
    
    private final KeyGenerator keyGenerator;
    
    private final String logicIndex;
    
    public TableRule(final String defaultDataSourceName, final String logicTableName) {
        logicTable = logicTableName.toLowerCase();
        actualDataNodes = Collections.singletonList(new DataNode(defaultDataSourceName, logicTableName));
        perMonthTables = 0;
        subTableShardingStrategies = Collections.emptyList();
        dataNodeIndexMap = Collections.emptyMap();
        databaseShardingStrategy = null;
        tableShardingStrategy = null;
        generateKeyColumn = null;
        keyGenerator = null;
        logicIndex = null;
    }
    
    public TableRule(final TableRuleConfiguration tableRuleConfig, final ShardingDataSourceNames shardingDataSourceNames) {
        Preconditions.checkNotNull(tableRuleConfig.getLogicTable(), "Logic table cannot be null.");
        Preconditions.checkArgument(!shardingDataSourceNames.getDataSourceNames().isEmpty(), "数据源不能为空");
        logicTable = tableRuleConfig.getLogicTable().toLowerCase();
        perMonthTables = tableRuleConfig.getPerMonthTables();
        subTableShardingStrategies = newShardingStrategies(tableRuleConfig.getSubTableShardingStrategyConfigs());
        List<String> dataNodes = new InlineExpressionParser(tableRuleConfig.getActualDataNodes()).splitAndEvaluate();
        dataNodeIndexMap = new HashMap<>(dataNodes.size(), 1);
        actualDataNodes = isEmptyDataNodes(dataNodes)
            ? generateDataNodes(tableRuleConfig.getLogicTable(), shardingDataSourceNames.getDataSourceNames()) : generateDataNodes(dataNodes, shardingDataSourceNames.getDataSourceNames());
        databaseShardingStrategy = null == tableRuleConfig.getDatabaseShardingStrategyConfig() ? null : ShardingStrategyFactory.newInstance(tableRuleConfig.getDatabaseShardingStrategyConfig());
        tableShardingStrategy = null == tableRuleConfig.getTableShardingStrategyConfig() ? null : ShardingStrategyFactory.newInstance(tableRuleConfig.getTableShardingStrategyConfig());
        generateKeyColumn = tableRuleConfig.getKeyGeneratorColumnName();
        keyGenerator = tableRuleConfig.getKeyGenerator();
        logicIndex = null == tableRuleConfig.getLogicIndex() ? null : tableRuleConfig.getLogicIndex().toLowerCase();
    }
    
    private boolean isEmptyDataNodes(final List<String> dataNodes) {
        return null == dataNodes || dataNodes.isEmpty();
    }
    
    private List<DataNode> generateDataNodes(final String logicTable, final Collection<String> dataSourceNames) {
        List<DataNode> result = new LinkedList<>();
        int index = 0;
        for (String each : dataSourceNames) {
            DataNode dataNode = new DataNode(each, logicTable);
            result.add(dataNode);
            dataNodeIndexMap.put(dataNode, index);
            index++;
        }
        return result;
    }
    
    private List<DataNode> generateDataNodes(final List<String> actualDataNodes, final Collection<String> dataSourceNames) {
        List<DataNode> result = new LinkedList<>();
        int index = 0;
        //在此校验数据分表个数和实际配置个数是否一致
        String exampleDataSourceName = dataSourceNames.iterator().next();
        //格式为table180000, table180001 ... table180107
        Map<String, Integer> monthTables = new HashMap<>();
        int maxNumberOfTable = -1;
        for (String each : actualDataNodes) {
            DataNode dataNode = new DataNode(each);
            if (!dataSourceNames.contains(dataNode.getDataSourceName())) {
                throw new ShardingException("Cannot find data source in sharding rule, invalid actual data node is: '%s'", each);
            }
            result.add(dataNode);
            dataNodeIndexMap.put(dataNode, index);
            index++;
            //做分表配置验证使用
            if (Pattern.matches(SPLITTABLE_PATTERN, dataNode.getTableName())
                    && exampleDataSourceName.equalsIgnoreCase(dataNode.getDataSourceName())) {
                String numbers4 = first4Numbers(dataNode.getTableName());
                Integer oldValue = monthTables.get(numbers4);
                if (oldValue == null) {
                    monthTables.put(numbers4, oldValue = 1);
                } else {
                    monthTables.put(numbers4, ++oldValue);
                }
                if (oldValue > maxNumberOfTable) {
                    maxNumberOfTable = oldValue;
                }
            }
        }
        Preconditions.checkArgument(maxNumberOfTable == perMonthTables, "每个月分表个数和分表表达式不一致,TableName=" + logicTable);
        return result;
    }
    
    /**
     * Get data node groups.
     *
     * @return data node groups, key is data source name, value is tables belong to this data source
     */
    public Map<String, List<DataNode>> getDataNodeGroups() {
        Map<String, List<DataNode>> result = new LinkedHashMap<>(actualDataNodes.size(), 1);
        for (DataNode each : actualDataNodes) {
            String dataSourceName = each.getDataSourceName();
            if (!result.containsKey(dataSourceName)) {
                result.put(dataSourceName, new LinkedList<DataNode>());
            }
            result.get(dataSourceName).add(each);
        }
        return result;
    }
    
    /**
     * Get actual data source names.
     *
     * @return actual data source names
     */
    public Collection<String> getActualDatasourceNames() {
        Collection<String> result = new LinkedHashSet<>(actualDataNodes.size());
        for (DataNode each : actualDataNodes) {
            result.add(each.getDataSourceName());
        }
        return result;
    }
    
    /**
     * Get actual table names via target data source name.
     *
     * @param targetDataSource target data source name
     * @return names of actual tables
     */
    public Collection<String> getActualTableNames(final String targetDataSource) {
        Collection<String> result = new LinkedHashSet<>(actualDataNodes.size());
        for (DataNode each : actualDataNodes) {
            if (targetDataSource.equals(each.getDataSourceName())) {
                result.add(each.getTableName());
            }
        }
        return result;
    }
    
    int findActualTableIndex(final String dataSourceName, final String actualTableName) {
        DataNode dataNode = new DataNode(dataSourceName, actualTableName);
        return dataNodeIndexMap.containsKey(dataNode) ? dataNodeIndexMap.get(dataNode) : -1;
    }
    
    boolean isExisted(final String actualTableName) {
        for (DataNode each : actualDataNodes) {
            if (each.getTableName().equalsIgnoreCase(actualTableName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 返回前4个字符组成的字符串
     * @param tableName
     * @return
     */
    private String first4Numbers(String tableName) {
        StringBuilder builder = new StringBuilder();
        int i = 0;
        for (char c : tableName.toCharArray()) {
            if (i < 4 && '0' <= c && c <= '9') {
                 builder.append(c);
                 i++;
            }
            if (i == 4) {
                break;
            }
        }
        return builder.toString();
    }

    /**
     * 创建子分表字段分表策略
     * @param shardingStrategyConfigurations
     * @return
     */
    private List<ShardingStrategy> newShardingStrategies(List<ShardingStrategyConfiguration> shardingStrategyConfigurations) {
        if (shardingStrategyConfigurations == null || shardingStrategyConfigurations.isEmpty()) {
            return Collections.emptyList();
        }
        List<ShardingStrategy> result = new ArrayList<>();
        for (ShardingStrategyConfiguration configuration : shardingStrategyConfigurations) {
            result.add( ShardingStrategyFactory.newInstance(configuration));
        }
        return result;
    }

    /**
     * 是否是分片列
     * @param columnName
     * @return
     */
    public boolean isShardingColumn(String columnName) {
        if (null != databaseShardingStrategy && databaseShardingStrategy.getShardingColumns().contains(columnName)) {
            return true;
        }
        if (null != tableShardingStrategy && tableShardingStrategy.getShardingColumns().contains(columnName)) {
            return true;
        }
        for (ShardingStrategy shardingStrategy : subTableShardingStrategies) {
            if (shardingStrategy.getShardingColumns().contains(columnName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 获取所有分表策略
     * @return
     */
    public List<ShardingStrategy> getTableShardingStrategies() {
        List<ShardingStrategy> result = new ArrayList<>(subTableShardingStrategies);
        if (tableShardingStrategy != null) {
            result.add(0, tableShardingStrategy);
        }
        return result;
    }
}
