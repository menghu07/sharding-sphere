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

package io.shardingsphere.core.optimizer.query;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import io.shardingsphere.api.algorithm.sharding.ListShardingValue;
import io.shardingsphere.api.algorithm.sharding.RangeShardingValue;
import io.shardingsphere.api.algorithm.sharding.ShardingValue;
import io.shardingsphere.core.constant.ShardingOperator;
import io.shardingsphere.core.exception.ShardingException;
import io.shardingsphere.core.optimizer.OptimizeEngine;
import io.shardingsphere.core.optimizer.condition.ShardingCondition;
import io.shardingsphere.core.optimizer.condition.ShardingConditions;
import io.shardingsphere.core.parsing.parser.context.condition.AndCondition;
import io.shardingsphere.core.parsing.parser.context.condition.Column;
import io.shardingsphere.core.parsing.parser.context.condition.Condition;
import io.shardingsphere.core.parsing.parser.context.condition.OrCondition;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Query optimize engine.
 *
 * @author zhangliang
 */
@RequiredArgsConstructor
public final class QueryOptimizeEngine implements OptimizeEngine {
    
    private final OrCondition orCondition;
    
    private final List<Object> parameters;
    
    @Override
    public ShardingConditions optimize() {
        List<ShardingCondition> result = new ArrayList<>(orCondition.getAndConditions().size());
        for (AndCondition each : orCondition.getAndConditions()) {
            result.add(optimize(each.getConditionsMap()));
        }
        return new ShardingConditions(result);
    }
    
    private ShardingCondition optimize(final Map<Column, List<Condition>> conditionsMap) {
        ShardingCondition result = new ShardingCondition();
        for (Entry<Column, List<Condition>> entry : conditionsMap.entrySet()) {
            try {
                ShardingValue shardingValue = optimize(entry.getKey(), entry.getValue());
                if (shardingValue instanceof AlwaysFalseShardingValue) {
                    return new AlwaysFalseShardingCondition();
                }
                result.getShardingValues().add(shardingValue);
            } catch (final ClassCastException ex) {
                throw new ShardingException("Found different types for sharding value `%s`.", entry.getKey());
            }
        }
        return result;
    }
    
    private ShardingValue optimize(final Column column, final List<Condition> conditions) {
        List<Comparable<String>> listValue = null;
        Range<Comparable<String>> rangeValue = null;
        for (Condition each : conditions) {
            List<Comparable<String>> conditionValues = getComparableStrings(each.getConditionValues(parameters));
            if (ShardingOperator.EQUAL == each.getOperator() || ShardingOperator.IN == each.getOperator()) {
                listValue = optimize(conditionValues, listValue);
                if (listValue.isEmpty()) {
                    return new AlwaysFalseShardingValue();
                }
            }
            if (ShardingOperator.BETWEEN == each.getOperator()) {
                try {
                    Range<Comparable<String>> betweenRange = Range.range(conditionValues.get(0), BoundType.CLOSED, conditionValues.get(1), BoundType.CLOSED);
                    rangeValue = optimize(betweenRange, rangeValue);
                } catch (final IllegalArgumentException ex) {
                    return new AlwaysFalseShardingValue();
                }
            }
            //支持范围< <= > >=关系操作符字段分表
            if (ShardingOperator.LT == each.getOperator()) {
                try {
                    Range<Comparable<String>> rightValue = Range.lessThan(conditionValues.get(0));
                    rangeValue = optimize(rightValue, rangeValue);
                } catch (final IllegalArgumentException ex) {
                    return new AlwaysFalseShardingValue();
                }
            }
            if (ShardingOperator.LT_EQ == each.getOperator()) {
                try {
                    Range<Comparable<String>> rightValue = Range.atMost(conditionValues.get(0));
                    rangeValue = optimize(rightValue, rangeValue);
                } catch (final IllegalArgumentException ex) {
                    return new AlwaysFalseShardingValue();
                }
            }
            if (ShardingOperator.GT == each.getOperator()) {
                try {
                    Range<Comparable<String>> rightValue = Range.greaterThan(conditionValues.get(0));
                    rangeValue = optimize(rightValue, rangeValue);
                } catch (final IllegalArgumentException ex) {
                    return new AlwaysFalseShardingValue();
                }
            }
            if (ShardingOperator.GT_EQ == each.getOperator()) {
                try {
                    Range<Comparable<String>> rightValue = Range.atLeast(conditionValues.get(0));
                    rangeValue = optimize(rightValue, rangeValue);
                } catch (final IllegalArgumentException ex) {
                    return new AlwaysFalseShardingValue();
                }
            }
        }
        if (null == listValue) {
            return new RangeShardingValue<>(column.getTableName(), column.getName(), rangeValue);
        }
        if (null == rangeValue) {
            return new ListShardingValue<>(column.getTableName(), column.getName(), listValue);
        }
        listValue = optimize(listValue, rangeValue);
        return listValue.isEmpty() ? new AlwaysFalseShardingValue() : new ListShardingValue<>(column.getTableName(), column.getName(), listValue);
    }
    
    private List<Comparable<String>> optimize(final List<Comparable<String>> value1, final List<Comparable<String>> value2) {
        if (null == value2) {
            return value1;
        }
        value1.retainAll(value2);
        return value1;
    }
    
    private Range<Comparable<String>> optimize(final Range<Comparable<String>> value1, final Range<Comparable<String>> value2) {
        return null == value2 ? value1 : value1.intersection(value2);
    }
    
    private List<Comparable<String>> optimize(final List<Comparable<String>> listValue, final Range<Comparable<String>> rangeValue) {
        List<Comparable<String>> result = new LinkedList<>();
        for (Comparable<String> each : listValue) {
            if (rangeValue.contains(each)) {
                result.add(each);
            }
        }
        return result;
    }

    private List<Comparable<String>> getComparableStrings(List<Comparable<?>> comparableList) {
        List<Comparable<String>> elements = new ArrayList<>();
        for (Comparable<?> e : comparableList) {
            elements.add((Comparable) e);
        }
        return elements;
    }
}
