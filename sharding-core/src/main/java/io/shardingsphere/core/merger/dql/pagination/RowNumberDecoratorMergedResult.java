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

package io.shardingsphere.core.merger.dql.pagination;

import io.shardingsphere.core.merger.MergedResult;
import io.shardingsphere.core.merger.dql.common.DecoratorMergedResult;
import io.shardingsphere.core.parsing.parser.context.limit.Limit;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Decorator merged result for rownum pagination.
 *
 * @author zhangliang
 */
public final class RowNumberDecoratorMergedResult extends DecoratorMergedResult {
    
    private final Limit limit;
    
    private final boolean skipAll;
    
    private int rowNumber;
    
    public RowNumberDecoratorMergedResult(final MergedResult mergedResult, final Limit limit) throws SQLException {
        super(mergedResult);
        this.limit = limit;
        System.out.println("skip begin: ");
        long begin = System.currentTimeMillis();
        skipAll = skipOffset();
        System.out.println(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS").format(new Date()) + " use time: " + (System.currentTimeMillis() - begin) / 1000.0 + "s");
    }
    
    private boolean skipOffset() throws SQLException {
        int end;
        if (null == limit.getOffset()) {
            end = 0;
        } else {
            end = limit.getOffset().isBoundOpened() ? limit.getOffsetValue() - 1 : limit.getOffsetValue();
        }
        for (int i = 0; i < end; i++) {
            if (!getMergedResult().next()) {
                return true;
            }
        }
        rowNumber = end + 1;
        return false;
    }
    
    @Override
    public boolean next() throws SQLException {
        if (skipAll) {
            return false;
        }
        if (limit.getRowCountValue() < 0) {
            return getMergedResult().next();
        }
        if (limit.getRowCount().isBoundOpened()) {
            return rowNumber++ <= limit.getRowCountValue() && getMergedResult().next();
        }
        return rowNumber++ < limit.getRowCountValue() && getMergedResult().next();
    }
}
