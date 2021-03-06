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

package io.shardingsphere.core.executor;

import java.sql.SQLException;

/**
 * Sharding execute callback.
 * 
 * @author zhangliang
 * 
 * @param <I> type of input value
 * @param <O> type of output value
 */
public interface ShardingExecuteCallback<I, O> {
    
    /**
     * Execute callback.
     * 
     * @param input input value
     * @param isTrunkThread is execution in trunk thread
     * @return execute result
     * @throws SQLException throw when execute failure
     */
    O execute(I input, boolean isTrunkThread) throws SQLException;
}
