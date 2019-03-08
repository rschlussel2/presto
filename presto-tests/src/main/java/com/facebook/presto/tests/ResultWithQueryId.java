/*
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
 */
package com.facebook.presto.tests;

import com.facebook.presto.spi.QueryId;

public class ResultWithQueryId<T>
{
    private final QueryId queryId;
    private final T result;

    public ResultWithQueryId(QueryId queryId, T result)
    {
        this.queryId = queryId;
        this.result = result;
    }

    public QueryId getQueryId()
    {
        return queryId;
    }

    public T getResult()
    {
        return result;
    }
}