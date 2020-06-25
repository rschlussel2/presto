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
package com.facebook.presto.hive;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.hive.EncryptionProperties.ENCRYPT_COLUMNS_KEY;
import static com.facebook.presto.hive.EncryptionProperties.ENCRYPT_TABLE_KEY;
import static com.facebook.presto.hive.HiveTableProperties.ENCRYPT_COLUMNS;
import static com.facebook.presto.hive.HiveTableProperties.ENCRYPT_TABLE;
import static com.google.common.collect.ImmutableMap.builder;
import static java.util.Objects.requireNonNull;

public abstract class TableEncryptionProperties
{
    private final Optional<String> encryptTable;
    private final Optional<ColumnEncryptionInformation> columnEncryptionInformation;

    protected TableEncryptionProperties(Optional<String> encryptTable, Optional<ColumnEncryptionInformation> columnEncryptionInformation)
    {
        this.encryptTable = requireNonNull(encryptTable, "encryptTable is null");
        this.columnEncryptionInformation = requireNonNull(columnEncryptionInformation, "columnEncryptionInformation is null");
    }

    public Optional<String> getEncryptTable()
    {
        return encryptTable;
    }

    public Optional<ColumnEncryptionInformation> getColumnEncryptionInformation()
    {
        return columnEncryptionInformation;
    }

    protected abstract Map<String, String> getFormatSpecificHiveProperties();

    public Map<String, String> toHiveProperties()
    {
        ImmutableMap.Builder<String, String> tableProperties = builder();
        tableProperties.putAll(getFormatSpecificHiveProperties());
        encryptTable.ifPresent(reference -> tableProperties.put(ENCRYPT_TABLE_KEY, reference));
        columnEncryptionInformation.ifPresent(columnInformation -> tableProperties.put(ENCRYPT_COLUMNS_KEY, columnInformation.toHiveProperty()));
        return tableProperties.build();
    }

    protected abstract Map<String, Object> getFormatSpecificTableProperties();

    public Map<String, Object> toTableProperties()
    {
        ImmutableMap.Builder<String, Object> tableProperties = builder();
        tableProperties.putAll(getFormatSpecificTableProperties());
        encryptTable.ifPresent(reference -> tableProperties.put(ENCRYPT_TABLE, reference));
        columnEncryptionInformation.ifPresent(columnInformation -> tableProperties.put(ENCRYPT_COLUMNS, columnInformation));
        return tableProperties.build();
    }
}
