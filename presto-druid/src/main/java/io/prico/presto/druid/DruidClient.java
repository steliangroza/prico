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
package io.prico.presto.druid;

import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.ConnectionFactory;
import com.facebook.presto.plugin.jdbc.DriverConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcTableHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import javax.inject.Inject;
import org.apache.calcite.jdbc.Driver;
import org.codehaus.commons.compiler.CompilerFactoryFactory;

import static com.facebook.presto.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;
import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;

public class DruidClient
        extends BaseJdbcClient {

    static {
        try {
            //we need to register janino in plugin classloader, otherwise calcite will fail
            CompilerFactoryFactory.getDefaultCompilerFactory();
        } catch (Exception e) {
            //do nothing
        }
    }

    @Inject
    public DruidClient(
            JdbcConnectorId connectorId,
            BaseJdbcConfig config,
            DruidConfig druidConfig) throws SQLException {
        super(connectorId, config, "", connectionFactory(config, druidConfig));
    }

    private static ConnectionFactory connectionFactory(
            BaseJdbcConfig config,
            DruidConfig druidConfig) throws SQLException {

        Properties connectionProperties = basicConnectionProperties(config);
        connectionProperties.setProperty("schemaFactory", "org.apache.calcite.adapter.druid.DruidSchemaFactory");
        connectionProperties.setProperty("schema.url", druidConfig.getBrokerUrl());
        connectionProperties.setProperty("schema", druidConfig.getSchema());
        connectionProperties.setProperty("QUOTED_CASING", "UNCHANGED");
        connectionProperties.setProperty("UNQUOTED_CASING", "UNCHANGED");
        connectionProperties.setProperty("CASE_SENSITIVE", "true");
        connectionProperties.setProperty("schema.coordinatorUrl", druidConfig.getCoordinatorUrl());

        return new DriverConnectionFactory(
                new Driver(),
                config.getConnectionUrl(),
                connectionProperties);
    }

    protected Type toPrestoType(
            int jdbcType,
            int columnSize,
            int decimalDigits) {

        if (jdbcType != Types.LONGNVARCHAR) {
            return super.toPrestoType(jdbcType, columnSize, decimalDigits);
        }
        //for longvarchar
        if (columnSize > VarcharType.MAX_LENGTH) {
            return createUnboundedVarcharType();
        } else if (columnSize <= 0) {
            return createUnboundedVarcharType();
        }
        return createVarcharType(columnSize);
    }

    @Override
    public List<JdbcColumnHandle> getColumns(
            JdbcTableHandle tableHandle) {

        try (Connection connection = connectionFactory.openConnection()) {
            try (ResultSet resultSet = getColumns(tableHandle, connection.getMetaData())) {
                List<JdbcColumnHandle> columns = new ArrayList<>();
                while (resultSet.next()) {
                    Type columnType = toPrestoType(
                            resultSet.getInt("DATA_TYPE"),
                            resultSet.getInt("COLUMN_SIZE"),
                            resultSet.getInt("DECIMAL_DIGITS"));
                    // skip unsupported column types
                    String columnName = resultSet.getString("COLUMN_NAME");
                    if (columnType != null) {
                        columns.add(new JdbcColumnHandle(connectorId, columnName, columnType));
                    } else {
                        if ("__time".equals(columnName)) {
                            columns.add(new JdbcColumnHandle(connectorId, columnName, TimestampType.TIMESTAMP));
                        }
                    }
                }
                if (columns.isEmpty()) {
                    // In rare cases (e.g. PostgreSQL) a table might have no columns.
                    throw new TableNotFoundException(tableHandle.getSchemaTableName());
                }
                return ImmutableList.copyOf(columns);
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    private static ResultSet getColumns(
            JdbcTableHandle tableHandle,
            DatabaseMetaData metadata) throws SQLException {
        String escape = metadata.getSearchStringEscape();
        return metadata.getColumns(
                tableHandle.getCatalogName(),
                escapeNamePattern(tableHandle.getSchemaName(), escape),
                escapeNamePattern(tableHandle.getTableName(), escape),
                null);
    }

}


