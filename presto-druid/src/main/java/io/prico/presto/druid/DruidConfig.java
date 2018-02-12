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

import io.airlift.configuration.Config;

public class DruidConfig {
    private String brokerUrl;
    private String coordinatorUrl;
    private String schema;

    public String getBrokerUrl() {
        return brokerUrl;
    }

    @Config("druid-broker-url")
    public DruidConfig setBrokerUrl(final String brokerUrl) {
        this.brokerUrl = brokerUrl;
        return this;
    }

    @Config("druid-schema")
    public DruidConfig setSchema(final String schema) {
        this.schema = schema;
        return this;
    }

    public String getSchema() {
        return schema;
    }

    public String getCoordinatorUrl() {
        return coordinatorUrl;
    }

    @Config("druid-coordinator-url")
    public DruidConfig setCoordinatorUrl(final String coordinatorUrl) {
        this.coordinatorUrl = coordinatorUrl;
        return this;
    }
}
