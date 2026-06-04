/*
 * Teragrep Neon log format plugin for AKV_01
 * Copyright (C) 2025 Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */
package com.teragrep.nlf_01;

import com.teragrep.akv_01.event.ParsedEvent;
import com.teragrep.akv_01.event.ParsedEventFactory;
import com.teragrep.akv_01.event.UnparsedEventImpl;
import com.teragrep.akv_01.event.metadata.offset.EventOffsetImpl;
import com.teragrep.akv_01.event.metadata.partitionContext.EventPartitionContextImpl;
import com.teragrep.akv_01.event.metadata.properties.EventPropertiesImpl;
import com.teragrep.akv_01.event.metadata.systemProperties.EventSystemPropertiesImpl;
import com.teragrep.akv_01.event.metadata.time.EnqueuedTimeImpl;
import com.teragrep.nlf_01.fakes.FakeSourceable;
import com.teragrep.nlf_01.types.DefaultEventType;
import com.teragrep.rlo_14.SDElement;
import com.teragrep.rlo_14.SDParam;
import com.teragrep.rlo_14.SyslogMessage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

final class NLFPluginDatabricksTest {

    @Test
    @DisplayName("Test NLFPlugin with DatabricksAccounts")
    void testNlfPluginWithDatabricksAccounts() {

        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files.readString(Paths.get("src/test/resources/databricks/databricksaccounts.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksAccounts", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksApps")
    void testNlfPluginWithDatabricksApps() {
        final String json = Assertions

                .assertDoesNotThrow(() -> Files.readString(Paths.get("src/test/resources/databricks/databricksapps.json")));
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions

                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksApps", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksBrickStoreHTTPGateway")
    void testNlfPluginWithDatabricksBrickStoreHttpGateway() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files
                                .readString(
                                        Paths.get("src/test/resources/databricks/databricksbrickstorehttpgateway.json")
                                )
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksBrickStoreHTTPGateway", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksBudgetPolicyCentral")
    void testNlfPluginWithDatabricksBudgetPolicyCentral() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files
                                .readString(Paths.get("src/test/resources/databricks/databricksbudgetpolicycentral.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksBudgetPolicyCentral", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksCapsule8Dataplane")
    void testNlfPluginWithDatabricksCapsule8Dataplane() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files
                                .readString(Paths.get("src/test/resources/databricks/databrickscapsule8dataplane.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksCapsule8Dataplane", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksClamAVScan")
    void testNlfPluginWithDatabricksClamAvScan() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files.readString(Paths.get("src/test/resources/databricks/databricksclamavscan.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksClamAVScan", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksCloudStorageMetadata")
    void testNlfPluginWithDatabricksCloudStorageMetadata() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files
                                .readString(
                                        Paths.get("src/test/resources/databricks/databrickscloudstoragemetadata.json")
                                )
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksCloudStorageMetadata", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksClusterLibraries")
    void testNlfPluginWithDatabricksClusterLibraries() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files.readString(Paths.get("src/test/resources/databricks/databricksclusterlibraries.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksClusterLibraries", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksClusterPolicies")
    void testNlfPluginWithDatabricksClusterPolicies() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files.readString(Paths.get("src/test/resources/databricks/databricksclusterpolicies.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksClusterPolicies", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksClusters")
    void testNlfPluginWithDatabricksClusters() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files.readString(Paths.get("src/test/resources/databricks/databricksclusters.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksClusters", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksDashboards")
    void testNlfPluginWithDatabricksDashboards() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files.readString(Paths.get("src/test/resources/databricks/databricksdashboards.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksDashboards", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksDatabricksSQL")
    void testNlfPluginWithDatabricksDatabricksSql() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files.readString(Paths.get("src/test/resources/databricks/databricksdatabrickssql.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksDatabricksSQL", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksDataMonitoring")
    void testNlfPluginWithDatabricksDataMonitoring() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files.readString(Paths.get("src/test/resources/databricks/databricksdatamonitoring.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksDataMonitoring", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksDataRooms")
    void testNlfPluginWithDatabricksDataRooms() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files.readString(Paths.get("src/test/resources/databricks/databricksdatarooms.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksDataRooms", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksDBFS")
    void testNlfPluginWithDatabricksDbfs() {
        final String json = Assertions
                .assertDoesNotThrow(() -> Files.readString(Paths.get("src/test/resources/databricks/databricksdbfs.json")));
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksDBFS", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksDeltaPipelines")
    void testNlfPluginWithDatabricksDeltaPipelines() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files.readString(Paths.get("src/test/resources/databricks/databricksdeltapipelines.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksDeltaPipelines", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksFeatureStore")
    void testNlfPluginWithDatabricksFeatureStore() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files.readString(Paths.get("src/test/resources/databricks/databricksfeaturestore.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksFeatureStore", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksFiles")
    void testNlfPluginWithDatabricksFiles() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files.readString(Paths.get("src/test/resources/databricks/databricksfiles.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksFiles", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksFilesystem")
    void testNlfPluginWithDatabricksFilesystem() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files.readString(Paths.get("src/test/resources/databricks/databricksfilesystem.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksFilesystem", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksGenie")
    void testNlfPluginWithDatabricksGenie() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files.readString(Paths.get("src/test/resources/databricks/databricksgenie.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksGenie", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksGitCredentials")
    void testNlfPluginWithDatabricksGitCredentials() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files.readString(Paths.get("src/test/resources/databricks/databricksgitcredentials.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksGitCredentials", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksGlobalInitScripts")
    void testNlfPluginWithDatabricksGlobalInitScripts() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files
                                .readString(Paths.get("src/test/resources/databricks/databricksglobalinitscripts.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksGlobalInitScripts", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksGroups")
    void testNlfPluginWithDatabricksGroups() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files.readString(Paths.get("src/test/resources/databricks/databricksgroups.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksGroups", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksIAMRole")
    void testNlfPluginWithDatabricksIamRole() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files.readString(Paths.get("src/test/resources/databricks/databricksiamrole.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksIAMRole", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksIngestion")
    void testNlfPluginWithDatabricksIngestion() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files.readString(Paths.get("src/test/resources/databricks/databricksingestion.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksIngestion", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksInstancePools")
    void testNlfPluginWithDatabricksInstancePools() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files.readString(Paths.get("src/test/resources/databricks/databricksinstancepools.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksInstancePools", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksJobs")
    void testNlfPluginWithDatabricksJobs() {
        final String json = Assertions
                .assertDoesNotThrow(() -> Files.readString(Paths.get("src/test/resources/databricks/databricksjobs.json")));
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksJobs", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksLakeviewConfig")
    void testNlfPluginWithDatabricksLakeviewConfig() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files.readString(Paths.get("src/test/resources/databricks/databrickslakeviewconfig.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksLakeviewConfig", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksLineageTracking")
    void testNlfPluginWithDatabricksLineageTracking() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files.readString(Paths.get("src/test/resources/databricks/databrickslineagetracking.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksLineageTracking", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksMarketplaceConsumer")
    void testNlfPluginWithDatabricksMarketplaceConsumer() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files
                                .readString(Paths.get("src/test/resources/databricks/databricksmarketplaceconsumer.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksMarketplaceConsumer", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksMarketplaceProvider")
    void testNlfPluginWithDatabricksMarketplaceProvider() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files
                                .readString(Paths.get("src/test/resources/databricks/databricksmarketplaceprovider.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksMarketplaceProvider", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksMLflowAcledArtifact")
    void testNlfPluginWithDatabricksMLflowAcledArtifact() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files
                                .readString(Paths.get("src/test/resources/databricks/databricksmlflowacledartifact.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksMLflowAcledArtifact", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksMLflowExperiment")
    void testNlfPluginWithDatabricksMLflowExperiment() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files.readString(Paths.get("src/test/resources/databricks/databricksmlflowexperiment.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksMLflowExperiment", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksModelRegistry")
    void testNlfPluginWithDatabricksModelRegistry() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files.readString(Paths.get("src/test/resources/databricks/databricksmodelregistry.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksModelRegistry", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksNotebook")
    void testNlfPluginWithDatabricksNotebook() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files.readString(Paths.get("src/test/resources/databricks/databricksnotebook.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksNotebook", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksOnlineTables")
    void testNlfPluginWithDatabricksOnlineTables() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files.readString(Paths.get("src/test/resources/databricks/databricksonlinetables.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksOnlineTables", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksPartnerHub")
    void testNlfPluginWithDatabricksPartnerHub() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files.readString(Paths.get("src/test/resources/databricks/databrickspartnerhub.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksPartnerHub", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksPredictiveOptimization")
    void testNlfPluginWithDatabricksPredictiveOptimization() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files
                                .readString(
                                        Paths.get("src/test/resources/databricks/databrickspredictiveoptimization.json")
                                )
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksPredictiveOptimization", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksRBAC")
    void testNlfPluginWithDatabricksRbac() {
        final String json = Assertions
                .assertDoesNotThrow(() -> Files.readString(Paths.get("src/test/resources/databricks/databricksrbac.json")));
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksRBAC", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksRemoteHistoryService")
    void testNlfPluginWithDatabricksRemoteHistoryService() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files
                                .readString(
                                        Paths.get("src/test/resources/databricks/databricksremotehistoryservice.json")
                                )
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksRemoteHistoryService", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksRepos")
    void testNlfPluginWithDatabricksRepos() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files.readString(Paths.get("src/test/resources/databricks/databricksrepos.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksRepos", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksRFA")
    void testNlfPluginWithDatabricksRfa() {
        final String json = Assertions
                .assertDoesNotThrow(() -> Files.readString(Paths.get("src/test/resources/databricks/databricksrfa.json")));
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksRFA", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksSecrets")
    void testNlfPluginWithDatabricksSecrets() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files.readString(Paths.get("src/test/resources/databricks/databrickssecrets.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksSecrets", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksServerlessRealTimeInference")
    void testNlfPluginWithDatabricksServerlessRealTimeInference() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files
                                .readString(
                                        Paths
                                                .get(
                                                        "src/test/resources/databricks/databricksserverlessrealtimeinference.json"
                                                )
                                )
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksServerlessRealTimeInference", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksSQL")
    void testNlfPluginWithDatabricksSql() {
        final String json = Assertions
                .assertDoesNotThrow(() -> Files.readString(Paths.get("src/test/resources/databricks/databrickssql.json")));
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksSQL", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksSQLPermissions")
    void testNlfPluginWithDatabricksSqlPermissions() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files.readString(Paths.get("src/test/resources/databricks/databrickssqlpermissions.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksSQLPermissions", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksSSH")
    void testNlfPluginWithDatabricksSsh() {
        final String json = Assertions
                .assertDoesNotThrow(() -> Files.readString(Paths.get("src/test/resources/databricks/databricksssh.json")));
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksSSH", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksUnityCatalog")
    void testNlfPluginWithDatabricksUnityCatalog() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files.readString(Paths.get("src/test/resources/databricks/databricksunitycatalog.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksUnityCatalog", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksVectorSearch")
    void testNlfPluginWithDatabricksVectorSearch() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files.readString(Paths.get("src/test/resources/databricks/databricksvectorsearch.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksVectorSearch", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksWebhookNotifications")
    void testNlfPluginWithDatabricksWebhookNotifications() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files
                                .readString(
                                        Paths.get("src/test/resources/databricks/databrickswebhooknotifications.json")
                                )
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksWebhookNotifications", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksWebTerminal")
    void testNlfPluginWithDatabricksWebTerminal() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files.readString(Paths.get("src/test/resources/databricks/databrickswebterminal.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksWebTerminal", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksWorkspace")
    void testNlfPluginWithDatabricksWorkspace() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files.readString(Paths.get("src/test/resources/databricks/databricksworkspace.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksWorkspace", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    @DisplayName("Test NLFPlugin with DatabricksWorkspaceFiles")
    void testNlfPluginWithDatabricksWorkspaceFiles() {
        final String json = Assertions
                .assertDoesNotThrow(
                        () -> Files.readString(Paths.get("src/test/resources/databricks/databricksworkspacefiles.json"))
                );
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions.assertEquals("{}", syslogMessage.getMsg());
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("DatabricksWorkspaceFiles", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions
                .assertEquals(DefaultEventType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

}
