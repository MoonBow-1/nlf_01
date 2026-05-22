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
import com.teragrep.nlf_01.types.StorageType;
import com.teragrep.rlo_14.SDElement;
import com.teragrep.rlo_14.SDParam;
import com.teragrep.rlo_14.SyslogMessage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

final class NLFPluginStorageTypesTest {

    @Test
    void testStorageTypeWithStorageBlobLogsType() {
        final String json = Assertions
                .assertDoesNotThrow(() -> Files.readString(Paths.get("src/test/resources/storagebloblogs.json")));
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions
                .assertEquals(
                        "{\n" + "  \"AccessTier\": \"Hot\",\n" + "  \"AccountName\": \"storageaccount2\",\n"
                                + "  \"AuthenticationHash\": \"hash123456789\",\n"
                                + "  \"AuthenticationType\": \"token\",\n" + "  \"AuthorizationDetails\": {},\n"
                                + "  \"CallerIpAddress\": \"127.0.0.1\",\n" + "  \"Category\": \"category1\",\n"
                                + "  \"ClientRequestId\": \"123\",\n" + "  \"ConditionsUsed\": \"condition1=true;\",\n"
                                + "  \"ContentLengthHeader\": 234,\n" + "  \"CorrelationId\": \"345\",\n"
                                + "  \"DestinationUri\": \"https://example1.localhost\",\n" + "  \"DurationMs\": 12,\n"
                                + "  \"Etag\": \"123-a\",\n" + "  \"LastModifiedTime\": \"2010-10-01T11:59:26.256Z\",\n"
                                + "  \"Location\": \"countrycentral\",\n" + "  \"MetricResponseType\": \"type1\",\n"
                                + "  \"ObjectKey\": \"123456789abc\",\n" + "  \"OperationCount\": 2,\n"
                                + "  \"OperationName\": \"copy\",\n" + "  \"OperationVersion\": \"1.0.0\",\n"
                                + "  \"Protocol\": \"http\",\n" + "  \"ReferrerHeader\": \"referer1\",\n"
                                + "  \"RequestBodySize\": 23,\n" + "  \"RequesterAppId\": \"abc123456789\",\n"
                                + "  \"RequesterAudience\": \"audience1\",\n"
                                + "  \"RequesterObjectId\": \"12345678-1234-1234-abcd-1234567890ab\",\n"
                                + "  \"RequesterTenantId\": \"12345678-1234-1234-abcd-1234567890abc\",\n"
                                + "  \"RequesterTokenIssuer\": \"issuer1\",\n"
                                + "  \"RequesterUpn\": \"example1@example.localhost\",\n"
                                + "  \"RequestHeaderSize\": 34,\n"
                                + "  \"RequestMd5\": \"12345678901234567890123456789012\",\n"
                                + "  \"ResponseBodySize\": 45,\n" + "  \"ResponseHeaderSize\": 56,\n"
                                + "  \"ResponseMd5\": \"12345678901234567890123456789013\",\n"
                                + "  \"SasExpiryStatus\": \"status1\",\n" + "  \"SchemaVersion\": \"1.1.1\",\n"
                                + "  \"ServerLatencyMs\": 1.5,\n" + "  \"ServiceType\": \"service1\",\n"
                                + "  \"SourceSystem\": \"Azure\",\n"
                                + "  \"SourceUri\": \"https://example2.localhost\",\n" + "  \"StatusCode\": \"200\",\n"
                                + "  \"StatusText\": \"success\",\n"
                                + "  \"TenantId\": \"12345678-1234-1234-abcd-1234567890ab\",\n"
                                + "  \"TimeGenerated\": \"2020-10-01T11:59:26.256Z\",\n"
                                + "  \"TlsVersion\": \"1.3\",\n" + "  \"Type\": \"StorageBlobLogs\",\n"
                                + "  \"Uri\": \"https://example.localhost\",\n"
                                + "  \"UserAgentHeader\": \"User-Agent: product/1.0\",\n"
                                + "  \"_ResourceId\": \"/SUBSCRIPTIONS/uuid/RESOURCEGROUPS/ab-cd-efgh-ijklmn-xx-DEV-01/PROVIDERS/postgres-db/FLEXIBLESERVERS/efgh-ijklmn-xx-DEV-01\"\n"
                                + "}",
                        syslogMessage.getMsg()
                );
        Assertions.assertEquals("storageaccount2", syslogMessage.getHostname());
        Assertions.assertEquals("StorageBlobLogs", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions.assertEquals(StorageType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    void testStorageTypeWithStorageFileLogsType() {
        final String json = Assertions
                .assertDoesNotThrow(() -> Files.readString(Paths.get("src/test/resources/storagefilelogs.json")));
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions
                .assertEquals(
                        "{\n" + "  \"AccountName\": \"storageaccount1\",\n"
                                + "  \"AuthenticationHash\": \"hash123456789\",\n"
                                + "  \"AuthenticationType\": \"token\",\n" + "  \"AuthorizationDetails\": {},\n"
                                + "  \"CallerIpAddress\": \"127.0.0.1\",\n" + "  \"Category\": \"category1\",\n"
                                + "  \"ClientRequestId\": \"123\",\n" + "  \"ConditionsUsed\": \"condition1=true;\",\n"
                                + "  \"ContentLengthHeader\": 234,\n" + "  \"CorrelationId\": \"345\",\n"
                                + "  \"DurationMs\": 12,\n" + "  \"Etag\": \"123-a\",\n"
                                + "  \"LastModifiedTime\": \"2010-10-01T11:59:26.256Z\",\n"
                                + "  \"Location\": \"countrycentral\",\n" + "  \"MetricResponseType\": \"type1\",\n"
                                + "  \"ObjectKey\": \"123456789abc\",\n" + "  \"OperationCount\": 2,\n"
                                + "  \"OperationName\": \"copy\",\n" + "  \"OperationVersion\": \"1.0.0\",\n"
                                + "  \"Protocol\": \"http\",\n" + "  \"ReferrerHeader\": \"referer1\",\n"
                                + "  \"RequestBodySize\": 23,\n" + "  \"RequesterAppId\": \"abc123456789\",\n"
                                + "  \"RequesterAudience\": \"audience1\",\n"
                                + "  \"RequesterObjectId\": \"12345678-1234-1234-abcd-1234567890ab\",\n"
                                + "  \"RequesterTenantId\": \"12345678-1234-1234-abcd-1234567890abc\",\n"
                                + "  \"RequesterTokenIssuer\": \"issuer1\",\n"
                                + "  \"RequesterUpn\": \"example1@example.localhost\",\n"
                                + "  \"RequesterUserName\": \"example1\",\n" + "  \"RequestHeaderSize\": 34,\n"
                                + "  \"RequestMd5\": \"12345678901234567890123456789012\",\n"
                                + "  \"ResponseBodySize\": 45,\n" + "  \"ResponseHeaderSize\": 56,\n"
                                + "  \"ResponseMd5\": \"12345678901234567890123456789013\",\n"
                                + "  \"SasExpiryStatus\": \"status1\",\n" + "  \"SchemaVersion\": \"1.1.1\",\n"
                                + "  \"ServerLatencyMs\": 1.5,\n" + "  \"ServiceType\": \"service1\",\n"
                                + "  \"SmbCommandDetail\": \"0x2000 bytes at offset 0xf2000\",\n"
                                + "  \"SmbCommandMajor\": \"0x6\",\n"
                                + "  \"SmbCommandMinor\": \"DirectoryCloseAndDelete\",\n"
                                + "  \"SmbCreditsConsumed\": \"0x3\",\n" + "  \"SmbFileId\": \"0x9223442405598953\",\n"
                                + "  \"SmbMessageID\": \"0x3b165\",\n" + "  \"SmbPersistentHandleID\": \"0x6003f\",\n"
                                + "  \"SmbPrimarySID\": \"S-1-5-21-1111111111-2222222222-33333333-4444\",\n"
                                + "  \"SmbSessionID\": \"0x8530280128000049\",\n" + "  \"SmbStatusCode\": \"206\",\n"
                                + "  \"SmbTreeConnectID\": \"0x3\",\n"
                                + "  \"SmbVolatileHandleID\": \"0xFFFFFFFF00000065\",\n"
                                + "  \"SourceSystem\": \"Azure\",\n" + "  \"StatusCode\": \"200\",\n"
                                + "  \"StatusText\": \"success\",\n"
                                + "  \"TenantId\": \"12345678-1234-1234-abcd-1234567890ab\",\n"
                                + "  \"TimeGenerated\": \"2020-10-01T11:59:26.256Z\",\n"
                                + "  \"TlsVersion\": \"1.3\",\n" + "  \"Type\": \"StorageFileLogs\",\n"
                                + "  \"Uri\": \"https://example.localhost\",\n"
                                + "  \"UserAgentHeader\": \"User-Agent: product/1.0\",\n"
                                + "  \"_ResourceId\": \"/SUBSCRIPTIONS/uuid/RESOURCEGROUPS/ab-cd-efgh-ijklmn-xx-DEV-01/PROVIDERS/postgres-db/FLEXIBLESERVERS/efgh-ijklmn-xx-DEV-01\"\n"
                                + "}",
                        syslogMessage.getMsg()
                );
        Assertions.assertEquals("storageaccount1", syslogMessage.getHostname());
        Assertions.assertEquals("StorageFileLogs", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions.assertEquals(StorageType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    void testStorageTypeWithStorageQueueLogsType() {
        final String json = Assertions
                .assertDoesNotThrow(() -> Files.readString(Paths.get("src/test/resources/storagequeuelogs.json")));
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions
                .assertEquals(
                        "{\n" + "  \"AccountName\": \"storageaccount3\",\n"
                                + "  \"AuthenticationHash\": \"hash123456789\",\n"
                                + "  \"AuthenticationType\": \"token\",\n" + "  \"AuthorizationDetails\": {},\n"
                                + "  \"CallerIpAddress\": \"127.0.0.1\",\n" + "  \"Category\": \"category1\",\n"
                                + "  \"ClientRequestId\": \"123\",\n" + "  \"ConditionsUsed\": \"condition1=true;\",\n"
                                + "  \"ContentLengthHeader\": 234,\n" + "  \"CorrelationId\": \"345\",\n"
                                + "  \"DurationMs\": 12,\n" + "  \"Etag\": \"123-a\",\n"
                                + "  \"LastModifiedTime\": \"2010-10-01T11:59:26.256Z\",\n"
                                + "  \"Location\": \"countrycentral\",\n" + "  \"MetricResponseType\": \"type1\",\n"
                                + "  \"ObjectKey\": \"123456789abc\",\n" + "  \"OperationCount\": 2,\n"
                                + "  \"OperationName\": \"copy\",\n" + "  \"OperationVersion\": \"1.0.0\",\n"
                                + "  \"Protocol\": \"http\",\n" + "  \"ReferrerHeader\": \"referer1\",\n"
                                + "  \"RequestBodySize\": 23,\n" + "  \"RequesterAppId\": \"abc123456789\",\n"
                                + "  \"RequesterAudience\": \"audience1\",\n"
                                + "  \"RequesterObjectId\": \"12345678-1234-1234-abcd-1234567890ab\",\n"
                                + "  \"RequesterTenantId\": \"12345678-1234-1234-abcd-1234567890abc\",\n"
                                + "  \"RequesterTokenIssuer\": \"issuer1\",\n"
                                + "  \"RequesterUpn\": \"example1@example.localhost\",\n"
                                + "  \"RequestHeaderSize\": 34,\n"
                                + "  \"RequestMd5\": \"12345678901234567890123456789012\",\n"
                                + "  \"ResponseBodySize\": 45,\n" + "  \"ResponseHeaderSize\": 56,\n"
                                + "  \"ResponseMd5\": \"12345678901234567890123456789013\",\n"
                                + "  \"SasExpiryStatus\": \"status1\",\n" + "  \"SchemaVersion\": \"1.1.1\",\n"
                                + "  \"ServerLatencyMs\": 1.5,\n" + "  \"ServiceType\": \"service1\",\n"
                                + "  \"SourceSystem\": \"Azure\",\n" + "  \"StatusCode\": \"200\",\n"
                                + "  \"StatusText\": \"success\",\n"
                                + "  \"TenantId\": \"12345678-1234-1234-abcd-1234567890ab\",\n"
                                + "  \"TimeGenerated\": \"2020-10-01T11:59:26.256Z\",\n"
                                + "  \"TlsVersion\": \"1.3\",\n" + "  \"Type\": \"StorageQueueLogs\",\n"
                                + "  \"Uri\": \"https://example.localhost\",\n"
                                + "  \"UserAgentHeader\": \"User-Agent: product/1.0\",\n"
                                + "  \"_ResourceId\": \"/SUBSCRIPTIONS/uuid/RESOURCEGROUPS/ab-cd-efgh-ijklmn-xx-DEV-01/PROVIDERS/postgres-db/FLEXIBLESERVERS/efgh-ijklmn-xx-DEV-01\"\n"
                                + "}",
                        syslogMessage.getMsg()
                );
        Assertions.assertEquals("storageaccount3", syslogMessage.getHostname());
        Assertions.assertEquals("StorageQueueLogs", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions.assertEquals(StorageType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }

    @Test
    void testStorageTypeWithStorageTableLogsType() {
        final String json = Assertions
                .assertDoesNotThrow(() -> Files.readString(Paths.get("src/test/resources/storagetablelogs.json")));
        final ParsedEvent parsedEvent = new ParsedEventFactory(
                new UnparsedEventImpl(json, new EventPartitionContextImpl(new HashMap<>()), new EventPropertiesImpl(new HashMap<>()), new EventSystemPropertiesImpl(new HashMap<>()), new EnqueuedTimeImpl("2020-01-01T00:00:00"), new EventOffsetImpl("0"))
        ).parsedEvent();

        final NLFPlugin plugin = new NLFPlugin(new FakeSourceable());
        final List<SyslogMessage> syslogMessages = Assertions
                .assertDoesNotThrow(() -> plugin.syslogMessage(parsedEvent));
        Assertions.assertEquals(1, syslogMessages.size());

        final SyslogMessage syslogMessage = syslogMessages.get(0);
        Assertions
                .assertEquals(
                        "{\n" + "  \"AccountName\": \"storageaccount4\",\n"
                                + "  \"AuthenticationHash\": \"hash123456789\",\n"
                                + "  \"AuthenticationType\": \"token\",\n" + "  \"AuthorizationDetails\": {},\n"
                                + "  \"CallerIpAddress\": \"127.0.0.1\",\n" + "  \"Category\": \"category1\",\n"
                                + "  \"ClientRequestId\": \"123\",\n" + "  \"ConditionsUsed\": \"condition1=true;\",\n"
                                + "  \"ContentLengthHeader\": 234,\n" + "  \"CorrelationId\": \"345\",\n"
                                + "  \"DurationMs\": 12,\n" + "  \"Etag\": \"123-a\",\n"
                                + "  \"LastModifiedTime\": \"2010-10-01T11:59:26.256Z\",\n"
                                + "  \"Location\": \"countrycentral\",\n" + "  \"MetricResponseType\": \"type1\",\n"
                                + "  \"ObjectKey\": \"123456789abc\",\n" + "  \"OperationCount\": 2,\n"
                                + "  \"OperationName\": \"copy\",\n" + "  \"OperationVersion\": \"1.0.0\",\n"
                                + "  \"Protocol\": \"http\",\n" + "  \"ReferrerHeader\": \"referer1\",\n"
                                + "  \"RequestBodySize\": 23,\n" + "  \"RequesterAppId\": \"abc123456789\",\n"
                                + "  \"RequesterAudience\": \"audience1\",\n"
                                + "  \"RequesterObjectId\": \"12345678-1234-1234-abcd-1234567890ab\",\n"
                                + "  \"RequesterTenantId\": \"12345678-1234-1234-abcd-1234567890abc\",\n"
                                + "  \"RequesterTokenIssuer\": \"issuer1\",\n"
                                + "  \"RequesterUpn\": \"example1@example.localhost\",\n"
                                + "  \"RequestHeaderSize\": 34,\n"
                                + "  \"RequestMd5\": \"12345678901234567890123456789012\",\n"
                                + "  \"ResponseBodySize\": 45,\n" + "  \"ResponseHeaderSize\": 56,\n"
                                + "  \"ResponseMd5\": \"12345678901234567890123456789013\",\n"
                                + "  \"SasExpiryStatus\": \"status1\",\n" + "  \"SchemaVersion\": \"1.1.1\",\n"
                                + "  \"ServerLatencyMs\": 1.5,\n" + "  \"ServiceType\": \"service1\",\n"
                                + "  \"SourceSystem\": \"Azure\",\n" + "  \"StatusCode\": \"200\",\n"
                                + "  \"StatusText\": \"success\",\n"
                                + "  \"TenantId\": \"12345678-1234-1234-abcd-1234567890ab\",\n"
                                + "  \"TimeGenerated\": \"2020-10-01T11:59:26.256Z\",\n"
                                + "  \"TlsVersion\": \"1.3\",\n" + "  \"Type\": \"StorageTableLogs\",\n"
                                + "  \"Uri\": \"https://example.localhost\",\n"
                                + "  \"UserAgentHeader\": \"User-Agent: product/1.0\",\n"
                                + "  \"_ResourceId\": \"/SUBSCRIPTIONS/uuid/RESOURCEGROUPS/ab-cd-efgh-ijklmn-xx-DEV-01/PROVIDERS/postgres-db/FLEXIBLESERVERS/efgh-ijklmn-xx-DEV-01\"\n"
                                + "}",
                        syslogMessage.getMsg()
                );
        Assertions.assertEquals("storageaccount4", syslogMessage.getHostname());
        Assertions.assertEquals("StorageTableLogs", syslogMessage.getAppName());
        Assertions.assertEquals("2020-10-01T11:59:26.256Z", syslogMessage.getTimestamp());

        final Map<String, Map<String, String>> sdElementMap = syslogMessage
                .getSDElements()
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals(1, sdElementMap.get("nlf_01@48577").size());
        Assertions.assertEquals(StorageType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));

        Assertions.assertTrue(sdElementMap.get("aer_event@48577").containsKey("properties"));
    }
}
