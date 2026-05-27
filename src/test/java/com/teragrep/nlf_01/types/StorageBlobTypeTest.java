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
package com.teragrep.nlf_01.types;

import com.teragrep.akv_01.event.ParsedEvent;
import com.teragrep.akv_01.event.ParsedEventFactory;
import com.teragrep.akv_01.event.UnparsedEventImpl;
import com.teragrep.akv_01.event.metadata.offset.EventOffset;
import com.teragrep.akv_01.event.metadata.offset.EventOffsetImpl;
import com.teragrep.akv_01.event.metadata.offset.EventOffsetStub;
import com.teragrep.akv_01.event.metadata.partitionContext.EventPartitionContext;
import com.teragrep.akv_01.event.metadata.partitionContext.EventPartitionContextImpl;
import com.teragrep.akv_01.event.metadata.partitionContext.EventPartitionContextStub;
import com.teragrep.akv_01.event.metadata.properties.EventProperties;
import com.teragrep.akv_01.event.metadata.properties.EventPropertiesImpl;
import com.teragrep.akv_01.event.metadata.properties.EventPropertiesStub;
import com.teragrep.akv_01.event.metadata.systemProperties.EventSystemProperties;
import com.teragrep.akv_01.event.metadata.systemProperties.EventSystemPropertiesImpl;
import com.teragrep.akv_01.event.metadata.systemProperties.EventSystemPropertiesStub;
import com.teragrep.akv_01.event.metadata.time.EnqueuedTime;
import com.teragrep.akv_01.event.metadata.time.EnqueuedTimeImpl;
import com.teragrep.akv_01.event.metadata.time.EnqueuedTimeStub;
import com.teragrep.akv_01.plugin.PluginException;
import com.teragrep.nlf_01.fakes.EventPartitionContextFake;
import com.teragrep.nlf_01.fakes.EventPropertiesFake;
import com.teragrep.nlf_01.fakes.EventSystemPropertiesFake;
import com.teragrep.rlo_14.Facility;
import com.teragrep.rlo_14.SDElement;
import com.teragrep.rlo_14.SDParam;
import com.teragrep.rlo_14.Severity;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

final class StorageBlobTypeTest {

    private ParsedEvent testEvent(
            final String path,
            final EventPartitionContext partitionCtx,
            final EventProperties props,
            final EventSystemProperties sysProps,
            final EnqueuedTime enqueuedTime,
            final EventOffset offset
    ) {
        final InputStream is = Assertions.assertDoesNotThrow(() -> Files.newInputStream(Paths.get(path)));
        final JsonReader reader = Json.createReader(is);

        final JsonObject json = reader.readObject();

        Assertions.assertDoesNotThrow(is::close);
        Assertions.assertDoesNotThrow(reader::close);

        return new ParsedEventFactory(
                new UnparsedEventImpl(json.toString(), partitionCtx, props, sysProps, enqueuedTime, offset)
        ).parsedEvent();
    }

    @Test
    void testIdealCase() {
        final ParsedEvent parsedEvent = testEvent(
                "src/test/resources/storagebloblogs.json", new EventPartitionContextFake(), new EventPropertiesFake(),
                new EventSystemPropertiesFake(), new EnqueuedTimeImpl("2010-01-01T00:00:00"), new EventOffsetImpl("0")
        );

        final StorageType type = new StorageType(parsedEvent, "localhost", "aer");

        final String actualAppName = Assertions.assertDoesNotThrow(type::appName);
        final Facility actualFacility = Assertions.assertDoesNotThrow(type::facility);
        final String actualHostname = Assertions.assertDoesNotThrow(type::hostname);
        final String actualMsg = Assertions.assertDoesNotThrow(type::msg);
        final String actualMsgId = Assertions.assertDoesNotThrow(type::msgId);
        final Severity actualSeverity = Assertions.assertDoesNotThrow(type::severity);
        final Long actualTimestamp = Assertions.assertDoesNotThrow(type::timestamp);
        final Set<SDElement> actualSDElements = Assertions.assertDoesNotThrow(type::sdElements);

        Assertions.assertEquals("StorageBlobLogs", actualAppName);
        Assertions.assertEquals(Facility.AUDIT, actualFacility);
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", actualHostname);
        Assertions
                .assertEquals(
                        "{\"AccessTier\":\"Hot\",\"AccountName\":\"storageaccount2\",\"AuthenticationHash\":\"hash123456789\",\"AuthenticationType\":\"token\",\"AuthorizationDetails\":{},\"CallerIpAddress\":\"127.0.0.1\",\"Category\":\"category1\",\"ClientRequestId\":\"123\",\"ConditionsUsed\":\"condition1=true;\",\"ContentLengthHeader\":234,\"CorrelationId\":\"345\",\"DestinationUri\":\"https://example1.localhost\",\"DurationMs\":12,\"Etag\":\"123-a\",\"LastModifiedTime\":\"2010-10-01T11:59:26.256Z\",\"Location\":\"countrycentral\",\"MetricResponseType\":\"type1\",\"ObjectKey\":\"123456789abc\",\"OperationCount\":2,\"OperationName\":\"copy\",\"OperationVersion\":\"1.0.0\",\"Protocol\":\"http\",\"ReferrerHeader\":\"referer1\",\"RequestBodySize\":23,\"RequesterAppId\":\"abc123456789\",\"RequesterAudience\":\"audience1\",\"RequesterObjectId\":\"12345678-1234-1234-abcd-1234567890ab\",\"RequesterTenantId\":\"12345678-1234-1234-abcd-1234567890abc\",\"RequesterTokenIssuer\":\"issuer1\",\"RequesterUpn\":\"example1@example.localhost\",\"RequestHeaderSize\":34,\"RequestMd5\":\"12345678901234567890123456789012\",\"ResponseBodySize\":45,\"ResponseHeaderSize\":56,\"ResponseMd5\":\"12345678901234567890123456789013\",\"SasExpiryStatus\":\"status1\",\"SchemaVersion\":\"1.1.1\",\"ServerLatencyMs\":1.5,\"ServiceType\":\"service1\",\"SourceSystem\":\"Azure\",\"SourceUri\":\"https://example2.localhost\",\"StatusCode\":\"200\",\"StatusText\":\"success\",\"TenantId\":\"12345678-1234-1234-abcd-1234567890ab\",\"TimeGenerated\":\"2020-10-01T11:59:26.256Z\",\"TlsVersion\":\"1.3\",\"Type\":\"StorageBlobLogs\",\"Uri\":\"https://example.localhost\",\"UserAgentHeader\":\"User-Agent: product/1.0\",\"_ResourceId\":\"/SUBSCRIPTIONS/uuid/RESOURCEGROUPS/ab-cd-efgh-ijklmn-xx-DEV-01/PROVIDERS/postgres-db/FLEXIBLESERVERS/efgh-ijklmn-xx-DEV-01\"}",
                        actualMsg
                );
        Assertions.assertEquals("12345678900", actualMsgId);
        Assertions.assertEquals(Severity.NOTICE, actualSeverity);
        Assertions.assertEquals(1601553566256L, actualTimestamp);

        final Map<String, Map<String, String>> sdElementMap = actualSDElements
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions
                .assertEquals("fully-qualified-namespace", sdElementMap.get("aer_partition@48577").get("fully_qualified_namespace"));
        Assertions.assertEquals("event-hub-name", sdElementMap.get("aer_partition@48577").get("eventhub_name"));
        Assertions.assertEquals("123", sdElementMap.get("aer_partition@48577").get("partition_id"));
        Assertions.assertEquals("consumer-group", sdElementMap.get("aer_partition@48577").get("consumer_group"));

        Assertions.assertEquals("0", sdElementMap.get("aer_event@48577").get("offset"));
        Assertions.assertEquals("2010-01-01T00:00Z", sdElementMap.get("aer_event@48577").get("enqueued_time"));
        Assertions.assertEquals("456", sdElementMap.get("aer_event@48577").get("partition_key"));
        Assertions
                .assertEquals(
                        "{\"null\":\"important-null-value\",\"prop-key\":\"prop-value\",\"important-key\":null}",
                        sdElementMap.get("aer_event@48577").get("properties")
                );

        Assertions.assertEquals("timeEnqueued", sdElementMap.get("aer@48577").get("timestamp_source"));

        Assertions.assertEquals(StorageType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));
    }

    @Test
    void testWithAllMetadataStubs() {
        final ParsedEvent parsedEvent = testEvent(
                "src/test/resources/storagebloblogs.json", new EventPartitionContextStub(), new EventPropertiesStub(),
                new EventSystemPropertiesStub(), new EnqueuedTimeStub(), new EventOffsetStub()
        );

        final StorageType type = new StorageType(parsedEvent, "localhost", "aer");

        final String actualAppName = Assertions.assertDoesNotThrow(type::appName);
        final Facility actualFacility = Assertions.assertDoesNotThrow(type::facility);
        final String actualHostname = Assertions.assertDoesNotThrow(type::hostname);
        final String actualMsg = Assertions.assertDoesNotThrow(type::msg);
        final String actualMsgId = Assertions.assertDoesNotThrow(type::msgId);
        final Severity actualSeverity = Assertions.assertDoesNotThrow(type::severity);
        final Long actualTimestamp = Assertions.assertDoesNotThrow(type::timestamp);
        final Set<SDElement> actualSDElements = Assertions.assertDoesNotThrow(type::sdElements);

        Assertions.assertEquals("StorageBlobLogs", actualAppName);
        Assertions.assertEquals(Facility.AUDIT, actualFacility);
        Assertions.assertEquals("md5-ebf717aa119f6e89e96bd95779913f6c-storageaccount2", actualHostname);
        Assertions
                .assertEquals(
                        "{\"AccessTier\":\"Hot\",\"AccountName\":\"storageaccount2\",\"AuthenticationHash\":\"hash123456789\",\"AuthenticationType\":\"token\",\"AuthorizationDetails\":{},\"CallerIpAddress\":\"127.0.0.1\",\"Category\":\"category1\",\"ClientRequestId\":\"123\",\"ConditionsUsed\":\"condition1=true;\",\"ContentLengthHeader\":234,\"CorrelationId\":\"345\",\"DestinationUri\":\"https://example1.localhost\",\"DurationMs\":12,\"Etag\":\"123-a\",\"LastModifiedTime\":\"2010-10-01T11:59:26.256Z\",\"Location\":\"countrycentral\",\"MetricResponseType\":\"type1\",\"ObjectKey\":\"123456789abc\",\"OperationCount\":2,\"OperationName\":\"copy\",\"OperationVersion\":\"1.0.0\",\"Protocol\":\"http\",\"ReferrerHeader\":\"referer1\",\"RequestBodySize\":23,\"RequesterAppId\":\"abc123456789\",\"RequesterAudience\":\"audience1\",\"RequesterObjectId\":\"12345678-1234-1234-abcd-1234567890ab\",\"RequesterTenantId\":\"12345678-1234-1234-abcd-1234567890abc\",\"RequesterTokenIssuer\":\"issuer1\",\"RequesterUpn\":\"example1@example.localhost\",\"RequestHeaderSize\":34,\"RequestMd5\":\"12345678901234567890123456789012\",\"ResponseBodySize\":45,\"ResponseHeaderSize\":56,\"ResponseMd5\":\"12345678901234567890123456789013\",\"SasExpiryStatus\":\"status1\",\"SchemaVersion\":\"1.1.1\",\"ServerLatencyMs\":1.5,\"ServiceType\":\"service1\",\"SourceSystem\":\"Azure\",\"SourceUri\":\"https://example2.localhost\",\"StatusCode\":\"200\",\"StatusText\":\"success\",\"TenantId\":\"12345678-1234-1234-abcd-1234567890ab\",\"TimeGenerated\":\"2020-10-01T11:59:26.256Z\",\"TlsVersion\":\"1.3\",\"Type\":\"StorageBlobLogs\",\"Uri\":\"https://example.localhost\",\"UserAgentHeader\":\"User-Agent: product/1.0\",\"_ResourceId\":\"/SUBSCRIPTIONS/uuid/RESOURCEGROUPS/ab-cd-efgh-ijklmn-xx-DEV-01/PROVIDERS/postgres-db/FLEXIBLESERVERS/efgh-ijklmn-xx-DEV-01\"}",
                        actualMsg
                );
        Assertions.assertEquals("", actualMsgId);
        Assertions.assertEquals(Severity.NOTICE, actualSeverity);
        Assertions.assertEquals(1601553566256L, actualTimestamp);

        final Map<String, Map<String, String>> sdElementMap = actualSDElements
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals("", sdElementMap.get("aer_partition@48577").get("fully_qualified_namespace"));
        Assertions.assertEquals("", sdElementMap.get("aer_partition@48577").get("eventhub_name"));
        Assertions.assertEquals("", sdElementMap.get("aer_partition@48577").get("partition_id"));
        Assertions.assertEquals("", sdElementMap.get("aer_partition@48577").get("consumer_group"));

        Assertions.assertEquals("", sdElementMap.get("aer_event@48577").get("offset"));
        Assertions.assertEquals("", sdElementMap.get("aer_event@48577").get("enqueued_time"));
        Assertions.assertEquals("", sdElementMap.get("aer_event@48577").get("partition_key"));
        Assertions.assertEquals("{}", sdElementMap.get("aer_event@48577").get("properties"));

        Assertions.assertEquals("generated", sdElementMap.get("aer@48577").get("timestamp_source"));

        Assertions.assertEquals(StorageType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));
    }

    @Test
    void testWithMissingJsonKeys() {
        final ParsedEvent parsedEvent = testEvent(
                "src/test/resources/storagebloblogs_missing_keys.json", new EventPartitionContextStub(),
                new EventPropertiesStub(), new EventSystemPropertiesStub(), new EnqueuedTimeStub(),
                new EventOffsetStub()
        );

        final StorageType type = new StorageType(parsedEvent, "localhost", "aer");

        // Should throw an Exception since the Type field is missing, but this would probably hinder the actual logic of using this class in a real scenario
        Assertions.assertThrows(PluginException.class, type::appName);
        final Facility actualFacility = Assertions.assertDoesNotThrow(type::facility);
        Assertions.assertThrows(PluginException.class, type::hostname);
        final String actualMsg = Assertions.assertDoesNotThrow(type::msg);
        final String actualMsgId = Assertions.assertDoesNotThrow(type::msgId);
        final Severity actualSeverity = Assertions.assertDoesNotThrow(type::severity);
        Assertions.assertThrows(PluginException.class, type::timestamp);
        final Set<SDElement> actualSDElements = Assertions.assertDoesNotThrow(type::sdElements);

        Assertions.assertEquals(Facility.AUDIT, actualFacility);
        Assertions
                .assertEquals(
                        "{\"AccessTier\":\"Hot\",\"AuthenticationHash\":\"hash123456789\",\"AuthenticationType\":\"token\",\"AuthorizationDetails\":{},\"CallerIpAddress\":\"127.0.0.1\",\"Category\":\"category1\",\"ClientRequestId\":\"123\",\"ConditionsUsed\":\"condition1=true;\",\"ContentLengthHeader\":234,\"CorrelationId\":\"345\",\"DestinationUri\":\"https://example1.localhost\",\"DurationMs\":12,\"Etag\":\"123-a\",\"LastModifiedTime\":\"2010-10-01T11:59:26.256Z\",\"Location\":\"countrycentral\",\"MetricResponseType\":\"type1\",\"ObjectKey\":\"123456789abc\",\"OperationCount\":2,\"OperationName\":\"copy\",\"OperationVersion\":\"1.0.0\",\"Protocol\":\"http\",\"ReferrerHeader\":\"referer1\",\"RequestBodySize\":23,\"RequesterAppId\":\"abc123456789\",\"RequesterAudience\":\"audience1\",\"RequesterObjectId\":\"12345678-1234-1234-abcd-1234567890ab\",\"RequesterTenantId\":\"12345678-1234-1234-abcd-1234567890abc\",\"RequesterTokenIssuer\":\"issuer1\",\"RequesterUpn\":\"example1@example.localhost\",\"RequestHeaderSize\":34,\"RequestMd5\":\"12345678901234567890123456789012\",\"ResponseBodySize\":45,\"ResponseHeaderSize\":56,\"ResponseMd5\":\"12345678901234567890123456789013\",\"SasExpiryStatus\":\"status1\",\"SchemaVersion\":\"1.1.1\",\"ServerLatencyMs\":1.5,\"ServiceType\":\"service1\",\"SourceSystem\":\"Azure\",\"SourceUri\":\"https://example2.localhost\",\"StatusCode\":\"200\",\"StatusText\":\"success\",\"TenantId\":\"12345678-1234-1234-abcd-1234567890ab\",\"TlsVersion\":\"1.3\",\"Uri\":\"https://example.localhost\",\"UserAgentHeader\":\"User-Agent: product/1.0\",\"_ResourceId\":\"/SUBSCRIPTIONS/uuid/RESOURCEGROUPS/ab-cd-efgh-ijklmn-xx-DEV-01/PROVIDERS/postgres-db/FLEXIBLESERVERS/efgh-ijklmn-xx-DEV-01\"}",
                        actualMsg
                );
        Assertions.assertEquals("", actualMsgId);
        Assertions.assertEquals(Severity.NOTICE, actualSeverity);

        final Map<String, Map<String, String>> sdElementMap = actualSDElements
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions.assertEquals("", sdElementMap.get("aer_partition@48577").get("fully_qualified_namespace"));
        Assertions.assertEquals("", sdElementMap.get("aer_partition@48577").get("eventhub_name"));
        Assertions.assertEquals("", sdElementMap.get("aer_partition@48577").get("partition_id"));
        Assertions.assertEquals("", sdElementMap.get("aer_partition@48577").get("consumer_group"));

        Assertions.assertEquals("", sdElementMap.get("aer_event@48577").get("offset"));
        Assertions.assertEquals("", sdElementMap.get("aer_event@48577").get("enqueued_time"));
        Assertions.assertEquals("", sdElementMap.get("aer_event@48577").get("partition_key"));
        Assertions.assertEquals("{}", sdElementMap.get("aer_event@48577").get("properties"));

        Assertions.assertEquals("generated", sdElementMap.get("aer@48577").get("timestamp_source"));

        Assertions.assertEquals(StorageType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));
    }

    @Test
    @DisplayName("test sdElement() return value")
    void testSdElementReturnValue() {
        final Map<String, Object> partitionContextMap = new HashMap<>();
        partitionContextMap.put("FullyQualifiedNamespace", "fully-qualified-namespace");
        partitionContextMap.put("EventHubName", "event-hub-name");
        partitionContextMap.put("PartitionId", "123");
        partitionContextMap.put("ConsumerGroup", "consumer-group");

        final Map<String, Object> systemPropertiesMap = new HashMap<>();
        systemPropertiesMap.put("PartitionKey", "456");
        systemPropertiesMap.put("SequenceNumber", "12345678900");

        final Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put("prop-key", "prop-value");
        propertiesMap.put(null, "important-null-value");
        propertiesMap.put("important-key", null);

        final ParsedEvent parsedEvent = testEvent(
                "src/test/resources/appserviceconsolelogs.json", new EventPartitionContextImpl(partitionContextMap), new EventPropertiesImpl(propertiesMap), new EventSystemPropertiesImpl(systemPropertiesMap), new EnqueuedTimeImpl("2010-01-01T00:00:00"), new EventOffsetImpl("0")
        );

        final StorageType type = new StorageType(parsedEvent, "localhost", "aer");

        final Set<SDElement> actualSDElements = Assertions.assertDoesNotThrow(type::sdElements);

        final Map<String, Map<String, String>> sdElementMap = actualSDElements
                .stream()
                .collect(Collectors.toMap((SDElement::getSdID), (sdElem) -> sdElem.getSdParams().stream().collect(Collectors.toMap(SDParam::getParamName, SDParam::getParamValue))));

        Assertions
                .assertEquals("fully-qualified-namespace", sdElementMap.get("aer_partition@48577").get("fully_qualified_namespace"));
        Assertions.assertEquals("event-hub-name", sdElementMap.get("aer_partition@48577").get("eventhub_name"));
        Assertions.assertEquals("123", sdElementMap.get("aer_partition@48577").get("partition_id"));
        Assertions.assertEquals("consumer-group", sdElementMap.get("aer_partition@48577").get("consumer_group"));

        Assertions.assertEquals("0", sdElementMap.get("aer_event@48577").get("offset"));
        Assertions.assertEquals("2010-01-01T00:00Z", sdElementMap.get("aer_event@48577").get("enqueued_time"));
        Assertions.assertEquals("456", sdElementMap.get("aer_event@48577").get("partition_key"));
        Assertions
                .assertEquals(
                        "{\"null\":\"important-null-value\",\"prop-key\":\"prop-value\",\"important-key\":null}",
                        sdElementMap.get("aer_event@48577").get("properties")
                );

        Assertions.assertEquals("timeEnqueued", sdElementMap.get("aer@48577").get("timestamp_source"));

        Assertions.assertEquals(StorageType.class.getSimpleName(), sdElementMap.get("nlf_01@48577").get("eventType"));
    }
}
