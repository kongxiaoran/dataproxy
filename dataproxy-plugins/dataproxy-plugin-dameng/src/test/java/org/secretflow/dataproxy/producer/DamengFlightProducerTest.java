/*
 * Copyright 2025 Ant Group Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.secretflow.dataproxy.producer;

import com.google.protobuf.Any;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.secretflow.v1alpha1.kusciaapi.Domaindata;
import org.secretflow.v1alpha1.kusciaapi.Domaindatasource;
import org.secretflow.v1alpha1.kusciaapi.Flightdm;
import org.secretflow.v1alpha1.kusciaapi.Flightinner;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;


/**
 * @author kongxiaoran
 * @date 2025/11/07
 */
@ExtendWith(MockitoExtension.class)
public class DamengFlightProducerTest {
        
    @InjectMocks
    private DamengFlightProducer damengFlightProducer;

    @Mock
    private FlightProducer.CallContext context;

    @Mock
    private FlightDescriptor descriptor;

    private final Domaindatasource.DatabaseDataSourceInfo damengDataSourceInfo =
            Domaindatasource.DatabaseDataSourceInfo
                    .newBuilder()
                    .setEndpoint("jdbc:dm://localhost:5236")
                    .setDatabase("database")
                    .setUser("user")
                    .setPassword("password")
                    .build();
    private final Domaindatasource.DataSourceInfo dataSourceInfo =
            Domaindatasource.DataSourceInfo.newBuilder().setDatabase(damengDataSourceInfo).build();

    private final Domaindatasource.DomainDataSource domainDataSource =
            Domaindatasource.DomainDataSource.newBuilder()
                    .setDatasourceId("datasourceId")
                    .setName("datasourceName")
                    .setType("dameng")
                    .setInfo(dataSourceInfo)
                    .build();

    private final Domaindata.DomainData domainData =
            Domaindata.DomainData.newBuilder()
                    .setDatasourceId("datasourceId")
                    .setName("domainDataName")
                    .setRelativeUri("table_name")
                    .setDomaindataId("domainDataId")
                    .setType("table")
                    .build();

    private final Flightdm.CommandDomainDataQuery commandDomainDataQueryWithCSV =
            Flightdm.CommandDomainDataQuery.newBuilder()
                    .setContentType(Flightdm.ContentType.CSV)
                    .build();

    @Test
    public void testGetProducerName() {
        String producerName = damengFlightProducer.getProducerName();
        assertEquals("dameng", producerName);
    }

    @Test
    public void testGetFlightInfoWithTableCommand() {
        Flightinner.CommandDataMeshQuery dataMeshQuery =
                Flightinner.CommandDataMeshQuery.newBuilder()
                        .setQuery(commandDomainDataQueryWithCSV)
                        .setDatasource(domainDataSource)
                        .setDomaindata(domainData)
                        .build();
        when(descriptor.getCommand()).thenReturn(Any.pack(dataMeshQuery).toByteArray());

        assertDoesNotThrow(() -> {
            FlightInfo flightInfo = damengFlightProducer.getFlightInfo(context, descriptor);

            assertNotNull(flightInfo);
            assertFalse(flightInfo.getEndpoints().isEmpty());

            assertNotNull(flightInfo.getEndpoints().get(0).getLocations());
            assertFalse(flightInfo.getEndpoints().get(0).getLocations().isEmpty());

            assertNotNull(flightInfo.getEndpoints().get(0).getTicket());
            assertNotNull(flightInfo.getEndpoints().get(0).getTicket().getBytes());

        });
    }

    @Test
    public void testGetFlightInfoWithUnsupportedType() {
        when(descriptor.getCommand()).thenReturn("testCommand".getBytes(StandardCharsets.UTF_8));
        assertThrows(RuntimeException.class, () -> damengFlightProducer.getFlightInfo(context, descriptor));
    }

    @Test
    public void testGetFlightInfoWithSqlQuery() {
        Flightdm.CommandDataSourceSqlQuery commandDataSourceSqlQuery =
                Flightdm.CommandDataSourceSqlQuery.newBuilder()
                        .setSql("SELECT * FROM test_table")
                        .setDatasourceId("datasourceId")
                        .build();
        
        Flightinner.CommandDataMeshSqlQuery sqlQuery =
                Flightinner.CommandDataMeshSqlQuery.newBuilder()
                        .setQuery(commandDataSourceSqlQuery)
                        .setDatasource(domainDataSource)
                        .build();
        
        when(descriptor.getCommand()).thenReturn(Any.pack(sqlQuery).toByteArray());

        assertDoesNotThrow(() -> {
            FlightInfo flightInfo = damengFlightProducer.getFlightInfo(context, descriptor);
            assertNotNull(flightInfo);
            assertFalse(flightInfo.getEndpoints().isEmpty());
        });
    }

    @Test
    public void testGetFlightInfoWithUpdateCommand() {
        Flightdm.CommandDomainDataUpdate commandDomainDataUpdate =
                Flightdm.CommandDomainDataUpdate.newBuilder()
                        .setContentType(Flightdm.ContentType.CSV)
                        .build();
        
        Flightinner.CommandDataMeshUpdate updateCommand =
                Flightinner.CommandDataMeshUpdate.newBuilder()
                        .setUpdate(commandDomainDataUpdate)
                        .setDatasource(domainDataSource)
                        .setDomaindata(domainData)
                        .build();
        
        when(descriptor.getCommand()).thenReturn(Any.pack(updateCommand).toByteArray());

        assertDoesNotThrow(() -> {
            FlightInfo flightInfo = damengFlightProducer.getFlightInfo(context, descriptor);
            assertNotNull(flightInfo);
            assertFalse(flightInfo.getEndpoints().isEmpty());
        });
    }

    @Test
    public void testGetFlightInfoWithRawContentType() {
        Flightdm.CommandDomainDataQuery commandDomainDataQueryWithRaw =
                Flightdm.CommandDomainDataQuery.newBuilder()
                        .setContentType(Flightdm.ContentType.RAW)
                        .build();
        
        Flightinner.CommandDataMeshQuery dataMeshQuery =
                Flightinner.CommandDataMeshQuery.newBuilder()
                        .setQuery(commandDomainDataQueryWithRaw)
                        .setDatasource(domainDataSource)
                        .setDomaindata(domainData)
                        .build();
        
        when(descriptor.getCommand()).thenReturn(Any.pack(dataMeshQuery).toByteArray());

        assertDoesNotThrow(() -> {
            FlightInfo flightInfo = damengFlightProducer.getFlightInfo(context, descriptor);
            assertNotNull(flightInfo);
            assertFalse(flightInfo.getEndpoints().isEmpty());
        });
    }

    @Test
    public void testGetFlightInfoWithPartitionSpec() {
        Flightdm.CommandDomainDataQuery commandDomainDataQuery =
                Flightdm.CommandDomainDataQuery.newBuilder()
                        .setContentType(Flightdm.ContentType.CSV)
                        .setPartitionSpec("dt=20240101")
                        .build();
        
        Flightinner.CommandDataMeshQuery dataMeshQuery =
                Flightinner.CommandDataMeshQuery.newBuilder()
                        .setQuery(commandDomainDataQuery)
                        .setDatasource(domainDataSource)
                        .setDomaindata(domainData)
                        .build();
        
        when(descriptor.getCommand()).thenReturn(Any.pack(dataMeshQuery).toByteArray());

        assertDoesNotThrow(() -> {
            FlightInfo flightInfo = damengFlightProducer.getFlightInfo(context, descriptor);
            assertNotNull(flightInfo);
            assertFalse(flightInfo.getEndpoints().isEmpty());
        });
    }

    @Test
    public void testGetFlightInfoWithInvalidCommandType() {
        when(descriptor.getCommand()).thenReturn(
                Any.pack(Flightdm.CommandDomainDataQuery.newBuilder().build())
                        .toByteArray());

        assertThrows(RuntimeException.class, () -> {
            damengFlightProducer.getFlightInfo(context, descriptor);
        });
    }
}
