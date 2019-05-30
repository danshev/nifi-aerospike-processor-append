/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.danshev.aerospike_connection;

import com.aerospike.client.*;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.google.common.base.Strings;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


@Tags({ "aerospike"})
@CapabilityDescription("Aerospike Connection Service.")
public class AerospikeConnectionControllerService extends AbstractControllerService implements AerospikeConnectionService {

    private static final Logger log = LoggerFactory.getLogger(AerospikeConnectionControllerService.class);

    public static final PropertyDescriptor AEROSPIKE_HOSTS = new PropertyDescriptor
            .Builder().name("AEROSPIKE_HOSTS")
            .displayName("Aerospike Hosts List")
            .description("A pipe-delimited list of host.name,port sets for each of the Aerospike servers (example: a.host.name,3000|another.host.name,3000)")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor AEROSPIKE_USERNAME = new PropertyDescriptor
            .Builder().name("AEROSPIKE_USERNAME")
            .displayName("Aerospike username")
            .description("If necessary, the username for the Aerospike connection")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor AEROSPIKE_PASSWORD = new PropertyDescriptor
            .Builder().name("AEROSPIKE_PASSWORD")
            .displayName("Aerospike password")
            .description("If necessary, the password for the Aerospike connection")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static AerospikeClient aerospikeClient = null;
    private static ClientPolicy policy = new ClientPolicy();

    private static String aerospike_host_string = "";
    private static String aerospike_username = "";
    private static String aerospike_password = "";

    private static final List<PropertyDescriptor> properties;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(AEROSPIKE_HOSTS);
        props.add(AEROSPIKE_USERNAME);
        props.add(AEROSPIKE_PASSWORD);
        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    /**
     * @param context
     *            the configuration context
     * @throws InitializationException
     *             if unable to create a database connection
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {

        // Load properties
        aerospike_host_string = context.getProperty(AEROSPIKE_HOSTS).getValue();
        aerospike_username = context.getProperty(AEROSPIKE_USERNAME).getValue();
        aerospike_password = context.getProperty(AEROSPIKE_PASSWORD).getValue();

        if (!Strings.isNullOrEmpty(aerospike_password)) {
            policy.user = aerospike_username;
            policy.password = aerospike_password;
        }

        aerospikeClient = getConnection();
        if (aerospikeClient == null) {
            log.error("Error: Couldn't connect to Aerospike.");
        }
    }

    @OnDisabled
    public void shutdown() {
        aerospikeClient = null;
    }

    @Override
    public AerospikeClient getConnection() throws ProcessException {
        try {
            if (aerospikeClient == null) {
                final String[] hostStringsArray = aerospike_host_string.split("[\\r\\n]+");
                final Integer hostsCount = hostStringsArray.length;
                Host[] hosts = new Host[hostsCount];
                for (int i = 0; i < hostStringsArray.length; i++) {
                    String[] hostPortArray = hostStringsArray[i].split(",");
                    log.info(" - Aerospike host: " + hostPortArray[0]);
                    hosts[i] = new Host(hostPortArray[0].replaceAll("\\s+",""),
                            Integer.parseInt(hostPortArray[1].replaceAll("\\s+","")));
                }

                aerospikeClient = new AerospikeClient(policy, hosts);
            }
        } catch (Exception e) {
            log.error("Error: " + e.getMessage());
            e.printStackTrace();
        }

        return aerospikeClient;
    }

    @Override
    public AerospikeClient append(Key fullKey, Value value) throws ProcessException {
        try {
            if (aerospikeClient != null) {
                WritePolicy policy = new WritePolicy();
                policy.sendKey = true;
                aerospikeClient.operate(policy, fullKey, ListOperation.append("events", value));
            }
        } catch (Exception e) {
            log.error("Error: " + e.getMessage());
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public AerospikeClient remove(Key fullKey) throws ProcessException {
        try {
            if (aerospikeClient != null) {
                WritePolicy policy = new WritePolicy();
                policy.sendKey = true;
                aerospikeClient.delete(policy, fullKey);
            }
        } catch (Exception e) {
            log.error("Error: " + e.getMessage());
            e.printStackTrace();
        }

        return null;
    }

    /*
    @Override
    public AerospikeClient execute(String clientMethod, Map<String, String> clientArgs) throws
            SecurityException, NoSuchMethodException, InvocationTargetException {

        Method method;

        try {
            if (aerospikeClient != null) method = aerospikeClient.getClass().getMethod("clientMethod", null);
        } catch (SecurityException e) {

        } catch (NoSuchMethodException e) {

        }

        try {
            method.invoke(aerospikeClient, null);
        } catch (IllegalArgumentException e) {

        } catch (IllegalAccessException e) {

        } catch (InvocationTargetException e) {

        }

        return null;
    }
    */
}
