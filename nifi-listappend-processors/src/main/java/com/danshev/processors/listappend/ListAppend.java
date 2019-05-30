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
package com.danshev.processors.listappend;

import com.danshev.aerospike_connection.AerospikeConnectionService;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.ByteArrayOutputStream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.aerospike.client.Value;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;

@Tags({"aerospike append remove"})
@CapabilityDescription("Performs either an append (list) or remove operation on the specified Aerospike namespace-set-key.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class ListAppend extends AbstractProcessor {

    public static final PropertyDescriptor AEROSPIKE_SERVICE = new PropertyDescriptor
            .Builder().name("AEROSPIKE_SERVICE")
            .displayName("Aerospike Connection Service")
            .description("The Controller Service to use for the Aerospike connection.")
            .required(true)
            .identifiesControllerService(AerospikeConnectionService.class)
            .build();

    public static final PropertyDescriptor APPEND_OR_REMOVE = new PropertyDescriptor
            .Builder().name("APPEND_OR_REMOVE")
            .displayName("Method to Invoke")
            .defaultValue("Append")
            .allowableValues("Append", "Remove")
            .required(true)
            .build();

    public static final PropertyDescriptor AEROSPIKE_NAMESPACE = new PropertyDescriptor
            .Builder().name("AEROSPIKE_NAMESPACE")
            .displayName("Aerospike Namespace")
            .description("The Aerospike namespace")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor AEROSPIKE_SET = new PropertyDescriptor
            .Builder().name("AEROSPIKE_SET")
            .displayName("Aerospike Set")
            .description("The Aerospike set")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor AEROSPIKE_KEY = new PropertyDescriptor
            .Builder().name("AEROSPIKE_KEY")
            .displayName("Aerospike Key")
            .description("The Aerospike key")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successful operations are transferred to this relationship")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed operations are transferred to this relationship")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(AEROSPIKE_SERVICE);
        descriptors.add(APPEND_OR_REMOVE);
        descriptors.add(AEROSPIKE_NAMESPACE);
        descriptors.add(AEROSPIKE_SET);
        descriptors.add(AEROSPIKE_KEY);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        final String aero_ns = context.getProperty(AEROSPIKE_NAMESPACE).evaluateAttributeExpressions(flowFile).getValue();
        final String aero_set = context.getProperty(AEROSPIKE_SET).evaluateAttributeExpressions(flowFile).getValue();
        final String aero_key = context.getProperty(AEROSPIKE_KEY).evaluateAttributeExpressions(flowFile).getValue();
        final String desiredMethod = context.getProperty(APPEND_OR_REMOVE).getValue();

        final AerospikeConnectionService aerospikeClient = context.getProperty(AEROSPIKE_SERVICE).asControllerService(AerospikeConnectionService.class);

        try {
            Key fullKey = new Key(aero_ns, aero_set, aero_key);
            if (desiredMethod == "Append") {
                final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                session.exportTo(flowFile, bytes);
                final String flowFileContents = bytes.toString();
                final Value v = new Value.StringValue(flowFileContents);
                aerospikeClient.append(fullKey, v);
            } else {
                aerospikeClient.remove(fullKey);
            }
        } catch (Exception e) {
            session.transfer(flowFile, FAILURE);
            session.commit();
        }

        session.transfer(flowFile, SUCCESS);
        session.commit();
    }
}
