/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.python.util;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.python.PythonConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.python.AbstractDataStreamPythonFunctionOperator;
import org.apache.flink.streaming.api.operators.python.AbstractOneInputPythonFunctionOperator;
import org.apache.flink.streaming.api.operators.python.AbstractPythonFunctionOperator;
import org.apache.flink.streaming.api.transformations.AbstractMultipleInputTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;

import org.apache.flink.shaded.guava30.com.google.common.collect.Queues;
import org.apache.flink.shaded.guava30.com.google.common.collect.Sets;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

/** A Util class to handle the configurations of Python jobs. */
public class PythonConfigUtil {

    public static final String KEYED_STREAM_VALUE_OPERATOR_NAME = "_keyed_stream_values_operator";
    public static final String STREAM_KEY_BY_MAP_OPERATOR_NAME = "_stream_key_by_map_operator";
    public static final String STREAM_PARTITION_CUSTOM_MAP_OPERATOR_NAME =
            "_partition_custom_map_operator";

    /**
     * Get the private field {@link StreamExecutionEnvironment#configuration} by reflection
     * recursively. It allows modification to the configuration compared with {@link
     * StreamExecutionEnvironment#getConfiguration()}.
     */
    public static Configuration getEnvironmentConfig(StreamExecutionEnvironment env)
            throws InvocationTargetException, IllegalAccessException, NoSuchFieldException {
        Field configurationField = null;
        for (Class<?> clz = env.getClass(); clz != Object.class; clz = clz.getSuperclass()) {
            try {
                configurationField = clz.getDeclaredField("configuration");
                break;
            } catch (NoSuchFieldException e) {
                // ignore
            }
        }

        if (configurationField == null) {
            throw new NoSuchFieldException("Field 'configuration' not found.");
        }

        configurationField.setAccessible(true);
        return (Configuration) configurationField.get(env);
    }

    public static void configPythonOperator(StreamExecutionEnvironment env)
            throws IllegalAccessException, NoSuchFieldException {
        final Configuration config = extractPythonConfiguration(env, env.getConfiguration());

        for (Transformation<?> transformation : env.getTransformations()) {
            alignTransformation(transformation);

            if (isPythonOperator(transformation)) {
                // declare the use case of managed memory
                transformation.declareManagedMemoryUseCaseAtSlotScope(ManagedMemoryUseCase.PYTHON);

                AbstractPythonFunctionOperator<?> pythonFunctionOperator =
                        getPythonOperator(transformation);
                if (pythonFunctionOperator != null) {
                    pythonFunctionOperator.getConfiguration().addAll(config);
                }
            }
        }
    }

    /** Extract the configurations which is used in the Python operators. */
    public static Configuration extractPythonConfiguration(
            StreamExecutionEnvironment env, ReadableConfig config) {
        final Configuration pythonDependencyConfig =
                PythonDependencyUtils.configurePythonDependencies(env.getCachedFiles(), config);
        final PythonConfig pythonConfig = new PythonConfig(config, pythonDependencyConfig);
        return pythonConfig.toConfiguration();
    }

    /**
     * Configure the {@link AbstractOneInputPythonFunctionOperator} to be chained with the
     * upstream/downstream operator by setting their parallelism, slot sharing group, co-location
     * group to be the same, and applying a {@link ForwardPartitioner}. 1. operator with name
     * "_keyed_stream_values_operator" should align with its downstream operator. 2. operator with
     * name "_stream_key_by_map_operator" should align with its upstream operator.
     */
    private static void alignTransformation(Transformation<?> transformation)
            throws NoSuchFieldException, IllegalAccessException {
        String transformName = transformation.getName();
        if (transformation.getInputs().isEmpty()) {
            return;
        }
        Transformation<?> inputTransformation = transformation.getInputs().get(0);
        String inputTransformName = inputTransformation.getName();
        if (inputTransformName.equals(KEYED_STREAM_VALUE_OPERATOR_NAME)) {
            chainTransformation(inputTransformation, transformation);
            configForwardPartitioner(inputTransformation, transformation);
        }
        if (transformName.equals(STREAM_KEY_BY_MAP_OPERATOR_NAME)
                || transformName.equals(STREAM_PARTITION_CUSTOM_MAP_OPERATOR_NAME)) {
            chainTransformation(transformation, inputTransformation);
            configForwardPartitioner(inputTransformation, transformation);
        }
    }

    private static void chainTransformation(
            Transformation<?> firstTransformation, Transformation<?> secondTransformation) {
        secondTransformation
                .getSlotSharingGroup()
                .ifPresent(firstTransformation::setSlotSharingGroup);
        firstTransformation.setCoLocationGroupKey(secondTransformation.getCoLocationGroupKey());
        firstTransformation.setParallelism(secondTransformation.getParallelism());
    }

    private static void configForwardPartitioner(
            Transformation<?> upTransformation, Transformation<?> transformation)
            throws IllegalAccessException, NoSuchFieldException {
        // set ForwardPartitioner
        PartitionTransformation<?> partitionTransform =
                new PartitionTransformation<>(upTransformation, new ForwardPartitioner<>());
        Field inputTransformationField = transformation.getClass().getDeclaredField("input");
        inputTransformationField.setAccessible(true);
        inputTransformationField.set(transformation, partitionTransform);
    }

    private static AbstractPythonFunctionOperator<?> getPythonOperator(
            Transformation<?> transformation) {
        StreamOperatorFactory<?> operatorFactory = null;
        if (transformation instanceof OneInputTransformation) {
            operatorFactory = ((OneInputTransformation<?, ?>) transformation).getOperatorFactory();
        } else if (transformation instanceof TwoInputTransformation) {
            operatorFactory =
                    ((TwoInputTransformation<?, ?, ?>) transformation).getOperatorFactory();
        } else if (transformation instanceof AbstractMultipleInputTransformation) {
            operatorFactory =
                    ((AbstractMultipleInputTransformation<?>) transformation).getOperatorFactory();
        }

        if (operatorFactory instanceof SimpleOperatorFactory
                && ((SimpleOperatorFactory<?>) operatorFactory).getOperator()
                        instanceof AbstractPythonFunctionOperator) {
            return (AbstractPythonFunctionOperator<?>)
                    ((SimpleOperatorFactory<?>) operatorFactory).getOperator();
        }

        return null;
    }

    private static boolean isPythonOperator(Transformation<?> transform) {
        if (transform instanceof OneInputTransformation) {
            return isPythonOperator(
                    ((OneInputTransformation<?, ?>) transform).getOperatorFactory());
        } else if (transform instanceof TwoInputTransformation) {
            return isPythonOperator(
                    ((TwoInputTransformation<?, ?, ?>) transform).getOperatorFactory());
        } else if (transform instanceof AbstractMultipleInputTransformation) {
            return isPythonOperator(
                    ((AbstractMultipleInputTransformation<?>) transform).getOperatorFactory());
        } else {
            return false;
        }
    }

    private static boolean isPythonOperator(StreamOperatorFactory<?> streamOperatorFactory) {
        if (streamOperatorFactory instanceof SimpleOperatorFactory) {
            return ((SimpleOperatorFactory<?>) streamOperatorFactory).getOperator()
                    instanceof AbstractPythonFunctionOperator;
        } else {
            return false;
        }
    }

    public static boolean isPythonDataStreamOperator(Transformation<?> transform) {
        if (transform instanceof OneInputTransformation) {
            return isPythonDataStreamOperator(
                    ((OneInputTransformation<?, ?>) transform).getOperatorFactory());
        } else if (transform instanceof TwoInputTransformation) {
            return isPythonDataStreamOperator(
                    ((TwoInputTransformation<?, ?, ?>) transform).getOperatorFactory());
        } else {
            return false;
        }
    }

    private static boolean isPythonDataStreamOperator(
            StreamOperatorFactory<?> streamOperatorFactory) {
        if (streamOperatorFactory instanceof SimpleOperatorFactory) {
            return ((SimpleOperatorFactory<?>) streamOperatorFactory).getOperator()
                    instanceof AbstractDataStreamPythonFunctionOperator;
        } else {
            return false;
        }
    }

    public static void setPartitionCustomOperatorNumPartitions(
            List<Transformation<?>> transformations) {
        // Update the numPartitions of PartitionCustomOperator after aligned all operators.
        final Set<Transformation<?>> alreadyTransformed = Sets.newIdentityHashSet();
        final Queue<Transformation<?>> toTransformQueue = Queues.newArrayDeque(transformations);
        while (!toTransformQueue.isEmpty()) {
            final Transformation<?> transformation = toTransformQueue.poll();
            if (!alreadyTransformed.contains(transformation)
                    && !(transformation instanceof PartitionTransformation)) {
                alreadyTransformed.add(transformation);

                getNonPartitionTransformationInput(transformation)
                        .ifPresent(
                                input -> {
                                    AbstractPythonFunctionOperator<?> pythonFunctionOperator =
                                            getPythonOperator(input);
                                    if (pythonFunctionOperator
                                            instanceof AbstractDataStreamPythonFunctionOperator) {
                                        AbstractDataStreamPythonFunctionOperator<?>
                                                pythonDataStreamFunctionOperator =
                                                        (AbstractDataStreamPythonFunctionOperator<
                                                                        ?>)
                                                                pythonFunctionOperator;
                                        if (pythonDataStreamFunctionOperator
                                                .containsPartitionCustom()) {
                                            pythonDataStreamFunctionOperator.setNumPartitions(
                                                    transformation.getParallelism());
                                        }
                                    }
                                });

                toTransformQueue.addAll(transformation.getInputs());
            }
        }
    }

    private static Optional<Transformation<?>> getNonPartitionTransformationInput(
            Transformation<?> transformation) {
        if (transformation.getInputs().size() != 1) {
            return Optional.empty();
        }

        final Transformation<?> inputTransformation = transformation.getInputs().get(0);
        if (inputTransformation instanceof PartitionTransformation) {
            return getNonPartitionTransformationInput(inputTransformation);
        } else {
            return Optional.of(inputTransformation);
        }
    }
}
