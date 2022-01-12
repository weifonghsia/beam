/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.kafka;

import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.checkerframework.checker.nullness.qual.Nullable;

/** The result of a {@link KafkaIO.Write} transform. */
@SuppressWarnings({"rawtypes"})
public class KafkaWriteResult<ProducerRecord> implements POutput {
  private final Pipeline pipeline;
  private final TupleTag<String> writesTag;
  private final PCollection<ProducerRecord> failedWrites;
  private final PCollection<ProducerRecord> successfulWrites;

  private KafkaWriteResult(
      Pipeline pipeline,
      TupleTag<String> writesTag,
      PCollection<ProducerRecord> failedWrites,
      PCollection<ProducerRecord> successfulWrites) {
    this.pipeline = pipeline;
    this.writesTag = writesTag;
    this.failedWrites = failedWrites;
    this.successfulWrites = successfulWrites;
  }

  /** Creates a {@link KafkaWriteResult} in the given {@link Pipeline}. */
  static <ProducerRecord> KafkaWriteResult in(
      Pipeline pipeline,
      @Nullable TupleTag<String> writesTag,
      @Nullable PCollection<ProducerRecord> failedWrites,
      @Nullable PCollection<ProducerRecord> successfulWrites) {
    return new KafkaWriteResult(pipeline, null, null, successfulWrites);
  }

  /**
   * Returns a {@link PCollection} containing the {@link ProducerRecord}s that were written to
   * Kafka.
   */
  public PCollection<ProducerRecord> getSuccessfulWrites() {
    return successfulWrites;
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  @Override
  public Map<TupleTag<?>, PValue> expand() {
    return ImmutableMap.of(writesTag, failedWrites);
  }

  @Override
  public void finishSpecifyingOutput(String transformName, PInput input, PTransform transform) {}
}
