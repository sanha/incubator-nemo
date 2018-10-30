/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nemo.compiler.optimizer.pass.compiletime.reshaping;

import org.apache.nemo.common.HashRange;
import org.apache.nemo.common.KeyExtractor;
import org.apache.nemo.common.KeyRange;
import org.apache.nemo.common.Pair;
import org.apache.nemo.common.coder.*;
import org.apache.nemo.common.dag.DAG;
import org.apache.nemo.common.dag.DAGBuilder;
import org.apache.nemo.common.exception.UnsupportedCommPatternException;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.ir.edge.IREdge;
import org.apache.nemo.common.ir.edge.executionproperty.*;
import org.apache.nemo.common.ir.vertex.IRVertex;
import org.apache.nemo.common.ir.vertex.OperatorVertex;
import org.apache.nemo.common.ir.vertex.SourceVertex;
import org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty;
import org.apache.nemo.common.ir.vertex.transform.AggregateMetricTransform;
import org.apache.nemo.common.ir.vertex.transform.MetricCollectTransform;
import org.apache.nemo.compiler.optimizer.pass.compiletime.Requires;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Pass to reshape the IR DAG for skew handling.
 *
 * This pass inserts vertices to perform two-step dynamic optimization for skew handling.
 * 1) Task-level statistic collection is done via vertex with {@link MetricCollectTransform}
 * 2) Stage-level statistic aggregation is done via vertex with {@link AggregateMetricTransform}
 * inserted before shuffle edges.
 * */
@Requires(CommunicationPatternProperty.class)
public final class SamplingSkewReshapingPass extends ReshapingPass {
  private static final Logger LOG = LoggerFactory.getLogger(SamplingSkewReshapingPass.class.getName());
  private static final int sampleRate = 100; // 1%

  /**
   * Default constructor.
   */
  public SamplingSkewReshapingPass() {
    super(SamplingSkewReshapingPass.class);
  }

  @Override
  public DAG<IRVertex, IREdge> apply(final DAG<IRVertex, IREdge> dag) {
    final DAGBuilder<IRVertex, IREdge> builder = new DAGBuilder<>();
    final List<OperatorVertex> metricCollectVertices = new ArrayList<>();

    dag.topologicalDo(v -> {
      // We care about OperatorVertices that have shuffle incoming edges with main output.
      if (v instanceof OperatorVertex && dag.getIncomingEdgesOf(v).stream().anyMatch(irEdge ->
        CommunicationPatternProperty.Value.Shuffle
          .equals(irEdge.getPropertyValue(CommunicationPatternProperty.class).get()))
        && dag.getIncomingEdgesOf(v).stream().noneMatch(irEdge ->
        irEdge.getPropertyValue(AdditionalOutputTagProperty.class).isPresent())) {

        dag.getIncomingEdgesOf(v).forEach(edge -> {
          if (CommunicationPatternProperty.Value.Shuffle
              .equals(edge.getPropertyValue(CommunicationPatternProperty.class).get())) {

            final IRVertex vtxToSample = edge.getSrc();
            final int originalParallelism = vtxToSample.getPropertyValue(ParallelismProperty.class)
                .orElseThrow(() -> new RuntimeException("No parallelism!"));
            final int sampledParallelism = originalParallelism > sampleRate ? originalParallelism / sampleRate : 1;

            final List<Integer> randomIndices =
                IntStream.range(0, originalParallelism).boxed().collect(Collectors.toList());
            Collections.shuffle(randomIndices);
            final List<Integer> idxToSample = randomIndices.subList(0, sampledParallelism);
            final IRVertex lastSampledVtx =
                appendSampledDag(originalParallelism, idxToSample, vtxToSample, builder, dag);

            final OperatorVertex abv = generateMetricAggregationVertex();
            final OperatorVertex mcv = generateMetricCollectVertex(edge, abv);
            abv.setPropertyPermanently(ParallelismProperty.of(1)); // Fixed parallelism.
            mcv.setPropertyPermanently(ParallelismProperty.of(sampledParallelism));
            metricCollectVertices.add(mcv);
            builder.addVertex(v);
            builder.addVertex(mcv);
            builder.addVertex(abv);

            // We then insert the vertex with MetricCollectTransform and vertex with AggregateMetricTransform
            // between the vertex and incoming vertices.
            final IREdge edgeToMCV = generateEdgeToMCV(edge, lastSampledVtx, mcv);
            final IREdge edgeToABV = generateEdgeToABV(edge, mcv, abv);
            final IREdge edgeToOriginalDstV =
                new IREdge(edge.getPropertyValue(CommunicationPatternProperty.class).get(), edge.getSrc(), v);
            edge.copyExecutionPropertiesTo(edgeToOriginalDstV);

            builder.connectVertices(edgeToMCV);
            builder.connectVertices(edgeToABV);
            builder.connectVertices(edgeToOriginalDstV);
          } else {
            builder.connectVertices(edge);
          }
        });
      } else { // Others are simply added to the builder, unless it comes from an updated vertex
        builder.addVertex(v);
        dag.getIncomingEdgesOf(v).forEach(builder::connectVertices);
      }
    });
    return builder.build();
  }

  private IRVertex appendSampledDag(final int originalParallelism,
                                    final List<Integer> idxToSample,
                                    final IRVertex vtxToSample,
                                    final DAGBuilder<IRVertex, IREdge> builder,
                                    final DAG<IRVertex, IREdge> dag) {
    // Add sampled vertex
    final int sampledParallelism = idxToSample.size();
    final IRVertex sampledVtx = vtxToSample instanceof SourceVertex ?
        ((SourceVertex) vtxToSample).getSampledClone(idxToSample) : vtxToSample.getClone();
    vtxToSample.copyExecutionPropertiesTo(sampledVtx);
    sampledVtx.setPropertyPermanently(ParallelismProperty.of(sampledParallelism));
    builder.addVertex(sampledVtx);

    // Add edges toward the sampled vertex
    for (final IREdge edgeToVtxToSample : dag.getIncomingEdgesOf(vtxToSample)) {
      final IREdge edgeToSampledVtx;
      switch (edgeToVtxToSample.getPropertyValue(CommunicationPatternProperty.class)
          .orElseThrow(() -> new RuntimeException("No communication pattern on an edge."))) {
        case Shuffle:
          edgeToSampledVtx =
              new IREdge(CommunicationPatternProperty.Value.Shuffle, edgeToVtxToSample.getSrc(), sampledVtx);
          edgeToVtxToSample.copyExecutionPropertiesTo(edgeToSampledVtx);

          // Assign proper partition range to read for each sampled vertex.
          final HashMap<Integer, KeyRange> shuffleDistribution = new HashMap<>();
          for (int i = 0; i < idxToSample.size(); i++) {
            final int idxToRead = idxToSample.get(i);
            shuffleDistribution.put(i, HashRange.of(idxToRead, idxToRead + 1, false));
          }
          edgeToSampledVtx.setPropertyPermanently(
              ShuffleDistributionProperty.of(Pair.of(originalParallelism, shuffleDistribution)));

          builder.connectVertices(edgeToSampledVtx);
          break;
        case BroadCast:
          edgeToSampledVtx =
              new IREdge(CommunicationPatternProperty.Value.BroadCast, edgeToVtxToSample.getSrc(), sampledVtx);
          edgeToVtxToSample.copyExecutionPropertiesTo(edgeToSampledVtx);
          builder.connectVertices(edgeToSampledVtx);
          break;
        case OneToOne:
          edgeToSampledVtx =
            new IREdge(CommunicationPatternProperty.Value.OneToOne, edgeToVtxToSample.getSrc(), sampledVtx);
          edgeToVtxToSample.copyExecutionPropertiesTo(edgeToSampledVtx);

          if (DataStoreProperty.Value.MemoryStore.equals(
              edgeToVtxToSample.getPropertyValue(DataStoreProperty.class).get())) {
            builder.connectVertices(edgeToSampledVtx);
            appendSampledDag(originalParallelism, idxToSample, edgeToVtxToSample.getSrc(), builder, dag);
          } else {
            // Assign proper partition range to read for each sampled vertex.
            final HashMap<Integer, Integer> oneToOneDistribution = new HashMap<>();
            for (int i = 0; i < idxToSample.size(); i++) {
              oneToOneDistribution.put(i, idxToSample.get(i));
            }
            edgeToSampledVtx.setPropertyPermanently(
                OneToOneDistributionProperty.of(oneToOneDistribution));

            builder.connectVertices(edgeToSampledVtx);
          }
          break;
        default:
          throw new UnsupportedCommPatternException(new Throwable("Invalid communication pattern!"));
      }
    }

    return sampledVtx;
  }

  private OperatorVertex generateMetricAggregationVertex() {
    // Define a custom data aggregator for skew handling.
    // Here, the aggregator gathers key frequency data used in shuffle data repartitioning.
    final BiFunction<Object, Map<Object, Long>, Map<Object, Long>> dynOptDataAggregator =
      (BiFunction<Object, Map<Object, Long>, Map<Object, Long>> & Serializable)
        (element, aggregatedDynOptData) -> {
          final Object key = ((Pair<Object, Long>) element).left();
          final Long count = ((Pair<Object, Long>) element).right();

          final Map<Object, Long> aggregatedDynOptDataMap = (Map<Object, Long>) aggregatedDynOptData;
          if (aggregatedDynOptDataMap.containsKey(key)) {
            aggregatedDynOptDataMap.compute(key, (existingKey, accumulatedCount) -> accumulatedCount + count);
          } else {
            aggregatedDynOptDataMap.put(key, count);
          }
          return aggregatedDynOptData;
        };
    final AggregateMetricTransform abt =
      new AggregateMetricTransform<Pair<Object, Long>, Map<Object, Long>>(new HashMap<>(), dynOptDataAggregator);
    return new OperatorVertex(abt);
  }

  private OperatorVertex generateMetricCollectVertex(final IREdge edge, final OperatorVertex abv) {
    final KeyExtractor keyExtractor = edge.getPropertyValue(KeyExtractorProperty.class).get();
    final EncoderFactory encoderFactory = edge.getPropertyValue(EncoderProperty.class).get();
    edge.setPropertyPermanently(EncoderProperty.of(encoderFactory)); // Finalize

    // Define a custom data collector for skew handling.
    // Here, the collector gathers key frequency data used in shuffle data repartitioning.
    final BiFunction<Object, Map<Object, Object>, Map<Object, Object>> dynOptDataCollector =
      (BiFunction<Object, Map<Object, Object>, Map<Object, Object>> & Serializable)
        (element, dynOptData) -> {
          final Object key = keyExtractor.extractKey(element);
          if (dynOptData.containsKey(key)) {
            ((List<Object>) dynOptData.get(key)).add(element);
          } else {
            final List<Object> elementsPerKey = new ArrayList<>();
            elementsPerKey.add(element);
            dynOptData.put(key, elementsPerKey);
          }
          return dynOptData;
        };

    // Define a custom transform closer for skew handling.
    // Here, we emit key to frequency data map type data when closing transform.
    final BiFunction<Map<Object, Object>, OutputCollector, Map<Object, Object>> closer =
      (BiFunction<Map<Object, Object>, OutputCollector, Map<Object, Object>> & Serializable)
        (dynOptData, outputCollector)-> {
          try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            final EncoderFactory.Encoder encoder = encoderFactory.create(out);
            for (final Map.Entry<Object, Object> entry : dynOptData.entrySet()) {
              for (final Object element : ((List<Object>) entry.getValue())) {
                encoder.encode(element);
              }
              final Pair<Object, Object> pairData =
                Pair.of(entry.getKey(), new Long(out.size())); // Calculate actual size.
              outputCollector.emit(abv.getId(), pairData);
            }
            return dynOptData;
          } catch (final IOException e) {
            throw new RuntimeException(e);
          }
        };

    final MetricCollectTransform mct
      = new MetricCollectTransform(new HashMap<>(), dynOptDataCollector, closer);
    return new OperatorVertex(mct);
  }

  private IREdge generateEdgeToMCV(final IREdge edge,
                                   final IRVertex lastSampledVtx,
                                   final OperatorVertex mcv) {
    final IREdge newEdge =
      new IREdge(CommunicationPatternProperty.Value.OneToOne, lastSampledVtx, mcv);
    newEdge.setProperty(EncoderProperty.of(edge.getPropertyValue(EncoderProperty.class).get()));
    newEdge.setProperty(DecoderProperty.of(edge.getPropertyValue(DecoderProperty.class).get()));
    return newEdge;
  }

  private IREdge generateEdgeToABV(final IREdge edge,
                                   final OperatorVertex mcv,
                                   final OperatorVertex abv) {
    final IREdge newEdge = new IREdge(CommunicationPatternProperty.Value.Shuffle, mcv, abv);
    newEdge.setPropertyPermanently(DataStoreProperty.of(DataStoreProperty.Value.SerializedMemoryStore));
    newEdge.setPropertyPermanently(DataPersistenceProperty.of(DataPersistenceProperty.Value.Discard));
    newEdge.setPropertyPermanently(DataFlowProperty.of(DataFlowProperty.Value.Push));
    newEdge.setProperty(KeyExtractorProperty.of(edge.getPropertyValue(KeyExtractorProperty.class).get()));
    newEdge.setProperty(AdditionalOutputTagProperty.of("DynOptData"));

    // Dynamic optimization handles statistics on key-value data by default.
    // We need to get coders for encoding/decoding the keys to send data to
    // vertex with AggregateMetricTransform.
    if (edge.getPropertyValue(KeyEncoderProperty.class).isPresent()
      && edge.getPropertyValue(KeyDecoderProperty.class).isPresent()) {
      final EncoderFactory keyEncoderFactory = edge.getPropertyValue(KeyEncoderProperty.class).get();
      final DecoderFactory keyDecoderFactory = edge.getPropertyValue(KeyDecoderProperty.class).get();
      newEdge.setProperty(EncoderProperty.of(PairEncoderFactory.of(keyEncoderFactory, LongEncoderFactory.of())));
      newEdge.setProperty(DecoderProperty.of(PairDecoderFactory.of(keyDecoderFactory, LongDecoderFactory.of())));
    } else {
      // If not specified, follow encoder/decoder of the given shuffle edge.
      newEdge.setProperty(EncoderProperty.of(edge.getPropertyValue(EncoderProperty.class).get()));
      newEdge.setProperty(DecoderProperty.of(edge.getPropertyValue(DecoderProperty.class).get()));
    }

    return newEdge;
  }
}