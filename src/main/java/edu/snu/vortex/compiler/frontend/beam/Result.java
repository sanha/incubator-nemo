/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.vortex.compiler.frontend.beam;

import org.apache.beam.sdk.AggregatorRetrievalException;
import org.apache.beam.sdk.AggregatorValues;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.transforms.Aggregator;
import org.joda.time.Duration;

import java.io.IOException;

/**
 * Beam result.
 * TODO #32: Implement Beam Result
 */
public final class Result implements PipelineResult {
  @Override
  public State getState() {
    throw new UnsupportedOperationException("getState() in frontend.beam.Result");
  }

  @Override
  public State cancel() throws IOException {
    throw new UnsupportedOperationException("cancel() in frontend.beam.Result");
  }

  @Override
  public State waitUntilFinish(final Duration duration) {
    throw new UnsupportedOperationException("waitUntilFinish() in frontend.beam.Result");
  }

  @Override
  public State waitUntilFinish() {
    throw new UnsupportedOperationException("waitUntilFinish() in frontend.beam.Result");
  }

  @Override
  public <T> AggregatorValues<T> getAggregatorValues(final Aggregator<?, T> aggregator)
      throws AggregatorRetrievalException {
    throw new UnsupportedOperationException("getAggregatorValues() in frontend.beam.Result");
  }

  @Override
  public MetricResults metrics() {
    throw new UnsupportedOperationException("metrics() in frontend.beam.Result");
  }
}