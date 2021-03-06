/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nemo.compiler.optimizer.pass.compiletime.composite;

import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.DuplicateEdgeGroupSizePass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.reshaping.LoopExtractionPass;
import org.apache.nemo.compiler.optimizer.pass.compiletime.reshaping.LoopOptimizations;
import org.apache.nemo.compiler.optimizer.pass.compiletime.reshaping.LoopUnrollingPass;

import java.util.Arrays;

/**
 * A series of passes to perform LoopOptimization.
 */
public final class LoopOptimizationCompositePass extends CompositePass {
  /**
   * Default constructor.
   */
  public LoopOptimizationCompositePass() {
    super(Arrays.asList(
      new LoopExtractionPass(),
      LoopOptimizations.getLoopFusionPass(),
      LoopOptimizations.getLoopInvariantCodeMotionPass(),
      new LoopUnrollingPass(), // Groups then unrolls loops.
      new DuplicateEdgeGroupSizePass()
    ));
  }
}
