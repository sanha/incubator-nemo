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
package edu.snu.nemo.compiler.optimizer.pass.compiletime.sailfishskew;


import edu.snu.nemo.compiler.optimizer.pass.compiletime.annotating.*;
import edu.snu.nemo.compiler.optimizer.pass.compiletime.composite.CompositePass;

import java.util.Arrays;

/**
 * A series of passes to support Sailfish-like disk seek batching during shuffle.
 */
public final class SailfishPassForSkew extends CompositePass {
  /**
   * Default constructor.
   */
  public SailfishPassForSkew() {
    super(Arrays.asList(
        new DeprecatedSailfishSkewRelayReshapingPass(),
        new SailfishEdgeDataFlowModelPass(),
        new SailfishEdgeDataStorePass(),
        //new SailfishSkewEdgeDecoderPass(),
        //new SailfishEdgeEncoderPass(),
        new SailfishEdgeUsedDataHandlingPass(),
        new SailfishVertexExecutorSlotCompliancePass(),
        new SailfishVertexIntermediateLocationAwareSchedulingPass()
    ));
  }
}
