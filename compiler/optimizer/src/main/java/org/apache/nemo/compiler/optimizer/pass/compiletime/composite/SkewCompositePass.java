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
package org.apache.nemo.compiler.optimizer.pass.compiletime.composite;

import org.apache.nemo.compiler.optimizer.pass.compiletime.annotating.*;
import org.apache.nemo.compiler.optimizer.pass.compiletime.reshaping.SkewReshapingPass;

import java.util.Arrays;

/**
 * Pass to modify the DAG for a job to perform data skew.
 */
public final class SkewCompositePass extends CompositePass {
  /**
   * Default constructor.
   */
  public SkewCompositePass() {
    super(Arrays.asList(
      new SkewReshapingPass(),
      new SkewResourceSkewedDataPass(),
      new SkewPartitionerPass()
    ));
  }
}
