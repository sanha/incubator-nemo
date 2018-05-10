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
package edu.snu.nemo.compiler.frontend.spark.transform;

import edu.snu.nemo.common.ir.OutputCollector;
import edu.snu.nemo.common.ir.vertex.transform.Transform;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Transforms for co-group operation.
 */
public final class CoGroup {

  private CoGroup() {
    // Utility class.
  }

  public static <K, V> PairFunction<Tuple2<K, V>, K, Union> mapFunction(final int index) {
    return new MapToUnionFunction(index);
  }

  public static <K, V, W> Transform<Tuple2<K, Union>, Tuple2<K, Tuple2<Iterable<V>, Iterable<W>>>> coGroupTransform() {
    return new CoGroupTransform<>();
  }

  /**
   * Union class for co-group operation.
   */
  public static final class Union<T extends Serializable> implements Serializable {

    private final int index;
    private final T value;

    private Union(final int index,
                  final T value) {
      this.index = index;
      this.value = value;
    }

    private int getIndex() {
      return index;
    }

    private Object getValue() {
      return value;
    }
  }

  /**
   * Map function for group by key transformation.
   *
   * @param <K> key type.
   * @param <V> value type.
   */
  public static final
  class MapToUnionFunction<K, V extends Serializable> implements PairFunction<Tuple2<K, V>, K, Union> {
    private final int index;

    /**
     * Constructor.
     *
     * @param index the index in union to map.
     */
    private MapToUnionFunction(final int index) {
      this.index = index;
    }


    @Override
    public Tuple2<K, Union> call(final Tuple2<K, V> element) throws Exception {
      return new Tuple2<>(element._1, new Union<>(index, element._2));
    }
  }

  /**
   * Co-group transformation.
   *
   * @param <K> key type.
   * @param <V> value type 1.
   * @param <W> value type 2.
   */
  public static final class CoGroupTransform<K, V, W>
      implements Transform<Tuple2<K, Union>, Tuple2<K, Tuple2<Iterable<V>, Iterable<W>>>> {
    private final Map<K, Tuple2<List<V>, List<W>>> keyToValues;
    private OutputCollector<Tuple2<K, Tuple2<Iterable<V>, Iterable<W>>>> outputCollector;

    /**
     * Constructor.
     */
    private CoGroupTransform() {
      this.keyToValues = new HashMap<>();
    }

    @Override
    public void prepare(final Context context,
                        final OutputCollector<Tuple2<K, Tuple2<Iterable<V>, Iterable<W>>>> oc) {
      this.outputCollector = oc;
    }

    @Override
    public void onData(final Tuple2<K, Union> element) {
      final K key = element._1;
      final Union union = element._2;

      keyToValues.putIfAbsent(key, new Tuple2<>(new ArrayList<>(), new ArrayList<>()));
      switch (union.getIndex()) {
        case 0:
          keyToValues.get(key)._1().add((V) union.getValue());
          break;
        case 1:
          keyToValues.get(key)._2().add((W) union.getValue());
          break;
        default:
          throw new RuntimeException("Unexpected union index!");
      }
    }

    @Override
    public void close() {
      keyToValues.forEach((k, t) -> outputCollector.emit(new Tuple2<>(k, new Tuple2<>(t._1, t._2))));
      keyToValues.clear();
    }
  }
}
