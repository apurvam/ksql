/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.parser.tree;

import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;


import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.UdafAggregator;

public class SlidingWindowExpression extends KsqlWindowExpression {

  private final long size;
  private final TimeUnit sizeUnit;

  public SlidingWindowExpression(long size, TimeUnit sizeUnit) {
    this(Optional.empty(), size, sizeUnit);
  }

  private SlidingWindowExpression(Optional<NodeLocation> location, long size,
                                   TimeUnit sizeUnit) {
    super(location);
    this.size = size;
    this.sizeUnit = sizeUnit;
  }

  public JoinWindows joinWindow() {
    return JoinWindows.of(sizeUnit.toMillis(size));
  }

  @Override
  public String toString() {
    return " SLIDING ( SIZE " + size + " " + sizeUnit + " ) ";
  }

  @Override
  public int hashCode() {
    return Objects.hash(size, sizeUnit);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SlidingWindowExpression slidingWindowExpression = (SlidingWindowExpression) o;
    return slidingWindowExpression.size == size && slidingWindowExpression.sizeUnit == sizeUnit;
  }

  @SuppressWarnings("unchecked")
  @Override
  public KTable applyAggregate(final KGroupedStream groupedStream,
                               final Initializer initializer,
                               final UdafAggregator aggregator,
                               final Materialized<String, GenericRow, ?> materialized) {
    throw new UnsupportedOperationException("Sliding windows only apply to stream-stream joins");
  }


}
