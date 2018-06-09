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

package io.confluent.ksql.planner.plan;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.streams.StreamsBuilder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.parser.tree.SlidingWindowExpression;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.serde.KsqlTopicSerDe;
import io.confluent.ksql.structured.SchemaKStream;
import io.confluent.ksql.structured.SchemaKTable;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;

import static io.confluent.ksql.planner.plan.JoinNode.JoinType.INNER;

public class JoinNode extends PlanNode {


  public enum JoinType {
    INNER, LEFT, OUTER
  }

  private final JoinType joinType;
  private final PlanNode left;
  private final PlanNode right;
  private final Schema schema;
  private final String leftKeyFieldName;
  private final String rightKeyFieldName;

  private final String leftAlias;
  private final String rightAlias;
  private final Field keyField;
  private final SlidingWindowExpression slidingWindowExpression;
  private final DataSource.DataSourceType leftType;
  private final DataSource.DataSourceType rightType;

  public JoinNode(@JsonProperty("id") final PlanNodeId id,
                  @JsonProperty("type") final JoinType joinType,
                  @JsonProperty("left") final PlanNode left,
                  @JsonProperty("right") final PlanNode right,
                  @JsonProperty("leftKeyFieldName") final String leftKeyFieldName,
                  @JsonProperty("rightKeyFieldName") final String rightKeyFieldName,
                  @JsonProperty("leftAlias") final String leftAlias,
                  @JsonProperty("rightAlias") final String rightAlias,
                  @JsonProperty("slidingWindow") final SlidingWindowExpression windowExpression,
                  @JsonProperty("leftType") final DataSource.DataSourceType leftType,
                  @JsonProperty("rightType") final DataSource.DataSourceType rightType) {

    // TODO: Type should be derived.
    super(id);
    this.joinType = joinType;
    this.left = left;
    this.right = right;
    this.leftKeyFieldName = leftKeyFieldName;
    this.rightKeyFieldName = rightKeyFieldName;
    this.leftAlias = leftAlias;
    this.rightAlias = rightAlias;
    this.schema = buildSchema(left, right);
    this.keyField = this.schema.field((leftAlias + "." + leftKeyFieldName));
    this.slidingWindowExpression = windowExpression;
    this.leftType = leftType;
    this.rightType = rightType;
  }

  private Schema buildSchema(final PlanNode left, final PlanNode right) {

    Schema leftSchema = left.getSchema();
    Schema rightSchema = right.getSchema();

    SchemaBuilder schemaBuilder = SchemaBuilder.struct();

    for (Field field : leftSchema.fields()) {
      String fieldName = leftAlias + "." + field.name();
      schemaBuilder.field(fieldName, field.schema());
    }

    for (Field field : rightSchema.fields()) {
      String fieldName = rightAlias + "." + field.name();
      schemaBuilder.field(fieldName, field.schema());
    }
    return schemaBuilder.build();
  }

  @Override
  public Schema getSchema() {
    return this.schema;
  }

  @Override
  public Field getKeyField() {
    return this.keyField;
  }

  @Override
  public List<PlanNode> getSources() {
    return Arrays.asList(left, right);
  }

  @Override
  public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
    return visitor.visitJoin(this, context);
  }

  public PlanNode getLeft() {
    return left;
  }

  public PlanNode getRight() {
    return right;
  }

  public String getLeftKeyFieldName() {
    return leftKeyFieldName;
  }

  public String getRightKeyFieldName() {
    return rightKeyFieldName;
  }

  public String getLeftAlias() {
    return leftAlias;
  }

  public String getRightAlias() {
    return rightAlias;
  }

  public JoinType getJoinType() {
    return joinType;
  }

  public boolean isLeftJoin() {
    return joinType == JoinType.LEFT;
  }

  @Override
  public SchemaKStream buildStream(final StreamsBuilder builder,
                                   final KsqlConfig ksqlConfig,
                                   final KafkaTopicClient kafkaTopicClient,
                                   final FunctionRegistry functionRegistry,
                                   final Map<String, Object> props,
                                   final SchemaRegistryClient schemaRegistryClient) {

    final KsqlTopicSerDe joinSerDe = getResultTopicSerde(this);

    validateJoinType();
    validatePartitionCountsMatch(kafkaTopicClient);

    if (leftType.isStream() && rightType.isStream()) {
      final SchemaKStream left = getLeft().buildStream(builder,
                                                       ksqlConfig,
                                                       kafkaTopicClient,
                                                       functionRegistry,
                                                       props, schemaRegistryClient);

      final SchemaKStream right = getRight().buildStream(builder,
                                                         ksqlConfig,
                                                         kafkaTopicClient,
                                                         functionRegistry,
                                                         props, schemaRegistryClient);

      if (slidingWindowExpression == null) {
        throw new KsqlException("A stream-stream join must be windowed.");
      }
      final SchemaKStream leftStream = streamPartitionedByKey(left, getLeftKeyFieldName());
      final SchemaKStream rightStream = streamPartitionedByKey(right, getRightKeyFieldName());
      return leftStream.leftJoin(rightStream,
                                 getSchema(),
                                 getSchema().field(getLeftAlias()
                                                   + "."
                                                   + leftStream.getKeyField().name()),
                                 slidingWindowExpression.joinWindow(),
                                 getSerDeForSource(getLeft()),
                                 getSerDeForSource(getRight()),
                                 ksqlConfig);
    }
    final SchemaKTable table = tableForJoin(builder,
                                            ksqlConfig,
                                            kafkaTopicClient,
                                            functionRegistry,
                                            props, schemaRegistryClient);

    final SchemaKStream stream = streamPartitionedByKey(
        getLeft().buildStream(builder,
                              ksqlConfig,
                              kafkaTopicClient,
                              functionRegistry,
                              props,
                              schemaRegistryClient),
        getLeftKeyFieldName());


    return stream.leftJoin(table,
        getSchema(),
        getSchema().field(getLeftAlias() + "." + stream.getKeyField().name()),
        joinSerDe, ksqlConfig);

  }

  // package private for test
  SchemaKTable tableForJoin(
      final StreamsBuilder builder,
      final KsqlConfig ksqlConfig,
      final KafkaTopicClient kafkaTopicClient,
      final FunctionRegistry functionRegistry,
      final Map<String, Object> props,
      final SchemaRegistryClient schemaRegistryClient) {

    Map<String, Object> joinTableProps = new HashMap<>(props);
    joinTableProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    final SchemaKStream schemaKStream = right.buildStream(
        builder,
        ksqlConfig,
        kafkaTopicClient,
        functionRegistry,
        joinTableProps, schemaRegistryClient);
    if (!(schemaKStream instanceof SchemaKTable)) {
      throw new KsqlException("Unsupported Join. Only stream-table joins are supported, but was "
          + getLeft() + "-" + getRight());
    }

    return (SchemaKTable) schemaKStream;
  }


  private KsqlTopicSerDe getSerDeForSource(final PlanNode node) {
    if (!(node instanceof StructuredDataSourceNode)) {
      throw new KsqlException("The source for Join must be a primitive data source (Stream or "
                              + "Table)");
    }
    StructuredDataSourceNode dataSourceNode = (StructuredDataSourceNode) node;
    return dataSourceNode
        .getStructuredDataSource()
        .getKsqlTopic()
        .getKsqlTopicSerDe();
  }

  private KsqlTopicSerDe getResultTopicSerde(final PlanNode node) {
    if (node instanceof StructuredDataSourceNode) {
      StructuredDataSourceNode structuredDataSourceNode = (StructuredDataSourceNode) node;
      return structuredDataSourceNode.getStructuredDataSource().getKsqlTopic().getKsqlTopicSerDe();
    } else if (node instanceof JoinNode) {
      JoinNode joinNode = (JoinNode) node;

      return getResultTopicSerde(joinNode.getLeft());
    } else {
      return getResultTopicSerde(node.getSources().get(0));
    }
  }

  private void validateJoinType() {
    if (!(joinType == JoinType.LEFT || joinType == JoinType.OUTER || joinType == INNER)) {
      throw new KsqlException("Only LEFT, INNER, or OUTER joins are supported at this time");
    }
  }

  private void validatePartitionCountsMatch(KafkaTopicClient kafkaTopicClient) {
    int leftPartitions = left.getPartitions(kafkaTopicClient);
    int rightPartitions = right.getPartitions(kafkaTopicClient);
    if (leftPartitions != rightPartitions) {
      throw new KsqlException("Can't join " + leftType.getKqlType() + " with "
                              + rightType.getKqlType() + " since the number of partitions don't "
                              + "match. " + leftType.getKqlType() + " partitions = "
                              + leftPartitions + "; " + rightType.getKqlType() + " partitions = "
                              + rightPartitions);
    }
  }

  @Override
  protected int getPartitions(KafkaTopicClient kafkaTopicClient) {
    return right.getPartitions(kafkaTopicClient);
  }

  private SchemaKStream streamPartitionedByKey(final SchemaKStream stream,
                                               final String keyFieldName) {
    final Field field =
        SchemaUtil.getFieldByName(stream.getSchema(), keyFieldName).orElseThrow(() ->
          new KsqlException("couldn't find key field: "
                            + keyFieldName
                            + " in schema:"
                            + schema)
        );
    return stream.selectKey(field, true);
  }
}
