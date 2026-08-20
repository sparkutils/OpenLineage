/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.ColumnLineageConfig;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.AppendColumns;
import org.apache.spark.sql.catalyst.plans.logical.DeserializeToObject;
import org.apache.spark.sql.catalyst.plans.logical.FlatMapGroupsWithState;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.MapElements;
import org.apache.spark.sql.catalyst.plans.logical.MapGroups;
import org.apache.spark.sql.catalyst.plans.logical.MapPartitions;
import org.apache.spark.sql.catalyst.plans.logical.SerializeFromObject;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.ObjectType;

/**
 * <p>Emits a pessimistic fan-in across a typed Dataset boundary (the {@link DeserializeToObject} /
 * {@link SerializeFromObject} pair produced by {@code Dataset.map}, {@code mapPartitions},
 * {@code flatMap}, {@code groupByKey().mapGroups} and {@code groupByKey().mapGroupsWithState} /
 * {@code flatMapGroupsWithState} / {@code transformWithState}): every output attribute is linked to
 * every input attribute the deserializers read, as {@link TransformationInfo.Types#INDIRECT} /
 * {@link TransformationInfo.Subtypes#TRANSFORMATION}.
 *
 * <p>Per-field matching is not recoverable (the lambda body is bytecode), so the fan-in is gated
 * behind {@link ColumnLineageConfig#getTypedBoundaryFanInEnabled()} and ships dark. Above {@link
 * ColumnLineageConfig#getTypedBoundaryFanInMaxEdges()} it emits nothing rather than a truncated
 * fan-in, which would push the whole facet past the builder's returned-input-field limit.
 */
@Slf4j
public class TypedBoundaryFanInVisitor implements OperatorVisitor {

  private static final boolean DEFAULT_ENABLED = false;

  private static final int DEFAULT_MAX_EDGES = 10_000;

  @Override
  public boolean isDefinedAt(LogicalPlan operator) {
    return operator instanceof SerializeFromObject;
  }

  @Override
  public void apply(LogicalPlan operator, ColumnLevelLineageBuilder builder) {
    ColumnLineageConfig config = columnLineageConfig(builder);
    if (!isEnabled(config)) {
      return;
    }

    TypedBoundary boundary = describeBoundary((SerializeFromObject) operator);
    if (boundary.inputExprIds.isEmpty() || boundary.outputExprIds.isEmpty()) {
      return;
    }

    long edges = (long) boundary.outputExprIds.size() * (long) boundary.inputExprIds.size();
    int maxEdges = maxEdges(config);
    if (edges > maxEdges) {
      log.debug(
          "Typed boundary fan-in skipped for {}: {} output x {} input fields would emit {} "
              + "edges, above the configured limit of {}. Emitting nothing rather than a fan-in "
              + "that would push the column lineage facet past its own returned-input-field limit.",
          boundary.description(),
          boundary.outputExprIds.size(),
          boundary.inputExprIds.size(),
          edges,
          maxEdges);
      return;
    }

    TransformationInfo transformationInfo =
        new TransformationInfo(
            TransformationInfo.Types.INDIRECT,
            TransformationInfo.Subtypes.TRANSFORMATION,
            boundary.description(),
            false);

    for (ExprId output : boundary.outputExprIds) {
      for (ExprId input : boundary.inputExprIds) {
        if (!output.equals(input)) {
          builder.addDependency(output, input, transformationInfo);
        }
      }
    }
  }

  private static TypedBoundary describeBoundary(SerializeFromObject serialize) {
    TypedBoundary boundary = new TypedBoundary();
    ScalaConversionUtils.<Attribute>fromSeq(serialize.output())
        .forEach(attr -> boundary.outputExprIds.add(attr.exprId()));
    collectInputs(serialize.child(), boundary);
    return boundary;
  }

  private static void collectInputs(LogicalPlan node, TypedBoundary boundary) {
    if (node == null) {
      return;
    }

    if (node instanceof DeserializeToObject) {
      addReferences(((DeserializeToObject) node).deserializer(), boundary);
      return;
    }

    if (node instanceof AppendColumns) {
      boundary.opaqueOperators.add(node.getClass().getSimpleName());
      addReferences(((AppendColumns) node).deserializer(), boundary);
      return;
    }

    if (node instanceof MapGroups) {
      MapGroups mapGroups = (MapGroups) node;
      addGroupingOperator(
          node,
          mapGroups.keyDeserializer(),
          mapGroups.valueDeserializer(),
          mapGroups.outputObjAttr(),
          boundary);
    } else if (node instanceof FlatMapGroupsWithState) {
      FlatMapGroupsWithState flatMapGroupsWithState = (FlatMapGroupsWithState) node;
      addGroupingOperator(
          node,
          flatMapGroupsWithState.keyDeserializer(),
          flatMapGroupsWithState.valueDeserializer(),
          flatMapGroupsWithState.outputObjAttr(),
          boundary);
    } else if (TransformWithStateReflector.isTransformWithState(node)) {
      // TransformWithState (Spark 4+, absent on 3.5) — the same grouping shape as MapGroups /
      // FlatMapGroupsWithState, read reflectively. The left child is AppendColumns (groupByKey);
      // the right (initialState) is a dummy empty LocalRelation when hasInitialState is false and
      // is not a real input. The bottom descent reaches both children; the AppendColumns branch
      // collects the left side's references, and the empty right side yields nothing.
      Expression keyDeser =
          TransformWithStateReflector.keyDeserializer(node).orElse(null);
      Expression valueDeser =
          TransformWithStateReflector.valueDeserializer(node).orElse(null);
      org.apache.spark.sql.catalyst.expressions.Attribute outAttr =
          TransformWithStateReflector.outputObjAttr(node).orElse(null);
      addGroupingOperator(node, keyDeser, valueDeser, outAttr, boundary);
    } else if (node instanceof MapElements) {
      MapElements mapElements = (MapElements) node;
      String description =
          describeTypedOperator(
              node,
              argumentTypeName(mapElements.argumentClass()),
              expressionTypeName(mapElements.outputObjAttr()));
      if (isSameType(mapElements.argumentClass(), mapElements.outputObjAttr())) {
        description += ", sameType";
      }
      boundary.opaqueOperators.add(description);
    } else if (node instanceof MapPartitions) {
      MapPartitions mapPartitions = (MapPartitions) node;
      boundary.opaqueOperators.add(
          describeTypedOperator(node, null, expressionTypeName(mapPartitions.outputObjAttr())));
    }
    // Any other node — an untyped Filter/Project sitting between the boundary and the typed node,
    // or an AppendColumnsWithObject below a grouping — is a pass-through for lineage: descend
    // through it rather than stopping, so the DeserializeToObject below is still reached and its
    // deserializer's references collected. Stopping here is what silently silenced the fan-in for
    // ds.map(...).filter(...) and ds.filter(...).map(...).

    ScalaConversionUtils.<LogicalPlan>fromSeq(node.children())
        .forEach(child -> collectInputs(child, boundary));
  }

  private static void addReferences(Expression expression, TypedBoundary boundary) {
    if (expression == null) {
      return;
    }
    ScalaConversionUtils.<Attribute>fromSeq(expression.references().toSeq())
        .forEach(attr -> boundary.inputExprIds.add(attr.exprId()));
  }

  /** Renders an opaque operator as {@code Name(<argument> -> <output>)}, dropping either side the
   * node does not carry. */
  private static String describeTypedOperator(
      LogicalPlan node, String argumentType, String outputType) {
    String name = node.getClass().getSimpleName();
    if (argumentType == null && outputType == null) {
      return name;
    }
    return name
        + "("
        + (argumentType == null ? "" : argumentType + " ")
        + "-> "
        + (outputType == null ? "?" : outputType)
        + ")";
  }

  private static String argumentTypeName(Class<?> argumentClass) {
    return argumentClass == null ? null : argumentClass.getSimpleName();
  }

  /**
   * Whether a {@link MapElements} hop preserves the element type ({@code argumentClass()} equals the
   * output object's class). This is the one structural signal that is always correct: it is a plain
   * class comparison, not a field mapping, so it cannot be wrong the way a name-matched pairing can.
   * It tells a consumer whether the hop is a field-level transform (same type in and out) or a
   * reshape/type change, without claiming which field became which.
   */
  private static boolean isSameType(Class<?> argumentClass, Attribute outputObjAttr) {
    if (argumentClass == null || outputObjAttr == null) {
      return false;
    }
    DataType dataType = outputObjAttr.dataType();
    if (!(dataType instanceof ObjectType)) {
      return false;
    }
    Class<?> outputClass = ((ObjectType) dataType).cls();
    return outputClass != null && argumentClass.equals(outputClass);
  }

  /**
   * Records a grouping operator ({@link MapGroups}, {@link FlatMapGroupsWithState}, or
   * {@code TransformWithState}) in the boundary: names it as {@code Name((K, V) -> B)} and reads
   * the key/value deserializers' references as inputs.
   */
  private static void addGroupingOperator(
      LogicalPlan node,
      Expression keyDeserializer,
      Expression valueDeserializer,
      Attribute outputObjAttr,
      TypedBoundary boundary) {
    boundary.opaqueOperators.add(
        describeTypedOperator(
            node,
            keyValueTypeName(keyDeserializer, valueDeserializer),
            expressionTypeName(outputObjAttr)));
    addReferences(keyDeserializer, boundary);
    addReferences(valueDeserializer, boundary);
  }

  /**
   * Renders the key and value classes of a grouping operator as {@code (K, V)}. The classes are
   * recovered from the resolved deserializers' {@code dataType()} (an {@link ObjectType} for a
   * bean/case class, a primitive type otherwise), not from the lambda's bytecode. A side that
   * cannot be read renders as {@code ?}; when neither side is readable the whole pair is dropped.
   */
  private static String keyValueTypeName(Expression keyDeserializer, Expression valueDeserializer) {
    String key = expressionTypeName(keyDeserializer);
    String value = expressionTypeName(valueDeserializer);
    if (key == null && value == null) {
      return null;
    }
    return "(" + (key == null ? "?" : key) + ", " + (value == null ? "?" : value) + ")";
  }

  /**
   * Renders the type an expression produces as a simple name: the class for an {@link ObjectType}
   * (a bean/case class), the {@code simpleString()} otherwise. Returns {@code null} when the
   * expression is absent or its type is not inspectable (an unresolved deserializer throws on
   * {@code dataType()}), so a side that cannot be read is dropped rather than failing the boundary.
   */
  private static String expressionTypeName(Expression expression) {
    if (expression == null) {
      return null;
    }
    try {
      DataType dataType = expression.dataType();
      if (dataType instanceof ObjectType) {
        Class<?> cls = ((ObjectType) dataType).cls();
        return cls == null ? null : cls.getSimpleName();
      }
      return dataType == null ? null : dataType.simpleString();
    } catch (RuntimeException e) {
      return null;
    }
  }

  private static ColumnLineageConfig columnLineageConfig(ColumnLevelLineageBuilder builder) {
    try {
      return Optional.ofNullable(builder.getContext())
          .map(OpenLineageContext::getOpenLineageConfig)
          .map(SparkOpenLineageConfig::getColumnLineageConfig)
          .orElse(null);
    } catch (RuntimeException e) {
      log.debug("Could not read column lineage configuration", e);
      return null;
    }
  }

  private static boolean isEnabled(ColumnLineageConfig config) {
    return Optional.ofNullable(config)
        .map(ColumnLineageConfig::getTypedBoundaryFanInEnabled)
        .orElse(DEFAULT_ENABLED);
  }

  private static int maxEdges(ColumnLineageConfig config) {
    return Optional.ofNullable(config)
        .map(ColumnLineageConfig::getTypedBoundaryFanInMaxEdges)
        .orElse(DEFAULT_MAX_EDGES);
  }

  private static final class TypedBoundary {
    private final Set<ExprId> outputExprIds = new LinkedHashSet<>();
    private final Set<ExprId> inputExprIds = new LinkedHashSet<>();
    private final List<String> opaqueOperators = new ArrayList<>();

    private String description() {
      return opaqueOperators.isEmpty()
          ? SerializeFromObject.class.getSimpleName()
          : opaqueOperators.stream().distinct().collect(Collectors.joining(", "));
    }
  }
}