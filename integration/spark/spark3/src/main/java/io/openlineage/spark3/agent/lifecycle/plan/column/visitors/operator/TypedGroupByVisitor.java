/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import static io.openlineage.client.utils.TransformationInfo.Subtypes.GROUP_BY;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionTraverser;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.AppendColumns;
import org.apache.spark.sql.catalyst.plans.logical.AppendColumnsWithObject;
import org.apache.spark.sql.catalyst.plans.logical.DeserializeToObject;
import org.apache.spark.sql.catalyst.plans.logical.FlatMapGroupsWithState;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.MapGroups;
import scala.collection.immutable.Seq;

/**
 * Records the {@code INDIRECT/GROUP_BY} dependency of a <b>typed</b> grouping, i.e. {@code
 * Dataset.groupByKey(...).mapGroups/flatMapGroups/reduceGroups}, which produces a {@link MapGroups}
 * over an {@link AppendColumns}, and {@code groupByKey(...).mapGroupsWithState/flatMapGroupsWithState},
 * which produces a {@link FlatMapGroupsWithState} over an {@link AppendColumns}, and
 * {@code groupByKey(...).transformWithState}, which produces a {@code TransformWithState} (Spark 4+
 * arbitrary state API v2) over an {@link AppendColumns}. Without this the untyped
 *
 * <pre>{@code df.groupBy("dept").agg(...)}</pre>
 *
 * records a grouping (see {@link AggregateVisitor}) while its typed equivalent
 *
 * <pre>{@code ds.groupByKey(p -> p.getDept()).mapGroups(...)}</pre>
 *
 * records none, so a typed grouping is invisible to anything consuming the facet.
 *
 * <p>The {@code TransformWithState} node is absent on the 3.5.0 compile classpath, so it is matched
 * reflectively via {@link TransformWithStateReflector} (the same {@code safeIsInstanceOf} pattern
 * used for {@code CreateHiveTableAsSelectCommand}); the AppendColumns resolution is unchanged
 * because {@code groupByKey} wraps the child in {@link AppendColumns} identically.
 *
 * <h2>Why the grouping key needs resolving through AppendColumns</h2>
 *
 * <p>{@link MapGroups#groupingAttributes()} does not name a column of the source. It names the
 * <b>synthetic</b> key column that {@link AppendColumns} appended, measured as {@code [value#10]} on
 * Spark 3.5.0 / 4.0.x / 4.1.0. That attribute is not usable on its own: the expression that computes
 * it, {@code AppendColumns.serializer()}, is rooted at {@code input[0, <KeyClass>, true]} and so has
 * {@code references() == []} — the serialize side of a typed boundary carries names and types only,
 * never attribute linkage. Traversing the grouping attribute directly would therefore emit an edge
 * against {@code value#10}, which nothing links to the relation below, and the facet would stay
 * empty.
 *
 * <p>The linkage lives on the other side of the same node: {@code AppendColumns.deserializer()} is a
 * {@code createexternalrow(...)} over real {@link
 * org.apache.spark.sql.catalyst.expressions.AttributeReference}s into the child. So this visitor
 * matches each grouping attribute back to the {@link AppendColumns} that produced it, <em>by
 * exprId</em>, and traverses that node's deserializer. Matching on exprId rather than on position or
 * name is what makes an unbounded descent safe: a nested typed grouping lower in the plan has
 * different exprIds and cannot be picked up by mistake.
 *
 * <h2>Why this is emitted unconditionally, unlike {@link TypedBoundaryFanInVisitor}</h2>
 *
 * <p>The fan-in across a typed boundary is <em>pessimistic</em>: it claims every output may derive
 * from every input because the map lambda is opaque, and it ships behind {@code
 * typedBoundaryFanInEnabled} for that reason. This edge is not pessimistic. That a grouping happened
 * and that it consumed the columns the key deserializer materialised are both structurally provable
 * from the plan, exactly as they are for {@link AggregateVisitor} on the untyped path. Gating a
 * faithful signal behind the flag that exists to contain an unfaithful one would mean a typed
 * grouping stays invisible for everyone who does not opt into an unrelated pessimistic behaviour.
 *
 * <h2>Known limitation: the breadth is the deserialized schema, not the key lambda's reads</h2>
 *
 * <p>The key function is bytecode and is not in the plan, and the deserializer materialises the
 * whole object before it runs. So for {@code groupByKey(row -> row.getInt(0) % 2)} over {@code (a,
 * b, s)} the edge set is all three columns, not just {@code a} — measured on Spark 4.0.0. This is
 * the same over-breadth {@link TypedFilterVisitor} documents: over-broad, but never a false
 * <em>specific</em> pairing, and narrowing it would require bytecode analysis of the lambda.
 *
 * <p>{@code CoGroup} (from {@code cogroup}) is deliberately not handled here; it is a different node
 * with two grouping sides and no {@link AppendColumns} of the shape this visitor resolves.
 */
public class TypedGroupByVisitor implements OperatorVisitor {

  @Override
  public boolean isDefinedAt(LogicalPlan operator) {
    return operator instanceof MapGroups
        || operator instanceof FlatMapGroupsWithState
        || TransformWithStateReflector.isTransformWithState(operator);
  }

  @Override
  public void apply(LogicalPlan operator, ColumnLevelLineageBuilder builder) {
    Set<Expression> groupingSources = groupingSources(operator);
    if (groupingSources.isEmpty()) {
      return;
    }

    ExprId exprId = NamedExpression.newExprId();
    builder.addDatasetDependency(exprId);

    groupingSources.forEach(
        expression ->
            ExpressionTraverser.of(
                    expression, exprId, TransformationInfo.indirect(GROUP_BY), builder)
                .traverse());
  }

  /**
   * Resolves the grouping attributes to expressions that actually reference the relation below. For
   * each grouping attribute this is the deserializer of the {@link AppendColumns} that computed it;
   * when the grouping key was appended by an {@link AppendColumnsWithObject} (a preceding typed map
   * made the intermediate object-typed, so the append node is the object variant) the deserializer
   * of the {@link DeserializeToObject} below is used, because the value deserializer references the
   * intermediate fields re-serialized from the opaque object (the {@code _1.._n} of a tuple /
   * case-class encoder) rather than the real columns. A grouping attribute not produced by either
   * is used as-is, which is the correct handling for a grouping on a real column.
   */
  private static Set<Expression> groupingSources(LogicalPlan operator) {
    Set<Expression> sources = new LinkedHashSet<>();
    Seq<Attribute> groupingAttrs = groupingAttributes(operator);
    if (groupingAttrs == null) {
      return sources;
    }
    ScalaConversionUtils.<Attribute>fromSeq(groupingAttrs)
        .forEach(
            attribute -> {
              Optional<AppendColumns> appendColumns =
                  appendColumnsProducing(child(operator), attribute.exprId());
              if (appendColumns.isPresent()) {
                sources.add(appendColumns.get().deserializer());
              } else {
                Optional<AppendColumnsWithObject> appendColumnsWithObject =
                    appendColumnsWithObjectProducing(child(operator), attribute.exprId());
                if (appendColumnsWithObject.isPresent()) {
                  sources.add(
                      DeserializeToObjectUtils.deserializerBelow(appendColumnsWithObject.get())
                          .orElseGet(() -> valueDeserializer(operator)));
                } else {
                  sources.add(attribute);
                }
              }
            });
    return sources;
  }

  private static Seq<Attribute> groupingAttributes(LogicalPlan operator) {
    if (operator instanceof MapGroups) {
      return ((MapGroups) operator).groupingAttributes();
    }
    if (operator instanceof FlatMapGroupsWithState) {
      return ((FlatMapGroupsWithState) operator).groupingAttributes();
    }
    // TransformWithState (Spark 4+, absent on 3.5) — read reflectively.
    return TransformWithStateReflector.groupingAttributes(operator).orElse(null);
  }

  private static LogicalPlan child(LogicalPlan operator) {
    if (operator instanceof MapGroups) {
      return ((MapGroups) operator).child();
    }
    if (operator instanceof FlatMapGroupsWithState) {
      return ((FlatMapGroupsWithState) operator).child();
    }
    // TransformWithState — the left side (the data, wrapped in AppendColumns by groupByKey).
    // The right side is a dummy empty LocalRelation when hasInitialState is false and is NOT
    // a real input; the grouping's real input is the left child.
    return TransformWithStateReflector.child(operator).orElse(null);
  }

  /** The value deserializer of a grouping operator. See {@link #groupingSources} for the
   * role of the value deserializer when the intermediate was made object-typed by a preceding
   * typed map.
   */
  private static Expression valueDeserializer(LogicalPlan operator) {
    if (operator instanceof MapGroups) {
      return ((MapGroups) operator).valueDeserializer();
    }
    if (operator instanceof FlatMapGroupsWithState) {
      return ((FlatMapGroupsWithState) operator).valueDeserializer();
    }
    // TransformWithState — read reflectively; null when not readable.
    return TransformWithStateReflector.valueDeserializer(operator).orElse(null);
  }

  /**
   * Finds the {@link AppendColumnsWithObject} in the subtree whose {@code newColumnsSerializer}
   * include {@code exprId}. The object variant appears when a preceding typed map made the
   * intermediate object-typed; it has no {@code deserializer()}, so the grouping key is resolved
   * through the {@link DeserializeToObject} below it instead.
   */
  private static Optional<AppendColumnsWithObject> appendColumnsWithObjectProducing(
      LogicalPlan node, ExprId exprId) {
    if (node == null) {
      return Optional.empty();
    }

    if (node instanceof AppendColumnsWithObject) {
      AppendColumnsWithObject appendColumns = (AppendColumnsWithObject) node;
      boolean produces =
          ScalaConversionUtils.<NamedExpression>fromSeq(appendColumns.newColumnsSerializer())
              .stream()
              .anyMatch(column -> column.exprId().equals(exprId));
      if (produces) {
        return Optional.of(appendColumns);
      }
    }

    return ScalaConversionUtils.<LogicalPlan>fromSeq(node.children()).stream()
        .map(child -> appendColumnsWithObjectProducing(child, exprId))
        .filter(Optional::isPresent)
        .findFirst()
        .orElse(Optional.empty());
  }

  /**
   * Finds the {@link AppendColumns} in the subtree whose {@code newColumns} include {@code exprId}.
   * The match is on exprId, so the descent cannot pick up an unrelated grouping deeper in the plan.
   */
  private static Optional<AppendColumns> appendColumnsProducing(LogicalPlan node, ExprId exprId) {
    if (node == null) {
      return Optional.empty();
    }

    if (node instanceof AppendColumns) {
      AppendColumns appendColumns = (AppendColumns) node;
      boolean produces =
          ScalaConversionUtils.<Attribute>fromSeq(appendColumns.newColumns()).stream()
              .anyMatch(column -> column.exprId().equals(exprId));
      if (produces) {
        return Optional.of(appendColumns);
      }
    }

    return ScalaConversionUtils.<LogicalPlan>fromSeq(node.children()).stream()
        .map(child -> appendColumnsProducing(child, exprId))
        .filter(Optional::isPresent)
        .findFirst()
        .orElse(Optional.empty());
  }
}
