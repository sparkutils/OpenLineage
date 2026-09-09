/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import static io.openlineage.client.utils.TransformationInfo.Subtypes.GROUP_BY;
import static io.openlineage.client.utils.TransformationInfo.Subtypes.TRANSFORMATION;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.ColumnLineageConfig;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionTraverser;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.UserDefinedExpression;
import org.apache.spark.sql.catalyst.plans.logical.FlatMapGroupsInArrow;
import org.apache.spark.sql.catalyst.plans.logical.FlatMapGroupsInPandas;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.MapInArrow;
import org.apache.spark.sql.catalyst.plans.logical.MapInPandas;
import scala.collection.immutable.Seq;

/**
 * Emits column lineage for the pandas operator family: {@link MapInPandas}, {@link MapInArrow},
 * {@link FlatMapGroupsInPandas} and {@link FlatMapGroupsInArrow} — the Catalyst nodes produced by
 * PySpark {@code mapInPandas}/{@code mapInArrow} and {@code groupBy(...).applyInPandas}/{@code
 * applyInArrow}.
 *
 * <p>Unlike typed Dataset operators, these nodes never cross an opaque object boundary. The {@code
 * PythonUDF} sits in the node's {@code functionExpr} parameter with every marshalled input column
 * as an ordinary child expression, and {@code functionExpr.references()} is the exact set of columns
 * Spark marshals to the Python worker (PySpark builds {@code udf_column = udf(*[df[col] for col in
 * df.columns])}, and the exec sends {@code child.output} — grouping ++ data, deduped — to the
 * worker). So the output←input fan-in over {@code functionExpr.references()} is <b>exact, not
 * pessimistic</b>, and ships unflagged — the {@link TypedGroupByVisitor}/{@link TypedFilterVisitor}
 * side of the policy line, not the {@link TypedBoundaryFanInVisitor} side.
 *
 * <p>The one imprecision: a UDF body may read a subset of its arguments — over-broad, but never a
 * false specific pairing, since narrowing would require Python bytecode analysis. This is the same
 * class of imprecision {@link TypedFilterVisitor} documents.
 *
 * <p>For {@link FlatMapGroupsInPandas}/{@link FlatMapGroupsInArrow} the grouping signal is also
 * emitted: a synthetic {@link ExprId} is minted, {@link
 * ColumnLevelLineageBuilder#addDatasetDependency(ExprId)} is called, and each {@code
 * groupingAttributes} entry is traversed with {@link TransformationInfo#indirect(Object)
 * INDIRECT/GROUP_BY}. The grouping attributes are real child attributes (exprIds preserved), so
 * plain traversal lands on the input columns — the {@code sources.add(attribute)} fallback branch
 * of {@link TypedGroupByVisitor} is the exact precedent. An expression key resolves through the
 * {@code Alias} in the {@code Project} below by the same traversal.
 *
 * <p>{@code functionExpr} is never routed through an expression visitor; it would double-claim
 * with {@link
 * io.openlineage.spark3.agent.lifecycle.plan.column.visitors.expression.UserDefinedExpressionVisitor}
 * where UDFs appear as expressions.
 *
 * <p>Out of scope, consistent with {@link TypedGroupByVisitor}'s CoGroup decision: {@code
 * FlatMapCoGroupsInPandas}/{@code FlatMapCoGroupsInArrow} (BinaryNode, two grouping sides) and the
 * streaming-only nodes {@code FlatMapGroupsInPandasWithState} and {@code
 * TransformWithStateInPandas} (renamed {@code TransformWithStateInPySpark} in Spark 4.1).
 */
@Slf4j
public class PandasOperatorVisitor implements OperatorVisitor {

  private static final int DEFAULT_MAX_EDGES = 10_000;

  @Override
  public boolean isDefinedAt(LogicalPlan operator) {
    return operator instanceof MapInPandas
        || operator instanceof MapInArrow
        || operator instanceof FlatMapGroupsInPandas
        || operator instanceof FlatMapGroupsInArrow;
  }

  @Override
  public void apply(LogicalPlan operator, ColumnLevelLineageBuilder builder) {
    Expression functionExpr = functionExpr(operator);
    if (functionExpr == null) {
      return;
    }

    Set<ExprId> inputExprIds = inputExprIds(functionExpr);
    Set<ExprId> outputExprIds = outputExprIds(operator);
    if (inputExprIds.isEmpty() || outputExprIds.isEmpty()) {
      return;
    }

    long edges = (long) outputExprIds.size() * (long) inputExprIds.size();
    int maxEdges = maxEdges(columnLineageConfig(builder));
    if (edges > maxEdges) {
      log.debug(
          "Pandas operator fan-in skipped for {}: {} output x {} input fields would emit {} "
              + "edges, above the configured limit of {}. Emitting nothing rather than a fan-in "
              + "that would push the column lineage facet past its own returned-input-field limit.",
          operator.getClass().getSimpleName(),
          outputExprIds.size(),
          inputExprIds.size(),
          edges,
          maxEdges);
      return;
    }

    String description = description(operator, functionExpr);
    TransformationInfo transformationInfo =
        new TransformationInfo(
            TransformationInfo.Types.INDIRECT, TRANSFORMATION, description, false);

    for (ExprId output : outputExprIds) {
      for (ExprId input : inputExprIds) {
        if (!output.equals(input)) {
          builder.addDependency(output, input, transformationInfo);
        }
      }
    }

    if (operator instanceof FlatMapGroupsInPandas || operator instanceof FlatMapGroupsInArrow) {
      emitGrouping(operator, builder);
    }
  }

  /**
   * Emits the {@code INDIRECT/GROUP_BY} dataset dependency for a {@link FlatMapGroupsInPandas} or
   * {@link FlatMapGroupsInArrow}, mirroring {@link TypedGroupByVisitor} on the typed path. The
   * grouping attributes are real child attrs (exprIds preserved), so plain {@link
   * ExpressionTraverser} traversal lands on the input columns — no {@code AppendColumns}
   * resolution is needed.
   */
  private static void emitGrouping(
      LogicalPlan operator, ColumnLevelLineageBuilder builder) {
    Seq<Attribute> groupingAttrs = groupingAttributes(operator);
    if (groupingAttrs == null) {
      return;
    }
    ExprId exprId = NamedExpression.newExprId();
    builder.addDatasetDependency(exprId);
    ScalaConversionUtils.<Attribute>fromSeq(groupingAttrs)
        .forEach(
            attribute ->
                ExpressionTraverser.of(
                        attribute, exprId, TransformationInfo.indirect(GROUP_BY), builder)
                    .traverse());
  }

  private static Expression functionExpr(LogicalPlan operator) {
    if (operator instanceof MapInPandas) {
      return ((MapInPandas) operator).functionExpr();
    }
    if (operator instanceof MapInArrow) {
      return ((MapInArrow) operator).functionExpr();
    }
    if (operator instanceof FlatMapGroupsInPandas) {
      return ((FlatMapGroupsInPandas) operator).functionExpr();
    }
    if (operator instanceof FlatMapGroupsInArrow) {
      return ((FlatMapGroupsInArrow) operator).functionExpr();
    }
    return null;
  }

  private static Seq<Attribute> groupingAttributes(LogicalPlan operator) {
    if (operator instanceof FlatMapGroupsInPandas) {
      return ((FlatMapGroupsInPandas) operator).groupingAttributes();
    }
    if (operator instanceof FlatMapGroupsInArrow) {
      return ((FlatMapGroupsInArrow) operator).groupingAttributes();
    }
    return null;
  }

  private static Set<ExprId> inputExprIds(Expression functionExpr) {
    Set<ExprId> inputExprIds = new LinkedHashSet<>();
    ScalaConversionUtils.<Attribute>fromSeq(functionExpr.references().toSeq())
        .forEach(attr -> inputExprIds.add(attr.exprId()));
    return inputExprIds;
  }

  private static Set<ExprId> outputExprIds(LogicalPlan operator) {
    Set<ExprId> outputExprIds = new LinkedHashSet<>();
    ScalaConversionUtils.<Attribute>fromSeq(operator.output())
        .forEach(attr -> outputExprIds.add(attr.exprId()));
    return outputExprIds;
  }

  /**
   * Renders the transformation description as {@code <NodeName>(<udfName>)}, falling back to just
   * the node name when the UDF name is absent (PySpark leaves it empty for anonymous lambdas).
   *
   * <p>Mockito subclass mocks append {@code $MockitoMock$<hash>} to the simple class name; the
   * suffix is stripped so the description is stable in tests as well as in production.
   */
  private static String description(LogicalPlan operator, Expression functionExpr) {
    String nodeName = simpleClassName(operator.getClass());
    String udfName = udfName(functionExpr);
    if (udfName == null || udfName.trim().isEmpty()) {
      return nodeName;
    }
    return nodeName + "(" + udfName + ")";
  }

  private static String simpleClassName(Class<?> cls) {
    String name = cls.getSimpleName();
    int dollar = name.indexOf('$');
    return dollar < 0 ? name : name.substring(0, dollar);
  }

  private static String udfName(Expression functionExpr) {
    if (functionExpr instanceof UserDefinedExpression) {
      // PythonUDF implements UserDefinedExpression on every supported Spark version; the name is
      // what PySpark registered the UDF under (often "<lambda>").
      return ((UserDefinedExpression) functionExpr).name();
    }
    return null;
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

  private static int maxEdges(ColumnLineageConfig config) {
    return Optional.ofNullable(config)
        .map(ColumnLineageConfig::getTypedBoundaryFanInMaxEdges)
        .orElse(DEFAULT_MAX_EDGES);
  }
}
