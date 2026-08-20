/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import static io.openlineage.client.utils.TransformationInfo.Subtypes.FILTER;

import io.openlineage.client.utils.TransformationInfo;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark3.agent.lifecycle.plan.column.ExpressionTraverser;
import java.util.Optional;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.types.ObjectType;

/**
 * Extracts expression dependencies from a Filter operator in {@link LogicalPlan}. Example query:
 *
 * <pre>{@code
 * SELECT *
 * FROM employees
 * WHERE salary > 50000;
 * }</pre>
 *
 * <p>A {@code Filter} produced by {@code ds.map(...).filter(...)} or {@code ds.filter(...).map(...)}
 * has a condition rooted at the opaque object (the output of the {@code MapElements} / {@code
 * DeserializeToObject} boundary), which carries no linkage to real columns. In that case the
 * deserializer of the {@code DeserializeToObject} below is traversed instead, so the FILTER edge
 * resolves to the real columns rather than dead-ending. The breadth is the deserialized schema, not
 * the predicate's reads, for the same reason as {@link TypedFilterVisitor}: the predicate is opaque
 * bytecode.
 */
public class FilterVisitor implements OperatorVisitor {
  @Override
  public boolean isDefinedAt(LogicalPlan operator) {
    return operator instanceof Filter;
  }

  @Override
  public void apply(LogicalPlan operator, ColumnLevelLineageBuilder builder) {
    Filter filter = (Filter) operator;
    ExprId exprId = NamedExpression.newExprId();
    builder.addDatasetDependency(exprId);
    ExpressionTraverser.of(filterCondition(filter), exprId, TransformationInfo.indirect(FILTER), builder)
        .traverse();
  }

  /** The condition to traverse; an opaque-object filter is traversed through the deserializer below. */
  private static Expression filterCondition(Filter filter) {
    if (referencesOpaqueObject(filter.condition())) {
      Optional<Expression> deserializer = DeserializeToObjectUtils.deserializerBelow(filter.child());
      if (deserializer.isPresent()) {
        return deserializer.get();
      }
    }
    return filter.condition();
  }

  private static boolean referencesOpaqueObject(Expression condition) {
    return ScalaConversionUtils.<Attribute>fromSeq(condition.references().toSeq()).stream()
        .anyMatch(attribute -> attribute.dataType() instanceof ObjectType);
  }
}
