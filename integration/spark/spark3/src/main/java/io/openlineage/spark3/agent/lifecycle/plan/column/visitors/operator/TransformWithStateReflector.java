/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.collection.immutable.Seq;

/**
 * Reflective accessor for the Spark 4+ {@code TransformWithState} Catalyst node (arbitrary state
 * API v2, {@code KeyValueGroupedDataset.transformWithState}). The node is present on Spark 4.0.0+
 * classpaths but <em>absent</em> on the 3.5.0 classpath this module also compiles against, so it
 * cannot be referenced by type. It is matched by class name and its accessors read reflectively;
 * on 3.5 every method returns empty.
 *
 * <p>Structurally {@code TransformWithState} is a {@code BinaryNode}: {@code left = child} (the
 * data, wrapped in {@code AppendColumns} by {@code groupByKey}), {@code right = initialState} (a
 * dummy empty {@code LocalRelation} when {@code hasInitialState() == false}). The grouping
 * attributes, child, and key/value deserializers are read the same way as {@link
 * org.apache.spark.sql.catalyst.plans.logical.MapGroups} / {@link
 * org.apache.spark.sql.catalyst.plans.logical.FlatMapGroupsWithState}, so the {@link
 * TypedGroupByVisitor} and {@link TypedBoundaryFanInVisitor} extend to it without a new visitor
 * class.
 */
final class TransformWithStateReflector {

  private static final Logger LOG = Logger.getLogger(TransformWithStateReflector.class.getName());

  static final String TRANSFORM_WITH_STATE =
      "org.apache.spark.sql.catalyst.plans.logical.TransformWithState";

  private TransformWithStateReflector() {}

  /**
   * Whether {@code node} is a {@code TransformWithState} (reflective; false on 3.5). The check is
   * by class-name prefix so that Mockito subclasses (whose {@code getClass()} is a
   * {@code TransformWithState$$EnhancerByMockito} subtype) are also recognised; {@code
   * PlanUtils.safeIsInstanceOf} checks {@code instance.getClass().isAssignableFrom(c)} which is
   * false for a mock subclass, so it cannot be used in the {@code isDefinedAt} hot path that the
   * test exercises with a mock. The reflective accessor reads below still use {@code getMethod},
   * which resolves against the real class through the mock's hierarchy.
   */
  static boolean isTransformWithState(Object node) {
    if (node == null) {
      return false;
    }
    String className = node.getClass().getName();
    return className.equals(TRANSFORM_WITH_STATE)
        || className.startsWith(TRANSFORM_WITH_STATE + "$");
  }

  /** The grouping attributes, or empty when they cannot be read reflectively. */
  @SuppressWarnings("unchecked")
  static Optional<Seq<Attribute>> groupingAttributes(Object node) {
    return invoke(node, "groupingAttributes").map(o -> (Seq<Attribute>) o);
  }

  /** The child (left) plan, or empty when it cannot be read. */
  static Optional<LogicalPlan> child(Object node) {
    return invoke(node, "child").map(o -> (LogicalPlan) o);
  }

  /** The key deserializer expression, or empty when it cannot be read. */
  static Optional<Expression> keyDeserializer(Object node) {
    return invoke(node, "keyDeserializer").map(o -> (Expression) o);
  }

  /** The value deserializer expression, or empty when it cannot be read. */
  static Optional<Expression> valueDeserializer(Object node) {
    return invoke(node, "valueDeserializer").map(o -> (Expression) o);
  }

  /** The output object attribute, or empty when it cannot be read. */
  static Optional<org.apache.spark.sql.catalyst.expressions.Attribute> outputObjAttr(Object node) {
    return invoke(node, "outputObjAttr")
        .map(o -> (org.apache.spark.sql.catalyst.expressions.Attribute) o);
  }

  /** Whether an initial state relation is present (the right side is real, not a dummy). */
  static Optional<Boolean> hasInitialState(Object node) {
    return invoke(node, "hasInitialState").map(o -> (Boolean) o);
  }

  /** The initial state (right) plan, or empty when it cannot be read. */
  static Optional<LogicalPlan> initialState(Object node) {
    return invoke(node, "initialState").map(o -> (LogicalPlan) o);
  }

  /**
   * The children of the node, used by the fan-in's {@code collectInputs} descent. For a {@code
   * BinaryNode} this is {@code [child, initialState]}; the empty-dummy right side is filtered out by
   * the {@code hasInitialState} guard at the call site.
   */
  static Optional<scala.collection.immutable.Seq<LogicalPlan>> children(Object node) {
    return invoke(node, "children").map(o -> (scala.collection.immutable.Seq<LogicalPlan>) o);
  }

  @SuppressWarnings("unchecked")
  private static <T> Optional<T> invoke(Object node, String methodName) {
    if (node == null) {
      return Optional.empty();
    }
    try {
      Method method = node.getClass().getMethod(methodName);
      Object result = method.invoke(node);
      return Optional.ofNullable((T) result);
    } catch (NoSuchMethodException
        | IllegalAccessException
        | InvocationTargetException
        | ClassCastException e) {
      LOG.log(Level.FINE, "Could not read {0} from {1}", new Object[] {methodName, node.getClass()});
      return Optional.empty();
    }
  }

  /** Convenience: a Scala {@code Seq} from the given attributes, for mocking in tests. */
  static scala.collection.immutable.Seq<Attribute> fromJava(java.util.List<Attribute> attrs) {
    return ScalaConversionUtils.fromList(attrs);
  }
}
