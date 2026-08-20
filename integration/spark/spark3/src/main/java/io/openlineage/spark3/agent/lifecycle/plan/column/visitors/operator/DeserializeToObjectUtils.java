/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column.visitors.operator;

import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.Optional;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.plans.logical.DeserializeToObject;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * Shared helpers for typed-boundary plans. When a preceding typed map made the intermediate
 * object-typed, a grouping operator's value deserializer references the intermediate fields
 * re-serialized from the opaque object (the {@code _1.._n} of a tuple / case-class encoder) rather
 * than the real columns; the {@link DeserializeToObject} below is the node whose deserializer
 * references the real columns.
 */
final class DeserializeToObjectUtils {

  private DeserializeToObjectUtils() {}

  /**
   * The deserializer of the first {@link DeserializeToObject} in the subtree rooted at {@code
   * node}, or empty when there is none.
   */
  static Optional<Expression> deserializerBelow(LogicalPlan node) {
    if (node == null) {
      return Optional.empty();
    }
    if (node instanceof DeserializeToObject) {
      return Optional.of(((DeserializeToObject) node).deserializer());
    }
    return ScalaConversionUtils.<LogicalPlan>fromSeq(node.children()).stream()
        .map(DeserializeToObjectUtils::deserializerBelow)
        .filter(Optional::isPresent)
        .findFirst()
        .orElse(Optional.empty());
  }
}
