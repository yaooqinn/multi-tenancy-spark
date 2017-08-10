/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, Strategy}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{BroadcastHint, EventTimeWatermark, LogicalPlan}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution
import org.apache.spark.sql.execution.columnar.{InMemoryRelation, InMemoryTableScanExec}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.exchange.ShuffleExchange
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight}
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamingQuery

/**
 * Converts a logical plan into zero or more SparkPlans.  This API is exposed for experimenting
 * with the query planner and is not designed to be stable across spark releases.  Developers
 * writing libraries should instead consider using the stable APIs provided in
 * [[org.apache.spark.sql.sources]]
 */
abstract class SparkStrategy extends GenericStrategy[SparkPlan] {

  override protected def planLater(plan: LogicalPlan, user: String): SparkPlan =
    PlanLater(plan, user)
}

case class PlanLater(
     plan: LogicalPlan,
     override val user: String) extends LeafExecNode {

  override def output: Seq[Attribute] = plan.output

  protected override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException()
  }
}

abstract class SparkStrategies extends QueryPlanner[SparkPlan] {
  self: SparkPlanner =>

  /**
   * Plans special cases of limit operators.
   */
  object SpecialLimits extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.ReturnAnswer(rootPlan) => rootPlan match {
        case logical.Limit(IntegerLiteral(limit), logical.Sort(order, true, child)) =>
          execution.TakeOrderedAndProjectExec(
            limit, order, child.output, planLater(child, sparkContext.sparkUser)) :: Nil
        case logical.Limit(
            IntegerLiteral(limit),
            logical.Project(projectList, logical.Sort(order, true, child))) =>
          execution.TakeOrderedAndProjectExec(
            limit, order, projectList, planLater(child, sparkContext.sparkUser)) :: Nil
        case logical.Limit(IntegerLiteral(limit), child) =>
          execution.CollectLimitExec(limit, planLater(child, sparkContext.sparkUser)) :: Nil
        case other => planLater(other, sparkContext.sparkUser) :: Nil
      }
      case logical.Limit(IntegerLiteral(limit), logical.Sort(order, true, child)) =>
        execution.TakeOrderedAndProjectExec(
          limit, order, child.output, planLater(child, sparkContext.sparkUser)) :: Nil
      case logical.Limit(
          IntegerLiteral(limit), logical.Project(projectList, logical.Sort(order, true, child))) =>
        execution.TakeOrderedAndProjectExec(
          limit, order, projectList, planLater(child, sparkContext.sparkUser)) :: Nil
      case _ => Nil
    }
  }

  /**
   * Select the proper physical plan for join based on joining keys and size of logical plan.
   *
   * At first, uses the [[ExtractEquiJoinKeys]] pattern to find joins where at least some of the
   * predicates can be evaluated by matching join keys. If found,  Join implementations are chosen
   * with the following precedence:
   *
   * - Broadcast: if one side of the join has an estimated physical size that is smaller than the
   *     user-configurable [[SQLConf.AUTO_BROADCASTJOIN_THRESHOLD]] threshold
   *     or if that side has an explicit broadcast hint (e.g. the user applied the
   *     [[org.apache.spark.sql.functions.broadcast()]] function to a DataFrame), then that side
   *     of the join will be broadcasted and the other side will be streamed, with no shuffling
   *     performed. If both sides of the join are eligible to be broadcasted then the
   * - Shuffle hash join: if the average size of a single partition is small enough to build a hash
   *     table.
   * - Sort merge: if the matching join keys are sortable.
   *
   * If there is no joining keys, Join implementations are chosen with the following precedence:
   * - BroadcastNestedLoopJoin: if one side of the join could be broadcasted
   * - CartesianProduct: for Inner join
   * - BroadcastNestedLoopJoin
   */
  object JoinSelection extends Strategy with PredicateHelper {

    /**
     * Matches a plan whose output should be small enough to be used in broadcast join.
     */
    private def canBroadcast(plan: LogicalPlan): Boolean = {
      plan.statistics.isBroadcastable ||
        (plan.statistics.sizeInBytes >= 0 &&
          plan.statistics.sizeInBytes <= conf.autoBroadcastJoinThreshold)
    }

    /**
     * Matches a plan whose single partition should be small enough to build a hash table.
     *
     * Note: this assume that the number of partition is fixed, requires additional work if it's
     * dynamic.
     */
    private def canBuildLocalHashMap(plan: LogicalPlan): Boolean = {
      plan.statistics.sizeInBytes < conf.autoBroadcastJoinThreshold * conf.numShufflePartitions
    }

    /**
     * Returns whether plan a is much smaller (3X) than plan b.
     *
     * The cost to build hash map is higher than sorting, we should only build hash map on a table
     * that is much smaller than other one. Since we does not have the statistic for number of rows,
     * use the size of bytes here as estimation.
     */
    private def muchSmaller(a: LogicalPlan, b: LogicalPlan): Boolean = {
      a.statistics.sizeInBytes * 3 <= b.statistics.sizeInBytes
    }

    private def canBuildRight(joinType: JoinType): Boolean = joinType match {
      case _: InnerLike | LeftOuter | LeftSemi | LeftAnti => true
      case j: ExistenceJoin => true
      case _ => false
    }

    private def canBuildLeft(joinType: JoinType): Boolean = joinType match {
      case _: InnerLike | RightOuter => true
      case _ => false
    }

    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

      // --- BroadcastHashJoin --------------------------------------------------------------------

      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
        if canBuildRight(joinType) && canBroadcast(right) =>
        Seq(joins.BroadcastHashJoinExec(
          leftKeys, rightKeys, joinType, BuildRight, condition,
          planLater(left, sparkContext.sparkUser),
          planLater(right, sparkContext.sparkUser)))

      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
        if canBuildLeft(joinType) && canBroadcast(left) =>
        Seq(joins.BroadcastHashJoinExec(
          leftKeys, rightKeys, joinType, BuildLeft, condition,
          planLater(left, sparkContext.sparkUser),
          planLater(right, sparkContext.sparkUser)))

      // --- ShuffledHashJoin ---------------------------------------------------------------------

      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
         if !conf.preferSortMergeJoin && canBuildRight(joinType) && canBuildLocalHashMap(right)
           && muchSmaller(right, left) ||
           !RowOrdering.isOrderable(leftKeys) =>
        Seq(joins.ShuffledHashJoinExec(
          leftKeys, rightKeys, joinType, BuildRight, condition,
          planLater(left, sparkContext.sparkUser),
          planLater(right, sparkContext.sparkUser)))

      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
         if !conf.preferSortMergeJoin && canBuildLeft(joinType) && canBuildLocalHashMap(left)
           && muchSmaller(left, right) ||
           !RowOrdering.isOrderable(leftKeys) =>
        Seq(joins.ShuffledHashJoinExec(
          leftKeys, rightKeys, joinType, BuildLeft, condition,
          planLater(left, sparkContext.sparkUser),
          planLater(right, sparkContext.sparkUser)))

      // --- SortMergeJoin ------------------------------------------------------------

      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
        if RowOrdering.isOrderable(leftKeys) =>
        joins.SortMergeJoinExec(
          leftKeys, rightKeys, joinType, condition,
          planLater(left, sparkContext.sparkUser),
          planLater(right, sparkContext.sparkUser)) :: Nil

      // --- Without joining keys ------------------------------------------------------------

      // Pick BroadcastNestedLoopJoin if one side could be broadcasted
      case j @ logical.Join(left, right, joinType, condition)
          if canBuildRight(joinType) && canBroadcast(right) =>
        joins.BroadcastNestedLoopJoinExec(
          planLater(left, sparkContext.sparkUser),
          planLater(right, sparkContext.sparkUser),
          BuildRight, joinType, condition) :: Nil
      case j @ logical.Join(left, right, joinType, condition)
          if canBuildLeft(joinType) && canBroadcast(left) =>
        joins.BroadcastNestedLoopJoinExec(
          planLater(left, sparkContext.sparkUser),
          planLater(right, sparkContext.sparkUser), BuildLeft, joinType, condition) :: Nil

      // Pick CartesianProduct for InnerJoin
      case logical.Join(left, right, _: InnerLike, condition) =>
        joins.CartesianProductExec(
          planLater(left, sparkContext.sparkUser),
          planLater(right, sparkContext.sparkUser),
          condition) :: Nil

      case logical.Join(left, right, joinType, condition) =>
        val buildSide =
          if (right.statistics.sizeInBytes <= left.statistics.sizeInBytes) {
            BuildRight
          } else {
            BuildLeft
          }
        // This join could be very slow or OOM
        joins.BroadcastNestedLoopJoinExec(
          planLater(left, sparkContext.sparkUser),
          planLater(right, sparkContext.sparkUser),
          buildSide, joinType, condition) :: Nil

      // --- Cases where this strategy does not apply ---------------------------------------------

      case _ => Nil
    }
  }

  /**
   * Used to plan aggregation queries that are computed incrementally as part of a
   * [[StreamingQuery]]. Currently this rule is injected into the planner
   * on-demand, only when planning in a [[org.apache.spark.sql.execution.streaming.StreamExecution]]
   */
  object StatefulAggregationStrategy extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case EventTimeWatermark(columnName, delay, child) =>
        EventTimeWatermarkExec(columnName, delay, planLater(child, sparkContext.sparkUser)) :: Nil

      case PhysicalAggregation(
        namedGroupingExpressions, aggregateExpressions, rewrittenResultExpressions, child) =>

        aggregate.AggUtils.planStreamingAggregation(
          namedGroupingExpressions,
          aggregateExpressions,
          rewrittenResultExpressions,
          planLater(child, sparkContext.sparkUser))

      case _ => Nil
    }
  }

  /**
   * Used to plan the aggregate operator for expressions based on the AggregateFunction2 interface.
   */
  object Aggregation extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalAggregation(
          groupingExpressions, aggregateExpressions, resultExpressions, child) =>

        val (functionsWithDistinct, functionsWithoutDistinct) =
          aggregateExpressions.partition(_.isDistinct)
        if (functionsWithDistinct.map(_.aggregateFunction.children).distinct.length > 1) {
          // This is a sanity check. We should not reach here when we have multiple distinct
          // column sets. Our MultipleDistinctRewriter should take care this case.
          sys.error("You hit a query analyzer bug. Please report your query to " +
              "Spark user mailing list.")
        }

        val aggregateOperator =
          if (aggregateExpressions.map(_.aggregateFunction).exists(!_.supportsPartial)) {
            if (functionsWithDistinct.nonEmpty) {
              sys.error("Distinct columns cannot exist in Aggregate operator containing " +
                "aggregate functions which don't support partial aggregation.")
            } else {
              aggregate.AggUtils.planAggregateWithoutPartial(
                groupingExpressions,
                aggregateExpressions,
                resultExpressions,
                planLater(child, sparkContext.sparkUser))
            }
          } else if (functionsWithDistinct.isEmpty) {
            aggregate.AggUtils.planAggregateWithoutDistinct(
              groupingExpressions,
              aggregateExpressions,
              resultExpressions,
              planLater(child, sparkContext.sparkUser))
          } else {
            aggregate.AggUtils.planAggregateWithOneDistinct(
              groupingExpressions,
              functionsWithDistinct,
              functionsWithoutDistinct,
              resultExpressions,
              planLater(child, sparkContext.sparkUser))
          }

        aggregateOperator

      case _ => Nil
    }
  }

  protected lazy val singleRowRdd = sparkContext.parallelize(Seq(InternalRow()), 1)

  object InMemoryScans extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, filters, mem: InMemoryRelation) =>
        pruneFilterProject(
          projectList,
          filters,
          identity[Seq[Expression]], // All filters still need to be evaluated.
          InMemoryTableScanExec(_, filters, mem, sparkContext.sparkUser)) :: Nil
      case _ => Nil
    }
  }

  /**
   * This strategy is just for explaining `Dataset/DataFrame` created by `spark.readStream`.
   * It won't affect the execution, because `StreamingRelation` will be replaced with
   * `StreamingExecutionRelation` in `StreamingQueryManager` and `StreamingExecutionRelation` will
   * be replaced with the real relation using the `Source` in `StreamExecution`.
   */
  object StreamingRelationStrategy extends Strategy {
    def user: String = sparkContext.sparkUser
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case s: StreamingRelation =>
        StreamingRelationExec(s.sourceName, s.output, user) :: Nil
      case s: StreamingExecutionRelation =>
        StreamingRelationExec(s.toString, s.output, user) :: Nil
      case _ => Nil
    }
  }

  // Can we automate these 'pass through' operations?
  object BasicOperators extends Strategy {
    def numPartitions: Int = self.numPartitions

    def user: String = sparkContext.sparkUser

    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case r: RunnableCommand => ExecutedCommandExec(r) :: Nil

      case MemoryPlan(sink, output) =>
        val encoder = RowEncoder(sink.schema)
        LocalTableScanExec(output, sink.allData.map(r => encoder.toRow(r).copy()), user) :: Nil

      case logical.Distinct(child) =>
        throw new IllegalStateException(
          "logical distinct operator should have been replaced by aggregate in the optimizer")
      case logical.Intersect(left, right) =>
        throw new IllegalStateException(
          "logical intersect operator should have been replaced by semi-join in the optimizer")
      case logical.Except(left, right) =>
        throw new IllegalStateException(
          "logical except operator should have been replaced by anti-join in the optimizer")

      case logical.DeserializeToObject(deserializer, objAttr, child) =>
        execution.DeserializeToObjectExec(
          deserializer, objAttr, planLater(child, user)) :: Nil
      case logical.SerializeFromObject(serializer, child) =>
        execution.SerializeFromObjectExec(
          serializer, planLater(child, user)) :: Nil
      case logical.MapPartitions(f, objAttr, child) =>
        execution.MapPartitionsExec(f, objAttr, planLater(child, user)) :: Nil
      case logical.MapPartitionsInR(f, p, b, is, os, objAttr, child) =>
        execution.MapPartitionsExec(
          execution.r.MapPartitionsRWrapper(f, p, b, is, os),
          objAttr, planLater(child, user)) :: Nil
      case logical.FlatMapGroupsInR(f, p, b, is, os, key, value, grouping, data, objAttr, child) =>
        execution.FlatMapGroupsInRExec(f, p, b, is, os, key, value, grouping,
          data, objAttr, planLater(child, user)) :: Nil
      case logical.MapElements(f, _, _, objAttr, child) =>
        execution.MapElementsExec(f, objAttr, planLater(child, user)) :: Nil
      case logical.AppendColumns(f, _, _, in, out, child) =>
        execution.AppendColumnsExec(f, in, out, planLater(child, user)) :: Nil
      case logical.AppendColumnsWithObject(f, childSer, newSer, child) =>
        execution.AppendColumnsWithObjectExec(
          f, childSer, newSer, planLater(child, user)) :: Nil
      case logical.MapGroups(f, key, value, grouping, data, objAttr, child) =>
        execution.MapGroupsExec(
          f, key, value, grouping, data, objAttr, planLater(child, user)) :: Nil
      case logical.CoGroup(f, key, lObj, rObj, lGroup, rGroup, lAttr, rAttr, oAttr, left, right) =>
        execution.CoGroupExec(
          f, key, lObj, rObj, lGroup, rGroup, lAttr, rAttr, oAttr,
          planLater(left, user), planLater(right, user)) :: Nil

      case logical.Repartition(numPartitions, shuffle, child) =>
        if (shuffle) {
          ShuffleExchange(
            RoundRobinPartitioning(numPartitions), planLater(child, user)) :: Nil
        } else {
          execution.CoalesceExec(numPartitions, planLater(child, user)) :: Nil
        }
      case logical.SortPartitions(sortExprs, child) =>
        // This sort only sorts tuples within a partition. Its requiredDistribution will be
        // an UnspecifiedDistribution.
        execution.SortExec(
          sortExprs, global = false, child = planLater(child, user)) :: Nil
      case logical.Sort(sortExprs, global, child) =>
        execution.SortExec(sortExprs, global, planLater(child, user)) :: Nil
      case logical.Project(projectList, child) =>
        execution.ProjectExec(projectList, planLater(child, user)) :: Nil
      case logical.Filter(condition, child) =>
        execution.FilterExec(condition, planLater(child, user)) :: Nil
      case f: logical.TypedFilter =>
        execution.FilterExec(f.typedCondition(f.deserializer),
          planLater(f.child, user)) :: Nil
      case e @ logical.Expand(_, _, child) =>
        execution.ExpandExec(
          e.projections, e.output, planLater(child, user)) :: Nil
      case logical.Window(windowExprs, partitionSpec, orderSpec, child) =>
        execution.window.WindowExec(
          windowExprs, partitionSpec, orderSpec, planLater(child, user)) :: Nil
      case logical.Sample(lb, ub, withReplacement, seed, child) =>
        execution.SampleExec(
          lb, ub, withReplacement, seed, planLater(child, user)) :: Nil
      case logical.LocalRelation(output, data) =>
        LocalTableScanExec(output, data, user) :: Nil
      case logical.LocalLimit(IntegerLiteral(limit), child) =>
        execution.LocalLimitExec(limit, planLater(child, user)) :: Nil
      case logical.GlobalLimit(IntegerLiteral(limit), child) =>
        execution.GlobalLimitExec(limit, planLater(child, user)) :: Nil
      case logical.Union(unionChildren) =>
        execution.UnionExec(
          unionChildren.map(p => planLater(p, user))) :: Nil
      case g @ logical.Generate(generator, join, outer, _, _, child) =>
        execution.GenerateExec(
          generator, join = join, outer = outer, g.qualifiedGeneratorOutput,
          planLater(child, user)) :: Nil
      case logical.OneRowRelation =>
        execution.RDDScanExec(Nil, singleRowRdd, "OneRowRelation") :: Nil
      case r: logical.Range =>
        execution.RangeExec(r, user) :: Nil
      case logical.RepartitionByExpression(expressions, child, nPartitions) =>
        exchange.ShuffleExchange(HashPartitioning(
          expressions,
          nPartitions.getOrElse(numPartitions)),
          planLater(child, user)) :: Nil
      case ExternalRDD(outputObjAttr, rdd) => ExternalRDDScanExec(outputObjAttr, rdd) :: Nil
      case r: LogicalRDD =>
        RDDScanExec(r.output, r.rdd, "ExistingRDD", r.outputPartitioning, r.outputOrdering) :: Nil
      case BroadcastHint(child) => planLater(child, user) :: Nil
      case _ => Nil
    }
  }

  object DDLStrategy extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case CreateTable(tableDesc, mode, None)
        if tableDesc.provider.get == DDLUtils.HIVE_PROVIDER =>
        val cmd = CreateTableCommand(tableDesc, ifNotExists = mode == SaveMode.Ignore)
        ExecutedCommandExec(cmd) :: Nil

      case CreateTable(tableDesc, mode, None) =>
        val cmd =
          CreateDataSourceTableCommand(tableDesc, ignoreIfExists = mode == SaveMode.Ignore)
        ExecutedCommandExec(cmd) :: Nil

      // CREATE TABLE ... AS SELECT ... for hive serde table is handled in hive module, by rule
      // `CreateTables`

      case CreateTable(tableDesc, mode, Some(query))
        if tableDesc.provider.get != DDLUtils.HIVE_PROVIDER =>
        val cmd =
          CreateDataSourceTableAsSelectCommand(
            tableDesc,
            mode,
            query)
        ExecutedCommandExec(cmd) :: Nil

      case c: CreateTempViewUsing => ExecutedCommandExec(c) :: Nil

      case _ => Nil
    }
  }
}
