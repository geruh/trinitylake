package org.apache.spark.sql.catalyst.parser.extensions

import org.apache.spark.sql.catalyst.parser.extensions.TrinityLakeSparkSqlExtensionsParser._
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.BeginTransactionCommand
import org.apache.spark.sql.catalyst.parser.extensions.TrinityLakeSqlExtensionsParser.{BeginStatementContext, CommitStatementContext, RollbackStatementContext, SingleStatementContext}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan


class TrinityLakeSqlExtensionsAstBuilder(delegate: ParserInterface) extends TrinityLakeSqlExtensionsBaseVisitor[AnyRef] {

//  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = {
//    visit(ctx.statement())
//  }

  override def visitBeginStatement(ctx: BeginStatementContext): LogicalPlan = {
    BeginTransactionCommand
  }

  override def visitCommitStatement(ctx: CommitStatementContext): LogicalPlan = {
    CommitTransactionCommand()
  }

  override def visitRollbackStatement(ctx: RollbackStatementContext): LogicalPlan = {
    RollbackTransactionCommand()
  }
}
