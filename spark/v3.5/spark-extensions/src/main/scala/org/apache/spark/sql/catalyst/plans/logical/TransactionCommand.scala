package org.apache.spark.sql.catalyst.plans.logical

import io.trinitylake.TransactionManager

trait TransactionCommand extends LeafCommand {
  protected val transactionManager: TransactionManager = TransactionManager.getInstance()
}
