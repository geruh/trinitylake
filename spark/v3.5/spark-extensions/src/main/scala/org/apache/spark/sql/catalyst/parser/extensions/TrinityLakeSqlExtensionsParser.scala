
package org.apache.spark.sql.catalyst.parser.extensions

import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.misc.ParseCancellationException
import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.parser.{ParserInterface, UpperCaseCharStream}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.internal.{SQLConf, VariableSubstitution}
import org.apache.spark.sql.types.{DataType, StructType}

import scala.util.Try

class TrinityLakeSqlExtensionsParser(delegate: ParserInterface) extends ParserInterface {

    import TrinityLakeSqlExtensionsParser._
    private lazy val astBuilder = new TrinityLakeSqlExtensionsAstBuilder(delegate)
    private lazy val substitutor = {
      Try(substitutorCtor.newInstance(SQLConf.get))
        .getOrElse(substitutorCtor.newInstance())
    }

    /**
     * Parse a string to a LogicalPlan.
     */
    override def parsePlan(sqlText: String): LogicalPlan = {
      val sqlTextAfterSubstitution = substitutor.substitute(sqlText)
      if (isTrinityLakeCommand(sqlTextAfterSubstitution)) {
        parse(sqlTextAfterSubstitution) { parser =>
          astBuilder.visit(parser.singleStatement())
        }.asInstanceOf[LogicalPlan]
      } else {
        delegate.parsePlan(sqlText)
      }
    }

    /**
     * Parse a string to an Expression.
     */
    override def parseExpression(sqlText: String): Expression = {
      delegate.parseExpression(sqlText)
    }

    /**
     * Parse a string to a TableIdentifier.
     */
    override def parseTableIdentifier(sqlText: String): TableIdentifier = {
      delegate.parseTableIdentifier(sqlText)
    }

    /**
     * Parse a string to a FunctionIdentifier.
     */
    override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = {
      delegate.parseFunctionIdentifier(sqlText)
    }

    /**
     * Parse a string to a multi-part identifier.
     */
    override def parseMultipartIdentifier(sqlText: String): Seq[String] = {
      delegate.parseMultipartIdentifier(sqlText)
    }

    override def parseQuery(sqlText: String): LogicalPlan = ???

    /**
     * Creates StructType for a given SQL string, which is a comma separated list of field
     * definitions which will preserve the correct Hive metadata.
     */
    override def parseTableSchema(sqlText: String): StructType = {
      delegate.parseTableSchema(sqlText)
    }
    /**
     * Parse a string to a DataType.
     */
    override def parseDataType(sqlText: String): DataType = {
      delegate.parseDataType(sqlText)
    }

    private def isTrinityLakeCommand(sqlText: String): Boolean = {
      val normalized = sqlText.toLowerCase.trim()
        .replaceAll("--.*?\\n", " ")
        .replaceAll("\\s+", " ")
        .replaceAll("/\\*.*?\\*/", " ")
        .trim()

      normalized.startsWith("begin") ||
        normalized.startsWith("commit") ||
        normalized.startsWith("rollback")
    }

    protected def parse[T](command: String)(toResult: TrinityLakeSqlExtensionsParser => T): T = {
      val lexer = new TrinityLakeSqlExtensionsLexer(
        new UpperCaseCharStream(CharStreams.fromString(command)))
      lexer.removeErrorListeners()
      //    lexer.addErrorListener(TrinityLakeParseErrorListener)

      val tokenStream = new CommonTokenStream(lexer)
      val parser = new TrinityLakeSqlExtensionsParser(tokenStream)
      //    parser.addParseListener(TrinityLakePostProcessor)
      parser.removeErrorListeners()
      //    parser.addErrorListener(TrinityLakeParseErrorListener)

      try {
        // First try parsing with SLL mode
        parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
        toResult(parser)
      } catch {
        case _: ParseCancellationException =>
          // If SLL fails, try with LL mode
          tokenStream.seek(0)
          parser.reset()
          parser.getInterpreter.setPredictionMode(PredictionMode.LL)
          toResult(parser)
      }
    }
}

object TrinityLakeSqlExtensionsParser {
  private val substitutorCtor = {
    Try(classOf[VariableSubstitution].getConstructor(classOf[SQLConf]))
      .getOrElse(classOf[VariableSubstitution].getConstructor())
  }
}