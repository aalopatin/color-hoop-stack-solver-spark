package com.github.aalopatin

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object UDFs {

  val possibleMoves = (field: Seq[Seq[String]], stackSize: Int) => {
    for {
      (stackCurrent, indexCurrent) <- field.zipWithIndex
      if stackCurrent.nonEmpty && (stackCurrent.length != stackSize || !stackCurrent.forall(stackCurrent.head.equals(_)))
      (stackNext, indexNext) <- field.zipWithIndex
      if (
        indexCurrent != indexNext
          && stackNext.length != stackSize
          && (stackNext.isEmpty || stackCurrent.head == stackNext.head)
        )
    } yield {
      (indexCurrent, indexNext)
    }
  }

  val possibleMovesUdf: UserDefinedFunction = udf(possibleMoves)

  val fieldToString = (field: Seq[Seq[String]], stackSize: Int) =>
    (for (stack <- field) yield stack.mkString.padTo(stackSize, "_").mkString).mkString

  val fieldToStringUdf: UserDefinedFunction = udf(fieldToString)

  val doMove: (Array[Seq[String]], (Int, Int)) => Array[Seq[String]] = (field: Array[Seq[String]], move: (Int, Int)) => {
    field(move._2) = field(move._2) :+ field(move._1).head
    field(move._1) = field(move._1).tail
    field
  }

  val doMoveUdf: UserDefinedFunction = udf(doMove)

}
