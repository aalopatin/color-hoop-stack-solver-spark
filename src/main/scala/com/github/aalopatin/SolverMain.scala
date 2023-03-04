package com.github.aalopatin

import com.github.aalopatin.Columns._
import com.github.aalopatin.FilesNames.{FieldToProcess, Solution, SolutionStage}
import com.github.aalopatin.UDFs.possibleMovesUdf
import com.github.aalopatin.utils.ConfigUtils.getField
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.annotation.tailrec
import scala.language.postfixOps

object SolverMain {

  val spark = SparkSession
    .builder()
    .appName("color-hoop-stack-solver")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val field = getField()
    val stackSize = field.maxBy(_.size).size

    if (
      field.forall(stack => stack.size == stackSize || stack.isEmpty)
    ) {

      findSolutions(field, stackSize)

      spark
        .read
        .orc(SolutionStage)
        .repartition(1)
        .write
        .mode("append")
        .orc(Solution)

      println("Stop")

    } else {
      println("Error in field data")
    }

  }

  def sha(column: String, stackSize: Int) = {
    sha1(
      array_join(
        transform(
          col(column), stack => rpad(concat_ws("", stack), stackSize, "_")),
        ""
      )
    )
  }

  def findSolutions(field: Seq[Seq[String]], stackSize: Int): Unit = {

    @tailrec
    def findSolutionsTailRec(fieldDF: DataFrame = null): Unit = {

      val fieldToProcess =
        if (fieldDF == null)
          spark.read.orc(FieldToProcess).cache()
        else
          fieldDF

      val fieldDFTransformed =
        fieldToProcess
          .withColumn(PossibleMovies, possibleMovesUdf($"$Field", lit(stackSize)))
          .withColumn(Move, explode_outer($"$PossibleMovies"))
          .drop(PossibleMovies)
          .withColumn(From, when($"$Move".isNotNull, $"${Move}._1"))
          .withColumn(To, when($"$Move".isNotNull, $"${Move}._2"))
          .withColumn(
            Element,
            when(
              $"$Move" isNotNull,
              array_join(
                transform(
                  $"$Field",
                  (stack, index) =>
                    when(index === $"$From", stack.apply(0)).otherwise("")
                ),
                ""
              )
            )
          )
          .withColumn(
            Field,
            transform(
              $"$Field",
              (stack, index) =>
                when(index === $"$To", concat(array($"$Element"), stack))
                  .when(index === $"$From", slice(stack, lit(2), size(stack)))
                  .otherwise(stack)
            )
          )
          .withColumn(Moves, when($"$Move".isNotNull, concat($"$Moves", array($"$Move"))).otherwise($"$Moves"))
          .drop(From, To, Element)
          .withColumn(
            Solved,
            aggregate(
              transform(
                $"$Field",
                stack => repeat(stack.apply(0), stackSize) === array_join(stack, "") || size(stack) === 0
              ),
              lit(true),
              (acc, value) => acc && value
            )
          )
          .withColumn(Sha, sha(Field, stackSize))
          .filter($"$Move".isNull || !array_contains($"$Fields", $"$Sha"))
          .withColumn(
            Fields,
            when($"$Move".isNotNull, concat(array($"$Sha"), $"$Fields")).otherwise($"$Fields")
          )
          .withColumn(ToProcess, !$"$Solved" && $"$Move".isNotNull)
          .cache()

      val fieldDFNotToProcess = fieldDFTransformed.filter(!$"$ToProcess").select(Field, Moves, Solved)
      val fieldDFToProcess = fieldDFTransformed.filter($"$ToProcess").select(Field, Fields, Moves)

      val countNotToProcess = fieldDFNotToProcess.count()

      fieldToProcess.unpersist()

      if (countNotToProcess > 0) {
        saveSolution(fieldDFNotToProcess)
      }

      val countToProcess = fieldDFToProcess.count()

      if (countToProcess > 0) {

        val countPartitions = fieldDFToProcess.rdd.getNumPartitions

        val fieldDFToProcessRepartitioned =
          if (countToProcess / countPartitions > 100000)
            fieldDFToProcess.repartition((countToProcess / 100000).toInt, $"$Sha")
          else
            fieldDFToProcess

        saveFieldToProcess(fieldDFToProcessRepartitioned.drop($"$Sha"))

        fieldDFTransformed.unpersist()

        findSolutionsTailRec()

      }

    }

    def saveSolution(solutionsDF: DataFrame) = {

      solutionsDF
        .withColumn(InitialField, typedLit(field))
        .select(InitialField, Field, Moves, Solved)
        .write
        .mode("append")
        .orc(SolutionStage)

    }

    def saveFieldToProcess(fieldToProcess: DataFrame) = {
      fieldToProcess
        .write
        .mode("overwrite")
        .orc(FieldToProcess)
    }

    val fieldDF = Seq(
      (field, Seq(), Seq())
    ).toDF(Field, Fields, Moves)
      .withColumn(Sha, sha(Field, stackSize))
      .withColumn(
        Fields,
        concat(array($"$Sha"), $"$Fields")
      )

    findSolutionsTailRec(fieldDF)

  }
}
