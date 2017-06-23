package models

import scalikejdbc._

case class Segment(id: Long, unitId: Int, actionId: Long, frequency: Int, private val symbol: Int) {

  // symbolの値に応じたFreqSymbol(MORE_THAN, EQUAL, LESS_THAN)
  private val freqSymbol = FreqSymbol.valueOf(symbol)

  /**
    * @param count
    * */
  def isFill(count: Option[Int]): Boolean = freqSymbol.generateCondition(frequency)(count.getOrElse(0))
}

object Segment extends SQLSyntaxSupport[Segment] {
  override val tableName = "segment"
  private val s = syntax("s")

  def apply(column: ResultName[Segment])(rs: WrappedResultSet): Segment = {
    Segment(rs.long(column.id), rs.int(column.unitId), rs.long(column.actionId), rs.int(column.frequency), rs.int(column.symbol))
  }
}


trait FreqSymbol {
  def id: Int
  def generateCondition(frequency: Int): Int => Boolean
}

object FreqSymbol {

  // frequencyがvalue以上か判定
  case object MORE_THAN extends FreqSymbol {
    def id = 1
    def generateCondition(frequency: Int) = (count: Int) => count >= frequency
  }


  // frequencyがvalueと等しいか判定
  case object EQUAL extends FreqSymbol {
    def id = 2
    def generateCondition(frequency: Int) = (count: Int) => count == frequency
  }

  // frequencyがvalue以下か判定
  case object LESS_THAN extends FreqSymbol {
    def id = 3
    def generateCondition(frequency: Int) = (count: Int) => count <= frequency
  }


  private val symbols = Seq(MORE_THAN, EQUAL, LESS_THAN)
  private val freqSymbolMap = symbols.map(freqSymbol => (freqSymbol.id, freqSymbol)).toMap

  def valueOf(id: Int): FreqSymbol = freqSymbolMap(id)
}