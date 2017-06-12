package models.bq

/**
  *
  * */
case class UserHistory(unitId: Long, userId: String, actionHistory: List[Action])