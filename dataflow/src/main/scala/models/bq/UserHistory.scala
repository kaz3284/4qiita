package models.bq

import models.Action

/**
  *
  * */
case class UserHistory(unitId: Long, userId: String, actionHistory: List[Action])