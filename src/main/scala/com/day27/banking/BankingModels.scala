package com.day27.banking

case class Transaction(
  transactionId: String,
  accountId: String,
  branchId: String,
  amount: Double,
  transactionType: String,
  eventTime: Long
)

case class Branch(branchId: String, branchName: String, city: String, riskLevel: String)

case class RiskReference(accountId: String, riskScore: Int, riskCategory: String)

object BankingModels {
  def parseTransaction(line: String): Option[Transaction] = {
    val p = line.split(",", -1).map(_.trim)
    if (p.length != 6 || p(0).equalsIgnoreCase("transaction_id")) None
    else {
      try {
        Some(Transaction(
          p(0), p(1), p(2), p(3).toDouble, p(4), p(5).toLong
        ))
      } catch {
        case _: NumberFormatException => None
      }
    }
  }
}
