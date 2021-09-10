package org.example

package object task1 {
  // clickstream data columns names
  val COL_USER_ID = "userId"
  val COL_EVENT_ID = "eventId"
  val COL_EVENT_TYPE = "eventType"
  val COL_EVENT_TIME = "eventTime"
  val COL_ATTRIBUTES = "attributes"

  // purchases data columns names
  val COL_PURCHASE_ID = "purchaseId"
  val COL_PURCHASE_TIME = "purchaseTime"
  val COL_BILLING_COST = "billingCost"
  val COL_IS_CONFIRMED = "isConfirmed"

  // Build Purchases Attribution Projection additional columns names
  val COL_SESSION_ID = "sessionId"
  val COL_CAMPAIGN_ID = "campaignId"
  val COL_CHANNEL_ID = "channelId"
}
