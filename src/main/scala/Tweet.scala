package edu.najah.bigdata

import java.sql.Timestamp

final case class Tweet(
  _id: String,
  fullText: String,
  createdAtUTC: Timestamp,
  hashTags: Seq[String],
  latitude: Double,
  longitude: Double,
  country: String,
  countryCode: String,
  placeType: String,
  placeName: String,
  retweetCount: Int
)
