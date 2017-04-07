package com.yelp

/**
  * Created by goutamvenkat on 4/7/2017.
  */
class Models {
    case class Business(address: String, attrs: Array[String], business_id: String, categories: Array[String], city: String, hours: String, is_open: Int, latitude: String,
                        longitude: String,
                        name: String,
                        neighborhood: String,
                        postal_code: String,
                        review_count: Long,
                        stars: Float,
                        state: String)

    case class Review(review_id: String,
                      user_id: String,
                      business_id: String,
                      stars: Long,
                      date: String,
                      text: String,
                      useful: Long,
                      cool: Long);

}
