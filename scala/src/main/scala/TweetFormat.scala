package org.ecdc.twitter 

import org.apache.spark.sql.types._

object schemas {
  val searchAPI = 
    StructType(Seq(
      StructField("search_metadata",
        StructType(Seq(
          StructField("completed_in", DoubleType,true)
          , StructField("count",LongType,true)
          , StructField("max_id",LongType,true)
          , StructField("max_id_str",StringType,true)
          , StructField("next_results",StringType,true)
          , StructField("query",StringType,true)
          , StructField("refresh_url",StringType,true)
          , StructField("since_id",LongType,true)
          , StructField("since_id_str",StringType,true)
          ))
        ,true
        )
      , StructField("statuses",
          ArrayType(
            StructType(Seq(
              StructField("contributors",StringType,true)
              , StructField("coordinates",
                  StructType(Seq(
                    StructField("coordinates",ArrayType(DoubleType,true),true)
                    , StructField("type",StringType,true)
                  ))
                ,true)
              , StructField("created_at",TimestampType,true)
              , StructField("entities",
                  StructType(Seq(
                    StructField("hashtags",
                      ArrayType(
                        StructType(Seq(
                          StructField("indices",ArrayType(LongType,true),true)
                          , StructField("text",StringType,true)
                        )),true
                      ),true)
                    , StructField("media"
                       ,ArrayType(
                         StructType(Seq(
                           StructField("display_url",StringType,true)
                           , StructField("expanded_url",StringType,true)
                           , StructField("id",LongType,true)
                           , StructField("id_str",StringType,true)
                           , StructField("indices",ArrayType(LongType,true),true)
                           , StructField("media_url",StringType,true)
                           , StructField("media_url_https",StringType,true)
                           , StructField("source_status_id",LongType,true)
                           , StructField("source_status_id_str",StringType,true)
                           , StructField("source_user_id",LongType,true)
                           , StructField("source_user_id_str",StringType,true)
                           , StructField("type",StringType,true)
                           , StructField("url",StringType,true)
                         )),true),true)
                    , StructField("symbols"
                       ,ArrayType(
                         StructType(Seq(
                           StructField("indices",ArrayType(LongType,true),true)
                           , StructField("text",StringType,true)
                         ))
                      ,true),true)
                    , StructField("urls"
                       ,ArrayType(
                          StructType(Seq(
                            StructField("display_url",StringType,true)
                            , StructField("expanded_url",StringType,true)
                            , StructField("indices",ArrayType(LongType,true),true)
                            , StructField("url",StringType,true)
                          ))
                       ,true),true)
                    , StructField("user_mentions"
                      ,ArrayType(
                        StructType(Seq(StructField("id",LongType,true)
                        , StructField("id_str",StringType,true)
                        , StructField("indices",ArrayType(LongType,true),true)
                        , StructField("name",StringType,true)
                        , StructField("screen_name",StringType,true)
                        ))
                      ,true),true)
                    ))
                 ,true)
              , StructField("favorite_count",LongType,true)
              , StructField("favorited",BooleanType,true)
              , StructField("id",LongType,true)
              , StructField("id_str",StringType,true)
              , StructField("in_reply_to_screen_name",StringType,true)
              , StructField("in_reply_to_status_id",LongType,true)
              , StructField("in_reply_to_status_id_str",StringType,true)
              , StructField("in_reply_to_user_id",LongType,true)
              , StructField("in_reply_to_user_id_str",StringType,true)
              , StructField("is_quote_status",BooleanType,true)
              , StructField("lang",StringType,true)
              , StructField("metadata",
                  StructType(Seq(
                    StructField("iso_language_code",StringType,true)
                    , StructField("result_type",StringType,true)
                  ))
                  ,true)
              , StructField("place",
                  StructType(Seq(
                    StructField("bounding_box",
                      StructType(Seq(
                        StructField("coordinates",ArrayType(ArrayType(ArrayType(DoubleType,true),true),true),true)
                        , StructField("type",StringType,true)
                      )),true
                    )
                    , StructField("contained_within",ArrayType(StringType,true),true)
                    , StructField("country",StringType,true)
                    , StructField("country_code",StringType,true)
                    , StructField("full_name",StringType,true)
                    , StructField("id",StringType,true)
                    , StructField("name",StringType,true)
                    , StructField("place_type",StringType,true)
                    , StructField("url",StringType,true)
                    )),true)
              , StructField("possibly_sensitive",BooleanType,true)
              , StructField("quoted_status",
                  StructType(Seq(
                    StructField("contributors",StringType,true)
                  , StructField("coordinates",
                      StructType(Seq(
                        StructField("coordinates",ArrayType(DoubleType,true),true)
                        , StructField("type",StringType,true)
                      ))
                    ,true)
                    , StructField("created_at",TimestampType,true)
                    , StructField("entities",
                      StructType(Seq(
                        StructField("hashtags",ArrayType(
                          StructType(Seq(
                            StructField("indices",ArrayType(LongType,true),true)
                            , StructField("text",StringType,true))),true),true)
                        , StructField("media"
                          ,ArrayType(
                             StructType(Seq(
                               StructField("display_url",StringType,true)
                               , StructField("expanded_url",StringType,true)
                               , StructField("id",LongType,true)
                               , StructField("id_str",StringType,true)
                               , StructField("indices",ArrayType(LongType,true),true)
                               , StructField("media_url",StringType,true)
                               , StructField("media_url_https",StringType,true)
                               , StructField("source_status_id",LongType,true)
                               , StructField("source_status_id_str",StringType,true)
                               , StructField("source_user_id",LongType,true)
                               , StructField("source_user_id_str",StringType,true)
                               , StructField("type",StringType,true), StructField("url",StringType,true)
                             ))
                           ,true),true)
                        , StructField("symbols"
                           , ArrayType(
                              StructType(Seq(
                                StructField("indices",ArrayType(LongType,true),true)
                                , StructField("text",StringType,true)
                              ))
                           ,true),true)
                        , StructField("urls"
                          , ArrayType(
                             StructType(Seq(
                               StructField("display_url",StringType,true)
                               , StructField("expanded_url",StringType,true)
                               , StructField("indices",ArrayType(LongType,true),true)
                               , StructField("url",StringType,true))),true
                            )
                          ,true)
                        , StructField("user_mentions"
                          , ArrayType(
                             StructType(Seq(
                               StructField("id",LongType,true)
                               , StructField("id_str",StringType,true)
                               , StructField("indices",ArrayType(LongType,true),true)
                               , StructField("name",StringType,true)
                               , StructField("screen_name",StringType,true)
                             ))
                             ,true
                           ),true
                          )
                        )),true)
                    , StructField("favorite_count",LongType,true)
                    , StructField("favorited",BooleanType,true)
                    , StructField("id",LongType,true)
                    , StructField("id_str",StringType,true)
                    , StructField("in_reply_to_screen_name",StringType,true)
                    , StructField("in_reply_to_status_id",LongType,true)
                    , StructField("in_reply_to_status_id_str",StringType,true)
                    , StructField("in_reply_to_user_id",LongType,true)
                    , StructField("in_reply_to_user_id_str",StringType,true)
                    , StructField("is_quote_status",BooleanType,true)
                    , StructField("lang",StringType,true)
                    , StructField("metadata",
                        StructType(Seq(
                          StructField("iso_language_code",StringType,true)
                          , StructField("result_type",StringType,true)
                        ))
                        ,true
                      )
                    , StructField("place",
                        StructType(Seq(
                          StructField("bounding_box",
                            StructType(Seq(
                              StructField("coordinates",ArrayType(ArrayType(ArrayType(DoubleType,true),true),true),true)
                              , StructField("type",StringType,true))),true
                            )
                          , StructField("contained_within",ArrayType(StringType,true),true)
                          , StructField("country",StringType,true)
                          , StructField("country_code",StringType,true)
                          , StructField("full_name",StringType,true)
                          , StructField("id",StringType,true)
                          , StructField("name",StringType,true)
                          , StructField("place_type",StringType,true)
                          , StructField("url",StringType,true)
                        ))
                        ,true
                      )
                    , StructField("possibly_sensitive",BooleanType,true)
                    , StructField("quoted_status_id",LongType,true)
                    , StructField("quoted_status_id_str",StringType,true)
                    , StructField("retweet_count",LongType,true)
                    , StructField("retweeted",BooleanType,true)
                    , StructField("scopes",StructType(Seq(StructField("followers",BooleanType,true))),true)
                    , StructField("source",StringType,true)
                    , StructField("text",StringType,true)
                    , StructField("truncated",BooleanType,true)
                    , StructField("user",
                        StructType(Seq(
                          StructField("contributors_enabled",BooleanType,true)
                          , StructField("created_at",TimestampType,true)
                          , StructField("default_profile",BooleanType,true)
                          , StructField("default_profile_image",BooleanType,true)
                          , StructField("description",StringType,true)
                          , StructField("entities",
                              StructType(Seq(
                                StructField("description",
                                  StructType(Seq(
                                    StructField("urls"
                                      ,ArrayType(
                                        StructType(Seq(
                                          StructField("display_url",StringType,true)
                                          , StructField("expanded_url",StringType,true)
                                          , StructField("indices",ArrayType(LongType,true),true)
                                          , StructField("url",StringType,true)
                                     )),true),true))),true
                                )
                              , StructField("url",
                                  StructType(Seq(
                                    StructField("urls",ArrayType(
                                      StructType(Seq(
                                        StructField("display_url",StringType,true)
                                        , StructField("expanded_url",StringType,true)
                                        , StructField("indices",ArrayType(LongType,true),true)
                                        , StructField("url",StringType,true)
                                      )),true),true))),true))),true
                            )
                          , StructField("favourites_count",LongType,true)
                          , StructField("follow_request_sent",BooleanType,true)
                          , StructField("followers_count",LongType,true)
                          , StructField("following",BooleanType,true)
                          , StructField("friends_count",LongType,true)
                          , StructField("geo_enabled",BooleanType,true)
                          , StructField("has_extended_profile",BooleanType,true)
                          , StructField("id",LongType,true)
                          , StructField("id_str",StringType,true)
                          , StructField("is_translation_enabled",BooleanType,true)
                          , StructField("is_translator",BooleanType,true)
                          , StructField("lang",StringType,true)
                          , StructField("listed_count",LongType,true)
                          , StructField("location",StringType,true)
                          , StructField("name",StringType,true)
                          , StructField("notifications",BooleanType,true)
                          , StructField("profile_background_color",StringType,true)
                          , StructField("profile_background_image_url",StringType,true)
                          , StructField("profile_background_image_url_https",StringType,true)
                          , StructField("profile_background_tile",BooleanType,true)
                          , StructField("profile_banner_url",StringType,true)
                          , StructField("profile_image_url",StringType,true)
                          , StructField("profile_image_url_https",StringType,true)
                          , StructField("profile_link_color",StringType,true)
                          , StructField("profile_sidebar_border_color",StringType,true)
                          , StructField("profile_sidebar_fill_color",StringType,true)
                          , StructField("profile_text_color",StringType,true)
                          , StructField("profile_use_background_image",BooleanType,true)
                          , StructField("protected",BooleanType,true)
                          , StructField("screen_name",StringType,true)
                          , StructField("statuses_count",LongType,true)
                          , StructField("time_zone",StringType,true)
                          , StructField("translator_type",StringType,true)
                          , StructField("url",StringType,true)
                          , StructField("utc_offset",StringType,true)
                          , StructField("verified",BooleanType,true)
                        ))
                        ,true
                      )
                    ))
                  ,true
                  )


              , StructField("quoted_status_id",LongType,true)
              , StructField("quoted_status_id_str",StringType,true)
              , StructField("retweet_count",LongType,true)
              , StructField("retweeted",BooleanType,true)
              , StructField("retweeted_status"
                ,StructType(Seq(
                  StructField("contributors",StringType,true)
                  , StructField("coordinates",
                      StructType(Seq(
                        StructField("coordinates",ArrayType(DoubleType,true),true)
                        , StructField("type",StringType,true)
                        )),true
                    )
                  , StructField("created_at",TimestampType,true)
                  , StructField("favorite_count",LongType,true)
                  , StructField("favorited",BooleanType,true)
                  , StructField("id",LongType,true)
                  , StructField("id_str",StringType,true)
                  , StructField("in_reply_to_screen_name",StringType,true)
                  , StructField("in_reply_to_status_id",LongType,true)
                  , StructField("in_reply_to_status_id_str",StringType,true)
                  , StructField("in_reply_to_user_id",LongType,true)
                  , StructField("in_reply_to_user_id_str",StringType,true)
                  , StructField("is_quote_status",BooleanType,true)
                  , StructField("lang",StringType,true)
                  , StructField("metadata",
                      StructType(Seq(
                        StructField("iso_language_code",StringType,true)
                        , StructField("result_type",StringType,true)
                      ))
                      ,true
                    )
                  , StructField("place",
                      StructType(Seq(
                        StructField("bounding_box",
                          StructType(Seq(
                            StructField("coordinates",ArrayType(ArrayType(ArrayType(DoubleType,true),true),true),true)
                            , StructField("type",StringType,true))),true
                          )
                        , StructField("contained_within",ArrayType(StringType,true),true)
                        , StructField("country",StringType,true)
                        , StructField("country_code",StringType,true)
                        , StructField("full_name",StringType,true)
                        , StructField("id",StringType,true)
                        , StructField("name",StringType,true)
                        , StructField("place_type",StringType,true)
                        , StructField("url",StringType,true)
                      ))
                      ,true
                    )
                  , StructField("possibly_sensitive",BooleanType,true)
                  , StructField("quoted_status_id",LongType,true)
                  , StructField("quoted_status_id_str",StringType,true)
                  , StructField("retweet_count",LongType,true)
                  , StructField("retweeted",BooleanType,true)
                  , StructField("scopes",StructType(Seq(StructField("followers",BooleanType,true))),true)
                  , StructField("source",StringType,true)
                  , StructField("text",StringType,true)
                  , StructField("truncated",BooleanType,true)
                  , StructField("user",
                      StructType(Seq(
                        StructField("contributors_enabled",BooleanType,true)
                        , StructField("created_at",TimestampType,true)
                        , StructField("default_profile",BooleanType,true)
                        , StructField("default_profile_image",BooleanType,true)
                        , StructField("description",StringType,true)
                        , StructField("entities",
                            StructType(Seq(
                              StructField("description",
                                StructType(Seq(
                                  StructField("urls"
                                    ,ArrayType(
                                      StructType(Seq(
                                        StructField("display_url",StringType,true)
                                        , StructField("expanded_url",StringType,true)
                                        , StructField("indices",ArrayType(LongType,true),true)
                                        , StructField("url",StringType,true)
                                   )),true),true))),true
                              )
                            , StructField("url",
                                StructType(Seq(
                                  StructField("urls",ArrayType(
                                    StructType(Seq(
                                      StructField("display_url",StringType,true)
                                      , StructField("expanded_url",StringType,true)
                                      , StructField("indices",ArrayType(LongType,true),true)
                                      , StructField("url",StringType,true)
                                    )),true),true))),true))),true
                          )
                        , StructField("favourites_count",LongType,true)
                        , StructField("follow_request_sent",BooleanType,true)
                        , StructField("followers_count",LongType,true)
                        , StructField("following",BooleanType,true)
                        , StructField("friends_count",LongType,true)
                        , StructField("geo_enabled",BooleanType,true)
                        , StructField("has_extended_profile",BooleanType,true)
                        , StructField("id",LongType,true)
                        , StructField("id_str",StringType,true)
                        , StructField("is_translation_enabled",BooleanType,true)
                        , StructField("is_translator",BooleanType,true)
                        , StructField("lang",StringType,true)
                        , StructField("listed_count",LongType,true)
                        , StructField("location",StringType,true)
                        , StructField("name",StringType,true)
                        , StructField("notifications",BooleanType,true)
                        , StructField("profile_background_color",StringType,true)
                        , StructField("profile_background_image_url",StringType,true)
                        , StructField("profile_background_image_url_https",StringType,true)
                        , StructField("profile_background_tile",BooleanType,true)
                        , StructField("profile_banner_url",StringType,true)
                        , StructField("profile_image_url",StringType,true)
                        , StructField("profile_image_url_https",StringType,true)
                        , StructField("profile_link_color",StringType,true)
                        , StructField("profile_sidebar_border_color",StringType,true)
                        , StructField("profile_sidebar_fill_color",StringType,true)
                        , StructField("profile_text_color",StringType,true)
                        , StructField("profile_use_background_image",BooleanType,true)
                        , StructField("protected",BooleanType,true)
                        , StructField("screen_name",StringType,true)
                        , StructField("statuses_count",LongType,true)
                        , StructField("time_zone",StringType,true)
                        , StructField("translator_type",StringType,true)
                        , StructField("url",StringType,true)
                        , StructField("utc_offset",StringType,true)
                        , StructField("verified",BooleanType,true)
                      ))
                      ,true
                    )
                  )),true
                )
              , StructField("source",StringType,true)
              , StructField("text",StringType,true)
              , StructField("truncated",BooleanType,true)
              , StructField("user",
                  StructType(Seq(
                    StructField("contributors_enabled",BooleanType,true)
                    , StructField("created_at",TimestampType,true)
                    , StructField("default_profile",BooleanType,true)
                    , StructField("default_profile_image",BooleanType,true)
                    , StructField("description",StringType,true)
                    , StructField("entities",
                        StructType(Seq(
                          StructField("description",
                            StructType(Seq(
                              StructField("urls"
                                ,ArrayType(
                                  StructType(Seq(
                                    StructField("display_url",StringType,true)
                                    , StructField("expanded_url",StringType,true)
                                    , StructField("indices",ArrayType(LongType,true),true)
                                    , StructField("url",StringType,true)
                               )),true),true))),true
                          )
                        , StructField("url",
                            StructType(Seq(
                              StructField("urls",ArrayType(
                                StructType(Seq(
                                  StructField("display_url",StringType,true)
                                  , StructField("expanded_url",StringType,true)
                                  , StructField("indices",ArrayType(LongType,true),true)
                                  , StructField("url",StringType,true)
                                )),true),true))),true))),true
                      )
                    , StructField("favourites_count",LongType,true)
                    , StructField("follow_request_sent",BooleanType,true)
                    , StructField("followers_count",LongType,true)
                    , StructField("following",BooleanType,true)
                    , StructField("friends_count",LongType,true)
                    , StructField("geo_enabled",BooleanType,true)
                    , StructField("has_extended_profile",BooleanType,true)
                    , StructField("id",LongType,true)
                    , StructField("id_str",StringType,true)
                    , StructField("is_translation_enabled",BooleanType,true)
                    , StructField("is_translator",BooleanType,true)
                    , StructField("lang",StringType,true)
                    , StructField("listed_count",LongType,true)
                    , StructField("location",StringType,true)
                    , StructField("name",StringType,true)
                    , StructField("notifications",BooleanType,true)
                    , StructField("profile_background_color",StringType,true)
                    , StructField("profile_background_image_url",StringType,true)
                    , StructField("profile_background_image_url_https",StringType,true)
                    , StructField("profile_background_tile",BooleanType,true)
                    , StructField("profile_banner_url",StringType,true)
                    , StructField("profile_image_url",StringType,true)
                    , StructField("profile_image_url_https",StringType,true)
                    , StructField("profile_link_color",StringType,true)
                    , StructField("profile_sidebar_border_color",StringType,true)
                    , StructField("profile_sidebar_fill_color",StringType,true)
                    , StructField("profile_text_color",StringType,true)
                    , StructField("profile_use_background_image",BooleanType,true)
                    , StructField("protected",BooleanType,true)
                    , StructField("screen_name",StringType,true)
                    , StructField("statuses_count",LongType,true)
                    , StructField("time_zone",StringType,true)
                    , StructField("translator_type",StringType,true)
                    , StructField("url",StringType,true)
                    , StructField("utc_offset",StringType,true)
                    , StructField("verified",BooleanType,true)
                    , StructField("withheld_in_countries",ArrayType(StringType,true),true)
                  ))
                  ,true
                )
              , StructField("withheld_in_countries",ArrayType(StringType,true),true)
              ))
            ,true
          )
          ,true
        )
    ))
    val geoLocationSchema = StructType(Seq(
       StructField("geo_code",StringType,true)
       , StructField("geo_country_code",StringType,true)
       , StructField("geo_id",LongType,true)
       , StructField("geo_latitude",DoubleType,true)
       , StructField("geo_longitude",DoubleType,true)
       , StructField("geo_name",StringType,true)
       , StructField("geo_type",StringType,true)
      ))

    val geoLocatedSchema = {
      StructType(Seq(
        StructField("file",StringType,true)
        , StructField("topic",StringType,true)
        , StructField("id",LongType,true)
        , StructField("is_geo_located",BooleanType,true)
        , StructField("lang",StringType,true)
        , StructField("linked_place_full_name_loc", geoLocationSchema,true)
        , StructField("linked_text_loc", geoLocationSchema,true)
        , StructField("place_full_name_loc", geoLocationSchema,true)
        , StructField("text_loc", geoLocationSchema,true)
        , StructField("user_description_loc", geoLocationSchema,true)
        , StructField("user_location_loc", geoLocationSchema,true)
        ))
    }
    val geoLocatedTweetSchema = {
      StructType(Seq(
        StructField("topic",StringType,true)
        , StructField("id",LongType,true)
        , StructField("created_date",StringType,true)
        , StructField("text",StringType,true)
        , StructField("linked_text",StringType,true)
        , StructField("user_description",StringType,true)
        , StructField("linked_user_description",StringType,true)
        , StructField("is_geo_located",BooleanType,true)
        , StructField("is_retweet",BooleanType,true)
        , StructField("lang",StringType,true)
        , StructField("linked_lang",StringType,true)
        , StructField("screen_name",StringType,true)
        , StructField("user_name",StringType,true)
        , StructField("user_id",LongType,true)
        , StructField("user_location",StringType,true)
        , StructField("linked_screen_name",StringType,true)
        , StructField("linked_user_name",StringType,true)
        , StructField("linked_user_location",StringType,true)
        , StructField("created_at",TimestampType,true)
        , StructField("tweet_longitude",DoubleType,true)  
        , StructField("tweet_latitude",DoubleType,true)  
        , StructField("linked_longitude",DoubleType,true)
        , StructField("linked_latitude" ,DoubleType,true)
        , StructField("place_type",StringType,true) 
        , StructField("place_name",StringType,true) 
        , StructField("place_full_name" ,StringType,true)
        , StructField("linked_place_full_name" ,StringType,true)
        , StructField("place_country_code"  ,StringType,true)   
        , StructField("place_country"  ,StringType,true)        
        , StructField("place_longitude",DoubleType,true)       
        , StructField("place_latitude",DoubleType,true)        
        , StructField("linked_place_longitude",DoubleType,true)
        , StructField("linked_place_latitude",DoubleType,true) 
        , StructField("linked_text_loc", geoLocationSchema,true)
        , StructField("linked_place_full_name_loc", geoLocationSchema,true)
        , StructField("place_full_name_loc", geoLocationSchema,true)
        , StructField("text_loc", geoLocationSchema,true)
        , StructField("user_description_loc", geoLocationSchema,true)
        , StructField("user_location_loc", geoLocationSchema,true)
        , StructField("hashtags", ArrayType(StringType, true),true)
        , StructField("urls", ArrayType(StringType, true),true)
        , StructField("entities", ArrayType(StringType, true),true)
        , StructField("contexts", ArrayType(StringType, true),true)
        ))
    }
    val toIgnoreSchema = {
      StructType(Seq(
        StructField("file",StringType,true)
        , StructField("topic",StringType,true)
        , StructField("id",LongType,true)
        ))
    }
}
