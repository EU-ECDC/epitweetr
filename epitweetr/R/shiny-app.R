#' @title Run the epitweetr Shiny app
#' @description Open the epitweetr Shiny app, used to setup the Data collection & processing pipeline, the Requirements & alerts pipeline and to visualise the outputs. 
#' @param data_dir Path to the 'data directory' containing application settings, models and collected tweets.
#' If not provided the system will try to reuse the existing one from last session call of \code{\link{setup_config}} or use the EPI_HOME environment variable, default: NA
#' @return The Shiny server object containing the launched application
#' @details The epitweetr app is the user entry point to the epitweetr package. This application will help the user to setup the tweet collection process, manage all settings, 
#' see the interactive dashboard visualisations, export them to Markdown or PDF, and setup the alert emails.
#'
#' All its functionality is described on the epitweetr vignette.
#' @examples 
#' if(FALSE){
#'    #Running the epitweetr app
#'    library(epitweetr)
#'    message('Please choose the epitweetr data directory')
#'    setup_config(file.choose())
#'    epitweetr_app()
#' }
#' @seealso 
#'  \code{\link{search_loop}}
#' 
#'  \code{\link{detect_loop}}
#' @rdname epitweetr_app
#' @export 
#' @importFrom shiny fluidPage fluidRow column selectInput h4 conditionalPanel dateRangeInput radioButtons checkboxInput sliderInput numericInput downloadButton h3 htmlOutput actionButton span textInput textAreaInput h2 passwordInput h5 fileInput uiOutput navbarPage tabPanel observe updateSliderInput updateDateRangeInput downloadHandler invalidateLater renderText observeEvent renderUI validate need shinyApp
#' @importFrom plotly plotlyOutput renderPlotly ggplotly config layout
#' @importFrom DT dataTableOutput renderDataTable datatable formatPercentage replaceData dataTableProxy
#' @importFrom rmarkdown render
#' @importFrom magrittr `%>%`
#' @importFrom stringr str_replace_all
#' @importFrom grDevices png
#' @importFrom ggplot2 ggsave
#' @importFrom dplyr select
#' @importFrom stats setNames 
#' @importFrom utils write.csv head 
epitweetr_app <- function(data_dir = NA) { 
  # Setting up configuration if not already done
  if(is.na(data_dir) )
    setup_config_if_not_already()
  else
    setup_config(data_dir)
  # Loading data for dashboard and configuration
  d <- refresh_dashboard_data() 
  cd <- refresh_config_data()

  # Defining dashboard page UI
  ################################################
  ######### DASHBOARD PAGE #######################
  ################################################
  dashboard_page <- 
    shiny::fluidPage(
      shiny::fluidRow(
        shiny::column(3, 
          ################################################
          ######### DASHBOARD FILTERS #####################
          ################################################
          shiny::actionButton("run_dashboard", "Run"),
          shiny::selectInput("topics", label = shiny::h4("Topics"), multiple = FALSE, choices = d$topics),
          shiny::selectInput("countries", label = shiny::h4("Countries & regions"), multiple = TRUE, choices = d$countries),
          shiny::selectInput(
            "fixed_period", 
            label = shiny::h4("Period"), 
            multiple = FALSE, 
            choices = list(
              "Last 7 days"="last 7 days", 
              "Last 30 days"="last 30 days", 
              "Last 60 days"="last 60 days", 
              "Last 180 days"="last 180 days", 
              "custom"
            ), 
            selected = d$fixed_period
          ),
          shiny::conditionalPanel(
            condition = "input.fixed_period == 'custom'",
            shiny::dateRangeInput("period", label = shiny::h4("Dates"), start = d$date_start, end = d$date_end, min = d$date_min, max = d$date_max, format = "yyyy-mm-dd", startview = "month")
          ), 
          shiny::radioButtons("period_type", label = shiny::h4("Time unit"), choices = list("Days"="created_date", "Weeks"="created_weeknum"), selected = "created_date", inline = TRUE),
          shiny::h4("Include retweets/quotes"),
	        shiny::checkboxInput("with_retweets", label = NULL, value = conf$alert_with_retweets),
          shiny::radioButtons("location_type", label = shiny::h4("Location type"), choices = list("Tweet"="tweet", "User"="user","Both"="both" ), selected = "tweet", inline = TRUE),
          shiny::sliderInput("alpha_filter", label = shiny::h4("Signal false positive rate"), min = 0, max = 0.3, value = conf$alert_alpha, step = 0.005),
          shiny::sliderInput("alpha_outlier_filter", label = shiny::h4("Outlier false positive rate"), min = 0, max = 0.3, value = conf$alert_alpha_outlier, step = 0.005),
          shiny::sliderInput("k_decay_filter", label = shiny::h4("Outlier downweight strength"), min = 0, max = 10, value = conf$alert_k_decay, step = 0.5),
          shiny::h4("Bonferroni correction"),
	        shiny::checkboxInput("bonferroni_correction", label = NULL, value = conf$alert_with_bonferroni_correction),
          shiny::numericInput("history_filter", label = shiny::h4("Days in baseline"), value = conf$alert_history),
          shiny::h4("Same weekday baseline"),
	        shiny::checkboxInput("same_weekday_baseline", label = NULL, value = conf$alert_same_weekday_baseline),
          shiny::fluidRow(
            shiny::column(4, 
              shiny::downloadButton("export_pdf", "PDF")
            ),
            shiny::column(4, 
              shiny::downloadButton("export_md", "Md")
            )
          )
        ), 
        shiny::column(9, 
          ################################################
          ######### DASHBOARD PLOTS #######################
          ################################################
          shiny::fluidRow(
            shiny::column(12, 
              shiny::fluidRow(
                shiny::column(2, shiny::downloadButton("download_line_data", "Data")),
                shiny::column(2, shiny::downloadButton("export_line", "image"))
		          ),
              plotly::plotlyOutput("line_chart")
            )
          ),
          shiny::fluidRow(
            shiny::column(6, 
              shiny::fluidRow(
                shiny::column(3, shiny::downloadButton("download_map_data", "Data")),
                shiny::column(3, shiny::downloadButton("export_map", "image"))
		          ),
              plotly::plotlyOutput(
                "map_chart" 
              )
            ),
            shiny::column(6, 
              shiny::fluidRow(
                shiny::column(3, shiny::downloadButton("download_top1_data", "Data")),
                shiny::column(3, shiny::downloadButton("export_top1", "image")),
                shiny::column(6, 
                  shiny::radioButtons("top_type1", label = NULL, choices = list(
                      "Hashtags"="hashtags", 
                      "Top words"="topwords" 
                    ), selected = "hashtags", inline = TRUE
                  ),
		            )
              ),
              plotly::plotlyOutput("top_chart1")
            )
          ),
          shiny::fluidRow(
            shiny::column(6, 
              shiny::fluidRow(
                shiny::column(3, shiny::downloadButton("download_top2_data", "Data")),
                shiny::column(3, shiny::downloadButton("export_top2", "image")),
                shiny::column(6, 
                  shiny::radioButtons("top_type2", label = NULL, choices = list(
                      "Entities"="entities", 
                      "Contexts" = "contexts"
                    ), selected = "entities", inline = TRUE
                  ),
		            )
              ),
              plotly::plotlyOutput("top_chart2")
            ),
            shiny::column(6, 
              shiny::fluidRow(
                shiny::column(3, shiny::downloadButton("download_top3_data", "Data")),
              ),
              shiny::fluidRow(
                shiny::column(12,
                  shiny::htmlOutput("top_table_title")
                )
              ),
              shiny::fluidRow(
                shiny::column(12,
                  DT::dataTableOutput("top_table")
                )
              ),
              shiny::fluidRow(
                shiny::column(12,
                  shiny::htmlOutput("top_table_disc")
                )
              )
            )
          )
        )
      )
    ) 
  # Defining alerts page UI
  ################################################
  ######### ALERTS PAGE ##########################
  ################################################
  alerts_page <- 
    shiny::fluidPage(
          shiny::h3("Find alerts"),
          ################################################
          ######### ALERTS FILTERS #######################
          ################################################
          shiny::fluidRow(
            shiny::column(3, 
              shiny::h4("Detection date") 
            ),
            shiny::column(2, 
              shiny::h4("Topics")
            ),
            shiny::column(2, 
              shiny::h4("Countries & regions")
            ),
            shiny::column(2, 
              shiny::h4("Display")
            ),
            shiny::column(1, 
              shiny::h4("Limit")
            ),
          ), 
          shiny::fluidRow(
            shiny::column(3, 
              shiny::dateRangeInput("alerts_period", label = "", start = d$date_end, end = d$date_end, min = d$date_min,max = d$date_max, format = "yyyy-mm-dd", startview = "month") 
            ),
            shiny::column(2, 
              shiny::selectInput("alerts_topics", label = NULL, multiple = TRUE, choices = d$topics[d$topics!=""])
            ),
            shiny::column(2, 
              shiny::selectInput("alerts_countries", label = NULL, multiple = TRUE, choices = d$countries)
            ),
            shiny::column(2, 
              shiny::radioButtons("alerts_display", label = NULL, choices = list("Tweets"="tweets", "Parameters"="parameters"), selected = "parameters", inline = TRUE),
            ),
            shiny::column(1, 
              shiny::selectInput("alerts_limit", label = NULL, multiple = FALSE, choices =  list("None"="0", "10"="10", "50"="50", "100"="100", "500"="500"))
            ),
          ), 
          shiny::fluidRow(
            ################################################
            ######### ALERTS FILTERS #######################
            ################################################
            shiny::column(2,
              shiny::actionButton("alerts_search", "Search alerts"),
            ),
            shiny::column(2,
              shiny::actionButton("alerts_close", "Hide search"),
              shiny::conditionalPanel(
                condition = "false",
                shiny::textInput("alerts_show_search", value = "false", label = NULL)
              )
            ),
            shiny::column(2, shiny::actionButton("alertsdb_add", "Add alerts to annotations")),
            shiny::column(6)
          ), 
          shiny::fluidRow(
            shiny::column(12, 
              ################################################
              ######### ALERTS TABLE #########################
              ################################################
              shiny::conditionalPanel(
                condition = "input.alerts_show_search == 'true'",
                DT::dataTableOutput("alerts_table"),
              )
          )),
          shiny::h3("Alerts annotations"),
          shiny::fluidRow(
            shiny::column(2, shiny::actionButton("alertsdb_search", "Show annotations")),
            shiny::column(2, shiny::actionButton("alertsdb_close", "Hide annotations")),
            shiny::column(2, shiny::downloadButton("alertsdb_download", "Download annotations")),
            shiny::column(4, shiny::fileInput("alertsdb_upload", label = NULL, buttonLabel = "Upload & evaluate annotations")),
            shiny::column(2,
              shiny::conditionalPanel(
                condition = "false",
                shiny::textInput("alertsdb_show", value = "false", label = NULL)
              )
            )
          ),
          shiny::fluidRow(
            shiny::conditionalPanel(
              condition = "input.alertsdb_show == 'true'",
              shiny::column(12, 
                ################################################
                ######### ALERT ANNOTATIONS EVALUATION #########
                ################################################ 
                shiny::h4("Performance evaluation of alert classification algorithm"),
                DT::dataTableOutput("alertsdb_runs_table"),
                ################################################
                ######### ANNOTATED ALERTS #####################
                ################################################ 
                shiny::h4("Database used for training the alert classification algorithm"),
                DT::dataTableOutput("alertsdb_table")
            ))
         )
       
  ) 
  # Defining configuration
  ################################################
  ######### CONFIGURATION PAGE####################
  ################################################
  config_page <- 
    shiny::fluidPage(
      shiny::fluidRow(
        shiny::column(4, 
          ################################################
          ######### STATUS PANEL #########################
          ################################################
          shiny::h3("Status"),
          shiny::fluidRow(
            shiny::column(4, "epitweetr database"), 
            shiny::column(4, shiny::htmlOutput("fs_running")),
            shiny::column(4, shiny::actionButton("activate_fs", "activate"))
          ),
          shiny::fluidRow(
            shiny::column(4, "Data collection & processing"), 
            shiny::column(4, shiny::htmlOutput("search_running")),
            shiny::column(4, shiny::actionButton("activate_search", "activate"))
          ),
          shiny::fluidRow(
            shiny::column(4, "Requirements & alerts"), 
            shiny::column(4, shiny::htmlOutput("detect_running")),
            shiny::column(4, shiny::actionButton("activate_detect", "activate"))
          ),
          ################################################
          ######### GENERAL PROPERTIES ###################
          ################################################
          shiny::h3("Signal detection"),
          shiny::fluidRow(shiny::column(3, "Signal false positive rate"), shiny::column(9, shiny::sliderInput("conf_alpha", label = NULL, min = 0, max = 0.3, value = conf$alert_alpha, step = 0.005))),
          shiny::fluidRow(shiny::column(3, "Outlier false positive rate"), shiny::column(9, shiny::sliderInput("conf_alpha_outlier", label = NULL, min = 0, max = 0.3, value = conf$alert_alpha_outlier, step = 0.005))),
          shiny::fluidRow(shiny::column(3, "Outlier downweight strength"), shiny::column(9, shiny::sliderInput("conf_k_decay", label = NULL, min = 0, max = 10, value = conf$alert_k_decay, step = 0.5))),
          shiny::fluidRow(shiny::column(3, "Days in baseline"), shiny::column(9, shiny::numericInput("conf_history", label = NULL , value = conf$alert_history))),
          shiny::fluidRow(shiny::column(3, "Same weekday baseline"), shiny::column(9, shiny::checkboxInput("conf_same_weekday_baseline", label = NULL , value = conf$alert_same_weekday_baseline))),
          shiny::fluidRow(shiny::column(3, "Include retweets/quotes"), shiny::column(9, shiny::checkboxInput("conf_with_retweets", label = NULL , value = conf$alert_with_retweets))),
          shiny::fluidRow(
            shiny::column(3, "Bonferroni correction"), 
            shiny::column(9, shiny::checkboxInput("conf_with_bonferroni_correction", label = NULL , value = conf$alert_with_bonferroni_correction))
          ),
          shiny::h3("General"),
          shiny::fluidRow(shiny::column(3, "Data dir"), shiny::column(9, shiny::span(conf$data_dir))),
          shiny::fluidRow(shiny::column(3, "Data collection & processing span (min)"), shiny::column(9, shiny::numericInput("conf_collect_span", label = NULL, value = conf$collect_span))), 
          shiny::fluidRow(shiny::column(3, "Requirements & alerts span (min)"), shiny::column(9, shiny::numericInput("conf_schedule_span", label = NULL, value = conf$schedule_span))), 
          shiny::fluidRow(shiny::column(3, "Launch slots"), shiny::column(9, shiny::htmlOutput("conf_schedule_slots"))), 
          shiny::fluidRow(shiny::column(3, "Password store"), shiny::column(9, 
            shiny::selectInput(
              "conf_keyring", 
              label = NULL, 
              choices = c("wincred", "macos", "file", "secret_service", "environment"), 
              selected = conf$keyring
            ))), 
          shiny::fluidRow(shiny::column(3, "Spark cores"), shiny::column(9, shiny::numericInput("conf_spark_cores", label = NULL, value = conf$spark_cores))) ,
          shiny::fluidRow(shiny::column(3, "Spark memory"), shiny::column(9, shiny::textInput("conf_spark_memory", label = NULL, value = conf$spark_memory))), 
          shiny::fluidRow(shiny::column(3, "Geolocation threshold"), shiny::column(9, shiny::textInput("geolocation_threshold", label = NULL, value = conf$geolocation_threshold))),
          shiny::fluidRow(shiny::column(3, "GeoNames URL"), shiny::column(9, shiny::textInput("conf_geonames_url", label = NULL, value = conf$geonames_url))),
          shiny::fluidRow(shiny::column(3, "Simplified GeoNames"), shiny::column(9, shiny::checkboxInput("conf_geonames_simplify", label = NULL, value = conf$geonames_simplify))),
          shiny::fluidRow(shiny::column(3, "Stream database results"), shiny::column(9, shiny::checkboxInput("conf_onthefly_api", label = NULL, value = conf$onthefly_api))),
          shiny::fluidRow(shiny::column(3, "Maven repository"), shiny::column(9,  shiny::textInput("conf_maven_repo", label = NULL, value = conf$maven_repo))),
          shiny::conditionalPanel(
            condition = "false",
            shiny::textInput("os_type", value = .Platform$OS.type, label = NULL)
          ),
          shiny::conditionalPanel(
            condition = "input.os_type == 'windows'",
            shiny::fluidRow(shiny::column(3, "Winutils URL"), shiny::column(9,  shiny::textInput("conf_winutils_url", label = NULL, value = conf$winutils_url)))
          ),
          shiny::fluidRow(shiny::column(3, "Region disclaimer"), shiny::column(9, shiny::textAreaInput("conf_regions_disclaimer", label = NULL, value = conf$regions_disclaimer))),
          shiny::h2("Twitter authentication"),
          shiny::fluidRow(shiny::column(3, "Mode"), shiny::column(9
            , shiny::radioButtons(
              "twitter_auth"
              , label = NULL
              , choices = list("User account" = "delegated", "App" = "app")
              , selected = if(cd$app_auth) "app" else "delegated" 
              ))
          ),
          shiny::conditionalPanel(
            condition = "input.twitter_auth == 'delegated'",
            shiny::fluidRow(shiny::column(12, "When choosing 'Twitter account' authentication you will have to use your Twitter credentials to authorize the Twitter application for the rtweet package (https://rtweet.info/) to access Twitter on your behalf (full rights provided).")), 
            shiny::fluidRow(shiny::renderText("&nbsp;")), 
            shiny::fluidRow(shiny::column(12, "DISCLAIMER: rtweet has no relationship with epitweetr and you have to evaluate by yourself if the provided security framework fits your needs.")),
            shiny::fluidRow(shiny::renderText("&nbsp;")) 
          ),
          shiny::conditionalPanel(
            condition = "input.twitter_auth == 'app'",
            shiny::fluidRow(shiny::column(3, "App name"), shiny::column(9, shiny::textInput("twitter_app", label = NULL, value = if(is_secret_set("app")) get_secret("app") else NULL))), 
            shiny::fluidRow(shiny::column(3, "API key"), shiny::column(9, shiny::passwordInput("twitter_api_key", label = NULL, value = if(is_secret_set("api_key")) get_secret("api_key") else NULL))),
            shiny::fluidRow(shiny::column(3, "API secret"), shiny::column(9, shiny::passwordInput("twitter_api_secret", label = NULL, value = if(is_secret_set("api_secret")) get_secret("api_secret") else NULL))), 
            shiny::fluidRow(shiny::column(3, "Access token"), shiny::column(9, 
              shiny::passwordInput("twitter_access_token", label = NULL, value = if(is_secret_set("access_token")) get_secret("access_token") else NULL))
            ), 
            shiny::fluidRow(shiny::column(3, "Token secret"), shiny::column(9, 
              shiny::passwordInput("twitter_access_token_secret", label = NULL, value = if(is_secret_set("access_token_secret")) get_secret("access_token_secret") else NULL))
            )
          ), 
          shiny::conditionalPanel(
            condition = "input.twitter_auth == 'delegated'",
            shiny::fluidRow(
              shiny::column(3, "Twitter API version"), 
              shiny::column(9, "Only v1.1 is supported for user authentication")
            )
          ),
          shiny::conditionalPanel(
            condition = "input.twitter_auth == 'app'",
            shiny::fluidRow(
              shiny::column(3, "Twitter API version"), 
              shiny::column(9, 
                shiny::checkboxGroupInput(
                  "conf_api_version"
                  , label = NULL
                  , choices = list("v1.1" = "1.1", "V2" = "2")
                  , selected = conf$api_version
                  , inline = T 
                )
              )
            )
          ),
          shiny::h2("Email authentication (SMTP)"),
          shiny::fluidRow(shiny::column(3, "Server"), shiny::column(9, shiny::textInput("smtp_host", label = NULL, value = conf$smtp_host))), 
          shiny::fluidRow(shiny::column(3, "Port"), shiny::column(9, shiny::numericInput("smtp_port", label = NULL, value = conf$smtp_port))), 
          shiny::fluidRow(shiny::column(3, "From"), shiny::column(9, shiny::textInput("smtp_from", label = NULL, value = conf$smtp_from))), 
          shiny::fluidRow(shiny::column(3, "Login"), shiny::column(9, shiny::textInput("smtp_login", label = NULL, value = conf$smtp_login))), 
          shiny::fluidRow(shiny::column(3, "Password"), shiny::column(9, shiny::passwordInput("smtp_password", label = NULL, value = conf$smtp_password))), 
          shiny::fluidRow(shiny::column(3, "Unsafe certificates"), shiny::column(9, shiny::checkboxInput("smtp_insecure", label = NULL, value = conf$smtp_insecure))), 
          shiny::fluidRow(shiny::column(3, "Admin email"), shiny::column(9, shiny::textInput("admin_email", label = NULL, value = conf$admin_email))), 
          shiny::h2("Task registering"),
          shiny::fluidRow(shiny::column(3, "Custom date format"), shiny::column(9, shiny::textInput("force_date_format", label = NULL, value = conf$force_date_format))), 
          shiny::actionButton("save_properties", "Save settings")
        ), 
        shiny::column(8,
          ################################################
          ######### Requirements & alerts PANEL ######################
          ################################################
          shiny::h3("Requirements & alerts pipeline"),
          shiny::h5("Manual tasks"),
          shiny::fluidRow(
            shiny::column(3, shiny::actionButton("update_dependencies", "Run dependencies")),
            shiny::column(3, shiny::actionButton("update_geonames", "Run GeoNames")),
            shiny::column(3, shiny::actionButton("update_languages", "Run languages")),
            shiny::column(3, shiny::actionButton("request_alerts", "Run alerts"))
          ),
          DT::dataTableOutput("tasks_df"),
          ################################################
          ######### TOPICS PANEL ######################
          ################################################
          shiny::h3("Topics"),
          shiny::fluidRow(
            shiny::column(3, shiny::h5("Available topics")),
            shiny::column(2, shiny::downloadButton("conf_topics_download", "Download")),
            shiny::column(2, shiny::downloadButton("conf_orig_topics_download", "Download default")),
            shiny::column(3, shiny::fileInput("conf_topics_upload", label = NULL, buttonLabel = "Upload")),
            shiny::column(2, shiny::actionButton("conf_dismiss_past_tweets", "Dismiss past tweets"))
          ),
          shiny::fluidRow(
            shiny::column(4, shiny::h5("Limit topic history"))
          ),
          DT::dataTableOutput("config_topics"),
          ################################################
          ######### LANGUAGES PANEL ######################
          ################################################
          shiny::h3("Languages"),
          shiny::fluidRow(
            shiny::column(4, shiny::h5("Available languages")),
            shiny::column(2, shiny::downloadButton("conf_lang_download", "Download")),
            shiny::column(2, shiny::downloadButton("conf_orig_lang_download", "Download default")),
            shiny::column(4, shiny::fileInput("conf_lang_upload", label = NULL , buttonLabel = "Upload")),
          ),
          shiny::fluidRow(
            shiny::column(4, shiny::h5("Active languages")),
            shiny::column(6, shiny::uiOutput("lang_items_0")),
            shiny::column(1, shiny::actionButton("conf_lang_add", "+")),
            shiny::column(1, shiny::actionButton("conf_lang_remove", "-")),
          ),
          DT::dataTableOutput("config_langs"),
          ################################################
          ######### IMPORTANT USERS ######################
          ################################################
          shiny::h3("Important users"),
          shiny::fluidRow(
            shiny::column(4, shiny::h5("User file")),
            shiny::column(2, shiny::downloadButton("conf_users_download", "Download")),
            shiny::column(2, shiny::downloadButton("conf_orig_users_download", "Download default")),
            shiny::column(4, shiny::fileInput("conf_users_upload", label = NULL , buttonLabel = "Upload")),
          ),
          ################################################
          ######### SUSCRIBERS PANEL #####################
          ################################################
          shiny::h3("Subscribers"),
          shiny::fluidRow(
            shiny::column(4, shiny::h5("Subscribers")),
            shiny::column(2, shiny::downloadButton("conf_subscribers_download", "Download")),
            shiny::column(2, shiny::downloadButton("conf_orig_subscribers_download", "Download default")),
            shiny::column(4, shiny::fileInput("conf_subscribers_upload", label = NULL, buttonLabel = "Upload")),
          ),
          DT::dataTableOutput("config_subscribers"),
          ################################################
          ######### COUNTRIES / REGIONS PANEL ############
          ################################################
          shiny::h3("Countries & regions"),
          shiny::fluidRow(
            shiny::column(4, shiny::h5("Countries & regions")),
            shiny::column(2, shiny::downloadButton("conf_countries_download", "Download")),
            shiny::column(2, shiny::downloadButton("conf_orig_countries_download", "Download defaults")),
            shiny::column(4, shiny::fileInput("conf_countries_upload", label = NULL, buttonLabel = "Upload")),
          ),
          DT::dataTableOutput("config_regions")
        ))
  ) 
  # Defining geo tuning page UI
  ################################################
  ######### GEOTRAINING PAGE #####################
  ################################################
  geotraining_page <- 
    shiny::fluidPage(
          shiny::h3("Geo tagging training"),
          shiny::fluidRow(
            ################################################
            ######### GEO TAG FILTERS ######################
            ################################################
            shiny::column(4, shiny::numericInput("geotraining_tweets2add", label = shiny::h4("Tweets to add"), value = 100)),
            shiny::column(2, shiny::actionButton("geotraining_update", "Geolocate annotations")),
            shiny::column(2, shiny::downloadButton("geotraining_download", "Download annotations")),
            shiny::column(4, shiny::fileInput("geotraining_upload", label = NULL, buttonLabel = "Upload & evaluate annotations"))
          ), 
          shiny::fluidRow(
            shiny::column(12, 
              ################################################
              ######### GEO TAG EVALUATION####################
              ################################################ 
              shiny::h4("Performance evaluation of geo tagging algorithm (finding location position in text)"),
              DT::dataTableOutput("geotraining_eval_df"),
              ################################################
              ######### GEO TAG TABLE #########################
              ################################################ 
              shiny::h4("Database used for training the geo tagging algorithm"),
              DT::dataTableOutput("geotraining_table")
          ))
  ) 
  ################################################
  ######### DATA PROTECTION PAGE #################
  ################################################
  dataprotection_page <- 
    shiny::fluidPage(
          shiny::h3("Data protection"),
          shiny::fluidRow(
            ################################################
            ######### TWEET SEARCH FILTERS #################
            ################################################
            shiny::column(2, shiny::h4("Topic")),
            shiny::column(2, shiny::h4("Period")),
            shiny::column(2, shiny::h4("Countries & regions")),
            shiny::column(2, 
              shiny::h4("Users"),
              shiny::radioButtons("data_mode", label = NULL, choices = list("Mentioning"="mentioning", "From User"="users", "Both"="all"), selected = "all", inline = TRUE),
            ),
            shiny::column(1, shiny::h4("Limit")),
            shiny::column(3, shiny::h4("Action"))
          ), 
          shiny::fluidRow(
            shiny::column(2, shiny::selectInput("data_topics", label = NULL, multiple = TRUE, choices = d$topics[d$topics!=""])),
            shiny::column(2, shiny::dateRangeInput("data_period", label = "", start = d$date_end, end = d$date_end, min = d$date_min,max = d$date_max, format = "yyyy-mm-dd", startview = "month")),
            shiny::column(2, shiny::selectInput("data_countries", label = NULL, multiple = TRUE, choices = d$countries)),
            shiny::column(2, shiny::textInput("data_users", label = NULL, value = NULL)),
            shiny::column(1, 
              shiny::selectInput("data_limit", label = NULL, multiple = FALSE, choices =  list("50"="50", "100"="100", "500"="500"), selected = "50")
            ),
            shiny::column(3, 
              shiny::actionButton("data_search", "Search"),
              shiny::actionButton("data_search_ano", "Anonym Search"),
              shiny::actionButton("data_anonymise", "Anonymize"),
              shiny::actionButton("data_delete", "Delete"),
            ),
          ), 
          shiny::fluidRow(
            shiny::column(12,
              shiny::htmlOutput("data_message")
          )),
          shiny::fluidRow(
            shiny::column(12, 
              ################################################
              ######### TWEET SEARCH RESULTS #################
              ################################################ 
              DT::dataTableOutput("data_search_df")
          ))
  ) 
  ################################################
  ######### TROUBLESHOOT PAGE ####################
  ################################################
  troubleshoot_page <- 
    shiny::fluidPage(
          shiny::h3("Diagnostics"),
          shiny::h5("Automated diagnostic tasks"),
          shiny::fluidRow(
            shiny::column(12, 
              shiny::actionButton("run_diagnostic", "run diagnostics")
            )
          ),
          shiny::fluidRow(
            shiny::column(12, 
              ################################################
              ######### DIAGNOSTIC TABLE #####################
              ################################################
              DT::dataTableOutput("diagnostic_table")
          ))
  ) 
  
  # Defining navigation UI
  ui <- 
    shiny::navbarPage("epitweetr"
      , shiny::tabPanel("Dashboard", dashboard_page)
      , shiny::tabPanel("Alerts", alerts_page)
      , shiny::tabPanel("Geotag", geotraining_page)
      , shiny::tabPanel("Data protection", dataprotection_page)
      , shiny::tabPanel("Configuration", config_page)
      , shiny::tabPanel("Troubleshoot", troubleshoot_page)
    )


  # Defining line chart from shiny app filters
  line_chart_from_filters <- function(topics, countries, period_type, period, with_retweets, location_type, alpha, alpha_outlier, k_decay, no_history, bonferroni_correction, same_weekday_baseline) {
    trend_line(
      topic = topics
      ,countries= if(length(countries) == 0) c(1) else as.integer(countries)
      ,date_type= period_type
      ,date_min = period[[1]]
      ,date_max = period[[2]]
      ,with_retweets= with_retweets
      ,location_type = location_type
      ,alpha = alpha
      ,alpha_outlier = alpha_outlier
      ,k_decay = k_decay
      ,no_historic = no_history
      ,bonferroni_correction = bonferroni_correction
      ,same_weekday_baseline = same_weekday_baseline
    )
    
  }
  # Defining line chart from shiny app filters
  map_chart_from_filters <- function(topics, countries, period, with_retweets, location_type) {
    create_map(
      topic= topics
      ,countries= if(length(countries) == 0) c(1) else as.integer(countries)
      ,date_min = period[[1]]
      ,date_max = period[[2]]
      ,with_retweets= with_retweets
      ,location_type = location_type
      ,caption = conf$regions_disclaimer
      ,forplotly = TRUE 
    )
    
  }
  # Defining top words chart from shiny app filters
  top_chart_from_filters <- function(topics, serie, fcountries, period, with_retweets, location_type, top) {
    fcountries= if(length(fcountries) == 0 || 1 %in%fcountries) c(1) else as.integer(fcountries)
    regions <- get_country_items()
    countries <- Reduce(function(l1, l2) {unique(c(l1, l2))}, lapply(fcountries, function(i) unlist(regions[[i]]$codes)))
    create_topchart(
      topic= topics
      ,serie = serie     
      ,country_codes = countries
      ,date_min = period[[1]]
      ,date_max = period[[2]]
      ,with_retweets= with_retweets
      ,location_type = location_type
      ,top
    )
    
  }
  # Rmarkdown dasboard export bi writing the dashboard on the provided file$
  # it uses the markdown template inst/rmarkdown/dashboard.Rmd
  export_dashboard <- function(
    format, 
    file, 
    topics, 
    countries, 
    period_type, 
    period, 
    with_retweets, 
    location_type, 
    alpha, 
    alpha_outlier,
    k_decay, 
    no_historic, 
    bonferroni_correction, 
    same_weekday_baseline, 
    top_type1,
    top_type2
    ) {
    tryCatch({
        progress_start("Exporting dashboard") 
        r <- rmarkdown::render(
          system.file("rmarkdown", "dashboard.Rmd", package=get_package_name()), 
          output_format = format, 
          output_file = file,
          params = list(
            "topics" = topics
            , "countries" = countries
            , "period_type" = period_type
            , "period" = period
            , "with_retweets"= with_retweets
            , "location_type" = location_type
            , "alert_alpha" = alpha
            , "alert_alpha_outlier" = alpha_outlier
            , "alert_k_decay" = k_decay
            , "alert_historic" = no_historic
            , "bonferroni_correction" = bonferroni_correction
            , "same_weekday_baseline" = same_weekday_baseline
            , "top_type1" = top_type1
            , "top_type2" = top_type2
          ),
          quiet = TRUE
        )
        progress_close()
        r
      },
      error = function(w) {app_error(w, env = cd)}
    )
  }
  
  # Defining server logic
  server <- function(input, output, session, ...) {
    `%>%` <- magrittr::`%>%`
    ################################################
    ######### FILTERS LOGIC ########################
    ################################################
    
    # updating alpha filter based on selected topic value
    shiny::observe({
      # reading input$topics will automatically trigger the update on change 
      val <- {
      if(length(input$topics)==0 || input$topics == "") 
        conf$alert_alpha
      else
        unname(get_topics_alphas()[stringr::str_replace_all( input$topics, "%20", " ")])
      }
      shiny::updateSliderInput(session, "alpha_filter", value = val, min = 0, max = 0.3, step = 0.005)
    })
    
    # upadating outliers alpha filter based on selected topic value
    shiny::observe({
      # reading input$topics will automatically trigger the update on change 
      val <- {
      if(length(input$topics)==0 || input$topics == "") 
        conf$alert_alpha_outlier
      else
        unname(get_topics_alpha_outliers()[stringr::str_replace_all( input$topics, "%20", " ")])
      }
      shiny::updateSliderInput(session, "alpha_outlier_filter", value = val, min = 0, max = 0.3, step = 0.005)
    })

    # update the date ranges based on type of range selected
    shiny::observe({
      # reading input$fixed_period will automatically trigger the update on change 
      # refresh dashboard data is necessary to update active dates
      refresh_dashboard_data(d, input$fixed_period)
      shiny::updateDateRangeInput(session, "period", start = d$date_start, end = d$date_end, min = d$date_min,max = d$date_max)
    }) 
    
    ################################################
    ######### DASHBOARD LOGIC ######################
    ################################################
    shiny::observeEvent(input$run_dashboard, {
      # rendering line chart
      rep = new.env()
      progress_start("Generating report", rep) 
      output$line_chart <- plotly::renderPlotly({
         # Validate if minimal requirements for rendering are met 
         progress_set(value = 0.15, message = "Generating line chart", rep)
         shiny::isolate({
           can_render(input, d)
	         # getting the chart
           chart <- line_chart_from_filters(
             input$topics, 
             input$countries, 
             input$period_type, 
             input$period, 
             input$with_retweets, 
             input$location_type , 
             input$alpha_filter, 
             input$alpha_outlier_filter, 
             input$k_decay_filter, 
             input$history_filter, 
             input$bonferroni_correction,
             input$same_weekday_baseline
           )$chart
           
           # Setting size based on container size
           height <- session$clientData$output_line_chart_height
           width <- session$clientData$output_line_chart_width
	         
           # returning empty chart if no data is found on chart
           chart_not_empty(chart)
           
           # transforming chart on plotly
           gg <- plotly::ggplotly(chart, height = height, width = width, tooltip = c("label")) %>% plotly::config(displayModeBar = FALSE) 
           
           # Fixing bad entries on ggplotly chart
           for(i in 1:length(gg$x$data)) {
             if(startsWith(gg$x$data[[i]]$name, "(") && endsWith(gg$x$data[[i]]$name, ")")) 
               gg$x$data[[i]]$name = gsub("\\(|\\)|,|[0-9]", "", gg$x$data[[i]]$name) 
             else 
               gg$x$data[[i]]$name = "                        "
             gg$x$data[[i]]$legendgroup = gsub("\\(|\\)|,|[0-9]", "", gg$x$data[[i]]$legendgroup)
           }
           gg
        })
      }) 
       
      # rendering map chart
      output$map_chart <- plotly::renderPlotly({
         # Validate if minimal requirements for rendering are met 
         progress_set(value = 0.30, message = "Generating map chart", rep)
         
         shiny::isolate({
           can_render(input, d)
           # Setting size based on container size
           height <- session$clientData$output_map_chart_height
           width <- session$clientData$output_map_chart_width
           
           # getting the chart 
           chart <- map_chart_from_filters(input$topics, input$countries, input$period, input$with_retweets, input$location_type)$chart
           
           # returning empty chart if no data is found on chart
	         chart_not_empty(chart)
           
           # transforming chart on plotly
           gg <- chart %>%
             plotly::ggplotly(height = height, width = width, tooltip = c("label")) %>% 
	           plotly::layout(
               title=list(text= paste("<b>", chart$labels$title, "</b>")), 
               margin = list(l = 5, r=5, b = 50, t = 80),
               annotations = list(
                 text = chart$labels$caption,
                 font = list(size = 10),
                 showarrow = FALSE,
                 xref = 'paper', 
                 x = 0,
                 yref = 'paper', 
                 y = -0.15),
               legend = list(orientation = 'h', x = 0.5, y = 0.08)
             ) %>%
             plotly::config(displayModeBar = FALSE) 
      
             # Fixing bad entries on ggplotly chart
             for(i in 1:length(gg$x$data)) {
               if(substring(gg$x$data[[i]]$name, 1, 2) %in% c("a.", "b.", "c.", "d.", "e.", "f.", "g.", "h."))
                 gg$x$data[[i]]$name <- substring(gg$x$data[[i]]$name, 4, nchar(gg$x$data[[i]]$name)) 
                 #gg$x$data[[i]]$legendgroup <- gsub("\\(|\\)|,|[0-9]", "", gg$x$data[[i]]$legendgroup)
             }
             gg
         })
      })

      output$top_chart1 <- plotly::renderPlotly({
         # Validate if minimal requirements for rendering are met 
         progress_set(value = 0.45, message = "Generating top chart 1", rep)
         it <- input$top_type1 #this is for allow refreshing the report on radio button update
         shiny::isolate({
           can_render(input, d)
           # Setting size based on container size
           height <- session$clientData$output_top_chart1_height
           width <- session$clientData$output_top_chart1_width
           
           # getting the chart 
           chart <- top_chart_from_filters(input$topics, input$top_type1, input$countries, input$period, input$with_retweets, input$location_type, 20)$chart
           
           # returning empty chart if no data is found on chart
	         chart_not_empty(chart)

           # transforming chart on plotly
           chart %>%
             plotly::ggplotly(height = height, width = width) %>% 
	           plotly::layout(
               title=list(text= paste("<b>", chart$labels$title, "<br>", chart$labels$subtitle), "</b>"), 
               margin = list(l = 30, r=30, b = 100, t = 80),
               annotations = list(
                 text = chart$labels$caption,
                 font = list(size = 10),
                 showarrow = FALSE,
                 xref = 'paper', 
                 x = 0,
                 yref = 'paper', 
                 y = -0.4)
             ) %>%
	           plotly::config(displayModeBar = FALSE)
         })
      })

      output$top_chart2 <- plotly::renderPlotly({
         # Validate if minimal requirements for rendering are met 
         progress_set(value = 0.7, message = "Generating top chart 2", rep)
         it <- input$top_type2 #this is for allow refreshing the report on radio button update
         shiny::isolate({
           can_render(input, d)
           # Setting size based on container size
           height <- session$clientData$output_top_chart2_height
           width <- session$clientData$output_top_chart2_width
           
           # getting the chart 
           chart <- top_chart_from_filters(input$topics, input$top_type2, input$countries, input$period, input$with_retweets, input$location_type, 20)$chart
           
           # returning empty chart if no data is found on chart
	         chart_not_empty(chart)

           # transforming chart on plotly
           chart %>%
             plotly::ggplotly(height = height, width = width) %>% 
	           plotly::layout(
               title=list(text= paste("<b>", chart$labels$title, "<br>", chart$labels$subtitle), "</b>"), 
               margin = list(l = 30, r=30, b = 100, t = 80),
               annotations = list(
                 text = chart$labels$caption,
                 font = list(size = 10),
                 showarrow = FALSE,
                 xref = 'paper', 
                 x = 0,
                 yref = 'paper', 
                 y = -0.4)
             ) %>%
	           plotly::config(displayModeBar = FALSE)
         })
      })

      output$top_table_title <- shiny::isolate({shiny::renderText({paste("<h4>Top URLS of tweets mentioning", input$topics, "from", input$period[[1]], "to", input$period[[2]],"</h4>")})})
      output$top_table <- DT::renderDataTable({
          # Validate if minimal requirements for rendering are met 
          progress_set(value = 0.85, message = "Generating top links", rep)
          
          shiny::isolate({
            can_render(input, d)
            # Setting size based on container size
            height <- session$clientData$output_top_chart2_height
            width <- session$clientData$output_top_chart2_width
            
            # getting the chart to obtain the table 
            chart <- top_chart_from_filters(input$topics, "urls", input$countries, input$period, input$with_retweets, input$location_type, 200)$chart
            
            # returning empty if no data is found on chart
	          chart_not_empty(chart)

            data <- chart$data %>%
              dplyr::mutate(top = paste("<a href=\"",.data$top, "\"  target=\"_blank\">", .data$top, "</a>")) %>%
              dplyr::rename(Url = .data$top, Frequency = .data$frequency) %>%
            DT::datatable(escape = FALSE)

          })
      })
      
      output$top_table_disc <- shiny::isolate({shiny::renderText({
         progress_close(rep)
        "<br/><br/>Top urls table only considers tweet location, ignoring the location type parameter"
      })})
      
    })
    # Saving line chart as png and downloading it from shiny app
    output$export_line <- shiny::downloadHandler(
      filename = function() { 
        paste("line_dataset_", 
          "_", paste(input$topics, collapse="-"), 
          "_", paste(input$countries, collapse="-"), 
          "_", input$period[[1]], 
          "_", input$period[[2]],
          ".png", 
          sep = ""
        )
      },
      content = function(file) { 
        chart <-
          line_chart_from_filters(
            input$topics, 
            input$countries, 
            input$period_type, 
            input$period, 
            input$with_retweets, 
            input$location_type, 
            input$alpha_filter, 
            input$alpha_outlier_filter, 
            input$k_decay_filter, 
            input$history_filter,
            input$bonferroni_correction,
            input$same_weekday_baseline
            )$chart
        device <- function(..., width, height) grDevices::png(..., width = width, height = height, res = 300, units = "in")
        ggplot2::ggsave(file, plot = chart, device = device) 
      }
    ) 
    # Saving line chart as CSV and downloading it from shiny app
    output$download_line_data <- shiny::downloadHandler(
      filename = function() { 
        paste("line_dataset_", 
          "_", paste(input$topics, collapse="-"), 
          "_", paste(input$countries, collapse="-"), 
          "_", input$period[[1]], 
          "_", input$period[[2]],
          ".csv", 
          sep = ""
        )
      },
      content = function(file) { 
        write.csv(
          line_chart_from_filters(
            input$topics, 
            input$countries, 
            input$period_type, 
            input$period, 
            input$with_retweets, 
            input$location_type, 
            input$alpha_filter, 
            input$alpha_outlier_filter, 
            input$k_decay_filter, 
            input$history_filter,
            input$bonferroni_correction,
            input$same_weekday_baseline
            )$data,
          file, 
          row.names = FALSE)
      }
    )
     
    # Saving map chart as CSV and downloading it from shiny app
    output$download_map_data <- shiny::downloadHandler(
      filename = function() { 
        paste("map_dataset_", 
          "_", paste(input$topics, collapse="-"), 
          "_", paste(input$countries, collapse="-"), 
          "_", input$period[[1]], 
          "_", input$period[[2]],
          ".csv", 
          sep = ""
        )
      },
      content = function(file) { 
        write.csv(
          map_chart_from_filters(input$topics, input$countries, input$period, input$with_retweets, input$location_type)$data,
          file, 
          row.names = FALSE)
      }
    )
     
    # Saving map chart as png and downloading it from shiny app
    output$export_map <- shiny::downloadHandler(
      filename = function() { 
        paste("map_dataset_", 
          "_", paste(input$topics, collapse="-"), 
          "_", paste(input$countries, collapse="-"), 
          "_", input$period[[1]], 
          "_", input$period[[2]],
          ".png", 
          sep = ""
        )
      },
      content = function(file) { 
	      chart <- map_chart_from_filters(input$topics, input$countries, input$period, input$with_retweets, input$location_type)$chart
        device <- function(..., width, height) grDevices::png(..., width = width, height = height, res = 300, units = "in")
        ggplot2::ggsave(file, plot = chart, device = device) 
        
      }
    )
     
    # Saving top chart 1 as CSV and downloading it from shiny app
    output$download_top1_data <- shiny::downloadHandler(
      filename = function() { 
        paste("top_dataset_",input$top_type1, 
          "_", paste(input$topics, collapse="-"), 
          "_", paste(input$countries, collapse="-"), 
          "_", input$period[[1]], 
          "_", input$period[[2]],
          ".csv", 
          sep = ""
        )
      },
      content = function(file) { 
        write.csv(
          top_chart_from_filters(input$topics, input$top_type1, input$countries, input$period, input$with_retweets, input$location_type, 200)$data,
          file, 
          row.names = FALSE)
      }
    ) 
    # Saving top chart 2 as CSV and downloading it from shiny app
    output$download_top2_data <- shiny::downloadHandler(
      filename = function() { 
        paste("top_dataset_",input$top_type2, 
          "_", paste(input$topics, collapse="-"), 
          "_", paste(input$countries, collapse="-"), 
          "_", input$period[[1]], 
          "_", input$period[[2]],
          ".csv", 
          sep = ""
        )
      },
      content = function(file) { 
        write.csv(
          top_chart_from_filters(input$topics, input$top_type2, input$countries, input$period, input$with_retweets, input$location_type, 200)$data,
          file, 
          row.names = FALSE)
      }
    ) 
    # Saving top table as CSV and downloading it from shiny app
    output$download_top3_data <- shiny::downloadHandler(
      filename = function() { 
        paste("top_dataset_", "urls" , 
          "_", paste(input$topics, collapse="-"), 
          "_", paste(input$countries, collapse="-"), 
          "_", input$period[[1]], 
          "_", input$period[[2]],
          ".csv", 
          sep = ""
        )
      },
      content = function(file) { 
        write.csv(
          top_chart_from_filters(input$topics, "urls", input$countries, input$period, input$with_retweets, input$location_type, 200)$data,
          file, 
          row.names = FALSE)
      }
    ) 
    
    # Saving top chart 1 as png and downloading it from shiny app
    output$export_top1 <- shiny::downloadHandler(
      filename = function() { 
        paste("top_",input$top_type1, 
          "_", paste(input$topics, collapse="-"), 
          "_", paste(input$countries, collapse="-"), 
          "_", input$period[[1]], 
          "_", input$period[[2]],
          ".png", 
          sep = ""
        )
      },
      content = function(file) { 
        chart <-
          top_chart_from_filters(input$topics, input$top_type1, input$countries, input$period, input$with_retweets, input$location_type, 50)$chart
        device <- function(..., width, height) grDevices::png(..., width = width, height = height, res = 300, units = "in")
        ggplot2::ggsave(file, plot = chart, device = device) 
      }
    )
    
    # Saving top chart 2 as png and downloading it from shiny app
    output$export_top2 <- shiny::downloadHandler(
      filename = function() { 
        paste("top_",input$top_type2,
          "_", paste(input$topics, collapse="-"), 
          "_", paste(input$countries, collapse="-"), 
          "_", input$period[[1]], 
          "_", input$period[[2]],
          ".png", 
          sep = ""
        )
      },
      content = function(file) { 
        chart <-
          top_chart_from_filters(input$topics, input$top_type2, input$countries, input$period, input$with_retweets, input$location_type, 50)$chart
        device <- function(..., width, height) grDevices::png(..., width = width, height = height, res = 300, units = "in")
        ggplot2::ggsave(file, plot = chart, device = device) 
      }
    )
    
    # Saving dashboard as PDF downloading it from shiny app
    output$export_pdf <- shiny::downloadHandler(
      filename = function() { 
        paste("epitweetr_dashboard_", 
          "_", paste(input$topics, collapse="-"), 
          "_", paste(input$countries, collapse="-"), 
          "_", input$period[[1]], 
          "_", input$period[[2]],
          "_", input$top_type1,
          "_", input$top_type2,
          ".pdf", 
          sep = ""
        )
      },
      content = function(file) { 
         export_dashboard(
           "pdf_document", 
           file, 
           input$topics, 
           input$countries, 
           input$period_type, 
           input$period, 
           input$with_retweets, 
           input$location_type, 
           input$alpha_filter, 
           input$alpha_outlier_filter, 
           input$k_decay_filter, 
           input$history_filter,
           input$bonferroni_correction,
           input$same_weekday_baseline,
           input$top_type1,
           input$top_type2
         )
      }
    ) 
    # Saving dashboard as markdown downloading it from shiny app
    output$export_md <- shiny::downloadHandler(
      filename = function() { 
        paste("epitweetr_dashboard_", 
          "_", paste(input$topics, collapse="-"), 
          "_", paste(input$countries, collapse="-"), 
          "_", input$period[[1]], 
          "_", input$period[[2]],
          "_", input$top_type1,
          "_", input$top_type2,
          ".md", 
          sep = ""
        )
      },
      content = function(file) { 
         export_dashboard(
           "md_document",
           file, 
           input$topics, 
           input$countries, 
           input$period_type, 
           input$period, 
           input$with_retweets, 
           input$location_type, 
           input$alpha_filter, 
           input$alpha_outlier_filter, 
           input$k_decay_filter, 
           input$history_filter,
           input$bonferroni_correction,
           input$same_weekday_baseline,
           input$top_type1,
           input$top_type2
         )
      }
    )
    ################################################
    ######### CONFIGURATION LOGIC ##################
    ################################################
    
    ######### STATUS PANEL LOGIC ##################
    
    # Timer for updating task statuses 
    # each ten seconds config data will be reloaded to capture changes from data collection & processing and requirements & alerts pipelines
    # each ten seconds a process refresh flag will be invalidated to trigger process status recalculation
    shiny::observe({
      # Setting the timer
      shiny::invalidateLater(10000)
      
      # updating config data
      refresh_config_data(cd, list("tasks", "topics", "langs"))
      
      # invalidating the process refresh flag
      cd$process_refresh_flag(Sys.time())
    }) 

    # rendering the Data collection & processing running status
    output$search_running <- shiny::renderText({
      # Adding a dependency to task refresh (each time a task has changed by the Requirements & alerts pipeline)
      cd$tasks_refresh_flag()
      # Adding a dependency to process update (each 10 seconds)
      cd$process_refresh_flag()

      # updating the label
      paste(
        "<span",
        " style='color:", if(cd$search_running) "#348017'" else "#F75D59'",
        ">",
        if(is.na(cd$search_diff)) "Stopped"
	      else paste(
          if(cd$search_running) "Running" else "Stopped"
          , "("
          , round(cd$search_diff, 2)
          , units(cd$search_diff)
          , "ago)" 
          ),
        "</span>"
        ,sep=""
    )})

    # rendering the fs running status
    output$fs_running <- shiny::renderText({
      # Adding a dependency to task refresh (each time a task has changed by the Requirements & alerts pipeline)
      cd$tasks_refresh_flag()
      # Adding a dependency to process update (each 10 seconds)
      cd$process_refresh_flag()

      # updating the label
      paste(
        "<span",
        " style='color:", if(cd$fs_running) "#348017'" else "#F75D59'",
        ">",
        if(cd$fs_running) "Running" else "Stopped",
        "</span>"
        ,sep=""
    )})

    # rendering the Requirements & alerts running status
    output$detect_running <- shiny::renderText({
      # Adding a dependency to task refresh (each time a task has changed by the Requirements & alerts pipeline)
      cd$tasks_refresh_flag()
      # Adding a dependency to process update (each 10 seconds)
      cd$process_refresh_flag()
      
      # updating the label
      paste(
        "<span",
        " style='color:", if(cd$detect_running) "#348017'" else "#F75D59'",
        ">",
        if(cd$detect_running) "Running" else "Stopped",
        "</span>"
        ,sep=""
      )})

    # rendering the slots each time properties are changed
    output$conf_schedule_slots <- shiny::renderText({
      # Adding a dependency to config refresh
      cd$properties_refresh_flag()

      #rendering the label
      paste(head(lapply(get_task_day_slots(get_tasks()$alerts), function(d) strftime(d, format="%H:%M")), -1), collapse=", ")
    })

    # registering the Data collection & processing runner after button is clicked
    shiny::observeEvent(input$activate_search, {
      # registering the scheduled task
      register_search_runner_task()
      # refresh task data to check if tasks are to be updated
      refresh_config_data(cd, list("tasks"))
    })

    # registering the fs runner after button is clicked
    shiny::observeEvent(input$activate_fs, {
      # registering the scheduled task
      register_fs_runner_task()
      # refresh task data to check if tasks are to be updated
      refresh_config_data(cd, list("tasks"))
    })

    # registering the Requirements & alerts runner after button is clicked
    shiny::observeEvent(input$activate_detect, {
      # Setting values configuration to trigger one shot tasks for the first time
      if(is.na(conf$dep_updated_on)) conf$dep_updated_on <- strftime(Sys.time(), "%Y-%m-%d %H:%M:%S")
      if(is.na(conf$geonames_updated_on)) conf$geonames_updated_on <- strftime(Sys.time(), "%Y-%m-%d %H:%M:%S")
      if(is.na(conf$lang_updated_on)) conf$lang_updated_on <- strftime(Sys.time(), "%Y-%m-%d %H:%M:%S")
      # Forcing refresh of tasks
      cd$tasks_refresh_flag(Sys.time())
      # saving properties to ensure the Requirements & alerts pipeline can see the changes and start running the tasks
      save_config(data_dir = conf$data_dir, properties= TRUE, topics = FALSE)

      # registering the scheduled task
      register_detect_runner_task()
      
      # refresh task data to check if tasks are to be updated
      refresh_config_data(cd, list("tasks"))
    })

    # action to activate de dependency download tasks 
    shiny::observeEvent(input$update_dependencies, {
      # Setting values on the configuration so the Requirements & alerts pipeline know it has to launch the task
      conf$dep_updated_on <- strftime(Sys.time(), "%Y-%m-%d %H:%M:%S")
      # forcing a task refresh :TODO test if this is still necessary 
      cd$tasks_refresh_flag(Sys.time())
      # saving configuration so the Requirements & alerts pipeline will see the changes
      save_config(data_dir = conf$data_dir, properties= TRUE, topics = FALSE)
      # refreshing the tasks data
      refresh_config_data(cd, list("tasks"))
    })
   
    # action to activate the update geonames task 
    shiny::observeEvent(input$update_geonames, {
      # Setting values on the configuration so the Requirements & alerts pipeline know it has to launch the task
      conf$geonames_updated_on <- strftime(Sys.time(), "%Y-%m-%d %H:%M:%S")
      # forcing a task refresh 
      cd$tasks_refresh_flag(Sys.time())
      # saving configuration so the Requirements & alerts pipeline will see the changes
      save_config(data_dir = conf$data_dir, properties= TRUE, topics = FALSE)
      # refreshing the tasks data
      refresh_config_data(cd, list("tasks"))
    })

    # action to activate the update languages task
    shiny::observeEvent(input$update_languages, {
      # Setting values on the configuration so the Requirements & alerts pipeline know it has to launch the task
      conf$lang_updated_on <- strftime(Sys.time(), "%Y-%m-%d %H:%M:%S")
      # forcing a task refresh 
      cd$tasks_refresh_flag(Sys.time())
      # saving configuration so the Requirements & alerts pipeline will see the changes
      save_config(data_dir = conf$data_dir, properties= TRUE, topics = FALSE)
      # refreshing the tasks data
      refresh_config_data(cd, list("tasks"))
    })
    
    # action to activate the alerts task
    shiny::observeEvent(input$request_alerts, {
      # Setting values on the configuration so the Requirements & alerts pipeline know it has to launch the task
      conf$alerts_requested_on <- strftime(Sys.time(), "%Y-%m-%d %H:%M:%S")
      # forcing a task refresh 
      cd$tasks_refresh_flag(Sys.time())
      # saving configuration so the Requirements & alerts pipeline will see the changes
      save_config(data_dir = conf$data_dir, properties= TRUE, topics = FALSE)
      # refreshing the tasks data
      refresh_config_data(cd, list("tasks"))
    })

    ######### PROPERTIES LOGIC ##################
    # action to save properties
    shiny::observeEvent(input$save_properties, {
      # copying input data to conf
      conf$collect_span <- input$conf_collect_span
      conf$schedule_span <- input$conf_schedule_span
      conf$keyring <- input$conf_keyring
      conf$spark_cores <- input$conf_spark_cores 
      conf$spark_memory <- input$conf_spark_memory
      conf$onthefly_api <- input$conf_onthefly_api
      conf$geolocation_threshold <- input$geolocation_threshold 
      conf$geonames_url <- input$conf_geonames_url 
      conf$maven_repo <- input$conf_maven_repo 
      conf$winutils_url <- input$conf_winutils_url 
      conf$api_version <- input$conf_api_version
      conf$geonames_simplify <- input$conf_geonames_simplify 
      conf$regions_disclaimer <- input$conf_regions_disclaimer 
      conf$alert_alpha <- input$conf_alpha 
      conf$alert_alpha_outlier <- input$conf_alpha_outlier 
      conf$alert_k_decay <- input$conf_k_decay 
      conf$alert_history <- input$conf_history 
      conf$alert_same_weekday_baseline <- input$conf_same_weekday_baseline 
      conf$alert_with_bonferroni_corection <- input$conf_with_bonferroni_correction
      conf$alert_with_retweets <- input$conf_with_retweets
      # Setting secrets
      if(input$twitter_auth == "app") {
        set_twitter_app_auth(
          app = input$twitter_app, 
          api_key = input$twitter_api_key, 
          api_secret = input$twitter_api_secret, 
          access_token = input$twitter_access_token, 
          access_token_secret = input$twitter_access_token_secret
        )
      }
      else {
        set_twitter_app_auth(app = "", api_key = "", api_secret = "", access_token = "", access_token_secret = "")
        get_token()
      }
      conf$smtp_host <- input$smtp_host
      conf$smtp_port <- input$smtp_port
      conf$smtp_from <- input$smtp_from
      conf$smtp_login <- input$smtp_login
      conf$smtp_insecure <- input$smtp_insecure
      conf$smtp_password <- input$smtp_password
      conf$admin_email <- input$admin_email
      conf$force_date_format <- input$force_date_format

      # Saving properties.json
      if(input$twitter_auth != "app")
        conf$api_version = "1.1"
      save_config(data_dir = conf$data_dir, properties= TRUE, topics = FALSE)

      # Forcing update on properties dependant refresh (e.g. time slots)
      cd$properties_refresh_flag(Sys.time())
    })


    ######### IMPORTANT USERS LOGIC ###########
    # downloading current important users file
    output$conf_users_download <- shiny::downloadHandler(
      filename = function() "users.xlsx",
      content = function(file) { 
        file.copy(get_known_users_path(), file) 
      }
    )
    
    # downloading default important users file
    output$conf_orig_users_download <- shiny::downloadHandler(
      filename = function() "users.xlsx",
      content = function(file) { 
        file.copy(get_default_known_users_path(), file) 
      }
    )
    
    # uploading new important users
    shiny::observe({
      df <- input$conf_users_upload
      if(!is.null(df) && nrow(df)>0) {
        uploaded <- df$datapath[[1]]
        file.copy(uploaded, paste(conf$data_dir, "users.xlsx", sep = "/"), overwrite=TRUE) 
      }
    })

    ######### LANGUAGE LOGIC ##################
    # rendering languages each time a change is registered on languges
    output$config_langs <- DT::renderDataTable({
      # Adding dependency with lang refresh
      cd$langs_refresh_flag()
      DT::datatable(cd$langs)
    })
    
    # downloading current languages
    output$conf_lang_download <- shiny::downloadHandler(
      filename = function() "languages.xlsx",
      content = function(file) { 
        file.copy(get_available_languages_path(), file) 
      }
    )
    
    # downloading original languages file
    output$conf_orig_lang_download <- shiny::downloadHandler(
      filename = function() "languages.xlsx",
      content = function(file) { 
        file.copy(get_default_available_languages_path(), file) 
      }
    )
    
    # uploading new language file
    shiny::observe({
      df <- input$conf_lang_upload
      if(!is.null(df) && nrow(df)>0) {
        uploaded <- df$datapath[[1]]
        file.copy(uploaded, paste(conf$data_dir, "languages.xlsx", sep = "/"), overwrite=TRUE) 
        refresh_config_data(e = cd, limit = list("langs"))
      }
    })

    # rendering active languages when something has changed on languages
    output$lang_items_0 <- shiny::renderUI({
      # Adding a dependency to lang refresh
      cd$langs_refresh_flag()
      shiny::selectInput("lang_items", label = NULL, multiple = FALSE, choices = cd$lang_items)
    })

    # adding the current language
    shiny::observeEvent(input$conf_lang_add, {
      add_config_language(input$lang_items, cd$lang_names[input$lang_items])
      save_config(data_dir = conf$data_dir, properties= TRUE, topics = FALSE)
      refresh_config_data(e = cd, limit = list("langs"))
    })
    
    #removing a language
    shiny::observeEvent(input$conf_lang_remove, {
      remove_config_language(input$lang_items)
      save_config(data_dir = conf$data_dir, properties= TRUE, topics = FALSE)
      refresh_config_data(e = cd, limit = list("langs"))
    })
    
   
 
    
     ######### TASKS LOGIC ##################
    # rendering the tasks each time something changes in the tasks
    output$tasks_df <- DT::renderDataTable({
      # Adding dependency with tasks refresh
      cd$tasks_refresh_flag()
      DT::datatable(cd$tasks_df)
    })
    
     
    ######### TOPICS LOGIC ##################
    # rendering the topics only on first load update is done on next statement
    output$config_topics <- DT::renderDataTable({
      `%>%` <- magrittr::`%>%`
       cd$topics_df %>%
        DT::datatable(
          colnames = c("Query length" = "QueryLength", "Active plans" = "ActivePlans",  "Progress" = "Progress", "Requests" = "Requests", "Signal alpha (FPR)" = "Alpha", "Outlier alpha (FPR)" = "OutliersAlpha"),
          filter = "top",
          escape = TRUE
        ) %>%
        DT::formatPercentage(columns = c("Progress"))
    })
    # this is for updating the topics
    shiny::observe({
      # Adding a dependency to topics refresh or plans refresh
      cd$topics_refresh_flag()
      cd$plans_refresh_flag()
      DT::replaceData(DT::dataTableProxy('config_topics'), cd$topics_df)
       
    }) 

    # download the current topics file 
    output$conf_topics_download <- shiny::downloadHandler(
      filename = function() "topics.xlsx",
      content = function(file) { 
        file.copy(get_topics_path(), file) 
      }
    )

    #download the original topics file
    output$conf_orig_topics_download <- shiny::downloadHandler(
      filename = function() "topics.xlsx",
      content = function(file) { 
        file.copy(get_default_topics_path(), file) 
      }
    )

    # uploading a new topics file
    shiny::observe({
      df <- input$conf_topics_upload
      if(!is.null(df) && nrow(df)>0) {
        uploaded <- df$datapath[[1]]
        file.copy(uploaded, paste(conf$data_dir, "topics.xlsx", sep = "/"), overwrite=TRUE) 
        refresh_config_data(e = cd, limit = list("topics"))
      }
    }) 
    # action to dismiss past tweets
    shiny::observeEvent(input$conf_dismiss_past_tweets, {
      # Setting values on the configuration so the search loop knows history needs to be dismissed
      conf$dismiss_past_request <- strftime(Sys.time(), "%Y-%m-%d %H:%M:%S")
      # saving configuration so the Requirements & alerts pipeline will see the changes
      save_config(data_dir = conf$data_dir, properties= TRUE, topics = FALSE)
      # refreshing the tasks data
    })

    ######### SUBSCRIBERS LOGIC ##################
    # rendering subscribers (first run update on next statement)
    output$config_subscribers <- DT::renderDataTable({
      `%>%` <- magrittr::`%>%`
      get_subscribers() %>%
        DT::datatable(
          colnames = c(
            "User" = "User", 
            "Email" = "Email",  
            "Topics" = "Topics", 
            "Excluded topics" = "Excluded Topics", 
            "Immediate topics" = "Real time Topics", 
            "Regions" = "Regions", 
            "Immediate regions" = "Real time Regions", 
            "Alert category" = "Alert category",
            "Alert slots" = "Alert Slots"
          ),
          filter = "top",
          escape = TRUE
        )
    })

    # updating the subscriber table on change of subscribers file
    shiny::observe({
      # Adding a dependency to subscribers refresh
      cd$subscribers_refresh_flag()
      DT::replaceData(DT::dataTableProxy('config_subscribers'), get_subscribers())
    })
     
    # downloading subscribers default file
    output$conf_orig_subscribers_download <- shiny::downloadHandler(
      filename = function() "subscribers.xlsx",
      content = function(file) { 
        file.copy(get_default_subscribers_path(), file) 
      }
    )

    # downloading subscriber current file
    output$conf_subscribers_download <- shiny::downloadHandler(
      filename = function() "subscribers.xlsx",
      content = function(file) { 
        file.copy(get_subscribers_path(), file) 
      }
    )

    # uploading subscribers file
    shiny::observe({
      df <- input$conf_subscribers_upload
      if(!is.null(df) && nrow(df)>0) {
        uploaded <- df$datapath[[1]]
        file.copy(uploaded, paste(conf$data_dir, "subscribers.xlsx", sep = "/"), overwrite=TRUE) 
        cd$subscribers_refresh_flag(Sys.time())
      }
    }) 


    ######### REGIONS LOGIC ##################
    # rendering reguions for the first time (update on next statement)
    output$config_regions <- DT::renderDataTable({
      `%>%` <- magrittr::`%>%`
      get_regions_df() %>%
        DT::datatable(
          colnames = c(
            "Name" = "Name", 
            "Codes" = "Codes",  
            "Level" = "Level", 
            "Min. lat." = "MinLatitude", 
            "Max. lat." = "MaxLatitude", 
            "Min. long." = "MinLongitude", 
            "Max. long." = "MaxLingityde"
          ),
          filter = "top",
          escape = TRUE
        )
    })

    # Updating regions when the new file is uploaded
    shiny::observe({
      # Adding a dependency to country refresh
      cd$countries_refresh_flag()
      DT::replaceData(DT::dataTableProxy('config_regions'), get_regions_df())
    })
     
    # Download the current country file
    output$conf_countries_download <- shiny::downloadHandler(
      filename = function() "countries.xlsx",
      content = function(file) { 
        file.copy(get_countries_path(), file) 
      }
    )
    
    # Dowload the original country file
    output$conf_orig_countries_download <- shiny::downloadHandler(
      filename = function() "countries.xlsx",
      content = function(file) { 
        file.copy(get_default_countries_path(), file) 
      }
    )

    #Update the new country file
    shiny::observe({
      df <- input$conf_countries_upload
      if(!is.null(df) && nrow(df)>0) {
        uploaded <- df$datapath[[1]]
        file.copy(uploaded, paste(conf$data_dir, "countries.xlsx", sep = "/"), overwrite=TRUE) 
        cd$countries_refresh_flag(Sys.time())
      }
    }) 


    ######### ALERTS LOGIC ##################
    # rendering the alerts 
    # updates are launched automatically when any input value changes
       
    # setting default value for number of alerts to return depending on wether we are displaying tweets or not
    shiny::observe({
      # Can also set the label and select items
      shiny::updateSelectInput(session, "alerts_limit",
        selected = if(input$alerts_display == "tweets") "10" else "0" 
      )
    })

  
    shiny::observeEvent(input$alerts_search, {
      shiny::updateTextInput(session, "alerts_show_search", label = NULL, value = "true")
      output$alerts_table <- DT::renderDataTable({
        `%>%` <- magrittr::`%>%`
        shiny::isolate({
          toptweets <- if(input$alerts_display == "tweets") 10 else 0
          progress_start("Getting alerts") 
          alerts <- get_alerts(
            topic = input$alerts_topics, 
            countries = as.numeric(input$alerts_countries), 
            from = input$alerts_period[[1]], 
            until = input$alerts_period[[2]], 
            toptweets = toptweets,
            limit = as.integer(input$alerts_limit),
            progress = function(value, message) {progress_set(value = value, message = message)}
          )
        })
        shiny::validate(
          shiny::need(!is.null(alerts), 'No alerts generated for the selected period')
        )

        dt <- if(toptweets == 0) {
          alerts %>%
            dplyr::select(
              "date", "hour", "topic", "country", "epitweetr_category", "tops", "number_of_tweets", "known_ratio", "limit", 
              "no_historic", "bonferroni_correction", "same_weekday_baseline", "rank", "with_retweets", "location_type",
              "alpha", "alpha_outlier", "k_decay"
            ) %>%
            DT::datatable(
              colnames = c(
                "Date" = "date", 
                "Hour" = "hour", 
                "Topic" = "topic",  
                "Region" = "country",
                "Category" = "epitweetr_category",
                "Tops" = "tops", 
                "Tweets" = "number_of_tweets", 
                "% from important user" = "known_ratio",
                "Threshold" = "limit",
                "Baseline" = "no_historic", 
                "Bonf. corr." = "bonferroni_correction",
                "Same weekday baseline" = "same_weekday_baseline",
                "Day rank" = "rank",
                "With retweets" = "with_retweets",
                "Location" = "location_type",
                "Alert FPR (alpha)" = "alpha",
                "Outlier FPR (alpha)" = "alpha_outlier",
                "Downweight strenght" = "k_decay"
              ),
              filter = "top",
              escape = FALSE
            )
        } else {
          alerts$toptweets <- sapply(alerts$toptweets, function(tweetsbylang) {
            if(length(tweetsbylang) == 0)
              ""
            else {
              paste(
                "<UL>",
                lapply(1:length(tweetsbylang), function(i) {
                  paste(
                   "<LI>",
                   names(tweetsbylang)[[i]],
                   "<OL>",
                   paste(
                     lapply(tweetsbylang[[i]], function(t) {
                       paste0(
                         "<LI>",
                         htmltools::htmlEscape(t),
                         "</LI>"
                       )
                     }),
                     collapse = ""
                   ),
                   "</OL>",
                   "</LI>"
                  )
                }),
                "</UL>",
                collapse = ""
             )
            }
          })
          
          alerts %>%
            dplyr::select("date", "hour", "topic", "country", "epitweetr_category", "tops", "number_of_tweets", "toptweets") %>%
            DT::datatable(
              colnames = c(
                "Date" = "date", 
                "Hour" = "hour", 
                "Topic" = "topic",  
                "Region" = "country",
                "Category" = "epitweetr_category",
                "Tops" = "tops", 
                "Tweets" = "number_of_tweets", 
                "Top tweets" = "toptweets"
              ),
              filter = "top",
              escape = FALSE
            )
        }
        progress_close()
        dt
      })
    })

    shiny::observeEvent(input$alerts_close, {
      shiny::updateTextInput(session, "alerts_show_search", label = NULL, value = "false")
    })

    shiny::observeEvent(input$alertsdb_search, {
      shiny::updateTextInput(session, "alertsdb_show", label = NULL, value = "true")
      output$alertsdb_table <- DT::renderDataTable(get_alertsdb_html() %>%
        DT::datatable(
          colnames = c(
            "Date" = "date", 
            "Topic" = "topic",  
            "Region" = "country",
            "Top words" = "topwords", 
            "Tweets" = "number_of_tweets", 
            "Top tweets" = "toptweets",
            "Given Category" = "given_category",
            "Epitweetr Category" = "epitweetr_category"
          ),
          filter = "top",
          escape = FALSE
        )
      )
        
      output$alertsdb_runs_table <- DT::renderDataTable(get_alertsdb_runs_html() %>%
        DT::datatable(
          colnames = c(
            "Ranking" = "ranking",
            "Models" = "models", 
            "Alerts" = "alerts", 
            "Runs" = "runs", 
            "F1Score" = "f1score",
            "Accuracy" = "accuracy",
            "Precision By Class" = "precision_by_class",
            "Sensitivity By Class" = "sensitivity_by_class",
            "FScore By Class" = "fscore_by_class",
            "Last run" = "last_run",
            "Active" = "active",
            "Documentation" = "documentation",
            "Custom Parameters" = "custom_parameters"
          ),
          filter = "top",
          escape = FALSE
        )
      )
    })
    shiny::observeEvent(input$alertsdb_close, {
      shiny::updateTextInput(session, "alertsdb_show", label = NULL, value = "false")
    })
    output$alertsdb_download <- shiny::downloadHandler(
      filename = function() "alert-training.xlsx",
      content = function(file) {
        progress_start("updating geo training dataset for download")
        #updating geotraining file before downloading
        tryCatch({
           if(get_alert_training_path() == get_default_alert_training_path()) {
             alerts <- get_alerts(
               topic = input$alerts_topics, 
               countries = as.numeric(input$alerts_countries), 
               from = input$alerts_period[[1]], 
               until = input$alerts_period[[2]], 
               toptweets = 10,
               limit = if(!is.na(input$alerts_limit) && as.integer(input$alerts_limit) > 0 ) as.integer(input$alerts_limit) else 20,
               progress = function(value, message) {progress_set(value = value, message = message)}
             )
             if(!is.null(alerts)) {
               alerts$given_category <- NA
               write_alert_training_db(alerts)
               cd$alert_training_refresh_flag(Sys.time())
               progress_close()
               file.copy(get_alert_training_path(), file) 
             } else
               app_error("Cannot download alerts since no alerts where found with the selected filters", env = cd)
           }
           else {  
             progress_close()
             file.copy(get_alert_training_path(), file) 
           }
          },
          error = function(w) {app_error(w, env = cd)}
        )
      }
    )
    
    shiny::observeEvent(input$alertsdb_add, {
        progress_start("Adding new alerts to the alert database")
        #updating geotraining file before downloading
        tryCatch({
            new_alerts <- get_alerts(
              topic = input$alerts_topics, 
              countries = as.numeric(input$alerts_countries), 
              from = input$alerts_period[[1]], 
              until = input$alerts_period[[2]], 
              toptweets = 10,
              limit = if(!is.na(input$alerts_limit) && as.integer(input$alerts_limit) > 0 ) as.integer(input$alerts_limit) else 20,
              progress = function(value, message) {progress_set(value = value, message = message)}
            )

            existing_alerts <- get_alert_training_df()
            if(!is.null(new_alerts) && nrow(new_alerts) > 0) {
              new_alerts$given_category <- NA
              alerts <- Reduce(x = list(existing_alerts, new_alerts), f = function(df1, df2) {dplyr::bind_rows(df1, df2)})
              write_alert_training_db(alerts)
              cd$alert_training_refresh_flag(Sys.time())
            } else {
               app_error("Cannot add alerts to classify. No alerts where found with the selected filters", env = cd)
            }
          },
          error = function(w) {app_error(w, env = cd)}
        )
    })

    # uploading alerts training file
    shiny::observe({
      df <- input$alertsdb_upload
      if(!is.null(df) && nrow(df)>0) {
        progress_start("processing new training set")
        uploaded <- df$datapath[[1]]
        #copying file and making a backup of existing one
        if(file.exists(get_user_alert_training_path()))
          file.copy(get_user_alert_training_path(), paste(get_user_alert_training_path(), ".bak", sep = ""), overwrite=TRUE) 
        file.copy(uploaded, get_user_alert_training_path(), overwrite=TRUE) 
        #updating geotraining df taking in condideration new uploaded file
        
        tryCatch({
           progress_set(value = 0.5, message = "Retraining models and evaluating")
           retrain_alert_classifier()
           cd$alert_training_refresh_flag(Sys.time())
          },
          warning = function(w) {app_error(w, env = cd)},
          error = function(w) {app_error(w, env = cd)}
        )
      }
    })

    # updating alerts table on change of geotraining file
    shiny::observe({
      # Adding a dependency to alert refresh
      cd$alert_training_refresh_flag()
      DT::replaceData(DT::dataTableProxy('alertsdb_table'),get_alertsdb_html())
      DT::replaceData(DT::dataTableProxy('alertsdb_runs_table'), get_alertsdb_runs_html())
      progress_close()
    })



    ######### GEOTRAINING EVALUATION DATAFRAME ####
    # rendering a dataframe showing evaluation metrics 
    output$geotraining_eval_df <- DT::renderDataTable({
      
      df <-get_geotraining_eval_df()
      DT::datatable(df,
        escape = TRUE
      )
    })
    
    get_geotraining_eval_df <- function() {
      `%>%` <- dplyr::`%>%`
      ret <- data.frame(Category=character(),
           `True positives`=integer(),
           `False positives`=integer(),
           `True negatives`=integer(),
           `False negatives`=integer(),
           `Precision` = double(),
           `Sensitivity` = double(),
           `F1Score` = double(),
            check.names = FALSE
        )
      if(file.exists(get_geotraining_evaluation_path())) {
        df <- jsonlite::fromJSON(get_geotraining_evaluation_path(), flatten=TRUE)
        df <- as.data.frame(df)
        if(nrow(df) > 0) {
          ret <- df %>%
            dplyr::select(.data$test,.data$tp,.data$tn,.data$fp,.data$fn)%>%
            dplyr::group_by(.data$test)%>%
            dplyr::summarise(tp = sum(.data$tp), fp=sum(.data$fp), tn=sum(.data$tn),  fn = sum(.data$fn))%>%
            janitor:: adorn_totals("row")%>%
            dplyr::mutate(Precision=round(.data$tp/(.data$tp+.data$fp),3))%>%
            dplyr::mutate(Sensitivity = round(.data$tp/(.data$tp +.data$fn),3))%>%
            dplyr::mutate(F1Score =round((2*.data$tp)/(2*.data$tp + .data$fn + .data$fp),3))%>%
            dplyr::rename(Category=.data$test)%>%
            dplyr::rename("True positives"=.data$tp)%>%
            dplyr::rename("False positives"=.data$fp)%>%
            dplyr::rename("True negatives"=.data$tn)%>%
            dplyr::rename("False negatives"=.data$fn)
        }
      }
      ret
    }
    
    ######### GEOTEST LOGIC ##################
    # rendering geotest data, updates are done automatically whenever an input changes
    output$geotraining_table <- DT::renderDataTable({
      `%>%` <- magrittr::`%>%`
       df <- get_geotraining_df()
       df %>%
         DT::datatable(
           filter = "top",
           escape = TRUE
         )
    })
    # uploading geotagging training file
    shiny::observe({
      df <- input$geotraining_upload
      if(!is.null(df) && nrow(df)>0) {
        progress_start("processing new training set")
        uploaded <- df$datapath[[1]]
        #copying file and making a backup of existing one
        if(file.exists(get_user_geotraining_path()))
          file.copy(get_user_geotraining_path(), paste(get_user_geotraining_path(), ".bak", sep = ""), overwrite=TRUE) 
        file.copy(uploaded, get_user_geotraining_path(), overwrite=TRUE) 
        #updating geotraining df taking in condideration new uploaded file
        
        tryCatch({
           #update_geotraining_df(input$geotraining_tweets2add, progress = function(value, message) {progress_set(value = value /3, message = message)})
           progress_set(value = 0.5, message = "Retraining models and evaluating")
           retrain_languages()
           update_geotraining_df(input$geotraining_tweets2add, progress = function(value, message) {progress_set(value = 0.5 + value/2, message = message)})
           cd$geotraining_refresh_flag(Sys.time())
           #TO DO : afficher les résultats + observe qui met à jour si de nouveelles données sont uploadées
          },
          error = function(w) {app_error(w, env = cd)}
        )
      }
    })
    # Updating geolocation data
    shiny::observeEvent(input$geotraining_update, {
      progress_start("updating geo training dataset")
      #updating geotraining df taking in condideration new uploaded file
      tryCatch({
         update_geotraining_df(input$geotraining_tweets2add, progress = function(value, message) {progress_set(value = value, message = message)})
         cd$geotraining_refresh_flag(Sys.time())
        },
        error = function(w) {app_error(w, env = cd)}
      )
    })
    
    # download geotraining data
    output$geotraining_download <- shiny::downloadHandler(
      filename = function() "geo-training.xlsx",
      content = function(file) {
        progress_start("updating geo training dataset for download")
        #updating geotraining file before downloading
        tryCatch({
           if(get_geotraining_path() == get_default_geotraining_path()) {
             update_geotraining_df(input$geotraining_tweets2add, progress = function(value, message) {progress_set(value = value, message = message)})
           }
           file.copy(get_geotraining_path(), file) 
           cd$geotraining_refresh_flag(Sys.time())
          },
          error = function(w) {app_error(w, env = cd)}
        )
      }
    )
    
    # updating geotraining table on change of geotraining file
    shiny::observe({
      # Adding a dependency to subscribers refresh
      cd$geotraining_refresh_flag()
      DT::replaceData(DT::dataTableProxy('geotraining_table'), get_geotraining_df())
      DT::replaceData(DT::dataTableProxy('geotraining_eval_df'), get_geotraining_eval_df())
      progress_close()
    })

    ######### DATA PROTECTION LOGIC ##################
    # rendering the data protection search table
    shiny::observeEvent(input$data_anonymise, {
      shiny::showModal(shiny::modalDialog(
        title = "Warning",
        "Please confirm you want to anonymise the tweets matching this search criteria, this action cannot be undone",
        footer = shiny::tagList(shiny::actionButton("data_perform_anonymise", "Yes anonymise tweets"), shiny::modalButton("Cancel"))
      ))
    })

    shiny::observeEvent(input$data_delete, {
      shiny::showModal(shiny::modalDialog(
        title = "Warning",
        "Please confirm you want to delete the tweets matching this search criteria, this action cannot be undone",
        footer = shiny::tagList(shiny::actionButton("data_perform_delete", "Yes delete tweets"), shiny::modalButton("Cancel"))
      ))
    })

    shiny::observeEvent(input$data_perform_delete, {
      search_tweets_exp(hide_users = FALSE, action = "delete") 

    })
    shiny::observeEvent(input$data_perform_anonymise, {
      search_tweets_exp(hide_users = FALSE, action = "anonymise") 

    })

    shiny::observeEvent(input$data_search_ano, {
      search_tweets_exp(hide_users = TRUE) 
    })

    shiny::observeEvent(input$data_search, {
      search_tweets_exp(hide_users = FALSE)
    })
    search_tweets_exp <- function(hide_users, action = NULL) { 
      output$data_message <- shiny::renderText("")
      output$data_search_df <- DT::renderDataTable({
        `%>%` <- magrittr::`%>%`
        shiny::removeModal()
        query <- (if(!is.null(action) && action == "anonymise") "created_at:[0 TO Z] NOT screen_name:user" else NULL)
        progress_start("Searching") 
        shiny::isolate({
          tweets <- search_tweets(
            query = query,
            topic = input$data_topics, 
            from = input$data_period[[1]], 
            to = input$data_period[[2]], 
            countries = as.numeric(input$data_countries), 
            mentioning = if(input$data_mode %in% c("all", "mentioning") && input$data_users != "") strsplit(input$data_users, "\\s+")[[1]] else NULL,
            users = if(input$data_mode %in% c("all", "users") && input$data_users != "") strsplit(input$data_users, "\\s+")[[1]] else NULL,
            hide_users = hide_users,
            action = action,
            max = as.integer(input$data_limit)
          )
        })

        if(!is.null(action) && nrow(tweets)> 0 ) {
          processed <- 0
          while(nrow(tweets)> 0) {
            processed <- processed + nrow(tweets) 
            progress_set(value = 0.5, message = paste("Performing", action, processed, "tweets processed"))

            shiny::isolate({
              tweets <- search_tweets(
                query = query,
                topic = input$data_topics, 
                from = input$data_period[[1]], 
                to = input$data_period[[2]], 
                countries = as.numeric(input$data_countries), 
                mentioning = if(input$data_mode %in% c("all", "mentioning") && input$data_users != "") strsplit(input$data_users, "\\s+")[[1]] else NULL,
                users = if(input$data_mode %in% c("all", "users") && input$data_users != "") strsplit(input$data_users, "\\s+")[[1]] else NULL,
                hide_users = hide_users,
                action = action,
                max = as.integer(input$data_limit)
              )
            })
          }
          progress_close()
        }

        shiny::validate(
          shiny::need(!is.null(tweets) && nrow(tweets) > 0, 'No tweets found for the provided filters')
        )
        output$data_message <- if(tweets$totalCount[[1]] > 1000) {
          shiny::renderText(paste("more than ", tweets$totalCount[[1]], " tweets found"))
        } else {  shiny::renderText(paste(tweets$totalCount[[1]], " tweets found"))}

        dt <- {
          tweets$country_code <- if("text_loc" %in% colnames(tweets)) tweets$text_loc$geo_country_code else ""
          tweets$geo_name <-  if("text_loc" %in% colnames(tweets)) tweets$text_loc$geo_name else ""
          tweets %>%
            dplyr::select(
              "tweet_id", "topic",  "country_code", "geo_name", "created_at", "text","screen_name", "linked_text", "linked_screen_name"
            ) %>%
            DT::datatable(
              colnames = c(
                "Tweet Id" = "tweet_id",
                "Topic" = "topic",
                "Created" = "created_at",
                "Country in text" = "country_code",
                "Location in text" = "geo_name",
                "User" = "screen_name", 
                "Text" = "text", 
                "Retweet/Quoted text" = "linked_text", 
                "Retweet/Quoted user" = "linked_screen_name"
              ),
              filter = "top",
              escape = TRUE
            )

        }
        progress_close()
        dt
      })
    }


    ######## DIAGNOSTIC LOGIC ############
    # running the diagnostics when the button is clicked
    shiny::observeEvent(input$run_diagnostic, {
      `%>%` <- magrittr::`%>%`
       
      output$diagnostic_table <- DT::renderDataTable(
      check_all() %>%
        DT::datatable(
          colnames = c(
            "Check Code" = "check",  
            "Passed" =  "passed",
            "Message" = "message"
          ),
          filter = "top",
          escape = TRUE,
          rownames= FALSE,
          options = list(pageLength = 50)
        ) %>%
        DT::formatStyle(
          columns = c("Passed"),
          valueColumns = c("Passed"),
          color = DT::styleEqual(c(TRUE, FALSE), c("green", "red"))
        )
      )
    })

  } 

  # Functions for managing calculation progress
  progress_start <- function(start_message = "processing", env = cd) {
    progress_close(env = env)
    env$progress <- shiny::Progress$new()
    env$progress$set(message = start_message, value = 0)
  }
  
  progress_close <- function(env = cd) {
    if(exists("progress", where = env)) {
      env$progress$close()
      rm("progress", envir = env)
    }
  }
  
  progress_inc <- function(value, message  = "processing", env = cd) {
    if(!is.null(env$progress$set))
      env$progress$set(value = env$progress$getValue() + value, detail = message) 
  }
  
  progress_set <- function(value, message  = "processing", env = cd) {
    if(!is.null(env$progress$set))
      env$progress$set(value = value, detail = message)
  }

  app_error <- function(e, env = cd) {
    message(e)
    progress_close(env = env)
    shiny::showNotification(paste(e), type = "error")
  }

# Printing PID 
  message(Sys.getpid())
  # Launching the app
  old <- options()
  on.exit(options(old))
  shiny::shinyApp(ui = ui, server = server, options = options(shiny.fullstacktrace = TRUE))
}

# Get or updates data used for dashboard filters based on configuration values and aggregated period
refresh_dashboard_data <- function(e = new.env(), fixed_period = NULL) {
  e$topics <- {
    codes <- unique(sapply(conf$topics, function(t) t$topic))
    names <- get_topics_labels()[codes]
    sort(setNames(c("", codes), c("", unname(names))))
  }
  e$countries <- {
    regions <- get_country_items()
    setNames(1:length(regions), sapply(regions, function(r) r$name))   
  }
  agg_dates <- get_aggregated_period() 
  if(is.na(agg_dates$first) || is.na(agg_dates$last)) {
    agg_dates$first = Sys.Date()
    agg_dates$last = Sys.Date()
  }
  e$date_min <- strftime(agg_dates$first, format = "%Y-%m-%d")
  e$date_max <- strftime(agg_dates$last, format = "%Y-%m-%d")
  collected_days <- agg_dates$last - agg_dates$first
  e$fixed_period <- (
    if(!is.null(fixed_period)) fixed_period
    else if(is.na(collected_days)) "custom"
    else if(collected_days < 7) "custom"
    else "last 7 days"
  )
  e$date_start <- ( 
    if(e$fixed_period == "custom" && exists("date_start", e))
      e$date_start 
    else if(e$fixed_period == "last 7 days" && collected_days > 7)
      strftime(agg_dates$last - 7, format = "%Y-%m-%d")
    else if(e$fixed_period == "last 30 days" && collected_days > 30)
      strftime(agg_dates$last - 30, format = "%Y-%m-%d")
    else if(e$fixed_period == "last 60 days" && collected_days > 60)
      strftime(agg_dates$last - 60, format = "%Y-%m-%d")
    else  if(e$fixed_period == "last 180 days" && collected_days > 180)
      strftime(agg_dates$last - 180, format = "%Y-%m-%d")
    else
      e$date_min 
  )
  e$date_end <- e$date_max
  return(e)
}


# Get or update data for config page from configuration, Data collection & processing and Requirements & alerts pipeline files
refresh_config_data <- function(e = new.env(), limit = list("langs", "topics", "tasks", "geo")) {
  # Refreshing configuration
  setup_config(data_dir = conf$data_dir, ignore_properties = TRUE)
  
  #Creating the flag for properties refresh
  if(!exists("properties_refresh_flag", where = e)) {
    e$properties_refresh_flag <- shiny::reactiveVal()
  }
  #Creating the flag for status refredh
  if(!exists("process_refresh_flag", where = e)) {
    e$process_refresh_flag <- shiny::reactiveVal()
  }
  #Creating the flag for subscribers refresh
  if(!exists("subscribers_refresh_flag", where = e)) {
    e$subscribers_refresh_flag <- shiny::reactiveVal()
  }
  #Creating the flag for geotraining refresh
  if(!exists("geotraining_refresh_flag", where = e)) {
    e$geotraining_refresh_flag <- shiny::reactiveVal()
  }
  #Creating the flag for alert training refresh
  if(!exists("alert_training_refresh_flag", where = e)) {
    e$alert_training_refresh_flag <- shiny::reactiveVal()
  }
  #Creating the flag for countries refresh
  if(!exists("countries_refresh_flag", where = e)) {
    e$countries_refresh_flag <- shiny::reactiveVal()
  }

  # Updating language related fields
  if("langs" %in% limit) {
    langs <- get_available_languages()
    lang_tasks <- get_tasks()$language
    if(!exists("langs_refresh_flag", where = e)) {
      e$langs_refresh_flag <- shiny::reactiveVal(0)
    } else if(file.exists(get_tasks_path()) && file.info(get_tasks_path())$mtime > e$langs_refresh_flag()){
      e$langs_refresh_flag(file.info(get_tasks_path())$mtime)
    } else if(file.exists(get_properties_path()) && file.info(get_properties_path())$mtime > e$langs_refresh_flag()){
      e$langs_refresh_flag(file.info(get_properties_path())$mtime)
    } 
    e$lang_items <- setNames(langs$Code, langs$`Full Label`)
    e$lang_names <- setNames(langs$Label, langs$Code)
    e$langs <- data.frame(
      Language = unlist(lang_tasks$names), 
      Code = unlist(lang_tasks$codes), 
      Status = unlist(lang_tasks$statuses), 
      URL = unlist(lang_tasks$urls)
    )
  }
  # Updating topics related fields
  if("topics" %in% limit) {
    # Updating the reactive value tasks_refresh to force dependencies invalidation
    update <- FALSE
    if(!exists("topics_refresh_flag", envir = e) || !exists("plans_refresh_flag", envir = e)) {
      e$topics_refresh_flag <- shiny::reactiveVal(0)
      e$plans_refresh_flag <- shiny::reactiveVal(0)
      update <- TRUE
    } 
    if(!update && file.exists(get_topics_path()) && file.info(get_topics_path())$mtime != e$topics_refresh_flag()) {
      update <- TRUE
      e$topics_refresh_flag(file.info(get_topics_path())$mtime)
    } 
    if(!update && file.exists(get_plans_path()) && file.info(get_plans_path())$mtime != e$plans_refresh_flag()) {
      update <- TRUE
      e$plans_refresh_flag(file.info(get_plans_path())$mtime)
    }
    if(update) {
      e$topics_df <- get_topics_df()
    }
  }
  # Updating tasks related fields
  if("tasks" %in% limit) {
    # Updating the reactive value tasks_refresh to force dependencies invalidation
    update <- FALSE
    e$detect_running <- is_detect_running() 
    e$fs_running <- is_fs_running() 
    e$search_running <- is_search_running() 
    e$search_diff <- Sys.time() - last_search_time()

    # Detecting if some change has happened since last evaluation
    if(!exists("tasks_refresh_flag", where = e)) {
      e$tasks_refresh_flag <- shiny::reactiveVal()
      update <- TRUE
    } else if(file.exists(get_tasks_path()) && file.info(get_tasks_path())$mtime != e$tasks_refresh_flag()){
      update <- TRUE
    }
    if(update) {
      if(file.exists(get_tasks_path())) e$tasks_refresh_flag(file.info(get_tasks_path())$mtime)
      tasks <- get_tasks()
      sorted_tasks <- order(sapply(tasks, function(l) l$order)) 
      e$tasks <- tasks[sorted_tasks] 
      e$app_auth <- exists('app', where = conf$twitter_auth) && conf$twitter_auth$app != ''
      e$api_version <- conf$api_version
      e$tasks_df <- data.frame(
        Task = sapply(e$tasks, function(t) t$task), 
        Status = sapply(e$tasks, function(t) if(in_pending_status(t)) "pending" else t$status), 
        Scheduled = sapply(e$tasks, function(t) strftime(t$scheduled_for, format = "%Y-%m-%d %H:%M:%OS", origin = '1970-01-01')), 
        `Last Start` = sapply(e$tasks, function(t) strftime(t$started_on, format = "%Y-%m-%d %H:%M:%OS", origin = '1970-01-01')), 
        `Last End` = sapply(e$tasks, function(t) strftime(t$end_on, format = "%Y-%m-%d %H:%M:%OS", origin = '1970-01-01')),
        Message = sapply(e$tasks, function(t) if(exists("message", where = t) && !is.null(t$message)) t$message else "")
      )
      row.names(e$tasks_df) <- sapply(e$tasks, function(t) t$order) 
    }
  }
  # Updating geolocation test related fields
  if("geo" %in% limit) {
  }
  return(e)
}

# validate that dashboard can be rendered
can_render <- function(input, d) {
  shiny::validate(
      shiny::need(is_fs_running(), 'Embedded database service is not running, please make sure you have activated it (running update dependencies is necessary after upgrade from epitweetr version 0.1+ to epitweetr 1.0+)')
      , shiny::need(file.exists(conf$data_dir), 'Please go to configuration tab and setup tweet collection (no data directory found)')
      , shiny::need(check_series_present(), paste('No aggregated data found on ', paste(conf$data_dir, "series", sep = "/"), " please make sure the Requirements & alerts pipeline has successfully ran"))
      , shiny::need(
          is.null(input) || (
            !is.na(input$period[[1]]) 
              && !is.na(input$period[[2]]) 
              && (input$period[[1]] <= input$period[[2]])
          )
          ,'Please select a start and end period for the report. The start period must be a date earlier than the end period'
        )
      , shiny::need(is.null(input) || (input$topics != ''), 'Please select a topic')
  )
}

# validate that chart is not empty
chart_not_empty <- function(chart) {
  shiny::validate(
     shiny::need(!("waiver" %in% class(chart$data)), chart$labels$title)
  )
} 


get_alertsdb_html <- function() {
  alerts <- get_alert_training_df() 
  shiny::validate(
    shiny::need(!is.null(alerts), 'No alerts generated for the selected period')
  )
  alerts$toptweets <- sapply(alerts$toptweets, function(tweetsbylang) {
    if(length(tweetsbylang) == 0)
      ""
    else {
      paste(
        "<UL>",
        lapply(1:length(tweetsbylang), function(i) {
          paste(
           "<LI>",
           names(tweetsbylang)[[i]],
           "<OL>",
           paste(
             lapply(tweetsbylang[[i]], function(t) {
               paste0(
                 "<LI>",
                 htmltools::htmlEscape(t),
                 "</LI>"
               )
             }),
             collapse = ""
           ),
           "</OL>",
           "</LI>"
          )
        }),
        "</UL>",
        collapse = ""
     )
    }
  })
  alerts
} 

get_alertsdb_runs_html <- function(){
  runs <- get_alert_training_runs_df()
  runs$f1score <- format(runs$f1score, digits = 3) 
  runs$accuracy <- format(runs$accuracy, digits = 3)
  runs$precision_by_class <- format(runs$precision_by_class, digits = 3)
  runs$sensitivity_by_class <- format(runs$sensitivity_by_class, digits = 3)
  runs$fscore_by_class <- format(runs$fscore_by_class, digits = 3)
  runs$custom_parameters <- sapply(runs$custom_parameters, function(params) {
    if(length(params) == 0)
      ""
    else {
      paste(
        "<UL>",
        lapply(1:length(params), function(i) {
          paste(
           "<LI>",
           names(params)[[i]],
           ":",
           params[[i]],
           "</LI>"
          )
        }),
        "</UL>",
        collapse = ""
     )
    }
  })
  runs
}

