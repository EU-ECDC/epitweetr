
#' Running the epitwitter app
#' @export
epitweetr_app <- function(data_dir = NA) { 
  # Seting up configuration if not already done
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
          ######### DASBOARD FILTERS #####################
          ################################################
          shiny::selectInput("topics", label = shiny::h4("Topics"), multiple = FALSE, choices = d$topics),
          shiny::selectInput("countries", label = shiny::h4("Countries & regions"), multiple = TRUE, choices = d$countries),
          shiny::dateRangeInput("period", label = shiny::h4("Time Period"), start = d$date_start, end = d$date_end, min = d$date_min,max = d$date_max, format = "yyyy-mm-dd", startview = "month"), 
          shiny::radioButtons("period_type", label = shiny::h4("Time Unit"), choices = list("Days"="created_date", "Weeks"="created_weeknum"), selected = "created_date", inline = TRUE),
          shiny::h4("Include Retweets/Quotes"),
	        shiny::checkboxInput("with_retweets", label = NULL, value = conf$alert_with_retweets),
          shiny::radioButtons("location_type", label = shiny::h4("Location type"), choices = list("Tweet"="tweet", "User"="user","both"="both" ), selected = "tweet", inline = TRUE),
          shiny::sliderInput("alpha_filter", label = shiny::h4("Signal detection confidence"), min = 0, max = 0.3, value = conf$alert_alpha, step = 0.005),
          shiny::h4("Bonferroni correction"),
	        shiny::checkboxInput("bonferroni_correction", label = NULL, value = conf$alert_with_bonferroni_correction),
          shiny::numericInput("history_filter", label = shiny::h4("Days in baseline"), value = conf$alert_history),
          shiny::h4("Same weekday baseline"),
	        shiny::checkboxInput("same_weekday_baseline", label = NULL, value = conf$alert_same_weekday_baseline),
          shiny::fluidRow(
            shiny::column(6, 
              shiny::downloadButton("export_pdf", "PDF")
            ),
            shiny::column(6, 
              shiny::downloadButton("export_md", "Md")
            )
          )
        ), 
        shiny::column(9, 
          ################################################
          ######### DASBOARD PLOTS #######################
          ################################################
          shiny::fluidRow(
            shiny::column(12, 
              shiny::fluidRow(
                shiny::column(1, shiny::downloadButton("download_line_data", "data")),
                shiny::column(1, shiny::downloadButton("export_line", "image"))
		          ),
              plotly::plotlyOutput("line_chart")
            )
          )
          ,shiny::fluidRow(
            shiny::column(6, 
              shiny::fluidRow(
                shiny::column(3, shiny::downloadButton("download_topword_data", "data")),
                shiny::column(3, shiny::downloadButton("export_topword", "image"))
		          ),
              plotly::plotlyOutput("topword_chart")
            )
            , shiny::column(6, 
              shiny::fluidRow(
                shiny::column(3, shiny::downloadButton("download_map_data", "data")),
                shiny::column(3, shiny::downloadButton("export_map", "image"))
		          ),
              plotly::plotlyOutput(
                "map_chart" 
              ),
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
          shiny::h3("Generated Alerts"),
          shiny::fluidRow(
            ################################################
            ######### ALERTS FILTERS #######################
            ################################################
            shiny::column(1, 
              shiny::h4("Detection date") 
            ),
            shiny::column(3, 
              shiny::dateRangeInput("alerts_period", label = NULL, start = d$date_end, end = d$date_end, min = d$date_min,max = d$date_max, format = "yyyy-mm-dd", startview = "month"), 
            ),
            shiny::column(1, 
              shiny::h4("Topics")
            ),
            shiny::column(2, 
              shiny::selectInput("alerts_topics", label = NULL, multiple = TRUE, choices = d$topics[d$topics!=""]),
            ),
            shiny::column(2, 
              shiny::h4("Countries & regions")
            ),
            shiny::column(3, 
              shiny::selectInput("alerts_countries", label = NULL, multiple = TRUE, choices = d$countries),
            )
          ), 
          shiny::fluidRow(
            shiny::column(12, 
              ################################################
              ######### ALERTS TABLE #########################
              ################################################
              DT::dataTableOutput("alerts_table")
          ))
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
            shiny::column(4, "Tweet Search"), 
            shiny::column(4, shiny::htmlOutput("search_running")),
            shiny::column(4, shiny::actionButton("activate_search", "activate"))
          ),
          shiny::fluidRow(
            shiny::column(4, "Geonames"), 
            shiny::column(4, shiny::htmlOutput("geonames_status")),
            shiny::column(4, shiny::actionButton("update_geonames", "update"))
          ),
          shiny::fluidRow(
            shiny::column(4, "Languages"), 
            shiny::column(4, shiny::htmlOutput("languages_status")),
            shiny::column(4, shiny::actionButton("update_languages", "update"))
          ),
          shiny::fluidRow(
            shiny::column(4, "Detection pipeline"), 
            shiny::column(4, shiny::htmlOutput("detect_running")),
            shiny::column(4, shiny::actionButton("activate_detect", "activate"))
          ),
          ################################################
          ######### GENERAL PROPERTIES ###################
          ################################################
          shiny::h3("Signal Detection"),
          shiny::fluidRow(shiny::column(3, "Default confidence"), shiny::column(9, shiny::sliderInput("conf_alpha", label = NULL, min = 0, max = 0.3, value = conf$alert_alpha, step = 0.005))),
          shiny::fluidRow(shiny::column(3, "Default days in baseline"), shiny::column(9, shiny::numericInput("conf_history", label = NULL , value = conf$alert_history))),
          shiny::fluidRow(shiny::column(3, "Default same weekday baseline"), shiny::column(9, shiny::checkboxInput("conf_same_weekday_baseline", label = NULL , value = conf$alert_same_weekday_baseline))),
          shiny::fluidRow(shiny::column(3, "Default with retweets/Quotes"), shiny::column(9, shiny::checkboxInput("conf_with_retweets", label = NULL , value = conf$alert_with_retweets))),
          shiny::fluidRow(
            shiny::column(3, "Default with bonferroni correction"), 
            shiny::column(9, shiny::checkboxInput("conf_with_bonferroni_correction", label = NULL , value = conf$alert_with_bonferroni_correction))
          ),
          shiny::h3("General"),
          shiny::fluidRow(shiny::column(3, "Data Dir"), shiny::column(9, shiny::span(conf$data_dir))),
          shiny::fluidRow(shiny::column(3, "Search span (m)"), shiny::column(9, shiny::numericInput("conf_collect_span", label = NULL, value = conf$collect_span))), 
          shiny::fluidRow(shiny::column(3, "Detect span (m)"), shiny::column(9, shiny::numericInput("conf_schedule_span", label = NULL, value = conf$schedule_span))), 
          shiny::fluidRow(shiny::column(3, "Launch Slots"), shiny::column(9, shiny::htmlOutput("conf_schedule_slots"))), 
          shiny::fluidRow(shiny::column(3, "Password Store"), shiny::column(9, 
            shiny::selectInput(
              "conf_keyring", 
              label = NULL, 
              choices = c("wincred", "macos", "file", "secret_service", "environment"), 
              selected = conf$keyring
            ))), 
          shiny::fluidRow(shiny::column(3, "Spark Cores"), shiny::column(9, shiny::numericInput("conf_spark_cores", label = NULL, value = conf$spark_cores))) ,
          shiny::fluidRow(shiny::column(3, "Spark Memory"), shiny::column(9, shiny::textInput("conf_spark_memory", label = NULL, value = conf$spark_memory))), 
          shiny::fluidRow(shiny::column(3, "Geolocation Threshold"), shiny::column(9, shiny::textInput("geolocation_threshold", label = NULL, value = conf$geolocation_threshold))),
          shiny::fluidRow(shiny::column(3, "Geonames URL"), shiny::column(9, shiny::textInput("conf_geonames_url", label = NULL, value = conf$geonames_url))),
          shiny::fluidRow(shiny::column(3, "Simplified Geonames"), shiny::column(9, shiny::checkboxInput("conf_geonames_simplify", label = NULL, value = conf$geonames_simplify))),
          shiny::fluidRow(shiny::column(3, "Regions Disclaimer"), shiny::column(9, shiny::textAreaInput("conf_regions_disclaimer", label = NULL, value = conf$regions_disclaimer))),
          shiny::h2("Twitter Authentication"),
          shiny::fluidRow(shiny::column(3, "Mode"), shiny::column(9
            , shiny::radioButtons(
              "twitter_auth"
              , label = NULL
              , choices = list("Twitter Account" = "delegated", "Twitter Developer App" = "app")
              , selected = if(cd$app_auth) "app" else "delegated" 
              ))
          ),
          shiny::conditionalPanel(
            condition = "input.twitter_auth == 'delegated'",
            shiny::fluidRow(shiny::column(12, "When choosing 'delegate' twitter authentication you will have to use your twitter credentials to authorize the twitter application for the rtweet package (https://rtweet.info/) to access twitter on your behalf (full rights provided).")), 
            shiny::fluidRow(shiny::column(12, "DISCLAIMER: Rtweet has no relationship with epitweetr and you have to evaluate by yourself if the provided security framework fits your needs.")),
          ),
          shiny::conditionalPanel(
            condition = "input.twitter_auth == 'app'",
            shiny::fluidRow(shiny::column(3, "App Name"), shiny::column(9, shiny::textInput("twitter_app", label = NULL, value = if(is_secret_set("app")) get_secret("app") else NULL))), 
            shiny::fluidRow(shiny::column(3, "Api Key"), shiny::column(9, shiny::passwordInput("twitter_api_key", label = NULL, value = if(is_secret_set("api_key")) get_secret("api_key") else NULL))),
            shiny::fluidRow(shiny::column(3, "Api Secret"), shiny::column(9, shiny::passwordInput("twitter_api_secret", label = NULL, value = if(is_secret_set("api_secret")) get_secret("api_secret") else NULL))), 
            shiny::fluidRow(shiny::column(3, "Access Token"), shiny::column(9, 
              shiny::passwordInput("twitter_access_token", label = NULL, value = if(is_secret_set("access_token")) get_secret("access_token") else NULL))
            ), 
            shiny::fluidRow(shiny::column(3, "Token Secret"), shiny::column(9, 
              shiny::passwordInput("twitter_access_token_secret", label = NULL, value = if(is_secret_set("access_token_secret")) get_secret("access_token_secret") else NULL))
            )
          ), 
          shiny::h2("Email Authentication (smtp)"),
          shiny::fluidRow(shiny::column(3, "Server"), shiny::column(9, shiny::textInput("smtp_host", label = NULL, value = conf$smtp_host))), 
          shiny::fluidRow(shiny::column(3, "Port"), shiny::column(9, shiny::numericInput("smtp_port", label = NULL, value = conf$smtp_port))), 
          shiny::fluidRow(shiny::column(3, "From"), shiny::column(9, shiny::textInput("smtp_from", label = NULL, value = conf$smtp_from))), 
          shiny::fluidRow(shiny::column(3, "Login"), shiny::column(9, shiny::textInput("smtp_login", label = NULL, value = conf$smtp_login))), 
          shiny::fluidRow(shiny::column(3, "Password"), shiny::column(9, shiny::passwordInput("smtp_password", label = NULL, value = conf$smtp_password))), 
          shiny::fluidRow(shiny::column(3, "Unsafe certificates"), shiny::column(9, shiny::checkboxInput("smtp_insecure", label = NULL, value = conf$smtp_insecure))), 
          shiny::actionButton("save_properties", "Update Properties")
        ), 
        shiny::column(8,
          ################################################
          ######### DETECTION PANEL ######################
          ################################################
          shiny::h3("Detection Pipeline"),
          DT::dataTableOutput("tasks_df"),
          ################################################
          ######### TOPICS PANEL ######################
          ################################################
          shiny::h3("Topics"),
          shiny::fluidRow(
            shiny::column(4, shiny::h5("Available Topics")),
            shiny::column(2, shiny::downloadButton("conf_topics_download", "Download")),
            shiny::column(2, shiny::downloadButton("conf_orig_topics_download", "Download Defaults")),
            shiny::column(4, shiny::fileInput("conf_topics_upload", label = NULL, buttonLabel = "Upload")),
          ),
          DT::dataTableOutput("config_topics"),
          ################################################
          ######### LANGUAGES PANEL ######################
          ################################################
          shiny::h3("Languages"),
          shiny::fluidRow(
            shiny::column(4, shiny::h5("Available Languages")),
            shiny::column(2, shiny::downloadButton("conf_lang_download", "Download")),
            shiny::column(2, shiny::downloadButton("conf_orig_lang_download", "Download Defaults")),
            shiny::column(4, shiny::fileInput("conf_lang_upload", label = NULL , buttonLabel = "Upload")),
          ),
          shiny::fluidRow(
            shiny::column(4, shiny::h5("Active Languages")),
            shiny::column(6, shiny::uiOutput("lang_items_0")),
            shiny::column(1, shiny::actionButton("conf_lang_add", "+")),
            shiny::column(1, shiny::actionButton("conf_lang_remove", "-")),
          ),
          DT::dataTableOutput("config_langs"),
          ################################################
          ######### IMPORTANT USERS ######################
          ################################################
          shiny::h3("Important Users"),
          shiny::fluidRow(
            shiny::column(4, shiny::h5("User file")),
            shiny::column(2, shiny::downloadButton("conf_users_download", "Download")),
            shiny::column(2, shiny::downloadButton("conf_orig_users_download", "Download Defaults")),
            shiny::column(4, shiny::fileInput("conf_users_upload", label = NULL , buttonLabel = "Upload")),
          ),
          ################################################
          ######### SUSCRIBERS PANEL #####################
          ################################################
          shiny::h3("Subscribers"),
          shiny::fluidRow(
            shiny::column(4, shiny::h5("Subscribers")),
            shiny::column(2, shiny::downloadButton("conf_subscribers_download", "Download")),
            shiny::column(2, shiny::downloadButton("conf_orig_subscribers_download", "Download Defaults")),
            shiny::column(4, shiny::fileInput("conf_subscribers_upload", label = NULL, buttonLabel = "Upload")),
          ),
          DT::dataTableOutput("config_subscribers"),
          ################################################
          ######### COUNTRIES / REGIONS PANEL ############
          ################################################
          shiny::h3("Countries / Regions"),
          shiny::fluidRow(
            shiny::column(4, shiny::h5("Countries / Territories")),
            shiny::column(2, shiny::downloadButton("conf_countries_download", "Download")),
            shiny::column(2, shiny::downloadButton("conf_orig_countries_download", "Download Defaults")),
            shiny::column(4, shiny::fileInput("conf_countries_upload", label = NULL, buttonLabel = "Upload")),
          ),
          DT::dataTableOutput("config_regions")
        ))
  ) 
  # Defining geo tuning page UI
  ################################################
  ######### GEOTAG PAGE ##########################
  ################################################
  geotest_page <- 
    shiny::fluidPage(
          shiny::h3("Geotagging sample"),
          shiny::h5("random today's tweets"),
          shiny::fluidRow(
            ################################################
            ######### GEO TAG FILTERS #######################
            ################################################
            shiny::column(1, 
              shiny::h4("Geo Field") 
            ),
            shiny::column(3, shiny::selectInput("geotest_fields", label = NULL, multiple = FALSE, choices = cd$geo_cols)),
            shiny::column(1, 
              shiny::h4("Sample Size") 
            ),
            shiny::column(3, 
              shiny::numericInput("geotest_size", label = NULL, value = 100), 
            ),
            shiny::column(4)
          ), 
          shiny::fluidRow(
            shiny::column(12, 
              ################################################
              ######### ALERTS TABLE #########################
              ################################################
              DT::dataTableOutput("geotest_table")
          ))
  ) 
  
  # Defining navigation UI
  ui <- 
    shiny::navbarPage("epitweetr"
      , shiny::tabPanel("Dashboard", dashboard_page)
      , shiny::tabPanel("Alerts", alerts_page)
      , shiny::tabPanel("Geotag evaluation", geotest_page)
      , shiny::tabPanel("Configuration", config_page)
    )


  # Defining line chart from shiny app filters
  line_chart_from_filters <- function(topics, countries, period_type, period, with_retweets, location_type,alpha, no_history, bonferroni_correction, same_weekday_baseline) {
    trend_line(
      topic = topics
      ,countries= if(length(countries) == 0) c(1) else as.integer(countries)
      ,date_type= period_type
      ,date_min = period[[1]]
      ,date_max = period[[2]]
      ,with_retweets= with_retweets
      ,location_type = location_type
      ,alpha = alpha
      ,no_historic = no_history
      ,bonferroni_correction = bonferroni_correction
      , same_weekday_baseline = same_weekday_baseline
    )
    
  }
  # Defining line chart from shiny app filters
  map_chart_from_filters <- function(topics, countries, period_type, period, with_retweets, location_type) {
    create_map(
      topic= topics
      ,countries= if(length(countries) == 0) c(1) else as.integer(countries)
      ,date_type= period_type
      ,date_min = period[[1]]
      ,date_max = period[[2]]
      ,with_retweets= with_retweets
      ,location_type = location_type
      ,caption = conf$regions_disclaimer
      ,forplotly = TRUE 
    )
    
  }
  # Defining top words chart from shiny app filters
  topwords_chart_from_filters <- function(topics, fcountries, period_type, period, with_retweets, location_type, top) {
    fcountries= if(length(fcountries) == 0 || 1 %in%fcountries) c(1) else as.integer(fcountries)
    regions <- get_country_items()
    countries <- Reduce(function(l1, l2) {unique(c(l1, l2))}, lapply(fcountries, function(i) unlist(regions[[i]]$codes)))
    create_topwords(
      topic= topics
      ,country_codes = countries
      ,date_min = period[[1]]
      ,date_max = period[[2]]
      ,with_retweets= with_retweets
      ,location_type = location_type
      ,top
    )
    
  }
  # Rmarkdown dasboard export
  export_dashboard <- function(format, file, topics, countries, period_type, period, with_retweets, location_type, alpha, no_historic, bonferroni_correction, same_weekday_baseline) {
    rmarkdown::render(
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
        , "alert_historic" = no_historic
        , "bonferroni_correction" = bonferroni_correction
        , "same_weekday_baseline" = same_weekday_baseline
      ),
      quiet = TRUE
    ) 
  }
  
  # Defining server loginc
  server <- function(input, output, session, ...) {
    `%>%` <- magrittr::`%>%`
    ################################################
    ######### FILTERS LOGIC ########################
    ################################################
    shiny::observe({
      val <- {
      if(length(input$topics)==0 || input$topics == "") 
        conf$alert_alpha
      else
        unname(get_topics_alphas()[stringr::str_replace_all( input$topics, "%20", " ")])
      }
      updateSliderInput(session, "alpha_filter", value = val, min = 0, max = 0.3, step = 0.005)
    })
    
    
    ################################################
    ######### DASHBOARD LOGIC ######################
    ################################################
    output$line_chart <- plotly::renderPlotly({
       can_render(input, d)
       chart <- line_chart_from_filters(
         input$topics, 
         input$countries, 
         input$period_type, 
         input$period, 
         input$with_retweets, 
         input$location_type , 
         input$alpha_filter, 
         input$history_filter, 
         input$bonferroni_correction,
         input$same_weekday_baseline
         )$chart
       height <- session$clientData$output_p_height
       width <- session$clientData$output_p_width
	     chart_not_empty(chart)
       gg <- plotly::ggplotly(chart, height = height, width = width, tooltip = c("label")) %>% plotly::config(displayModeBar = F) 
       # Fixing bad entries on ggplotly chart
       for(i in 1:length(gg$x$data)) {
         if(startsWith(gg$x$data[[i]]$name, "(") && endsWith(gg$x$data[[i]]$name, ")")) 
           gg$x$data[[i]]$name = gsub("\\(|\\)|,|[0-9]", "", gg$x$data[[i]]$name) 
         else 
           gg$x$data[[i]]$name = " "
         gg$x$data[[i]]$legendgroup = gsub("\\(|\\)|,|[0-9]", "", gg$x$data[[i]]$legendgroup)
       }
       gg
    })  
    output$map_chart <- plotly::renderPlotly({
       can_render(input, d)
       height <- session$clientData$output_p_height
       width <- session$clientData$output_p_width
       chart <- map_chart_from_filters(input$topics, input$countries, input$period_type, input$period, input$with_retweets, input$location_type)$chart
	     chart_not_empty(chart)
       gg <- chart %>%
         plotly::ggplotly(height = height, width = width, tooltip = c("label")) %>% 
	       plotly::layout(
           title=list(text= paste("<b>", chart$labels$title, "</b>")), 
           margin = list(l = 30, r=30, b = 70, t = 80),
           annotations = list(
             text = chart$labels$caption,
             font = list(size = 10),
             showarrow = FALSE,
             xref = 'paper', 
             x = 0,
             yref = 'paper', 
             y = -0.3)
           ) %>%
         plotly::config(displayModeBar = F) 
    
         # Fixing bad entries on ggplotly chart
         for(i in 1:length(gg$x$data)) {
           if(substring(gg$x$data[[i]]$name, 1, 2) %in% c("a.", "b.", "c.", "d.", "e.", "f.", "g.", "h."))
             gg$x$data[[i]]$name = substring(gg$x$data[[i]]$name, 4, nchar(gg$x$data[[i]]$name)) 
           #gg$x$data[[i]]$legendgroup = gsub("\\(|\\)|,|[0-9]", "", gg$x$data[[i]]$legendgroup)
         }
         gg
    })

    output$topword_chart <- plotly::renderPlotly({
       can_render(input, d)
       height <- session$clientData$output_p_height
       width <- session$clientData$output_p_width
       chart <- topwords_chart_from_filters(input$topics, input$countries, input$period_type, input$period, input$with_retweets, input$location_type, 20)$chart
	     chart_not_empty(chart)
       chart %>%
         plotly::ggplotly(height = height, width = width) %>% 
	       plotly::layout(
           title=list(text= paste("<b>", chart$labels$title, "<br>", chart$labels$subtitle), "</b>"), 
           margin = list(l = 30, r=30, b = 70, t = 80),
           annotations = list(
             text = chart$labels$caption,
             font = list(size = 10),
             showarrow = FALSE,
             xref = 'paper', 
             x = 0,
             yref = 'paper', 
             y = -0.3)
         ) %>%
	       plotly::config(displayModeBar = F)
    })  
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
            input$history_filter,
            input$bonferroni_correction,
            input$same_weekday_baseline
            )$chart
        device <- function(..., width, height) grDevices::png(..., width = width, height = height, res = 300, units = "in")
        ggplot2::ggsave(file, plot = chart, device = device) 
      }
    ) 
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
            input$history_filter,
            input$bonferroni_correction,
            input$same_weekday_baseline
            )$data,
          file, 
          row.names = FALSE)
      }
    ) 
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
          map_chart_from_filters(input$topics, input$countries, input$period_type, input$period, input$with_retweets, input$location_type)$data,
          file, 
          row.names = FALSE)
      }
    ) 
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
	chart <- 
	  map_chart_from_filters(input$topics, input$countries, input$period_type, input$period, input$with_retweets, input$location_type)$chart
        device <- function(..., width, height) grDevices::png(..., width = width, height = height, res = 300, units = "in")
        ggplot2::ggsave(file, plot = chart, device = device) 
        
      }
    ) 
    output$download_topword_data <- shiny::downloadHandler(
      filename = function() { 
        paste("topword_dataset_", 
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
          topwords_chart_from_filters(input$topics, input$countries, input$period_type, input$period, input$with_retweets, input$location_type, 200)$data,
          file, 
          row.names = FALSE)
      }
    ) 
    output$export_topword <- shiny::downloadHandler(
      filename = function() { 
        paste("topword_", 
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
          topwords_chart_from_filters(input$topics, input$countries, input$period_type, input$period, input$with_retweets, input$location_type, 50)$chart
        device <- function(..., width, height) grDevices::png(..., width = width, height = height, res = 300, units = "in")
        ggplot2::ggsave(file, plot = chart, device = device) 
      }
    ) 
    output$export_pdf <- shiny::downloadHandler(
      filename = function() { 
        paste("epitweetr_dashboard_", 
          "_", paste(input$topics, collapse="-"), 
          "_", paste(input$countries, collapse="-"), 
          "_", input$period[[1]], 
          "_", input$period[[2]],
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
           input$history_filter,
           input$bonferroni_correction,
           input$same_weekday_baseline
         )
      }
    ) 
    output$export_md <- shiny::downloadHandler(
      filename = function() { 
        paste("epitweetr_dashboard_", 
          "_", paste(input$topics, collapse="-"), 
          "_", paste(input$countries, collapse="-"), 
          "_", input$period[[1]], 
          "_", input$period[[2]],
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
           input$history_filter,
           input$bonferroni_correction,
           input$same_weekday_baseline
         )
      }
    )
    ################################################
    ######### CONFIGURATION LOGIC ##################
    ################################################
    
    ######### STATUS PANEL LOGIC ##################
    # Timer for updating task statuses  
    shiny::observe({
      shiny::invalidateLater(10000)
      refresh_config_data(cd, list("tasks", "topics"))
      cd$process_refresh_flag(Sys.time())
    }) 
    output$search_running <- shiny::renderText({
      # Adding a dependency to task refresh
      cd$tasks_refresh_flag()
      cd$process_refresh_flag()
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
    output$detect_running <- shiny::renderText({
      # Adding a dependency to task refresh
      cd$tasks_refresh_flag()
      cd$process_refresh_flag()
      paste(
        "<span",
        " style='color:", if(cd$detect_running) "#348017'" else "#F75D59'",
        ">",
        if(cd$detect_running) "Running" else "Stopped",
        "</span>"
        ,sep=""
      )})

    output$geonames_status <- shiny::renderText({
      # Adding a dependency to task refresh
      cd$tasks_refresh_flag()
      paste(
        "<span",
        " style='color:", 
          if(cd$geonames_status %in% c("n/a", "error"))
            "#F75D59'"
          else if(cd$geonames_status %in% c("success")) 
            "#348017'" 
          else 
            "#2554C7'", 
        ">",
        cd$geonames_status,
        "</span>"
        ,sep=""
      )})
    output$languages_status <- shiny::renderText({
      # Adding a dependency to task refresh
      cd$tasks_refresh_flag()
      paste(
        "<span",
        " style='color:", 
          if(cd$languages_status %in% c("n/a", "error"))
            "#F75D59'"
          else if(cd$languages_status %in% c("success")) 
            "#348017'" 
          else 
            "#2554C7'", 
        ">",
        cd$languages_status,
        "</span>"
        ,sep=""
      )})

    output$conf_schedule_slots <- shiny::renderText({
      # Adding a dependency to config refresh
      cd$properties_refresh_flag()
      paste(head(lapply(get_task_day_slots(get_tasks()$geotag), function(d) strftime(d, format="%H:%M")), -1), collapse=", ")
    })

    shiny::observeEvent(input$activate_search, {
      register_search_runner_task()
      refresh_config_data(cd, list("tasks"))
    })

    shiny::observeEvent(input$activate_detect, {
      register_detect_runner_task()
      refresh_config_data(cd, list("tasks"))
    })

    shiny::observeEvent(input$update_geonames, {
      conf$geonames_updated_on <- strftime(Sys.time(), "%Y-%m-%d %H:%M:%S")
      save_config(data_dir = conf$data_dir, properties= TRUE, topics = FALSE)
      refresh_config_data(cd, list("tasks"))
    })

    shiny::observeEvent(input$update_languages, {
      conf$lang_updated_on <- strftime(Sys.time(), "%Y-%m-%d %H:%M:%S")
      save_config(data_dir = conf$data_dir, properties= TRUE, topics = FALSE)
      refresh_config_data(cd, list("tasks"))
    })
    ######### PROPERTIES LOGIC ##################
    shiny::observeEvent(input$save_properties, {
      conf$collect_span <- input$conf_collect_span
      conf$schedule_span <- input$conf_schedule_span
      conf$keyring <- input$conf_keyring
      conf$spark_cores <- input$conf_spark_cores 
      conf$spark_memory <- input$conf_spark_memory
      conf$geolocation_threshold <- input$geolocation_threshold 
      conf$geonames_url <- input$conf_geonames_url 
      conf$geonames_simplify <- input$conf_geonames_simplify 
      conf$regions_disclaimer <- input$conf_regions_disclaimer 
      conf$alert_alpha <- input$conf_alpha 
      conf$alert_history <- input$conf_history 
      conf$alert_same_weekday_baseline <- input$conf_same_weekday_baseline 
      conf$alert_with_bonferroni_corection <- input$conf_with_bonferroni_correction
      conf$alert_with_retweets <- input$conf_with_retweets
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

      save_config(data_dir = conf$data_dir, properties= TRUE, topics = FALSE)

      cd$properties_refresh_flag(Sys.time())
    })


    ######### IMPORTANT USERS LOGIC ###########
    output$conf_users_download <- shiny::downloadHandler(
      filename = function() "users.xlsx",
      content = function(file) { 
        file.copy(get_known_users_path(), file) 
      }
    )
    
    output$conf_orig_users_download <- shiny::downloadHandler(
      filename = function() "users.xlsx",
      content = function(file) { 
        file.copy(get_default_known_users_path(), file) 
      }
    )
    
    shiny::observe({
      df <- input$conf_users_upload
      if(!is.null(df) && nrow(df)>0) {
        uploaded <- df$datapath[[1]]
        file.copy(uploaded, paste(conf$data_dir, "users.xlsx", sep = "/"), overwrite=TRUE) 
      }
    })
    ######### LANGUAGE LOGIC ##################
    output$config_langs <- DT::renderDataTable({
      # Adding dependency with lang refresh
      cd$langs_refresh_flag()
      DT::datatable(cd$langs)
    })
    shiny::observe({
      
    }) 
    
    output$conf_lang_download <- shiny::downloadHandler(
      filename = function() "languages.xlsx",
      content = function(file) { 
        file.copy(get_available_languages_path(), file) 
      }
    )
    
    output$conf_orig_lang_download <- shiny::downloadHandler(
      filename = function() "languages.xlsx",
      content = function(file) { 
        file.copy(get_default_available_languages_path(), file) 
      }
    )
     
    shiny::observe({
      df <- input$conf_lang_upload
      if(!is.null(df) && nrow(df)>0) {
        uploaded <- df$datapath[[1]]
        file.copy(uploaded, paste(conf$data_dir, "languages.xlsx", sep = "/"), overwrite=TRUE) 
        refresh_config_data(e = cd, limit = list("langs"))
      }
    })

    output$lang_items_0 <- shiny::renderUI({
      # Adding a dependency to lang refresh
      cd$langs_refresh_flag()
      shiny::selectInput("lang_items", label = NULL, multiple = FALSE, choices = cd$lang_items)
    })

    shiny::observeEvent(input$conf_lang_add, {
      add_config_language(input$lang_items, cd$lang_names[input$lang_items])
      save_config(data_dir = conf$data_dir, properties= TRUE, topics = FALSE)
      refresh_config_data(e = cd, limit = list("langs"))
    })
    
    shiny::observeEvent(input$conf_lang_remove, {
      remove_config_language(input$lang_items)
      save_config(data_dir = conf$data_dir, properties= TRUE, topics = FALSE)
      refresh_config_data(e = cd, limit = list("langs"))
    })
    
    ######### TASKS LOGIC ##################
    output$tasks_df <- DT::renderDataTable({
      # Adding dependency with tasks refresh
      cd$tasks_refresh_flag()
      DT::datatable(cd$tasks_df)
    })
    
     
    ######### TOPICS LOGIC ##################
    output$config_topics <- DT::renderDataTable({
      `%>%` <- magrittr::`%>%`
       cd$topics_df %>%
        DT::datatable(
          colnames = c("Query Length" = "QueryLength", "Active Plans" = "ActivePlans",  "Progress" = "Progress", "Requests" = "Requests"),
          filter = "top",
          escape = TRUE,
        ) %>%
        DT::formatPercentage(columns = c("Progress"))
    })
    shiny::observe({
      # Adding a dependency to topics refresh
      cd$topics_refresh_flag()
      cd$plans_refresh_flag()
      DT::replaceData(DT::dataTableProxy('config_topics'), cd$topics)
       
    }) 
    output$conf_topics_download <- shiny::downloadHandler(
      filename = function() "topics.xlsx",
      content = function(file) { 
        file.copy(get_topics_path(), file) 
      }
    )
    output$conf_orig_topics_download <- shiny::downloadHandler(
      filename = function() "topics.xlsx",
      content = function(file) { 
        file.copy(get_default_topics_path(), file) 
      }
    )
    shiny::observe({
      df <- input$conf_topics_upload
      if(!is.null(df) && nrow(df)>0) {
        uploaded <- df$datapath[[1]]
        file.copy(uploaded, paste(conf$data_dir, "topics.xlsx", sep = "/"), overwrite=TRUE) 
        refresh_config_data(e = cd, limit = list("topics"))
      }
    }) 
    ######### SUBSCRIBERS LOGIC ##################
    output$config_subscribers <- DT::renderDataTable({
      `%>%` <- magrittr::`%>%`
      get_subscribers() %>%
        DT::datatable(
          colnames = c("User", "Email",  "Topics", "Excluded Topics", "Immediate Topics", "Regions", "Immediate Regions", "Alert Slots"),
          filter = "top",
          escape = TRUE,
        )
    })
    shiny::observe({
      # Adding a dependency to subscribers refresh
      cd$subscribers_refresh_flag()
      DT::replaceData(DT::dataTableProxy('config_subscribers'), get_subscribers())
    })
     
    output$conf_orig_subscribers_download <- shiny::downloadHandler(
      filename = function() "subscribers.xlsx",
      content = function(file) { 
        file.copy(get_default_subscribers_path(), file) 
      }
    )
    output$conf_subscribers_download <- shiny::downloadHandler(
      filename = function() "subscribers.xlsx",
      content = function(file) { 
        file.copy(get_subscribers_path(), file) 
      }
    )
    shiny::observe({
      df <- input$conf_subscribers_upload
      if(!is.null(df) && nrow(df)>0) {
        uploaded <- df$datapath[[1]]
        file.copy(uploaded, paste(conf$data_dir, "subscribers.xlsx", sep = "/"), overwrite=TRUE) 
        cd$subscribers_refresh_flag(Sys.time())
      }
    }) 
    ######### REGIONS LOGIC ##################
    output$config_regions <- DT::renderDataTable({
      `%>%` <- magrittr::`%>%`
      get_regions_df() %>%
        DT::datatable(
          colnames = c("Name", "Codes",  "Level", "Min. Lat.", "Max. Lat.", "Min. Long.", "Max. Long"),
          filter = "top",
          escape = TRUE,
        )
    })
    shiny::observe({
      # Adding a dependency to country refresh
      cd$countries_refresh_flag()
      DT::replaceData(DT::dataTableProxy('config_regions'), get_regions_df())
    })
     
    output$conf_countries_download <- shiny::downloadHandler(
      filename = function() "countries.xlsx",
      content = function(file) { 
        file.copy(get_countries_path(), file) 
      }
    )
    
    output$conf_orig_countries_download <- shiny::downloadHandler(
      filename = function() "countries.xlsx",
      content = function(file) { 
        file.copy(get_default_countries_path(), file) 
      }
    )
    shiny::observe({
      df <- input$conf_countries_upload
      if(!is.null(df) && nrow(df)>0) {
        uploaded <- df$datapath[[1]]
        file.copy(uploaded, paste(conf$data_dir, "countries.xlsx", sep = "/"), overwrite=TRUE) 
        cd$countries_refresh_flag(Sys.time())
      }
    }) 
    ######### ALERTS LOGIC ##################
    output$alerts_table <- DT::renderDataTable({
      `%>%` <- magrittr::`%>%`
      alerts <- get_alerts(topic = input$alerts_topics, countries = input$alerts_countries, from = input$alerts_period[[1]], until = input$alerts_period[[2]])
      shiny::validate(
        shiny::need(!is.null(alerts), 'No alerts generated for the selected period')
      )
      alerts %>%
        dplyr::select(
          "date", "hour", "topic", "country", "topwords", "number_of_tweets", "known_ratio", "limit", 
          "no_historic", "bonferroni_correction", "same_weekday_baseline", "rank", "with_retweets", "location_type"
        ) %>%
        DT::datatable(
          colnames = c(
            "Date" = "date", 
            "Hour" = "hour", 
            "Topic" = "topic",  
            "Region" = "country",
            "Top words" = "topwords", 
            "Tweets" = "number_of_tweets", 
            "% important user" = "known_ratio",
            "Threshold" = "limit",
            "Baseline" = "no_historic", 
            "Bonf. corr." = "bonferroni_correction",
            "Same weekday baseline" = "same_weekday_baseline",
            "Day_rank" = "rank",
            "With retweets" = "with_retweets",
            "Location" = "location_type"
            ),
          filter = "top",
          escape = TRUE,
        )
    })
    ######### GEOTEST LOGIC ##################
    output$geotest_table <- DT::renderDataTable({
      `%>%` <- magrittr::`%>%`
       text_col <- strsplit(input$geotest_fields, ";")[[1]][[1]]
       lang_col <-  if(length(strsplit(input$geotest_fields, ";")[[1]]) > 1) strsplit(input$geotest_fields, ";")[[1]][[2]] else NA
       lang_col_name <- if(is.na(lang_col)) "lang" else lang_col
       message(paste(text_col, lang_col))

       get_todays_sample_tweets(limit = input$geotest_size, text_col = text_col, lang_col = lang_col) %>%
         DT::datatable(
           colnames = c(
             "Tweet Id" = "id",  
             "Text" =  text_col,
             "Language" = lang_col_name,
             "Location name" = "geo_name",
             "Location type" = "geo_type",
             "Country Code" =  "geo_country_code",
             "Country" = "country", 
             "Score" = "score", 
             "Tagged text" = "tagged"
           ),
           filter = "top",
           escape = TRUE,
         )
    })
  } 
  # Printing PID 
  message(Sys.getpid())
  # Launching the app
  shiny::shinyApp(ui = ui, server = server, options = options(shiny.fullstacktrace = TRUE))
}

#' Get default data for dashoard
refresh_dashboard_data <- function(e = new.env()) {
  e$topics <- {
    codes <- unique(sapply(conf$topics, function(t) t$topic))
    names <- get_topics_labels()[codes]
    sort(setNames(c("", codes), c("", unname(names))))
  }
  e$countries <- {
    regions <- get_country_items()
    setNames(1:length(regions), sapply(regions, function(r) r$name))   
  }
  agg_dates <- get_aggregated_period("country_counts") 
  e$date_min <- strftime(agg_dates$first, format = "%Y-%m-%d")
  e$date_max <- strftime(agg_dates$last, format = "%Y-%m-%d")
  e$date_start <- ( 
    if(agg_dates$last - agg_dates$first < 30)
      e$date_min 
    else 
      strftime(agg_dates$last - 30, format = "%Y-%m-%d")
  )
  e$date_end <- e$date_max
  return(e)
}

#' Get or update default data for config page 
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
  #Creating the flag for countries refresh
  if(!exists("countries_refresh_flag", where = e)) {
    e$countries_refresh_flag <- shiny::reactiveVal()
  }

  # Updating language related fields
  if("langs" %in% limit) {
    langs <- get_available_languages()
    lang_tasks <- get_tasks()$language
    if(!exists("langs_refresh_flag", where = e)) {
      e$langs_refresh_flag <- shiny::reactiveVal()
    } else 
      e$langs_refresh_flag(Sys.time())
    e$lang_items <- setNames(langs$Code, langs$`Full Label`)
    e$lang_names <- setNames(langs$Label, langs$Code)
    e$langs <- data.frame(
      Language = unlist(lang_tasks$names), 
      Code = unlist(lang_tasks$codes), 
      Status = unlist(lang_tasks$statuses), 
      Url = unlist(lang_tasks$urls)
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
    e$search_running <- is_search_running() 
    e$search_diff <- Sys.time() - last_search_time()

    if(!exists("tasks_refresh_flag", where = e)) {
      e$tasks_refresh_flag <- shiny::reactiveVal()
      update <- TRUE
    } else if(file.exists(get_tasks_path()) && file.info(get_tasks_path())$mtime != e$tasks_refresh_flag()){
      update <- TRUE
    }
    if(update) {
      if(file.exists(get_tasks_path())) e$tasks_refresh_flag(file.info(get_tasks_path())$mtime)
      e$tasks <- get_tasks() 
      e$geonames_status <- if(in_pending_status(e$tasks$geonames)) "pending" else if(is.na(e$tasks$geonames$status)) "n/a" else e$tasks$geonames$status 
      e$languages_status <- if(in_pending_status(e$tasks$languages))  "pending" else if(is.na(e$tasks$languages$status)) "n/a" else e$tasks$languages$status 
      e$app_auth <- exists('app', where = conf$twitter_auth) && conf$twitter_auth$app != ''
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
    e$geo_cols <- setNames(
      c("text;lang", "linked_text;linked_lang", "user_description;lang", "user_location;lang", "place_full_name", "linked_place_full_name"), 
      c("Tweet Text", "Retweeted/Quoted text", "User description", "User declared location", "API tweet location", "API retweeted/Quoted tweet location")
    )  
  }
  return(e)
}

# validate that dashboard can be rendered
can_render <- function(input, d) {
  shiny::validate(
      shiny::need(file.exists(conf$data_dir), 'Please go to configuration ans setup tweet collection (no data directory found)')
      , shiny::need(length(d$topics)>0, paste('No aggregated data found on ', paste(conf$data_dir, "series", sep = "/"), " please make sure this is the right folder, and that the aggregated task has successfully run"))
      , shiny::need(input$topics != '', 'Please select a topic')
  )
}

# validate that chart is not empty
chart_not_empty <- function(chart) {
  shiny::validate(
     shiny::need(!("waiver" %in% class(chart$data)), chart$labels$title)
  )
} 
 
