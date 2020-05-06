
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
  c <- refresh_config_data()

  # Defining dashboard page UI
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
              shiny::sliderInput("alpha_filter", label = shiny::h4("Alert confidence"), min = 0, max = 0.1, value = conf$alert_alpha, step = 0.005),
              shiny::numericInput("history_filter", label = shiny::h4("Days in baseline"), value = conf$alert_history),
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
                shiny::column(6, 
                              shiny::downloadButton("download_line_data", "data"),
                              plotly::plotlyOutput("line_chart")
                )
                , shiny::column(6, 
                              shiny::downloadButton("download_map_data", "data"),
                              shiny::plotOutput("map_chart")
                )
              )
              ,shiny::fluidRow(
                shiny::column(6, 
                              shiny::downloadButton("download_topword_data", "data"),
                              plotly::plotlyOutput("topword_chart")
                )
                , shiny::column(6, 
                )
              ))
          )) 
  # Defining configuration
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
            shiny::column(4, shiny::actionButton("activate_scheduler", "activate"))
          ),
          ################################################
          ######### GENERAL PROPERTIES ###################
          ################################################
          shiny::h3("Alert Detection"),
          shiny::fluidRow(shiny::column(3, "Default alpha"), shiny::column(9, shiny::sliderInput("conf_alpha", label = NULL, min = 0, max = 0.1, value = conf$alert_alpha, step = 0.005))),
          shiny::fluidRow(shiny::column(3, "Default detection lag"), shiny::column(9, shiny::numericInput("conf_history", label = NULL , value = conf$alert_history))),
          
          shiny::h3("General"),
          shiny::fluidRow(shiny::column(3, "Data Dir"), shiny::column(9, shiny::textInput("conf_data_dir", label = NULL, value = conf$data_dir))),
          shiny::fluidRow(shiny::column(3, "Schedule Span (m)"), shiny::column(9, shiny::numericInput("conf_schedule_span", label = NULL, value = conf$schedule_span))), 
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
          shiny::h4("Twitter Authentication"),
          shiny::fluidRow(shiny::column(3, "Geolocation Threshold"), shiny::column(9
            , shiny::radioButtons(
              "twitter_auth"
              , label = NULL
              , choices = list("Delegated" = "delegated", "App" = "app")
              , selected = if(c$app_auth) "app" else "delegated" 
              ))
          ),
          shiny::conditionalPanel(
            condition = "input.twitter_auth == 'delegated'",
            shiny::fluidRow(shiny::column(12, "When choosing 'delegate' twitter authentication you will have to use your twitter credentials to authorize the twitter application for the rtweet package (https://rtweet.info/) to access twitter on your behalf (full rights provided).")), 
            shiny::fluidRow(shiny::column(12, "DISCLAIMER: Rtweet has no relationship with epitweetr and you have to evaluate by yourself if the provided security framework fits your needs.")),
          ),
          shiny::conditionalPanel(
            condition = "input.twitter_auth == 'app'",
            shiny::fluidRow(shiny::column(3, "App Name"), shiny::column(9, shiny::passwordInput("twitter_app", label = NULL, value = if(is_secret_set("app")) get_secret("app") else NULL))), 
            shiny::fluidRow(shiny::column(3, "Api Key"), shiny::column(9, shiny::passwordInput("twitter_api_key", label = NULL, value = if(is_secret_set("api_key")) get_secret("api_key") else NULL))),
            shiny::fluidRow(shiny::column(3, "Api Secret"), shiny::column(9, shiny::passwordInput("twitter_api_secret", label = NULL, value = if(is_secret_set("api_secret")) get_secret("api_secret") else NULL))), 
            shiny::fluidRow(shiny::column(3, "Access Token"), shiny::column(9, 
              shiny::passwordInput("twitter_access_token", label = NULL, value = if(is_secret_set("access_token")) get_secret("access_token") else NULL))
            ), 
            shiny::fluidRow(shiny::column(3, "Token Secret"), shiny::column(9, 
              shiny::passwordInput("twitter_access_token_secret", label = NULL, value = if(is_secret_set("access_token_secret")) get_secret("access_token_secret") else NULL))
            )
          ), 
          shiny::actionButton("save_properties", "Update Properties"),
        ), 
        shiny::column(8,
          shiny::fluidRow(
            ################################################
            ######### LANGUAGES PANEL ######################
            ################################################
            shiny::column(6,
              shiny::h3("Languages"),
              shiny::fluidRow(
                shiny::column(4, shiny::h5("Available Languages")),
                shiny::column(2, shiny::downloadButton("conf_lang_download", "Download")),
                shiny::column(6, shiny::fileInput("conf_lang_upload", label = NULL , buttonLabel = "Upload")),
              ),
              shiny::fluidRow(
                shiny::column(4, shiny::h5("Active Languages")),
                shiny::column(6, shiny::uiOutput("lang_items_0")),
                shiny::column(1, shiny::actionButton("conf_lang_add", "+")),
                shiny::column(1, shiny::actionButton("conf_lang_remove", "-")),
              ),
              DT::dataTableOutput("config_langs")
            ),
            ################################################
            ######### DETECTION PANEL ######################
            ################################################
            shiny::column(6,
              shiny::h3("Detection Pipeline"),
              DT::dataTableOutput("tasks_df")
            )
          ),
          ################################################
          ######### TOPICS PANEL ######################
          ################################################
          shiny::h3("Topics"),
          shiny::fluidRow(
            shiny::column(2, shiny::h5("Available Topics")),
            shiny::column(1, shiny::downloadButton("conf_topics_download", "Download")),
            shiny::column(3, shiny::fileInput("conf_topics_upload", label = NULL, buttonLabel = "Upload")),
            shiny::column(4, shiny::span()),
          ),
          DT::dataTableOutput("config_topics"),
        ))
  ) 
  
  # Defining navigation UI
  ui <- 
    shiny::navbarPage("epitweetr"
                      , shiny::tabPanel("Dashboard", dashboard_page)
                      , shiny::tabPanel("Configuration", config_page)
    )
  # Defining line chart from shiny app filters
  line_chart_from_filters <- function(topics, fcountries, period_type, period, alpha, no_history) {
    regions <- get_country_items()
    countries <- Reduce(function(l1, l2) {unique(c(l1, l2))}, lapply(as.integer(fcountries), function(i) unlist(regions[[i]]$codes)))
    trend_line(
      s_topic= topics
      ,s_country= countries
      ,type_date= period_type
      ,geo_country_code = "tweet_geo_country_code"
      ,date_min = strftime(period[[1]], format = (if(isTRUE(period_type=="created_weeknum")) "%G%V" else "%Y-%m-%d" ))
      ,date_max = strftime(period[[2]], format = (if(isTRUE(period_type=="created_weeknum")) "%G%V" else "%Y-%m-%d" ))
      ,selected_countries = fcountries
      ,alpha = alpha
      ,no_historic = no_history
    )
    
  }
  # Defining line chart from shiny app filters
  map_chart_from_filters <- function(topics, fcountries, period_type, period) {
    regions <- get_country_items()
    countries <- Reduce(function(l1, l2) {unique(c(l1, l2))}, lapply(as.integer(fcountries), function(i) unlist(regions[[i]]$codes)))
    create_map(
      s_topic= topics
      ,s_country = countries
      ,geo_code = "tweet_geo_country_code"
      ,type_date= period_type
      ,date_min = strftime(period[[1]], format = (if(isTRUE(period_type=="created_weeknum")) "%G%V" else "%Y-%m-%d" ))
      ,date_max = strftime(period[[2]], format = (if(isTRUE(period_type=="created_weeknum")) "%G%V" else "%Y-%m-%d" ))
    )
    
  }
  # Defining top words chart from shiny app filters
  topwords_chart_from_filters <- function(topics, fcountries, period_type, period) {
    regions <- get_country_items()
    countries <- Reduce(function(l1, l2) {unique(c(l1, l2))}, lapply(as.integer(fcountries), function(i) unlist(regions[[i]]$codes)))
    create_topwords(
      s_topic= topics
      ,s_country = countries
      ,date_min = strftime(period[[1]], format = (if(isTRUE(period_type=="created_weeknum")) "%G%V" else "%Y-%m-%d" ))
      ,date_max = strftime(period[[2]], format = (if(isTRUE(period_type=="created_weeknum")) "%G%V" else "%Y-%m-%d" ))
    )
    
  }
  # Rmarkdown dasboard export
  export_dashboard <- function(format, file, topics, countries, period_type, period, alpha, no_historic) {
    rmarkdown::render(
      system.file("rmarkdown", "dashboard.Rmd", package=get_package_name()), 
      output_format = format, 
      output_file = file,
      params = list(
        "topics" = topics
        , "countries" = countries
        , "period_type" = period_type
        , "period" = period
        , "alert_alpha" = alpha
        , "alert_historic" = no_historic
      ),
      quiet = TRUE
    ) 
  }
  
  # Defining server loginc
  server <- function(input, output, session, ...) {
    ################################################
    ######### DASHBOARD LOGIC ######################
    ################################################
    output$line_chart <- plotly::renderPlotly({
       can_render(input, d)
       chart <- line_chart_from_filters(input$topics, input$countries, input$period_type, input$period, input$alpha_filter, input$history_filter)$chart
       height <- session$clientData$output_p_height
       width <- session$clientData$output_p_width
       plotly::ggplotly(chart, height = height, width = width)
    })  
    output$map_chart <- shiny::renderPlot({
       can_render(input, d)
       map_chart_from_filters(input$topics, input$countries, input$period_type, input$period)$chart
    })
    output$topword_chart <- plotly::renderPlotly({
       can_render(input, d)
       chart <- topwords_chart_from_filters(input$topics, input$countries, input$period_type, input$period)$chart
       height <- session$clientData$output_p_height
       width <- session$clientData$output_p_width
       plotly::ggplotly(chart, height = height, width = width)
    })  
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
          line_chart_from_filters(input$topics, input$countries, input$period_type, input$period, input$alpha_filter, input$history_filter)$data,
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
          map_chart_from_filters(input$topics, input$countries, input$period_type, input$period)$data,
          file, 
          row.names = FALSE)
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
         export_dashboard("pdf_document", file, input$topics, input$countries, input$period_type, input$period, input$alpha_filter, input$history_filter)
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
         export_dashboard("md_document", file, input$topics, input$countries, input$period_type, input$period, input$alpha_filter, input$history_filter)
      }
    )
    ################################################
    ######### CONFIGURATION LOGIC ##################
    ################################################
    
    ######### STATUS PANEL LOGIC ##################
    # Timer for updating task statuses  
    shiny::observe({
      shiny::invalidateLater(10000)
      refresh_config_data(c, list("tasks"))
    }) 
    output$search_running = shiny::renderText({
      # Adding a dependency to task refresh
      c$tasks_refresh_flag()
      paste(
        "<span",
        " style='color:", if(c$search_running) "#348017'" else "#F75D59'",
        ">",
        if(is.na(c$search_diff)) "Stopped"
	else paste(
          if(c$search_running) "Running" else "Stopped"
          , "("
          , round(c$search_diff, 2)
          , units(c$search_diff)
          , "ago)" 
          ),
        "</span>"
        ,sep=""
    )})
    output$detect_running <- shiny::renderText({
      # Adding a dependency to lang refresh
      c$langs_refresh_flag()
      paste(
        "<span",
        " style='color:", if(c$detect_running) "#348017'" else "#F75D59'",
        ">",
        if(c$detect_running) "Running" else "Stopped",
        "</span>"
        ,sep=""
      )})
    output$geonames_status <- shiny::renderText({
      # Adding a dependency to task refresh
      c$tasks_refresh_flag()
      paste(
        "<span",
        " style='color:", 
          if(c$geonames_status %in% c("n/a", "error"))
            "#F75D59'"
          else if(c$geonames_status %in% c("success")) 
            "#348017'" 
          else 
            "#2554C7'", 
        ">",
        c$geonames_status,
        "</span>"
        ,sep=""
      )})
    output$languages_status <- shiny::renderText({
      # Adding a dependency to task refresh
      c$tasks_refresh_flag()
      paste(
        "<span",
        " style='color:", 
          if(c$languages_status %in% c("n/a", "error"))
            "#F75D59'"
          else if(c$languages_status %in% c("success")) 
            "#348017'" 
          else 
            "#2554C7'", 
        ">",
        c$languages_status,
        "</span>"
        ,sep=""
      )})


    shiny::observeEvent(input$activate_search, {
      register_search_runner_task()
      refresh_config_data(c, list("tasks"))
    })

    ######### PROPERTIES LOGIC ##################
    shiny::observeEvent(input$save_properties, {
      conf$data_dir <- input$conf_data_dir
      conf$schedule_span <- input$conf_schedule_span
      conf$keyring <- input$conf_keyring
      conf$spark_cores <- input$conf_spark_cores 
      conf$spark_memory <- input$conf_spark_memory
      conf$geolocation_threshold <- input$geolocation_threshold 
      conf$alert_alpha <- input$conf_alpha 
      conf$alert_history <- input$conf_history 
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
      save_config(data_dir = conf$data_dir, properties= TRUE, topics = FALSE)
    })
    

    ######### LANGUAGE LOGIC ##################
    output$config_langs <- DT::renderDataTable({
      # Adding dependency with lang refresh
      c$langs_refresh_flag()
      DT::datatable(c$langs)
    }) 
    
    output$conf_lang_download <- shiny::downloadHandler(
      filename = function() "languages.xlsx",
      content = function(file) { 
        file.copy(get_available_languages_path(), file) 
      }
    )
     
    shiny::observe({
      df <- input$conf_lang_upload
      if(!is.null(df) && nrow(df)>0) {
        uploaded <- df$datapath[[1]]
        file.copy(uploaded, paste(conf$data_dir, "languages.xlsx", sep = "/"), overwrite=TRUE) 
        refresh_config_data(c, list("langs"))
      }
    })

    output$lang_items_0 <- shiny::renderUI({
      # Adding a dependency to lang refresh
      c$langs_refresh_flag()
      shiny::selectInput("lang_items", label = NULL, multiple = FALSE, choices = c$lang_items)
    })

    shiny::observeEvent(input$conf_lang_add, {
      add_config_language(input$lang_items, c$lang_names[input$lang_items])
      save_config(data_dir = conf$data_dir, properties= TRUE, topics = FALSE)
      refresh_config_data(e = c, limit = list("langs"))
    })
    
    shiny::observeEvent(input$conf_lang_remove, {
      remove_config_language(input$lang_items)
      save_config(data_dir = conf$data_dir, properties= TRUE, topics = FALSE)
      refresh_config_data(e = c, limit = list("langs"))
    })
    
    ######### TASKS LOGIC ##################
    output$tasks_df <- DT::renderDataTable({
      # Adding dependency with tasks refresh
      c$tasks_refresh_flag()
      DT::datatable(c$tasks_df)
    })
    
     
    ######### TOPICS LOGIC ##################
    output$config_topics <- DT::renderDataTable({
      `%>%` <- magrittr::`%>%`
      # Adding a dependency to topics refresh
      c$topics_refresh_flag()
      c$topics %>%
        DT::datatable(
          colnames = c("Query Length" = "QueryLength", "Active Plans" = "ActivePlans",  "Progress (last)" = "LastProgress", "Requests (last)" = "LastRequests"),
          filter = "top",
          escape = TRUE
        ) %>%
        DT::formatPercentage(columns = c("Progress (last)"))
    })
    output$conf_topics_download <- shiny::downloadHandler(
      filename = function() "topics.xlsx",
      content = function(file) { 
        file.copy(get_topics_path(), file) 
      }
    )
    shiny::observe({
      df <- input$conf_topics_upload
      if(!is.null(df) && nrow(df)>0) {
        uploaded <- df$datapath[[1]]
        file.copy(uploaded, paste(conf$data_dir, "topics.xlsx", sep = "/"), overwrite=TRUE) 
        refresh_config_data(c, list("topics"))
      }
    }) 
  } 
  # Printing PID 
  message(Sys.getpid())
  # Launching the app
  shiny::shinyApp(ui = ui, server = server)
}

#' Get default data for dashoard
refresh_dashboard_data <- function(e = new.env()) {
  dfs <- get_aggregates("country_counts")
  e$topics <- {
    codes <- c("",unique(dfs$topic))
    names <- stringr::str_replace_all(codes, "%20", " ")
    names <- firstup(names)
    sort(setNames(codes, names))
  }
  e$countries <- {
    regions <- get_country_items()
    setNames(1:length(regions), sapply(regions, function(r) r$name))   
  } 
  e$date_min <- strftime(as.Date(min(dfs$created_date), origin ='1970-01-01'), format = "%Y-%m-%d")
  e$date_max <- strftime(as.Date(max(dfs$created_date), origin ='1970-01-01'), format = "%Y-%m-%d")
  e$date_start <- 
    if(max(dfs$created_date) - min(dfs$created_date) < 90) 
      e$date_min 
  else 
    strftime(as.Date(max(dfs$created_date), origin ='1970-01-01') - 90, format = "%Y-%m-%d")
  e$date_end <- e$date_max
  return(e)
}

#' Get or update default data for config page 
refresh_config_data <- function(e = new.env(), limit = list("langs", "topics", "tasks")) {
  # Refreshing configuration
  setup_config(data_dir = conf$data_dir, ignore_properties = TRUE)

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
    if(!exists("topics_refresh_flag", where = e)) {
      e$topics_refresh_flag <- shiny::reactiveVal()
    } else 
      e$topics_refresh_flag(Sys.time())
    e$topics <- data.frame(
      Topics = sapply(conf$topics, function(t) t$topic), 
      Query = sapply(conf$topics, function(t) t$query), 
      QueryLength = sapply(conf$topics, function(t) nchar(t$query)), 
      ActivePlans = sapply(conf$topics, function(t) length(t$plan)), 
      LastProgress = sapply(conf$topics, function(t) {if(length(t$plan)>0) t$plan[[1]]$progress else 0}), 
      LastRequests = sapply(conf$topics, function(t) {if(length(t$plan)>0) t$plan[[1]]$requests else 0})
    )
  }
  # Updating tasks related fields
  if("tasks" %in% limit) {
    # Updating the reactive value tasks_refresh to force dependencies invalidation
    if(!exists("tasks_refresh_flag", where = e)) {
      e$tasks_refresh_flag <- shiny::reactiveVal()
    } else 
      e$tasks_refresh_flag(Sys.time())
    e$tasks <- get_tasks() 
    e$search_running <- is_search_running() 
    e$search_diff <- Sys.time() - last_search_time()
    e$detect_running <- is_detect_running() 
    e$geonames_status <- if(in_pending_status(e$tasks$geonames)) "pending" else if(is.na(e$tasks$geonames$status)) "n/a" else e$tasks$geonames$status 
    e$languages_status <- if(in_pending_status(e$tasks$languages))  "pending" else if(is.na(e$tasks$geonames$status)) "n/a" else e$tasks$languages$status 
    e$app_auth <- exists('app', where = conf$twitter_auth) && conf$twitter_auth$app != ''
    e$tasks_df <- data.frame(
      Task = sapply(e$tasks, function(t) t$task), 
      Status = sapply(e$tasks, function(t) if(in_pending_status(t)) "Pensing" else t$status), 
      Scheduled = sapply(e$tasks, function(t) strftime(t$scheduled_for, format = "%Y-%m-%d %H:%M:%OS")), 
      `Last Start` = sapply(e$tasks, function(t) strftime(t$last_start, format = "%Y-%m-%d %H:%M:%OS")), 
      `Last End` = sapply(e$tasks, function(t) strftime(t$last_end, format = "%Y-%m-%d %H:%M:%OS"))
    )
    row.names(e$tasks_df) <- sapply(e$tasks, function(t) t$order) 
  }
  return(e)
}

# Capitalize first letter of a string
firstup <- function(x) {
  x <- tolower(x)
  substr(x, 1, 1) <- toupper(substr(x, 1, 1))
  x
}

# validate that dashboard can be rendered
can_render <- function(input, d) {
  shiny::validate(
      shiny::need(file.exists(conf$data_dir), 'Please go to configuration ans setup tweet collection (no data directory found)')
      , shiny::need(length(d$topics)>0, paste('No aggregated data found on ', paste(conf$data_dir, "series", sep = "/"), " please make sure this is the right folder, and that the aggregated task has successfully run"))
      , shiny::need(input$topics != '', 'Please select a topic')
  )
} 
