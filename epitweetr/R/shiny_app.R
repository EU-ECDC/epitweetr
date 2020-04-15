
#' Running the epitwitter app
#' @export
epitweetr_app <- function() { 
  d <- get_dashboard_data() 
  
  # Defining dashbioard page UI
  dashboard_page <- 
    shiny::fluidPage(
      shiny::fluidRow(
        shiny::column(
          12, 
          shiny::fluidRow(
            shiny::column(3, 
                          "Parameters",
                          shiny::selectInput("topics", label = shiny::h3("Topics"), multiple = FALSE, choices = d$topics),
                          shiny::selectInput("countries", label = shiny::h3("Countries"), multiple = TRUE, choices = d$countries),
                          shiny::dateRangeInput("period", label = shiny::h3("Period"), start = d$date_start, end = d$date_end, min = d$date_min,max = d$date_max, format = "yyyy-mm-dd", startview = "month"), 
                          shiny::radioButtons("period_type", label = shiny::h3("Period type"), choices = list("Days"="created_date", "Weeks"="created_weeknum"), selected = "created_date", inline = TRUE),
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
                                          shiny::downloadButton("download_other_data", "data"),
                            )
                          ))
          )))) 
  
  # Defining navigation UI
  ui <- 
    shiny::navbarPage("epitweetr"
                      , shiny::tabPanel("Trend Line", dashboard_page)
                      , shiny::tabPanel("Configuration")
    )
  # Defining line chart from shiny app filters
  line_chart_from_filters <- function(topics, fcountries, period_type, period) {
    regions <- get_country_items()
    countries <- Reduce(function(l1, l2) {unique(c(l1, l2))}, lapply(as.integer(fcountries), function(i) unlist(regions[[i]]$codes)))
    trend_line(
      s_topic= topics
      ,s_country= countries
      ,type_date= period_type
      ,geo_country_code = "tweet_geo_country_code"
      ,date_min = strftime(period[[1]], format = (if(isTRUE(period_type=="created_weeknum")) "%G%V" else "%Y-%m-%d" ))
      ,date_max = strftime(period[[2]], format = (if(isTRUE(period_type=="created_weeknum")) "%G%V" else "%Y-%m-%d" ))
    )
    
  }
  # Defining line chart from shiny app filters
  map_chart_from_filters <- function(topics, fcountries, period_type, period) {
    regions <- get_country_items()
    countries <- Reduce(function(l1, l2) {unique(c(l1, l2))}, lapply(as.integer(fcountries), function(i) unlist(regions[[i]]$codes)))
    create_map(
      s_topic= topics
      ,s_country = countries
      ,geo_code = "tweet"
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

  export_dashboard <- function(format, file, topics, countries, period_type, period) {
    rmarkdown::render(
      system.file("rmarkdown", "dashboard.Rmd", package=get_package_name()), 
      output_format = format, 
      output_file = file,
      params = list(
        "topics" = topics
        , "countries" = countries
        , "period_type" = period_type
        , "period" = period
      ),
      quiet = TRUE
    ) 
  }
  
  # Defining server loginc
  server <- function(input, output, session, ...) {
    
    output$line_chart <- plotly::renderPlotly({
       shiny::validate(
         shiny::need(input$topics != '', 'Please select a topic')
       )
       chart <- line_chart_from_filters(input$topics, input$countries, input$period_type, input$period)$chart
       height <- session$clientData$output_p_height
       width <- session$clientData$output_p_width
       plotly::ggplotly(chart, height = height, width = width)
    })  
    output$map_chart <- shiny::renderPlot({
      shiny::validate(
        shiny::need(input$topics != '', 'Please select a topic')
      )
       regions <- get_country_items()
       countries <- Reduce(function(l1, l2) {unique(c(l1, l2))}, lapply(input$countries, function(i) unlist(regions[[i]]$codes)))
       map_chart_from_filters(input$topics, input$countries, input$period_type, input$period)$chart
    })
    output$topword_chart <- plotly::renderPlotly({
       shiny::validate(
         shiny::need(input$topics != '', 'Please select a topic')
       )
       chart <- topwords_chart_from_filters(input$topics, input$countries, input$period_type, input$period)$chart
       height <- session$clientData$output_p_height
       width <- session$clientData$output_p_width
       plotly::ggplotly(chart, height = height, width = width)
    })  
    output$download_line_data <- downloadHandler(
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
          line_chart_from_filters(input$topics, input$countries, input$period_type, input$period)$data,
          file, 
          row.names = FALSE)
      }
    ) 
    output$download_map_data <- downloadHandler(
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
    output$export_pdf <- downloadHandler(
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
         export_dashboard("pdf_document", file, input$topics, input$countries, input$period_type, input$period)
      }
    ) 
    output$export_md <- downloadHandler(
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
         export_dashboard("md_document", file, input$topics, input$countries, input$period_type, input$period)
      }
    ) 
  } 
  # Printing PID 
  message(Sys.getpid())
  # Launching the app
  shiny::shinyApp(ui = ui, server = server)
}

#' Get default data for dashoard
get_dashboard_data <- function() {
  dfs <- get_aggregates()
  d <- list()
  d$topics <- c("",unique(dfs$topic))
  d$topics <- stringr::str_replace_all(d$topics, "%20", " ")
  d$countries <- {
    regions <- get_country_items()
    setNames(1:length(regions), sapply(regions, function(r) paste(r$pad, r$name)))   
  } 
  d$date_min <- strftime(as.Date(min(dfs$created_date), origin ='1970-01-01'), format = "%Y-%m-%d")
  d$date_max <- strftime(as.Date(max(dfs$created_date), origin ='1970-01-01'), format = "%Y-%m-%d")
  d$date_start <- 
    if(max(dfs$created_date) - min(dfs$created_date) < 90) 
      d$date_min 
  else 
    strftime(as.Date(max(dfs$created_date), origin ='1970-01-01') - 90, format = "%Y-%m-%d")
  d$date_end <- d$date_max
  return(d)
}
