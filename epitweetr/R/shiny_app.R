
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
                          shiny::radioButtons("period_type", label = shiny::h3("Period type"), choices = list("Days"="days", "Weeks"="weeks"), selected = "days", inline = TRUE)
            ), 
            shiny::column(9, 
                          shiny::fluidRow(
                            shiny::column(6, 
                                          shiny::plotOutput("line_chart")
                            )
                            , shiny::column(6, 
                                            shiny::plotOutput("map_chart")
                            )
                          ))
          )))) 
  
  # Defining navigation UI
  ui <- 
    shiny::navbarPage("epitweetr"
                      , shiny::tabPanel("Trend Line", dashboard_page)
                      , shiny::tabPanel("Configuration")
    )
  
  # Defining server loginc
  server <- function(input, output) {
    output$line_chart <- shiny::renderPlot(
      trend_line(
        s_topic= input$topics
        ,s_country= input$countries
        ,type_date= input$period_type
        ,geo_country_code = "tweet_geo_country_code"
        ,date_min = strftime(input$period[[1]], format = (if(isTRUE(input$period_type=="weeks")) "%Y%V" else "%Y-%m-%d" ))
        ,date_max = strftime(input$period[[2]], format = (if(isTRUE(input$period_type=="weeks")) "%Y%V" else "%Y-%m-%d" ))
      )
    )  
    output$map_chart <- shiny::renderPlot(
      create_map(
        s_topic= input$topics
        ,type_date= input$period_type
        ,geo_code = "days"
        ,date_min = strftime(input$period[[1]], format = (if(isTRUE(input$period_type=="weeks")) "%Y%V" else "%Y-%m-%d" ))
        ,date_max = strftime(input$period[[2]], format = (if(isTRUE(input$period_type=="weeks")) "%Y%V" else "%Y-%m-%d" ))
      )
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
  d$topics <- unique(dfs$topic)
  d$countries <- {
    to_sort <- unique(union(unique(dfs$user_geo_country_code), unique(dfs$user_geo_country_code)))
    to_sort[order(to_sort)]
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
