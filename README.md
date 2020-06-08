# epitweetr: ECDC Epidemic Intelligence tool for tweet analysis

The epitweetr package allows you to automatically monitor trends of tweets by time, place and topic, in order to detect signals of a number of tweets that exceed what is expected. The epitweetr package was designed to do this for communicable diseases and other health topics, but could be used for other fields as well.

epitweetr is designed to access Twitter's free search API, which returns a collection of relevant tweets matching a specified query. epitweetr uses this API to automatically collect tweets by topic, geolocalise the tweets, aggregate the tweets by time, place and topic and detect signals. Signals are when the number of tweets exceed the threshold of what is expected. The package also has a functionality to send email alerts for detected signals.

The package includes an interactive Shiny application (Shiny app), in which the user can specify the configuration for the above-mentioned tasks. The Shiny app also includes an interactive visualisation component, the dashboard, in which users can view the aggregated number of tweets over time, the location of these tweets and the words most frequently found in these tweets. These visualizations can be filtered by the topic, location and time period you are interested in. Other filters are available to adjust the time unit of the timeline, whether retweets should be included, what kind of geolocation types you are interested in, the sensitivity of the prediction interval for the signal detection, and the number of days used to calculate the threshold for signals. This information is also downloadable directly from this interface in the form of data, pictures, or reports.



