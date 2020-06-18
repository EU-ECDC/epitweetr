# epitweetr: ECDC tool on Twitter trend analysis for early detection of public health threats

The epitweetr package allows you to automatically monitor trends of tweets by time, place and topic. This automated monitoring aims at early detecting public health threats through the detection of signals or unusual increase of the number of tweets for that time, place and topic. The epitweetr package was designed to focus on  infectious diseases and other related health topics, and it can be used for other fields as well modifying the topics.

epitweetr is designed to access Twitter's standard search API, which returns a collection of relevant tweets, according to Twitter, matching a specified query. epitweetr uses this API to automatically collect tweets by topic, geolocalise the tweets, aggregate the tweets by time, place and topic and detect signals. Signals are detected when the number of tweets exceed the threshold of what is expected. The package also has a functionality to send email alerts for detected signals.

The package includes an interactive Shiny application (Shiny app), in which the user can specify the configuration for the above-mentioned tasks. The Shiny app also includes an interactive visualisation component, the dashboard, in which users can view the aggregated number of tweets over time, the location of these tweets and the words most frequently found in these tweets. These visualizations can be filtered by the topic, location and time period you are interested in. Other filters are available to adjust the time unit of the timeline, whether retweets should be included, what kind of geolocation types you are interested in, the sensitivity of the prediction interval for the signal detection, and the number of days used to calculate the threshold for signals. This information is also downloadable directly from this interface in the form of data, pictures, or reports.



