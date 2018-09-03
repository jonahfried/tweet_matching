# tweet_matching
- run "build_SQL_database.py" to create database
- run "sort_tweets_by_person.py [1-5]" five times with arguments between 1 and 5 to sort each fifth of the tweets by person
- run "read_pickles.py" to read each of the pkl files created by sort_tweets_by_person.py into the database
- run "matching.py [-n NUM_TWEETS][-t csv/json/csv+json/none]" to find pairs of tweets and output data to csv, json, both, or neither 
- run "similarity_visualization.ipynb" to see the t-sne representation of the csv data outputted
- run "d3_visualization.html" to see a d3 visualization of the json data outputted (must be run in a localhost server)
