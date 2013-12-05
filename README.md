Yelp-data
=========
This code base consist of two python codes- YelpDataBase.py and analyzeYelpdata.py

YelpdataBase: reads in the Yelp data (json formatted) and create a mysql database, create four tables to store the businesses, users, checkins and reviews data. In order to user the code, one need to have mysql database access and also need to change the username. This is assuming you already have the Yelp dataset.

analyzeYelpdata: reads in Yelp data from mysql database, computes the mean and standard deviation (uses Jackknife sampling technique) for the full data and also for individual categories both for reviews and checkins data.


