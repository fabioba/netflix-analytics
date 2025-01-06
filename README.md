# netflix-analytics
Analyze personal Netflix usage


## Goal
The goal of this project is to analyze the Netflix usage starting from the first day of subscription (April 2024).

### Business Questions
- what are the most watched tv series and movie category per month? and lifetime?
- how often did I watch movies or tv series per week?
- binge watching analysis: average time to complete a tv series since the first watch

### North Star KPIs
- watch at least 2 different tv series during the same week
- bing watch (tv series): complete a tv series in less than 3 days


## Technical Guide
In order to reach the goal these are the steps to follow to develop the system:
1. ingest data from Netflix
2. for each movie, extract the details (via the TMDB API)
3. create the star schema
