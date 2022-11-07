# freemium_promotions

This repository contains data preparation and analysis code for the paper "Price Promotions and Freemium App Monetization" with Jonathan Levav and Harikesh Nair (https://link.springer.com/article/10.1007/s11129-022-09248-3). We conducted an eight-month experiment in a large-scale representative mobile game app. Datasets used in analysis were aggregated from low-level tracking data that the app developer observed and stored.

The file "trackingDataExtraction.sql" aggregates the low-level data into meaningful cross-sectional (by users contained in experiment) and panel (by user and calendar day) datasets. The code separates out each individual aggregation step -- it is written for comprehensibility not efficiency. It was run on the collaborating firm's data servers and the resulting datasets were stored in the firm's database. From there, they were extracted into local CSV files using the code shown in file "databaseToCsvExtraction.sql." The CSV files were then imported into Stata and further enriched to make them more amenable to repeat analysis and replication. The code run in Stata for data manipulation is shown in files "doFileToCreateWellBehavedXsection.do" and "doFileToCreateWellBehavedPanel.do."

The final datasets were analyzed in Stata and R using visualization, descriptive comparisons, statistical testing and regression analysis. The files "analysisInR.R" and "analysisInStata.do" contain the code used for these analyses.

The data can be made available for replication, please reach out to the corresponding author.
