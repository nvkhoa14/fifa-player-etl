# FiFa player information crawler end-to-end data engineering project.
## Introduction
This project is an ETL(Extract, Transform, Load) pipeline using Scrapy Spider, Pandas and AWS Lambda (on processing). The pipeline will crawl data from soFIFA (https://sofifa.com/), transform it to desired format and load it into anywhere(local, AWS data store,etc.).

## Project Excution Flow
Extract data from URLs (https://sofifa.com/players?col=oa&sort=desc&offset=0) to get player's IDs → Extract data from collected IDs → Store raw data (.json) → Trigger Transform Function → Transform and Load Data.

> More details about the flow go to [here](https://github.com/nvkhoa14/fifa-player-etl/blob/main/fifa-player-etl-pipeline.ipynb).
