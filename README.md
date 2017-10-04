# gameofderivatives
insight data engineering - fall 2017

# Business case
Companies with large asset base trade in the derivatives market to hedge their assets but not every company is successful in doing so. The objective of the project is to find 
  - how much money they owe in the market?
  - what is the company X’s investment risk if it trades with company Y’s?
  - what is company X's exposure to changing exchange rate?

# Industry
This problem focuses on the financial industry but the technical implementation can be used in any industry which require fast computation with realtime analytics

# Stream Processing
Ingested historical and projected interest rates, streamed current FX rates, streamed historically traded contracts, calculated the price of each trade (100K contracts), re-calculated the value of 100k contracts every time there is a change in exhange rate under 10 seconds

# Source of Data
Generated (and ingested) Swap contract data (ex: start date, end date, frequency of payments etc.), generated random FX and projected interest rate data

# Pipeline
![Game of Derivatives](https://raw.githubusercontent.com/github81/gameofderivatives/master/images/pipeline.png.tiff)

# Project highlights
- Continuous streaming of data and writing to Cassandra, 
- Usage of Redis database to store 4GB of trade contracts
- 3 sets of Kafka producers, spark streaming consumers
- Kafka streaming topics by currency
- Pricing algorithm

