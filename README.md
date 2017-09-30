# gameofderivatives
insight data engineering - fall 2017

# Business case
Companies with large asset base trade in the derivatives market to hedge their assets but not every company is successful in doing so. The goal is to find “how much money they owe in the market?” and also find out “what is the company X’s investment risk if it trades with company Y’s?

# Industry
This problem focuses on the financial industry but the technical implementation can be used in any industry which require fast computation with realtime analytics

# Stream Processing
Ingest historical and projected interest rates, stream current FX rates, stream historically traded contracts, calculate the price of each trade (100K contracts), re-calculate the value of each trade every time there is a change in FX rates

# Source of Data
Generate Swap contract data (ex: start date, end date, frequency of payments etc.), get realtime FX and interest rate data from Nasdaq or Bloomberg and also generate
