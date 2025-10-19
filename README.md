Largest Banks Web Scraping and Data Transformation

This project automates the process of extracting, transforming, and storing financial data using Python. It scrapes information about the world’s largest banks by market capitalization from a Wikipedia archive, cleans and converts the data, and saves it into both CSV and SQLite formats for analysis.

Project Overview

This project follows a simple ETL (Extract, Transform, Load) workflow.

 1. Extract
- Scrapes the table under "By market capitalization" from an archived Wikipedia pag e.
- Uses the requests library to fetch webpage content and BeautifulSoup to parse the HTML.
- Extracts bank names and their market capitalization (in USD billions) into a pandas DataFrame.

 2. Transform
- Cleans and converts market capitalization values by removing unwanted characters.
- Converts string values into float type.
- Uses exchange rates from a CSV file to create new columns for GBP, EUR, and INR.
- Rounds all calculated currency values to two decimal places.

 3. Load
- Saves the final cleaned and transformed dataset into:
  - Largest_banks_data.csv (CSV format)
  - Banks.db (SQLite database)
- Executes SQL queries to display all records, calculate the average market cap in GBP, and list the top 5 banks.

 Technologies Used
- Python 3.11
- BeautifulSoup4
- Requests
- Pandas
- NumPy
- SQLite3

 Output Files
- Largest_banks_data.csv: Cleaned and transformed dataset.
- Banks.db: SQLite database containing the table Largest_banks.
- exchange_rate.csv: File containing currency conversion rates.
- code_log.txt: Log file recording each stage of the ETL process.

 Key Learnings
- Building an ETL pipeline in Python.
- Performing web scraping using BeautifulSoup.
- Cleaning and transforming data using Pandas and NumPy.
- Converting and mapping data across multiple currencies.
- Executing SQL queries to analyze the results.

 How to Run
1. Clone this repository.
2. Install dependencies:
   ```bash
   pip install pandas numpy beautifulsoup4 requests
