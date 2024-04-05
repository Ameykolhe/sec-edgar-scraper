# SecEdgarScraper Library Documentation

[![Github Pages](https://github.com/Ameykolhe/sec-edgar-scraper/actions/workflows/jekyll-gh-pages.yml/badge.svg)](https://github.com/Ameykolhe/sec-edgar-scraper/actions/workflows/jekyll-gh-pages.yml)

The `SecEdgarScraper` library is a tool designed to facilitate the retrieval of financial statements from the SEC EDGAR database. This documentation provides a comprehensive guide on setting up and using the library to fetch financial information for financial analysis, research, or data ingestion pipelines.

## Setting Up

Installing the library.

```shell
pip3 install .
```

## Initialization

Create an instance of `SecEdgarScraper` with your name and email, which will be used in the User-Agent header for requests.

```python
from sec_edgar_scraper import SecEdgarScraper

name = "Your Name"
email = "your.email@example.com"

scraper = SecEdgarScraper(name, email)
```

## Fetching Company CIK

Retrieve the Central Index Key (CIK) for a company using its ticker symbol.

```python
cik = scraper.get_cik_matching_ticker("TSLA")
print(f"CIK for TSLA: {cik}")
```

## Retrieving Financial Statements

Fetch specific financial statements for the company for all filings of a certain type or for a specific filing.

### Retrieving Accession Numbers for Filings

```python
accession_numbers = scraper.get_filtered_filings(cik, "10-K")
print(f"Accession Numbers for 10-K filings: {accession_numbers}")
```


### Fetching a Specific Financial Statement for a Given Filing

Fetch the Balance Sheet from a specific filing using its accession number.

```python
cik = scraper.get_cik_matching_ticker("TSLA")
accession_number = "000156459020047486"

# Permitted values for statement 
# balance_sheet
# cash_flow_statement
# income_statement
statement = "balance_sheet"

df = scraper.get_one_statement(cik, accession_number, statement)
print(f"Balance Sheet for {accession_number}:\n{df}\n")
```

## Notes

- Ensure compliance with the SEC EDGAR system's fair access policy and terms of use.
- Implement appropriate error handling and retry logic to prevent overloading the servers.
