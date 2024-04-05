import calendar
import logging
import re
from io import StringIO

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

from sec_edgar_scraper.exceptions import InvalidStatementLinkException
from sec_edgar_scraper.utils import make_get_request

logging.basicConfig(level=logging.INFO)

"""
This module provides a scraper for SEC EDGAR financial statements. It facilitates
the retrieval of financial statement data from the SEC EDGAR database using
web requests, parsing the retrieved HTML content, and processing the data for
analysis. The scraper is designed to be used programmatically for financial
analysis, research, or as part of a larger data ingestion pipeline.

Classes:
    SecEdgarScraper: A scraper for retrieving and parsing financial statements
    from the SEC EDGAR database.
"""


class SecEdgarScraper:

    def __init__(self, name: str, email: str):
        """
                Initializes a new instance of the SecEdgarScraper.

                Sets up the request headers using the provided name and email, to be used
                for making requests to the SEC EDGAR system. It is important to comply
                with SEC EDGAR's terms of service by providing a valid user agent.

                Parameters:
                    name (str): The name of the individual or entity using the scraper.
                    email (str): The email address associated with the user of the scraper.

                Returns:
                    None
        """
        self.headers = {
            "User-Agent": f"{name} {email}"
        }

        self.statement_keys_map = {
            "balance_sheet": [
                "balance sheet",
                "balance sheets",
                "statement of financial position",
                "consolidated balance sheets",
                "consolidated balance sheet",
                "consolidated financial position",
                "consolidated balance sheets - southern",
                "consolidated statements of financial position",
                "consolidated statement of financial position",
                "consolidated statements of financial condition",
                "combined and consolidated balance sheet",
                "condensed consolidated balance sheets",
                "consolidated balance sheets, as of december 31",
                "dow consolidated balance sheets",
                "consolidated balance sheets (unaudited)"
            ],
            "income_statement": [
                "income statement",
                "income statements",
                "statement of earnings (loss)",
                "statements of consolidated income",
                "consolidated statements of operations",
                "consolidated statement of operations",
                "consolidated statements of earnings",
                "consolidated statement of earnings",
                "consolidated statements of income",
                "consolidated statement of income",
                "consolidated income statements",
                "consolidated income statement",
                "condensed consolidated statements of earnings",
                "condensed consolidated statements of income",
                "consolidated results of operations",
                "consolidated statements of income (loss)",
                "consolidated statements of income - southern",
                "consolidated statements of operations and comprehensive income",
                "consolidated statements of comprehensive income",
                "consolidated statements of comprehensive income (unaudited)"
            ],
            "cash_flow_statement": [
                "cash flows statement",
                "cash flows statements",
                "statement of cash flows",
                "statements of consolidated cash flows",
                "consolidated statements of cash flows",
                "consolidated statement of cash flows",
                "consolidated statement of cash flow",
                "consolidated cash flows statements",
                "consolidated cash flow statements",
                "condensed consolidated statements of cash flows",
                "consolidated statements of cash flows (unaudited)",
                "consolidated statements of cash flows - southern"
            ],
        }

        self.ticker_json_url = "https://www.sec.gov/files/company_tickers.json"
        self.cik_submission_data_url = "https://data.sec.gov/submissions/CIK{cik}.json"
        self.facts_url = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
        self.base_link = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession_number}"

    def get_cik_matching_ticker(self, ticker):
        """
            Retrieves the Central Index Key (CIK) matching a given stock ticker symbol
            from the SEC database.

            This method searches for a company's CIK based on its stock ticker symbol
            by making a request to the SEC database. The stock ticker is adjusted to
            match the SEC's formatting requirements (e.g., converting "." to "-"). If a
            matching CIK is found, it is returned as a 10-digit string, padded with
            zeros if necessary. If no matching ticker is found in the SEC database, a
            ValueError is raised.

            Parameters:
                ticker (str): The stock ticker symbol for which to find the matching CIK.

            Returns:
                str: The 10-digit CIK (Central Index Key) corresponding to the given ticker
                symbol, formatted as a string.

            Raises:
                ValueError: If the ticker symbol provided does not match any entries in
                the SEC database.
        """

        ticker = ticker.upper().replace(".", "-")
        ticker_json = make_get_request(self.ticker_json_url, self.headers)

        for company in ticker_json.values():
            if company["ticker"] == ticker:
                cik = str(company["cik_str"]).zfill(10)
                return cik
        raise ValueError(f"Ticker {ticker} not found in SEC database")

    def __get_submission_data_for_ticker(self, cik, only_filings_df=False):
        """
            Retrieves submission data for a given CIK (Central Index Key) from the SEC database.

            This method fetches submission data for a company identified by its CIK.
            It makes a request to the SEC database and can return the data either as a
            raw JSON object or as a pandas DataFrame of the company's recent filings,
            depending on the `only_filings_df` flag.

            Parameters:
                cik (str): The Central Index Key (CIK) of the company for which to retrieve
                           submission data.
                only_filings_df (bool, optional): A flag indicating whether to return only
                           the company's recent filings as a pandas DataFrame. If False,
                           the complete submission data as a JSON object is returned. Defaults
                           to False.

            Returns:
                pandas.DataFrame or dict: If `only_filings_df` is True, returns a pandas
                DataFrame containing the company's recent filings. Otherwise, returns the
                complete submission data as a JSON object.

            Note:
                This method is intended for internal use within the class, as denoted by
                the leading double underscores in its name.
        """

        url = self.cik_submission_data_url.format(cik=cik)
        company_json: dict = make_get_request(url, self.headers)
        if only_filings_df:
            return pd.DataFrame(company_json["filings"]["recent"])
        else:
            return company_json

    def get_filtered_filings(self, cik, form=None, just_accession_numbers=True):
        """
           Retrieves and optionally filters SEC filings for a given CIK, based on the
           form type (e.g., 10-K, 10-Q). It can return either a DataFrame with detailed
           filing information or just the accession numbers for the filings, based on
           the `just_accession_numbers` flag.

           Parameters:
               cik (str): The Central Index Key (CIK) of the company for which to retrieve
                          filings.
               form (str, optional): The form type to filter the filings by (e.g., '10-K',
                                     '10-Q'). If None, no filtering is applied. Defaults to None.
               just_accession_numbers (bool, optional): A flag indicating whether to return
                                                        only the accession numbers of the filings.
                                                        If True, returns a Series of accession numbers
                                                        indexed by report date. If False, returns a
                                                        DataFrame with detailed filing information.
                                                        Defaults to True.

           Returns:
               pandas.DataFrame or pandas.Series: If `just_accession_numbers` is True, returns a
               pandas Series of accession numbers indexed by report date. Otherwise, returns a
               DataFrame with detailed filing information, including the form type, filing date,
               and accession number for each filing.

           Note:
               This method allows for easy retrieval and filtering of company filings from the
               SEC database, making it particularly useful for financial analysis or research
               purposes.
        """

        # Fetch submission data for the given ticker
        company_filings_df = self.__get_submission_data_for_ticker(cik, only_filings_df=True)

        # Filter for 10-K or 10-Q forms
        df = company_filings_df[company_filings_df["form"] == form].copy()
        df.loc[:, "accessionNumber"] = df.accessionNumber.replace("-", "", regex=True)

        # Return accession numbers if specified
        if just_accession_numbers:
            df = df.set_index("reportDate")
            accession_df = df["accessionNumber"]
            return accession_df
        else:
            return df

    @staticmethod
    def __get_file_name(report):

        html_file_name_tag = report.find("HtmlFileName")
        xml_file_name_tag = report.find("XmlFileName")

        if html_file_name_tag:
            return html_file_name_tag.text
        elif xml_file_name_tag:
            return xml_file_name_tag.text
        else:
            return ""

    @staticmethod
    def __is_statement_file(short_name_tag, long_name_tag, file_name):

        return (
                short_name_tag is not None
                and long_name_tag is not None
                and file_name
                and "Statement" in long_name_tag.text
        )

    def get_statement_file_names_in_filing_summary(self, cik, accession_number):
        """
            Retrieves the file names of financial statements from a company's filing summary
            by accessing the SEC EDGAR database. It constructs the URL for the filing summary
            XML, fetches and parses it, and then extracts the names of files containing
            financial statements.

            Parameters:
                cik (str): The Central Index Key (CIK) of the company whose filing summary
                           is to be accessed.
                accession_number (str): The accession number of the filing from which to
                                        retrieve the statement file names.

            Returns:
                dict: A dictionary mapping the lowercased short names of financial statements
                      (e.g., 'balance sheet', 'income statement') to their respective file names
                      within the filing. If an error occurs during the request, an empty dictionary
                      is returned.

            Note:
                This method is essential for automating the retrieval of specific financial
                statements from a company's SEC filings, particularly useful for financial
                analysis, research, or data ingestion pipelines.

            Exceptions:
                prints: On encountering a RequestException, an error message is printed to
                        stdout, and an empty dictionary is returned.
        """

        try:
            session = requests.Session()
            base_link = self.base_link.format(cik=cik, accession_number=accession_number)
            filing_summary_link = f"{base_link}/FilingSummary.xml"
            filing_summary_response = session.get(
                filing_summary_link, headers=self.headers
            ).content.decode("utf-8")

            filing_summary_soup = BeautifulSoup(filing_summary_response, "lxml-xml")
            statement_file_names_dict = {}

            for report in filing_summary_soup.find_all("Report"):
                file_name = SecEdgarScraper.__get_file_name(report)
                short_name, long_name = report.find("ShortName"), report.find("LongName")

                if SecEdgarScraper.__is_statement_file(short_name, long_name, file_name):
                    statement_file_names_dict[short_name.text.lower()] = file_name

            return statement_file_names_dict

        except requests.RequestException as e:
            print(f"An error occurred: {e}")
            return {}

    def get_statement_soup(self, cik, accession_number, statement_name):
        """
            Retrieves the BeautifulSoup object for a specific financial statement from
            a company's filing on the SEC EDGAR database. This method constructs a URL
            for the financial statement by first determining the file name associated
            with the statement, then fetching and parsing the statement content.

            Parameters:
                cik (str): The Central Index Key (CIK) of the company whose financial
                           statement is to be retrieved.
                accession_number (str): The accession number of the filing that contains
                                        the desired financial statement.
                statement_name (str): The name of the financial statement to retrieve
                                      (e.g., 'Balance Sheet', 'Income Statement').

            Returns:
                BeautifulSoup: A BeautifulSoup object of the requested financial statement's
                               content, parsed from either HTML or XML format. Currently, XML
                               file formats are not supported and will raise an exception.

            Raises:
                ValueError: If the statement file name cannot be found based on the given
                            statement name, or if there is an error fetching the statement
                            content from the SEC website.
                InvalidStatementLinkException: Specifically raised if the statement file
                                                is in XML format, which is currently not supported.

            Note:
                This method is critical for parsing and analyzing the content of specific
                financial statements from SEC filings, aiding in financial analysis,
                research, or automated data processing tasks.
        """
        session = requests.Session()

        base_link = self.base_link.format(cik=cik, accession_number=accession_number)

        statement_file_name_dict = self.get_statement_file_names_in_filing_summary(cik, accession_number)

        statement_link = None
        for possible_key in self.statement_keys_map.get(statement_name.lower(), []):
            file_name = statement_file_name_dict.get(possible_key.lower())
            if file_name:
                statement_link = f"{base_link}/{file_name}"
                break

        if not statement_link:
            raise ValueError(f"Could not find statement file name for {statement_name}")

        try:
            statement_response = session.get(statement_link, headers=self.headers)
            statement_response.raise_for_status()  # Check if the request was successful
            logging.info(f"Statement Link - {statement_link}")
            if statement_link.endswith(".xml"):
                raise InvalidStatementLinkException("XML Files are currently not supported")
            else:
                return BeautifulSoup(statement_response.content, "lxml")

        except requests.RequestException as e:
            raise ValueError(f"Error fetching the statement: {e}")

        except InvalidStatementLinkException as e:
            logging.error(e, exc_info=True)

    @staticmethod
    def process_dict(df_dict):

        dict_items = list(df_dict.items())
        unit_multiplier = 1

        statement_name, attributes_dict = dict_items[0]
        statement_name = statement_name[0].lower() if type(statement_name) == tuple else statement_name.lower()

        attributes = list(attributes_dict.values())

        values_map = {}
        for date, values in dict_items[1:]:
            date = date[1] if type(date) == tuple else date
            if len(date) > 15:
                continue
            date = SecEdgarScraper.__standardize_date(date)

            values_map[date] = []

            for _, value in values.items():
                values_map[date].append(SecEdgarScraper.__standardize_number(value, unit_multiplier))

        date_index = pd.to_datetime(list(values_map.keys()))

        return attributes, date_index, list(values_map.values()), statement_name

    @staticmethod
    def __standardize_number(number_str: str, multiplier) -> int:

        if type(number_str) == float and np.isnan(number_str):
            return None

        if number_str.strip() in {'N/A', '--', '', "nan", "NAN"}:
            return None

        negative = False
        if '(' in number_str and ')' in number_str:
            negative = True
            number_str = number_str.replace('(', '').replace(')', '')

        is_num_currency = False
        if "$" in number_str:
            is_num_currency = True

        # Remove non-numeric characters except minus sign and decimal point
        number_str = re.sub(r'[^\d.-]', '', number_str)

        # Convert to float and apply negativity if necessary
        try:
            number = int(number_str)
            if negative:
                number = -number
        except ValueError:
            return None

        return number * multiplier if is_num_currency else number

    @staticmethod
    def __standardize_date(date: str | tuple) -> str:
        date = date[1] if type(date) == tuple else date
        for abbr, full in zip(calendar.month_abbr[1:], calendar.month_name[1:]):
            date = date.replace(abbr, full)
        return date

    def get_one_statement(self, cik, accession_number, statement_name):
        """
            Retrieves a single financial statement for a specified company filing from
            the SEC EDGAR database and converts it into a transposed pandas DataFrame.
            The method fetches the statement's HTML content, extracts the first table
            found (assuming it contains the statement data), and then processes it into
            a DataFrame.

            Parameters:
                cik (str): The Central Index Key (CIK) of the company whose financial
                           statement is to be retrieved.
                accession_number (str): The accession number of the filing that contains
                                        the desired financial statement.
                statement_name (str): The name of the financial statement to retrieve
                                      (e.g., 'Balance Sheet', 'Income Statement').

            Returns:
                pandas.DataFrame or None: A transposed DataFrame representing the financial
                statement data, with columns as the data attributes and the index as the
                dates. Returns None if there's an error in fetching the statement soup or
                processing the statement into a DataFrame.

            Note:
                This method is useful for users needing structured data from specific
                financial statements for analysis or research purposes. Errors encountered
                during the fetch or processing stages are logged, and None is returned to
                indicate failure.

            Raises:
                Logs an error: If unable to retrieve or process the statement soup for
                               the given parameters, an error is logged with details of
                               the exception and the method returns None.
        """
        try:
            # Fetch the statement HTML soup
            soup = self.get_statement_soup(cik, accession_number, statement_name)
        except Exception as e:
            logging.error(
                f"Failed to get statement soup: {e} for accession number: {accession_number}"
            )
            return None

        if soup:
            try:
                table = soup.find("table")
                df = pd.read_html(StringIO(str(table)))[0]

                df_dict = df.to_dict()

                attributes, date_index, values, notes = SecEdgarScraper.process_dict(df_dict)

                df = pd.DataFrame(values, columns=attributes, index=date_index)

                return df.T, notes

            except Exception as e:
                logging.error(f"Error processing statement: {e}")
                return None


def main():
    name = "YourName"
    email = "yourEmail@domain.tld"

    scraper = SecEdgarScraper(name, email)

    cik = scraper.get_cik_matching_ticker("tsla")

    accession_numbers = scraper.get_filtered_filings(cik, "10-K")
    print(accession_numbers)

    # for statement_name in ["balance_sheet", "income_statement", "cash_flow_statement"]:
    #     for accession_number in accession_numbers:
    #         df = scraper.get_one_statement(cik, accession_number, statement_name)
    #         print(f"{statement_name.capitalize()} for {accession_number}:\n{df}\n")

    accession_number = "000156459020047486"
    df = scraper.get_one_statement(cik, accession_number, "balance_sheet")
    print(df)


if __name__ == "__main__":
    main()
