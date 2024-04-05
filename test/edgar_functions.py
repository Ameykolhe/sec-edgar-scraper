import calendar
import logging

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

from sec_edgar_scraper.utils import make_get_request

logging.basicConfig(level=logging.INFO)


class SecEdgarScraper:

    def __init__(self, name: str, email: str):
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
                "consolidated balance sheets (unaudited)",
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
                "consolidated results of operations",
                "consolidated statements of income (loss)",
                "consolidated statements of income - southern",
                "consolidated statements of operations and comprehensive income",
                "consolidated statements of comprehensive income",
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
                "consolidated statements of cash flows - southern",
            ],
        }

        self.ticker_json_url = "https://www.sec.gov/files/company_tickers.json"
        self.cik_submission_data_url = "https://data.sec.gov/submissions/CIK{cik}.json"
        self.facts_url = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
        self.base_link = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession_number}"

    def get_cik_matching_ticker(self, ticker):

        ticker = ticker.upper().replace(".", "-")
        ticker_json = make_get_request(self.ticker_json_url, self.headers)

        for company in ticker_json.values():
            if company["ticker"] == ticker:
                cik = str(company["cik_str"]).zfill(10)
                return cik
        raise ValueError(f"Ticker {ticker} not found in SEC database")

    def __get_submission_data_for_ticker(self, cik, only_filings_df=False):

        url = self.cik_submission_data_url.format(cik=cik)
        company_json: dict = make_get_request(url, self.headers)
        if only_filings_df:
            return pd.DataFrame(company_json["filings"]["recent"])
        else:
            return company_json

    def get_filtered_filings(self, cik, ten_k=True, just_accession_numbers=False):

        # Fetch submission data for the given ticker
        company_filings_df = self.__get_submission_data_for_ticker(cik, only_filings_df=True)

        # Filter for 10-K or 10-Q forms
        df = company_filings_df[company_filings_df["form"] == ("10-K" if ten_k else "10-Q")]
        df.accessionNumber = df.accessionNumber.replace("-", "", regex=True)

        # Return accession numbers if specified
        if just_accession_numbers:
            df = df.set_index("reportDate")
            accession_df = df["accessionNumber"]
            return accession_df
        else:
            return df

    def __get_facts(self, cik):

        # Construct URL for company facts
        url = self.facts_url.format(cik=cik)

        # Fetch and return company facts
        company_facts = make_get_request(url, self.headers)
        return company_facts

    def __facts_df(self, cik):

        # Retrieve facts data
        facts: dict = self.__get_facts(cik)
        us_gaap_data = facts["facts"]["us-gaap"]
        df_data = []

        # Process each fact and its details
        for fact, details in us_gaap_data.items():
            for unit in details["units"]:
                for item in details["units"][unit]:
                    row = item.copy()
                    row["fact"] = fact
                    df_data.append(row)

        df = pd.DataFrame(df_data)

        # Convert 'end' and 'start' to datetime
        df["end"] = pd.to_datetime(df["end"])
        df["start"] = pd.to_datetime(df["start"])

        # Drop duplicates and set index
        df = df.drop_duplicates(subset=["fact", "end", "val"])
        df.set_index("end", inplace=True)

        # Create a dictionary of labels for facts
        labels_dict = {fact: details["label"] for fact, details in us_gaap_data.items()}
        return df, labels_dict

    def __annual_facts(self, cik):

        accession_nums = self.get_filtered_filings(cik, ten_k=True, just_accession_numbers=True)
        df, label_dict = self.__facts_df(cik)
        ten_k = df[df["accn"].isin(accession_nums)]
        ten_k = ten_k[ten_k.index.isin(accession_nums.index)]
        pivot = ten_k.pivot_table(values="val", columns="fact", index="end")
        pivot.rename(columns=label_dict, inplace=True)
        return pivot.T

    def __quarterly_facts(self, cik):

        accession_nums = self.get_filtered_filings(cik, ten_k=False, just_accession_numbers=True)
        df, label_dict = self.__facts_df(cik)
        ten_q = df[df["accn"].isin(accession_nums)]
        ten_q = ten_q[ten_q.index.isin(accession_nums.index)].reset_index(drop=False)
        ten_q = ten_q.drop_duplicates(subset=["fact", "end"], keep="last")
        pivot = ten_q.pivot_table(values="val", columns="fact", index="end")
        pivot.rename(columns=label_dict, inplace=True)
        return pivot.T

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

    def __get_statement_file_names_in_filing_summary(self, cik, accession_number):

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

        session = requests.Session()

        base_link = self.base_link.format(cik=cik, accession_number=accession_number)

        statement_file_name_dict = self.__get_statement_file_names_in_filing_summary(cik, accession_number)

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
                return BeautifulSoup(
                    statement_response.content, "lxml-xml", from_encoding="utf-8"
                )
            else:
                return BeautifulSoup(statement_response.content, "lxml")

        except requests.RequestException as e:
            raise ValueError(f"Error fetching the statement: {e}")

    @staticmethod
    def __extract_columns_values_and_dates_from_statement(soup):

        columns = []
        values_set = []
        date_time_index = SecEdgarScraper.__get_datetime_index_dates_from_statement(soup)

        for table in soup.find_all("table"):

            # default values are in thousands
            unit_multiplier = 1
            special_case = False

            # Check table headers for unit multipliers and special cases
            table_header = table.find("th")
            if table_header:
                header_text = table_header.get_text()
                # Determine unit multiplier based on header text
                if "in Thousands" in header_text:
                    unit_multiplier = 1
                elif "in Millions" in header_text:
                    unit_multiplier = 1000
                # Check for special case scenario
                if "unless otherwise specified" in header_text:
                    special_case = True

            # Process each row of the table
            for row in table.select("tr"):
                onclick_elements = row.select("td.pl a, td.pl.custom a")
                if not onclick_elements:
                    continue

                # Extract column title from 'onclick' attribute
                onclick_attr = onclick_elements[0]["onclick"]
                column_title = onclick_attr.split("defref_")[-1].split("',")[0]
                columns.append(column_title)

                # Initialize values array with NaNs
                values = [np.NaN] * len(date_time_index)

                # Process each cell in the row
                for i, cell in enumerate(row.select("td.text, td.nump, td.num")):
                    if "text" in cell.get("class"):
                        continue

                    # Clean and parse cell value
                    value = SecEdgarScraper.__keep_numbers_and_decimals_only_in_string(
                        cell.text.replace("$", "")
                        .replace(",", "")
                        .replace("(", "")
                        .replace(")", "")
                        .strip()
                    )
                    if value:
                        value = float(value)
                        # Adjust value based on special case and cell class
                        if special_case:
                            value /= 1000
                        else:
                            if "nump" in cell.get("class"):
                                values[i] = value * unit_multiplier
                            else:
                                values[i] = -value * unit_multiplier

                values_set.append(values)

        return columns, values_set, date_time_index

    @staticmethod
    def __get_datetime_index_dates_from_statement(soup: BeautifulSoup) -> pd.Series:

        table_headers = soup.find_all("th", {"class": "th"})
        dates = [str(th.div.string) for th in table_headers if th.div and th.div.string]
        dates = [SecEdgarScraper.__standardize_date(date).replace(".", "") for date in dates]
        index_dates = pd.to_datetime(dates)
        return index_dates

    @staticmethod
    def __standardize_date(date: str) -> str:

        for abbr, full in zip(calendar.month_abbr[1:], calendar.month_name[1:]):
            date = date.replace(abbr, full)
        return date

    @staticmethod
    def __keep_numbers_and_decimals_only_in_string(mixed_string: str):

        num = "1234567890."
        allowed = list(filter(lambda x: x in num, mixed_string))
        return "".join(allowed)

    @staticmethod
    def __create_dataframe_of_statement_values_columns_dates(values_set, columns, index_dates) -> pd.DataFrame:

        transposed_values_set = list(zip(*values_set))
        df = pd.DataFrame(transposed_values_set, columns=columns, index=index_dates)
        return df

    def get_one_statement(self, cik, accession_number, statement_name):

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
                # Extract data and create DataFrame
                columns, values, dates = SecEdgarScraper.__extract_columns_values_and_dates_from_statement(soup)
                df = SecEdgarScraper.__create_dataframe_of_statement_values_columns_dates(values, columns, dates)

                if not df.empty:
                    # Remove duplicate columns
                    df = df.T.drop_duplicates()
                else:
                    logging.warning(
                        f"Empty DataFrame for accession number: {accession_number}"
                    )
                    return None

                return df
            except Exception as e:
                logging.error(f"Error processing statement: {e}")
                return None


if __name__ == "__main__":
    name = "YourName"
    email = "yourEmail@domain.tld"

    scraper = SecEdgarScraper(name, email)

    cik = scraper.get_cik_matching_ticker("tsla")
    accession_number = "000162828023034847"
    df = scraper.get_one_statement(cik, accession_number, "cash_flow_statement")

    print(df)