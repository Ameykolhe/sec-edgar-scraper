import json

from sec_edgar_scraper import SecEdgarScraper

scraper = SecEdgarScraper("Amey", "kolheamey99@gmail.com")

cik = scraper.get_cik_matching_ticker("tsla")

filings = scraper.get_filtered_filings(cik, "10-K", just_accession_numbers=False)

statement_name = "income_statement"

for accession_number in filings.accessionNumber:

    print(accession_number)

    statement_file_name_dict = scraper.get_statement_file_names_in_filing_summary(cik, accession_number)
    base_link = scraper.base_link.format(cik=cik, accession_number=accession_number)

    for possible_key in scraper.statement_keys_map.get(statement_name.lower(), []):
        file_name = statement_file_name_dict.get(possible_key.lower())
        if file_name:
            statement_link = f"{base_link}/{file_name}"
            print(possible_key, statement_link, sep=" ")
