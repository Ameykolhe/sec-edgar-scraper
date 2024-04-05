# US SEC EDGAR - Financial Statements Scraper

from setuptools import setup, find_packages

# Read the contents of your README file
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

# Read the contents of the requirements file
with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name="sec-edgar-scraper",
    version="0.1.0",
    author="Amey Kolhe",
    author_email="kolheamey99@gmail.com",
    description="A package for scraping financial statements from the SEC EDGAR database",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Ameykolhe/sec-edgar-scraper",
    project_urls={
        "Bug Tracker": "https://github.com/Ameykolhe/sec-edgar-scraper/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.6",
    install_requires=requirements,
)
