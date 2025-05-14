# Food Scraper

A `Python` `scrapy` project to gather price + nutritional data from the internet.

## Setup

1. Clone the repository: `git clone https://github.com/maxsands700/food_scraper.git`
2. Create a virtual environment: `python3 -m venv venv`
3. Activate the virtual environment: `source venv/bin/activate` (macOS/Linux) or `venv\Scripts\activate` (Windows)
4. Install dependencies: `pip install -r requirements.txt`
5. Uses ScrapeOps for proxy service - make an account and get API key -> enter in `settings.py`
6. Change to inner `food_scraper` directory = `cd food_scraper` and then run `scrapy crawl <spider-name>`
