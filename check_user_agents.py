from db import Database
from Logger import Logger
from typing import Dict, List
import requests
from bs4 import BeautifulSoup
import asyncio


class ParallelScraper:
    def __init__(self, batch_size: int = 10, timeout: int = 10, max_retries: int = 3, delay_between_batches: int = 60):
        self.db = Database()
        self.batch_size = batch_size
        self.timeout = timeout
        self.max_retries = max_retries
        self.delay_between_batches = delay_between_batches
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
            "Mozilla/5.0 (Windows; U; Windows NT 10.4; x64) Gecko/20130401 Firefox/47.0",
            "Mozilla/5.0 (Windows; Windows NT 10.4; Win64; x64; en-US) Gecko/20130401 Firefox/59.8",
            "Mozilla/5.0 (Windows; U; Windows NT 10.0; x64) AppleWebKit/600.23 (KHTML, like Gecko) Chrome/54.0.3132.207 Safari/533.9 Edge/17.89535",
            "Mozilla/5.0 (Windows; U; Windows NT 10.5; x64; en-US) Gecko/20100101 Firefox/52.9",
            "Mozilla/5.0 (Windows; Windows NT 6.3; Win64; x64) Gecko/20100101 Firefox/67.2",
            "Mozilla/5.0 (Windows; U; Windows NT 6.1; WOW64; en-US) AppleWebKit/533.42 (KHTML, like Gecko) Chrome/50.0.2128.128 Safari/534.5 Edge/8.51659",
            "Mozilla/5.0 (Windows; U; Windows NT 10.5; Win64; x64; en-US) AppleWebKit/535.23 (KHTML, like Gecko) Chrome/54.0.3117.145 Safari/535",
            "Mozilla/5.0 (Windows NT 6.2; x64; en-US) AppleWebKit/602.7 (KHTML, like Gecko) Chrome/51.0.2749.255 Safari/537",
            "Mozilla/5.0 (Windows; U; Windows NT 6.0; Win64; x64) Gecko/20130401 Firefox/48.9",
            "Mozilla/5.0 (Windows NT 6.0; x64) AppleWebKit/535.34 (KHTML, like Gecko) Chrome/55.0.2147.331 Safari/602"
        ]
        self.proxies = self.load_proxies_from_file('proxies.txt')
        print(self.proxies)
        self.current_proxy_index = 0

    def load_proxies_from_file(self, file_name: str) -> List[Dict[str, str]]:
        proxies = []
        with open(file_name, 'r') as file:
            for line in file:
                parts = line.strip().split(':')
                if len(parts) == 4:
                    proxy_url = f"http://{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}"
                    proxies.append({
                        'http': proxy_url,
                        'https': proxy_url,
                    })
        return proxies

    async def fetch_url_data(self, url: str, user_agent: str) -> Dict[str, str]:
        """
        Fetch and parse data from the provided HTML
        """
        try:
            # Get the next proxy from the list
            proxy = self.proxies[self.current_proxy_index]
            Logger.info('Using proxy:', proxy)
            self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxies)

            # Make the request and get the HTML
            response = requests.get(url, timeout=self.timeout, headers={'User-Agent': user_agent}, proxies=proxy)
            response.raise_for_status()

            # Parse the HTML using BeautifulSoup
            soup = BeautifulSoup(response.text, 'html.parser')

            # Extract the address and image URL from the meta tags
            address = soup.select_one('meta[property="og:title"]').get('content')
            image_url = soup.select_one('meta[property="og:image"]').get('content')

            return {
                'address': address,
                'image_url': image_url,
            }

        except Exception as e:
            Logger.error(f"Error extracting data from HTML", e)

    async def run(self):
        """
        Main method to run the scraper
        """
        results = []
        for user_agent in self.user_agents:
            Logger.info(f"Using user agent: {user_agent}")
            result = await self.fetch_url_data(
                "https://www.google.com/maps/place/SpazeOne+Co-working+Space+Calicut+Bridgeway/data=!4m7!3m6!1s0x3ba65f91d261fa43:0x8d87095116a4bb89!8m2!3d11.2825204!4d75.7696205!16s%2Fg%2F11l1nz3rn0!19sChIJQ_ph0pFfpjsRibukFlEJh40?authuser=0&hl=en&rclk=1",
                user_agent)

            if result:
                results.append(user_agent)
            Logger.info(result)
        print(results)


async def main():
    scraper = ParallelScraper(
        batch_size=20,
        timeout=10,
        max_retries=10,
        delay_between_batches=10
    )
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(main())
