import time
from collections import deque
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, Column, Integer, String, Sequence
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import requests
import threading

Base = declarative_base()

class VisitedURL(Base):
    __tablename__ = 'visited_urls'
    id = Column(Integer, Sequence('url_id_seq'), primary_key=True)
    url = Column(String(500))

class WebCrawler:
    def __init__(self, rate_limit=1):  # 1 request per second by default
        self.rate_limit = rate_limit
        self.queue = deque()
        self.visited_urls = set()

        # Database setup
        engine = create_engine('sqlite:///urls.db')
        Base.metadata.create_all(engine)
        self.DBSession = sessionmaker(bind=engine)

    def fetch(self, url):
        try:
            response = requests.get(url)
            if response.status_code == 200 and 'text/html' in response.headers['Content-Type']:
                return response.content
            return None
        except (requests.RequestException, KeyError) as e:
            print(f"Failed to fetch {url}. Error: {e}")
            return None

    def extract_links(self, content):
        soup = BeautifulSoup(content, "html.parser")
        return [a['href'].split('#')[0] for a in soup.find_all('a', href=True) if a['href'].startswith('http')]

    def crawl(self, start_url, max_sites=50):
        self.queue.append(start_url)

        while self.queue and len(self.visited_urls) < max_sites:
            threads = []
            for i in range(min(self.rate_limit, len(self.queue))):
                t = threading.Thread(target=self.process_url)
                t.start()
                threads.append(t)
                time.sleep(1 / self.rate_limit)
            
            for t in threads:
                t.join()

    def process_url(self):
        current_url = self.queue.popleft()
        if current_url in self.visited_urls:
            return

        print(f"Crawling: {current_url}")
        content = self.fetch(current_url)
        if content:
            self.visited_urls.add(current_url)
            links = self.extract_links(content)
            self.queue.extend(links)

            # Save to database
            session = self.DBSession()
            new_url = VisitedURL(url=current_url)
            session.add(new_url)
            session.commit()
            session.close()

if __name__ == "__main__":
    crawler = WebCrawler(rate_limit=5)  # 5 threads
    crawler.crawl("https://www.example.com/")
