import requests
from bs4 import BeautifulSoup
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import unquote
import re
import clickhouse_connect
from datetime import datetime
import os

class NewsParsing:
    def __init__(self, base_url):
        self.base_url = base_url
        self.client = clickhouse_connect.Client(
            host=os.getenv('CLICKHOUSE_HOST'),
            user=os.getenv('CLICKHOUSE_USER'),
            password=os.getenv('CLICKHOUSE_PASSWORD')
        )

    def link_parsing(self, url):
        filtered_urls = []
        response = requests.get(url)
        if response.status_code == 200:
            html = response.text
            soup = BeautifulSoup(html, 'html.parser')
            links = soup.find_all('a', href=True)
            if 'cnews' in self.base_url:
                filtered_urls = [(link['href'], None) for link in links if link['href'].startswith('http://www.cnews.ru/news')]
            elif 'habr' in self.base_url:
                filtered_urls = [('https://habr.com' + link['href'], None) for link in links if link['href'].startswith('/ru/news/') and 'page' not in link['href'] and 'comment' not in link['href']]
            elif 'tadviser' in self.base_url:
                date_match = re.search(r'cdate=(\d{1,2})\.5\.2024', url)
                date_str = date_match.group(1) if date_match else None
                center_part = soup.find('div', class_='center_part')
                if center_part and date_str:
                    list_items = center_part.find_all('li')
                    filtered_urls = [('https://www.tadviser.ru' + unquote(link.find('a')['href']), f'{date_str}.05.2024') for link in list_items if link.find('a')['href'].startswith('/index.php/')]
            filtered_urls = list(set(filtered_urls))
        else:
            print(f"Не удалось получить доступ к сайту. Статус код: {response.status_code}")
        return filtered_urls

    def fetch_news(self, link, date):
        response = requests.get(link)
        if response.status_code != 200:
            print(f"Не удалось загрузить страницу {link}")
            return None

        try:
            response.encoding = response.apparent_encoding
            soup = BeautifulSoup(response.content, 'html.parser')
            title = soup.find('h1').get_text(strip=True) if soup.find('h1') else 'No title'

            if 'cnews' in self.base_url:
                article_block = soup.find(class_='news_container')
                date_pattern = re.compile(r'(\d{4})-(\d{2})-(\d{2})')
                match = date_pattern.search(link)
                if match:
                    year, month, day = match.groups()
                    time_published = f"{day}.{month}.{year}"
                source = 'cnews'

            elif 'habr' in self.base_url:
                title = soup.find('meta', property='og:title')['content'] if soup.find('meta', property='og:title') else 'No title'
                article_block = soup.find('div', class_='tm-article-body')
                time_published = soup.find('meta', property='aiturec:datetime')['content'] if soup.find('meta', property='aiturec:datetime') else None
                date_pattern = re.compile(r'(\d{4})-(\d{2})-(\d{2})')
                match = date_pattern.search(time_published)
                if match:
                    year, month, day = match.groups()
                    time_published = f"{day}.{month}.{year}"
                source = 'habr'

            elif 'tadviser' in self.base_url:
                article_block = soup.find('div', class_='js-mediator-article')
                time_published = date
                source = 'tadviser'

            if not article_block:
                print(f"Не удалось найти блок статьи для {link}")
                return None

            text = ''
            paragraphs = article_block.find_all('p')
            for paragraph in paragraphs:
                if len(text) > 5000:
                    text = text[:5000]
                    break
                paragraph_text = paragraph.get_text(strip=True) if not paragraph.find('a') else ' '.join([t for t in paragraph.stripped_strings])
                text += ' ' + paragraph_text

            keywords = soup.find('meta', attrs={'name': 'keywords'}).get('content') if soup.find('meta', attrs={'name': 'keywords'}) else ''
            if any(char in text for char in ['Å', 'æ', 'µ']):
                print('Некорректный текст')
                return None
            return [source, link, title, time_published, keywords, text]
        except Exception as e:
            print(f"Ошибка при обработке статьи {link}: {e}")
            return None

    def parse_news(self, links):
        news_data = []
        with ThreadPoolExecutor(max_workers=20) as executor:
            results = executor.map(lambda x: self.fetch_news(*x), links)
            for result in results:
                if result and not self.check_duplicate(result[1]):
                    news_data.append(result)
        df = pd.DataFrame(news_data, columns=['source', 'url', 'title', 'time', 'keywords', 'text'])
        self.save_to_clickhouse(df)
        return df

    def check_duplicate(self, url):
        query = f"SELECT COUNT() FROM news_db.news_articles WHERE url = '{url}'"
        result = self.client.query(query)
        return result['COUNT()'][0] > 0

    def save_to_clickhouse(self, df):
        data = df.to_dict('records')
        self.client.insert('news_db.news_articles', data)

def fetch_all_links(base_url, start, end, step=1):
    links = []
    with ThreadPoolExecutor(max_workers=2) as executor:
        if 'cnews' in base_url:
            futures = [executor.submit(NewsParsing(base_url).link_parsing, f"{base_url}/page_{i}") for i in range(start, end, step)]
        elif 'tadviser' in base_url:
            futures = [executor.submit(NewsParsing(base_url).link_parsing, f"{base_url}{i}.5.2024") for i in range(start, end, step)]
        elif 'habr' in base_url:
            futures = [executor.submit(NewsParsing(base_url).link_parsing, f"{base_url}/page{i}/") for i in range(start, end, step)]
        for future in futures:
            links.extend(future.result())
    return links

if __name__ == "__main__":
    cnews_url = 'https://www.cnews.ru/archive/type_top_lenta_articles'
    links_1 = fetch_all_links(cnews_url, 1, 51)
    cnews_parser_1 = NewsParsing(cnews_url)
    news_df_1 = cnews_parser_1.parse_news(links_1)

    tadviser_url = 'https://www.tadviser.ru/index.php/Архив_новостей?cdate='
    links_2 = fetch_all_links(tadviser_url, 16, 32)
    cnews_parser_2 = NewsParsing(tadviser_url)
    news_df_2 = cnews_parser_2.parse_news(links_2)

    habr_url = 'https://habr.com/ru/news'
    links_3 = fetch_all_links(habr_url, 1, 51)
    cnews_parser_3 = NewsParsing(habr_url)
    news_df_3 = cnews_parser_3.parse_news(links_3)
