from typing import Any

from crawlee.playwright_crawler import PlaywrightCrawler, PlaywrightCrawlingContext
import xml.etree.ElementTree as ET


class CrawleeClient:
    def __init__(self) -> None:
        self.crawler = PlaywrightCrawler(
            max_requests_per_crawl=20,
            headless=False,
            browser_type='chromium',
        )
        @self.crawler.router.default_handler
        async def request_handler(context: PlaywrightCrawlingContext) -> None:
            context.log.info(f'Processing {context.request.url} ...')
            
            inner_text = await context.page.inner_text(selector="body")

            if "sitemap.xml" in context.request.url:
                links = self._extract_links_from_sitemap(inner_text)
                await context.add_requests(requests=list(set(links)))
            else:
                await context.enqueue_links()

            data = {
                'url': context.request.url,
                'title': await context.page.title(),
                'inner_text': inner_text,
            }

            await context.push_data(data)

    def _extract_links_from_sitemap(self, sitemap_content: str):
        """
        extract link from sitemaps
        """
        root = ET.fromstring(sitemap_content)
        namespace = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        links = [element.text.strip() for element in root.findall("ns:url/ns:loc", namespace)]
        
        return links
    
    async def crawl(self, links: list[str]) -> list[dict[str, Any]]:
        """
        crawl a website and all inner links under the domain routes

        Parameters
        ------------
        links : list[str]
            the website link or links to crawl

        Returns
        --------
        crawled_data : list[dict[str, Any]]
            the data we've crawled from a website
        """
        await self.crawler.add_requests(requests=links)
        await self.crawler.run()
        crawled_data = await self.crawler.get_data()
        return crawled_data
