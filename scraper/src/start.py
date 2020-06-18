from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from apscheduler.schedulers.twisted import TwistedScheduler
from trulia_scrapper.spiders.trulia import TruliaScraper

process = CrawlerProcess(get_project_settings())
scheduler = TwistedScheduler()
scheduler.add_job(process.crawl, 'interval', args=[TruliaScraper], hours=6)
scheduler.start()
process.start(False) 