from CrawlScrape import InitiateProject


if __name__ == '__main__':
    # list of all website need to be crawled
    dataset = ['www.um.edu.my', 'https://upm.edu.my']

    # maximum number of crawled webpages in a single website
    max_crawling_number = 10

    # optional, the source of the collected website
    collection_source = 'google'

    # optional - the label of the website
    website_label = 'edu'

    # optional - the second level labeling of the website
    website_sub_label = 'malaysia'

    # by default 2 hour for each website
    crawl_time_out = 300

    # by default is 'crawled results/'
    saving_directory = 'Crawled Dataset/'

    cs = InitiateProject(domains=dataset, saving_directory=saving_directory, max_crawling_number=max_crawling_number,
                         collection_source=collection_source, label=website_label, sub_label=website_sub_label,
                         crawl_time_out=crawl_time_out)