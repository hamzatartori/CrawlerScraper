import multiprocessing
from multiprocessing import pool
import urllib.request
import os
import errno
import os.path
import json
import logging
from urllib.parse import urlparse, urljoin
import tldextract
import requests
from bs4 import BeautifulSoup, Comment
from urllib.request import Request
import urllib.request
from datetime import datetime
import time
import string
import sys
import traceback
import socket
from tld import get_tld

sys.setrecursionlimit(10000)

FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=FORMAT,
                    datefmt='%d/%m/%Y %H:%M:%S',
                    level=logging.DEBUG, )

logger = logging.getLogger()
handler = logging.FileHandler('logfile.log')
handler.setFormatter(logging.Formatter(FORMAT))
logger.addHandler(handler)


def get_url_tld(url):
    """
    :param url: the target URL
    :return: the top-level domain of the target URL
    """
    try:
        return get_tld(url)
    except Exception as err:
        traceback.print_tb(err.__traceback__)
        logger.error(traceback.format_exc())
        return None


def get_visual_content(soup):
    """
    :param soup: Beautiful soup object of the webpage HTML
    :return: list of visual sources of the target webpage
    """
    tags = ['img', 'video', 'audio']
    visuals = []
    for tag in tags:
        objects = soup.find_all(tag)
        for obj in objects:
            if obj.attrs is not None:
                if 'src' in obj.attrs:
                    visuals.append({'type': tag, 'link': obj.attrs['src']})
    return visuals


def get_tls_ssl_certificate(url):
    """
    :param url: the target URL
    :return: True if the target URL uses SSL certificate, False if not
    """
    if urlparse(url).scheme == "https":
        return True
    elif urlparse(url).scheme == "http":
        return False
    else:
        return None


def get_geo_loc(url):
    """
    :param url: the target URL
    :return: the IP geographical location of the target URL
    """
    try:
        url_ip = socket.gethostbyname(urlparse(url).netloc)
        url_geolocation = 'https://geolocation-db.com/json/' + url_ip
        req = requests.get(url_geolocation, timeout=15)
        returned_result = req.json()['country_name']
        return returned_result
    except Exception as err:
        traceback.print_tb(err.__traceback__)
        # logger.error(traceback.format_exc())
        return None


def is_valid(url):
    """
    :param url: the target URL
    :return: True if the target URL is valid URL, False if not
    """
    if ' ' in url:
        return False
    else:
        parsed = urlparse(url)
        return bool(parsed.netloc) and bool(parsed.scheme)


class WebCrawling:
    """
    This class will crawl a specify URL and scrape each of the extracted internal URLs
    """
    def __init__(self, url, file_n, label, label_details, max_crawling,
                 collection_source, crawl_time_out):
        """
        :param url: the target URL to be crawled
        :type url: str
        :param file_n: the directory of the saving the extracted JSON files of the target URL
        :type file_n: str
        :param label: the first level labeling of the target URL for classification purposes, if any
        :type label: str
        :param label_details: the second level labeling of the target URL for classification purposes, if any
        :type label_details: str
        :param max_crawling: the number of maximum internal URLs crawled of the target URL
        :type max_crawling: int
        :param collection_source: the source of collecting the URL target, if applicable
        :type collection_source: str
        :param crawl_time_out: the limit of crawling the target URL in seconds
        :type crawl_time_out: int

        """
        self.target_url = url
        self.internal_urls = []
        self.external_unique_domains = []
        self.external_urls = []
        self.max_crawling_links = max_crawling
        self.crawl_time_out = crawl_time_out
        self.file_n = file_n
        self.main_redirected = ''
        self.label = label
        self.label_details = label_details
        self.source = collection_source
        self.added_to_db = 0
        self.ts = time.time()
        self.time_now_org = datetime.utcfromtimestamp(self.ts).strftime('%Y-%m-%d %H:%M:%S')
        self.time_now = self.time_now_org.replace(':', '-')
        logger.info(" ("+self.target_url+") initiating the crawler ")
        self.first_url = True
        self.first_url_check = True
        self.total_time_minutes = 0
        self.extensions_img = ['JPEG', 'GIF', 'PNG ', 'JPG', 'TIFF']

        self.extensions = ['JPEG', 'GIF', 'PNG ', 'EPS', 'AI', 'PDF', 'JPG', 'TIFF', 'PSD', 'INDD', 'RAW', 'DOC',
                           'DOCM', 'DOCX', 'DOT', 'DOTM', 'DOTX', 'RTF', 'TXT', 'WPS', 'XPS', 'CSV', 'DBF', 'DIF',
                           'ODS', 'PRN', 'SLK', 'XLA', 'XLAM', 'XLS', 'XLW', 'XPS', 'MP4', 'MP3', 'ODP', 'POT',
                           'POTM', 'POTX', 'PPA', 'PPS', 'PPTM', 'PPTX', 'RTF', 'WMF', 'XML', 'XPS']
        self.crawled_number = 0
        self.status = ''
        self.geo_loc = []
        self.tld = []
        self.time_response = []
        self.tls_ssl_certificate = []

    def start(self):
        """
        Aims to start the crawling and scraping task for the target URL
        :return: the metadata resulting of crawling the target URL, which includes:
        'domain': the domain of the target URL,
        'target_url': the target URL itself,
        'geo_loc': the geographical location of all scrapped internal URLs of the target URL,
        'domain_length': the domain character length of the target URL,
        'tld': the top-level domain of the target URL,
        'avg_time_response': the average time response of all scrapped internal URLs of the target URL,
        'start_scrawling_timestamp': the exact time of starting the task of crawling the target URL in %Y-%m-%d %H:%M:%S format,
        'end_scrawling_timestamp': the exact time of ending the task of crawling the target URL in %Y-%m-%d %H:%M:%S format,
        'domain_tls_ssl_certificate': whether the target URL uses SSL certificate or not,
        'internal_urls_no': number of crawled internal URLs of the target URL,
        'internal_urls': listing all crawled internal URLs of the target URL,
        'source': the source of the target URL,
        'label': the first level labeling of the target URL,
        'sub-label': the second level labeling of the target URL,
        """

        logger.info(" ("+self.target_url+") starting the crawler ")
        time_start = time.time()
        self.crawl(self.target_url)
        self.status = 'Successful'
        self.total_time_minutes = (time.time() - time_start) / 60
        logger.info("Total time for crawling " + self.target_url + " was " + str(self.total_time_minutes) + " minutes.")
        meta_data = None
        tls_ssl_certificate_not_duplicate = list(dict.fromkeys(self.tls_ssl_certificate))
        not_none_tls_ssl_certificate = [x for x in tls_ssl_certificate_not_duplicate if x is not None]
        geo_loc_not_duplicate = list(dict.fromkeys(self.geo_loc))
        not_none_geo_loc = [x for x in geo_loc_not_duplicate if x is not None]
        tld_not_duplicate = list(dict.fromkeys(self.tld))
        extracted = tldextract.extract(self.target_url)
        domain = "{}.{}".format(extracted.domain, extracted.suffix)
        not_none_response_time = [x for x in self.time_response if x is not None]
        try:
            time_response_avg = sum(not_none_response_time) / len(not_none_response_time)
        except Exception as err:
            traceback.print_tb(err.__traceback__)
            time_response_avg = None
        try:
            meta_data = {
                'domain': domain,
                'target_url': self.target_url,
                'crawling_status': self.status,
                'geo_loc': not_none_geo_loc,
                'domain_length': len(domain),
                'tld': tld_not_duplicate[0],
                'avg_time_response': time_response_avg,
                'start_scrawling_timestamp': self.time_now_org,
                'end_scrawling_timestamp': datetime.utcfromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'),
                'domain_tls_ssl_certificate': True if True in not_none_tls_ssl_certificate else False,
                'internal_urls_no': len(self.internal_urls),
                'internal_urls': self.internal_urls,
                'source': self.source,
                'label': self.label,
                'sub-label': self.label_details,
            }
        except Exception as err:
            traceback.print_tb(err.__traceback__)
            logger.error(traceback.format_exc())
            pass
        return meta_data

    def scrape_url(self, url):
        """
        this function scrape the given URL, extract its features and get all found
        internal URLS belongs to the same website
        :param url: the internal URL to be scrapped
        :return: the non-duplicate found internal urls
        """
        if self.first_url:
            resp_redirect = self.check_response_redirecting(url)
            is_redirected = resp_redirect['redirected']
            redirected_url = resp_redirect['redirected_url']
            if is_redirected:
                logger.info(" ("+str(self.target_url)+") ******* The main url is redirected from "+str(url)+" --> "
                            + str(redirected_url))
                self.target_url = redirected_url
            self.first_url = False
            return [redirected_url]

        urls = []
        resp_redirect = self.check_response_redirecting(url)
        is_redirected = resp_redirect['redirected']
        redirected_url = resp_redirect['redirected_url']
        if is_redirected:
            logger.info(" ("+str(self.target_url)+") ******* The url is redirected from " + str(url) + " --> "
                        + str(redirected_url))
            self.internal_urls = [redirected_url if x == url else x for x in self.internal_urls]
            url = redirected_url
        if isinstance(url, (bool, int)) or url is None:
            return []
        url_main = url
        domain_name = urlparse(self.target_url).netloc
        domain_name_lower = domain_name.lower()
        extracted = tldextract.extract(domain_name)
        extracted_lower = tldextract.extract(domain_name_lower)

        logger.info(" ("+self.target_url+") ------- now crawling  "+str(url))
        if self.href_doc_img_existence(url):
            logger.warning(" ("+self.target_url+") This is a Document/Image url " + str(url))
            try:
                self.internal_urls.remove(url)
            except Exception as err:
                traceback.print_tb(err.__traceback__)
                pass
            return []

        if self.href_external_existence(domain_name, domain_name_lower, extracted, extracted_lower, url):
            logger.warning(" (" + self.target_url + ") This is an external url " + str(url))
            try:
                self.internal_urls.remove(url)
            except Exception as err:
                traceback.print_tb(err.__traceback__)
                pass
            return []

        if self.href_doc_img_existence(url):
            return []

        time_req = time.time()
        html = self.get_html(url)
        time_response = time.time() - time_req
        if html == -1 or len(html) < 1000:
            logger.error(" ("+self.target_url+") html is not valid/empty for " + str(url))
            req = Request(url, headers={'User-Agent': '''Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) 
                                                         AppleWebKit/537.36 (KHTML, like Gecko) 
                                                         Chrome/39.0.2171.95 Safari/537.36'''})
            try:
                time_req = time.time()
                html_urllib = urllib.request.urlopen(req, timeout=30).read()
                if len(html_urllib) < 1000:
                    logger.error(" ("+self.target_url+") html1 is N/A for " + str(url))
                    try:
                        self.internal_urls.remove(url)
                    except Exception as err:
                        traceback.print_tb(err.__traceback__)
                        logger.warning(" (" + self.target_url + ") Unable to remove URL from list ")
                        pass
                    return []
                else:
                    html = html_urllib
                    time_response = time.time() - time_req
            except urllib.error.HTTPError:
                logger.error(" ("+self.target_url+") HTTP request error for " + str(url))
                try:
                    self.internal_urls.remove(url)
                except Exception as err:
                    traceback.print_tb(err.__traceback__)
                    logger.error(" (" + self.target_url + ") Unable to remove URL from list")
                    pass
                return []
            except urllib.error.URLError:
                logger.error(" ("+self.target_url+") URL request error for " + str(url))
                try:
                    self.internal_urls.remove(url)
                except Exception as err:
                    traceback.print_tb(err.__traceback__)
                    logger.error(" (" + self.target_url + ") Unable to remove URL from list")
                    pass
                return []
            except Exception as err:
                traceback.print_tb(err.__traceback__)
                logger.error(" ("+self.target_url+") UNKNOWN error for " + str(url))
                try:
                    self.internal_urls.remove(url)
                except Exception as err:
                    traceback.print_tb(err.__traceback__)
                    logger.error(" - (" + self.target_url + ") Unable to remove URL from list")
                    pass
                return []
        try:
            try:
                soup = BeautifulSoup(html, features="html.parser")
            except Exception as err:
                traceback.print_tb(err.__traceback__)
                logger.info(" (" + self.target_url + ") html is not valid for " + str(url))
                logger.error(traceback.format_exc())
                try:
                    self.internal_urls.remove(url)
                except Exception as err:
                    traceback.print_tb(err.__traceback__)
                    logger.error(" (" + self.target_url + ") Unable to remove URL from list")
                    pass
                return []

            if soup.find('title') is not None:
                if "404" in soup.find('title') or "Not Found" in soup.find('title'):
                    logger.error(" ("+self.target_url+") page of "+url_main+" returns 404 error.")
                    self.internal_urls.remove(url)
                    return []

            p_texts = soup.findAll(text=True)
            filtered_prev_texts = list(filter(tag_visible, p_texts))
            cleaned_prev_text = [x for x in filtered_prev_texts if x]
            prev_text = [x for x in cleaned_prev_text if x != ' ']
            prev_text = [x for x in prev_text if x != '\n']
            prev_text_length = len(prev_text)
            extracted = tldextract.extract(url_main)
            domain = "{}.{}".format(extracted.domain, extracted.suffix)
            visuals = get_visual_content(soup)
            tld = get_url_tld(url_main)
            tls_ssl_certificate = get_tls_ssl_certificate(url_main)
            geo_loc = get_geo_loc(url_main)
            webpage_dict = {
                '_id': url_main,
                'url': url_main,
                'domain_name': domain,
                'created_time': time.strftime('%Y-%m-%d %H:%M:%S'),
                'html_char_length': len(html),
                'text_char_length': sum([len(t) for t in prev_text]),
                'textual_tags_cnt': prev_text_length,
                'label': self.label,
                'label_details': self.label_details,
                'source': self.source,
                'geo_loc': geo_loc,
                'url_length': len(url_main),
                'domain_length': len(domain),
                'tld': tld,
                'protocol': urlparse(url_main).scheme,
                'time_response': time_response,
                'tls_ssl_certificate': tls_ssl_certificate,
                'visual_content_no': len(visuals),
                'visual_content_src': [link['link'] for link in visuals],
                'text': prev_text,
                'html': str(soup),

            }

            try:
                self.print_export(webpage_dict)
                self.tld.append(tld)
                self.time_response.append(time_response)
                self.tls_ssl_certificate.append(tls_ssl_certificate)
                self.geo_loc.append(geo_loc)
                logger.info(" ("+self.target_url+") saving succeeded")
                self.added_to_db = self.added_to_db + 1
            except Exception as err:
                del webpage_dict
                del p_texts
                del filtered_prev_texts
                del cleaned_prev_text
                del prev_text
                del prev_text_length
                del html
                del soup
                del is_redirected
                del redirected_url
                traceback.print_tb(err.__traceback__)
                logger.error(" ("+self.target_url+") saving error")
                logger.error(traceback.format_exc())
                return False, {
                    'url': url_main,
                    'status': 'unsuccessful',
                    'details': "Saving Error - traceback: " + traceback.format_exc()
                }
            urls = self.add_refs(soup, url)
            del soup
            del webpage_dict
            del p_texts
            del filtered_prev_texts
            del cleaned_prev_text
            del prev_text
            del prev_text_length
            del html
            del is_redirected
            del redirected_url
        except requests.exceptions.ConnectionError:
            logger.error(" ("+self.target_url+") ERROR - Connection refused")

        return urls

    def href_doc_img_existence(self, href):
        """
        this function will check whether the provided url is image, document, executable, or webpage URL
        :param href: the internal found url
        :return: True if not webpage URL, False if it is
        """
        for ext in self.extensions_img:
            if href.endswith('.'+ext.lower()) or href.endswith('.'+ext):
                return True

        for ext in self.extensions:
            if href.endswith('.'+ext.lower()) or href.endswith('.'+ext):
                return True

        if href.endswith('.msi') or href.endswith('.exe') or href.endswith('.jar') or href.endswith(
                '.zip') or href.endswith('.deb') or href.endswith('.tar.gz'):
            return True

        if href.endswith('.pdf') or href.endswith('.ppsx'):
            return True
        return False

    def print_export(self, dict_save):
        """
        this function will save each webpage extracted information and features into a JSON file to the chosen directory
        :param dict_save: the dictionary of the information if a webpage
        :return: None
        """
        logger.info("[+] (" + self.target_url + ") --- Saved pages are : " + str(self.added_to_db))
        try:
            valid_file_name = get_valid_url_name(dict_save['url'])
            if check_file(self.file_n) == -1:
                return
            with open(self.file_n + valid_file_name + ".json", 'w') as f:
                json.dump(dict_save, f)
        except Exception as err:
            traceback.print_tb(err.__traceback__)
            logger.error(" (" + self.target_url + ") saving error")
            logger.error(traceback.format_exc())

    def add_refs(self, soup, url):
        """
        this function find all internal URLs existed in an HTML webpage source
        :param soup: Beautifulsoup object
        :param url: the URL of the HTML source webpage
        :return: list of all found internal URLs
        """
        urls = []
        domain_name = urlparse(self.target_url).netloc
        domain_name_lower = domain_name.lower()
        extracted = tldextract.extract(domain_name)
        extracted_lower = tldextract.extract(domain_name_lower)
        for a_tag in soup.findAll("a"):
            if self.max_crawling_links > len(self.internal_urls):
                href = a_tag.attrs.get("href")
                if href == "" or href is None:
                    continue
                href = urljoin(url, href)
                parsed_href = urlparse(href)
                href = parsed_href.scheme + '://' + parsed_href.netloc + parsed_href.path
                if not is_valid(href):
                    continue
                if self.href_external_existence(domain_name, domain_name_lower, extracted, extracted_lower, href):
                    continue
                if self.href_internal_existence(href):
                    continue
                if self.href_internal_existence(href):
                    continue
                if self.href_external_existence(domain_name, domain_name_lower, extracted, extracted_lower, href):
                    continue
                urls.append(href)
                self.internal_urls.append(href)
            else:
                logger.warning(" ("+self.target_url+") : Reached max_crawling_links.")
                break
        return urls

    def href_internal_existence(self, href):
        """
        this function checks if the given URL already has been found for the target URL
        :param href: the URL to be checked
        :return: True if the given URL has been found already, if not False
        """
        if href in self.internal_urls:
            return True
        return False

    def href_external_existence(self, domain_name, domain_name_lower, extracted, extracted_lower, href):
        """
        this function checks if the given URL does not belong to the same website of the target URL
        :param domain_name: the domain name of the target URL
        :param domain_name_lower: small letter domain name of the target URL
        :param extracted: tldextract object of the target URL
        :param extracted_lower: small letter tldextract object of the target URL
        :param href: the given URL to be checked
        :return: True if the given URL does not belong to the website of the target URL, if not False
        """
        if domain_name not in href and domain_name_lower not in href and extracted.domain not in href \
                and extracted_lower.domain not in href:
            if href not in self.external_urls:
                self.external_urls.append(href)
            if urlparse(href).path == '/':
                unique_domain = href
            else:
                unique_domain = href.replace(urlparse(href).path, '')
            if unique_domain not in self.external_unique_domains:
                self.external_unique_domains.append(unique_domain)
            return True
        else:
            return False

    def crawl(self, url):
        """
        this is a recursive to crawl all internal urls of the target URL
        :param url: the url to be crawled
        :return: None
        """
        crawled_cnt = 0
        if time.time() - self.ts > self.crawl_time_out:
            logger.warning(" (" + self.target_url + ") : Time out (processing time is exceeded the time out of " +
                           str(self.crawl_time_out)+" seconds)")
            return
        else:
            self.crawled_number = self.crawled_number + 1
            links = self.scrape_url(url)
            with pool.ThreadPool(multiprocessing.cpu_count()) as p_crawl:
                results_crawler = p_crawl.map(self.crawl, links)
                for i in range(len(results_crawler)):
                    crawled_cnt = crawled_cnt + 1

    def check_response_redirecting(self, url):
        """
        this function check redirecting url
        :param url: the given url to be checked
        :return: dictionary contain:
        redirected: False if the given URL is redirect to another URL, or True if not
        redirected_url: the url in case of redirection
        """
        try:
            req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            urllib_response = urllib.request.urlopen(req, timeout=15)
            html_urllib = urllib.request.urlopen(req, timeout=15).read()
            code = urllib_response.getcode()
            if not code == 200:
                logger.error(" ("+self.target_url+") Error requests for "+url+" (status_code: " + code + ").")
                return {'redirected': False, 'redirected_url': None}  # -1, -1, -1
            soup = BeautifulSoup(html_urllib, features="html.parser")
            if soup.find('title') is not None:
                if "404" in soup.find('title') or "Not Found" in soup.find('title'):
                    logger.error(" ("+self.target_url+") Error requests 404 for "+url)
                    return {'redirected': False, 'redirected_url': None}  # -1, -1, -1
            if not urllib_response.url == url:
                if urllib_response.url.replace(' ', '') == url + "/" or urllib_response.url.replace(' ',
                                                                                                    '') + "/" == url:
                    return {'redirected': False, 'redirected_url': urllib_response.url}  # False, urllib_response.url, html_urllib
                else:
                    logger.warning(" (" + self.target_url + ") : Redirected link (" + url + ") to " +
                                   urllib_response.url)
                    return {'redirected': True, 'redirected_url': urllib_response.url}  # True, urllib_response.url, -1
            return {'redirected': False, 'redirected_url': urllib_response.url}  # False, urllib_response.url, html_urllib
        except Exception as err:
            traceback.print_tb(err.__traceback__)
            logger.error(" (" + self.target_url + ") checking URL redirection error")
            logger.error(" (" + self.target_url + traceback.format_exc())
            return {'redirected': None, 'redirected_url': url}  # -1, url, -1

    def check_link_response(self, link):
        """
        this function checks the response of the given URL
        :param link: the given URL to be checked
        :return: True if the given URL response 200, or True if not
        """
        try:
            req_link_resp = requests.get(link)
            if not req_link_resp.status_code == 200:
                logger.error(" ("+self.target_url+") Error requests 404 for "+link+" (status_code: "
                             + str(req_link_resp.status_code)+").")
                return False
        except Exception as err:
            traceback.print_tb(err.__traceback__)
            logger.error(" (" + self.target_url + ") checking URL response error")
            logger.error(" (" + self.target_url + traceback.format_exc())
            return False
        try:
            req = Request(link, headers={'User-Agent': 'Mozilla/5.0'})
            html_urllib = urllib.request.urlopen(req, timeout=5).read()
            soup = BeautifulSoup(html_urllib, features="html.parser")
            if soup.find('title') is not None:
                if "404" in soup.find('title') or "Not Found" in soup.find('title'):
                    logger.error(" ("+self.target_url+") Error requests 404 for "+link)
                    return False
        except Exception as err:
            traceback.print_tb(err.__traceback__)
            logger.error(" (" + self.target_url + ") General error requests for "+link)
            logger.error(" (" + self.target_url + traceback.format_exc())
            return False
        return True

    def get_html(self, url_target):
        """
        this function gets the html source of a webpage
        :param url_target: given URL
        :return: the html source of the given URL
        """
        url = urllib.parse.quote(url_target, safe=string.printable)
        req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        try:
            html = urllib.request.urlopen(req, timeout=60).read()
        except Exception as err:
            traceback.print_tb(err.__traceback__)
            logger.error(" (" + self.target_url + ") error getting HTML for "+url_target)
            logger.error(" (" + self.target_url + traceback.format_exc())
            return -1
        return html


def tag_visible(element):
    """
    :param element: Beautifulsoup element (html tag)
    :return: whether the given element contains a visible text in the webpage
    """
    if element.parent.name in ['span']:
        return True
    if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
        return False
    if isinstance(element, Comment):
        return False
    return True


def get_valid_url_name(url_target):
    """
    :param url_target: given URL
    :return: valid name for filing system
    """
    url = url_target.replace("https://", "")
    url = url.replace("http://", "")
    url = url.replace(":", "")
    url = url.replace("*", "")
    url = url.replace("?", "")
    url = url.replace("<", "")
    url = url.replace(">", "")
    url = url.replace("|", "")
    url = url.replace("\n", "")
    url = url.replace("\r", "")
    url = url.replace("\t", "")
    url = url.replace("/", "-")
    if len(url) > 50:
        return url[:50]
    return url


def check_file(file):
    """
    :param file: the directory for a file
    :return: whether is does exist of not
    """
    try:
        if not os.path.exists(os.path.dirname(file)):
            logger.warning("Path not found " + file)
            try:
                os.makedirs(os.path.dirname(file))
                logger.info("Path created")
            except OSError as exc:  # Guard against race condition
                logger.error("Path creating error")
                if exc.errno != errno.EEXIST:
                    raise
            return False
        return True
    except Exception as err:
        traceback.print_tb(err.__traceback__)
        logger.error("Error file check for " + file)
        logger.error(traceback.format_exc())
        return -1


class InitiateProject:
    """
    this is a helper class acts as an interface for WebCrawling class
    """
    def __init__(self, domains, saving_directory='Crawled Dataset/', max_crawling_number=250,
                 collection_source=None, label=None, sub_label=None, crawl_time_out=7200):
        """
        :param domains: a list of the target URLs to be crawled
        :type domains: list
        :param saving_directory: the main directory of the saving the extracted JSON files of the target URL
        :type saving_directory: str
        :param max_crawling_number: the number of maximum internal URLs crawled of the target URL
        :type max_crawling_number: int
        :param collection_source: the source of collecting the URL target, if applicable
        :type collection_source: str
        :param label: the first level labeling of the target URL for classification purposes, if any
        :type label: str
        :param sub_label: the second level labeling of the target URL for classification purposes, if any
        :type sub_label: str
        :param crawl_time_out: the limit of crawling the target URL in seconds
        :type crawl_time_out: int

        """
        self.domains = domains
        self.main_file_n = saving_directory
        self.label = label
        self.sub_label = sub_label
        self.max_crawling_number = max_crawling_number
        self.collection_source = collection_source
        self.crawl_time_out = crawl_time_out
        self.full_ds = self.prepare_dataset()
        with pool.ThreadPool(multiprocessing.cpu_count() * 2) as p:
            results = p.map(self.start_crawling, self.full_ds)
            for r in results:
                logger.info("Returned " + str(r))

    def start_crawling(self, ds):
        """
        this function starts the WebCrawling object of the given task
        :param ds: a dictionary contains the initial parameters of this class
        :return: status of running WebCrawling object
        """
        if check_file(ds['file_n']):
            logger.info("The website " + ds['dataset'] + " already crawled (" + ds['file_n'] + ")")
            return
        elif check_file(ds['file_n']) == -1:
            return
        web_crawler = WebCrawling(
            url=ds['dataset'],
            file_n=ds['file_n'],
            label=ds['label'],
            label_details=ds['label_details'],
            max_crawling=ds['max_crawling_number'],
            collection_source=ds['collection_source'],
            crawl_time_out=ds['crawl_time_out']
        )
        meta_data = web_crawler.start()
        with open(ds['file_n'] + "Metadata.json", 'w') as f:
            json.dump(meta_data, f)
        ts = time.time()
        time_now = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        time_now = time_now.replace(':', '-')
        if meta_data is not None:
            return 'Done -' + str(meta_data['crawling_status']) + ' ... ' + str(ds['dataset']) + ' (' + time_now + ')'
        return 'Done - Unsuccessfully ... ' + str(ds['dataset']) + ' (' + time_now + ')'

    def prepare_dataset(self):
        """
        this function prepare the dataset of crawling URLs as workers for multiprocessing
        :return: the preprocessed dataset as a list of dictionaries for each item in the crawling URls
        """
        dataset = []
        for domain in self.domains:
            extracted = tldextract.extract(domain)
            d = "{}.{}".format(extracted.domain, extracted.suffix)
            if 'http://' not in domain:
                website = "http://" + d
            elif 'https://' not in domain:
                website = "https://" + d
            else:
                website = "https://" + d
            file_n = self.main_file_n + get_valid_url_name(website) + '/'
            dataset.append(
                {
                    'dataset': website,
                    'file_n': file_n,
                    'label': self.label,
                    'label_details': self.sub_label,
                    'max_crawling_number': self.max_crawling_number,
                    'collection_source': self.collection_source,
                    'crawl_time_out': self.crawl_time_out
                }
            )
        return dataset


