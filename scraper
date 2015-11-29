#! /usr/bin/python3
import json
from multiprocessing.dummy import Pool
import os
import pprint
import urllib.parse

from bs4 import BeautifulSoup as BS
import requests

"""
Note - pages sometimes link back to themselves or to other pages which do.
So it is important to avoid reprocessing a page that has already been
processed.

Also some links to subpages are hidden in invisible menus. We find those
and use them anyway.

Produces and consumes lots of sets because of plentiful duplications.
"""

MOE_SPREADSHEETS = "/home/grantps/Documents/training/storage/moe_spreadsheets"  ## change to yours
use_cached_spreadsheet_src_list = True  ## should be False the first time

def get_html(url):
    r = requests.get(url)
    if r.status_code != 200:
        raise Exception("Unsuccessful request - got back {}"
                        .format(r.status_code))
    html = r.text
    return html

def _get_linked_urls(page_url, html, link_markers):
    """
    Want to be able to match on more than one marker if required so
    link_markers should be a list.

    Won't contain duplicates.
    """
    if not html:
        raise Exception("No html supplied!")
    soup = BS(html, 'html.parser')
    links = soup.find_all("a")
    linked_urls = set()
    for link in links:
        href = link.get('href')
        if href:
            for link_marker in link_markers:
                if link_marker in href.lower():
                    #print("Found matching link '{}'".format(href))
                    full_href = urllib.parse.urljoin(page_url, href)
                    linked_urls.add(full_href)
    return linked_urls

def _get_subpage_urls(html):
    """
    Returns a set.
    """
    if not html:
        raise Exception("No html supplied!")
    soup = BS(html, 'html.parser')
    links = soup.find_all("a")
    subpage_urls = set()
    for link in links:
        label = link.string
        if label and label.startswith("Statistics: "):
            href = link.get('href')
            if href:
                subpage_urls.add(href)
    return subpage_urls

def get_page_xls_urls(page_url, page_urls_processed):
    """
    xls(x) spreadsheets can be directly on the page or inside a subpage
    linked to via a link with a title starting with "Statistics: ".
    Need to grab both. Actually will handle any level of nesting.

    page_urls_processed -- so we avoid infinite recursion by linking
    back to pages we have already processed. Do not visit these as subpages -
    they've already been processed thanks ;-)
    """
    print("Processing '{}'".format(page_url))
    page_urls_processed.add(page_url)
    xls_urls = set()
    ## Get xls's on actual page
    html = get_html(page_url)
    xls_urls_on_page = _get_linked_urls(
        page_url, html, link_markers=[".xls", ])  ## no point including .xlsx because .xls catches that as well
    xls_urls.update(xls_urls_on_page)
    ## Get xls's on subpage
    subpage_urls = _get_subpage_urls(html)
    subpage_urls2use = subpage_urls - page_urls_processed
    for subpage_url in subpage_urls2use:
        xls_urls_on_subpage = get_page_xls_urls(subpage_url,
                                                page_urls_processed)
        xls_urls.update(xls_urls_on_subpage)
    return xls_urls

def get_moe_xls_urls():
    moe_url = "https://www.educationcounts.govt.nz/data-services/data-collections"
    html = get_html(moe_url)
    top_level_page_urls = _get_linked_urls(
        moe_url, html, link_markers=["data-collections/national/", ])
    xls_urls = set()
    page_urls_processed = set()
    for top_level_page_url in top_level_page_urls:
        page_xls_urls = get_page_xls_urls(top_level_page_url,
                                          page_urls_processed)
        xls_urls.update(page_xls_urls)
    xls_urls = list(xls_urls)
    xls_urls.sort()
    return xls_urls

def grab_spreadsheet(moe_xls_url):
    r = requests.get(moe_xls_url, stream=True)
    if r.status_code != 200:
        msg = "Unsuccessful attempt to get '{}'".format(moe_xls_url)
    else:
        filename = moe_xls_url.split("/")[-1]
        with open(os.path.join(MOE_SPREADSHEETS, filename), 'wb') as f:
            for block in r.iter_content(1024):
                f.write(block)
        msg = "Downloaded '{}'".format(filename)
    return msg

MOE_XLS_URLS = "moe_xls_urls.json"
if not use_cached_spreadsheet_src_list:
    ## store the url data
    moe_xls_urls = get_moe_xls_urls()
    with open(MOE_XLS_URLS, "w") as f:
        json.dump(moe_xls_urls, f)
## read the data
with open(MOE_XLS_URLS) as f:
    moe_xls_urls = json.load(f)
if not os.path.exists(MOE_SPREADSHEETS):
    os.makedirs(MOE_SPREADSHEETS)
pool = Pool(processes=64)
results = pool.map(grab_spreadsheet, moe_xls_urls)
pool.close()
pool.join()
results.sort()
pprint.pprint(results)
print(len(results))
