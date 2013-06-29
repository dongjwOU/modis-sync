import time
import os
import datetime
from  urllib2 import urlopen

from bs4 import BeautifulSoup

import tiles as t
import transfer as trans

BASEURL = 'http://e4ftl01.cr.usgs.gov/MOLT/'
FIRSTDATE = '2000.02.18'
PRODUCT = 'MOD13A1.005'
ACCESSKEY = 'AKIAJ2R6HW5XJPY2GM3Q'
SECRETKEY = 'CaEB6a7T/7yqEysrvK/V86LDOT6BLRkgXrdkjsev'
COLLECTION = 5

def raw_to_iso_date(date_str, fmt="%Y.%m.%d"):
    """Convert a raw date from site (e.g. '2001.01.01') to iso format (e.g.
    '2001-01-01'."""
    date_struct = time.strptime(date_str, fmt)
    return datetime.date.fromtimestamp(time.mktime(date_struct)).isoformat()

def link_to_date(link):
    """Extract date from link element. Returns None if not a valid date. This is useful 
       for ignoring non-date links."""
    try:
        date_str = link.attrs['href'][:-1] # drop trailing '/'
        return raw_to_iso_date(date_str)
    except ValueError:
        return None

def get_links(soup):
    """Get all links on the page."""
    return soup.findAll('a')

def get_dates(soup):
    """Get all dates on the page, and format as NASA likes - '2000.01.01'"""
    links = get_links(soup)
    return [date.replace('-', '.') for date in map(link_to_date, links) if date]

def link_to_hdf(link):
    """Given a link, return the HDF filename if appropriate. Alternatives we ignore are 
       BROWSE links and .jpg or .xml filenames."""
    fname = link.attrs['href']
    if fname.endswith('hdf'):
        return fname
    else:
        return None

def parse_for_hdf_files(soup):
    """Extract all hdf filenames from page."""
    links = get_links(soup)
    return [link for link in map(link_to_hdf, links) if link]

def get_soup(url):
    """Make BeautifulSoup object from a url."""
    html = urlopen(url).read()
    bs = BeautifulSoup(html, 'lxml')
    return bs

def get_hdf_urls(date, product=PRODUCT):
    """Build urls for all HDF files on a given date page 
       (e.g. http://e4ftl01.cr.usgs.gov/MOLT/MOD13A1.005/2013.04.23/)."""
    url = os.path.join(BASEURL, product, date)
    print "Getting HDF file list for %s" % url
    file_soup = get_soup(url)
    files = parse_for_hdf_files(file_soup)
    return [url + '/' + hdf for hdf in files]

def should_get_date(date_str, min_date_str):
    '''Returns True if date is valid and >= min_date.'''
    date = time.strptime(date_str, "%Y.%m.%d")
    min_date = time.strptime(min_date_str, "%Y-%m-%d")
    return date >= min_date

def filter_dates(dates, min_date):
    '''Filter out dates that are not >= min_date.'''
    return [d for d in dates if should_get_date(d, min_date)]

def filter_tiles(file_list, tiles):
    '''Keep only files that correspond to elements in tiles list.'''
    return [f for f in file_list if t.matches_tiles(f, tiles)]

def get_file_list(product, tiles, min_date):
    """Extract urls for all MOD13A1 HDF files on NASA site. Filters 
    out old files based on min_date, and only keeps files corresponding
    to elements of the tiles list.

    Produces something like this:
    ['http://e4ftl01.cr.usgs.gov/MOLT/MOD13A1.005/2013.04.23/MOD13A1.A2013113.h00v09.005.2013130030546.hdf',
    'http://e4ftl01.cr.usgs.gov/MOLT/MOD13A1.005/2013.04.23/MOD13A1.A2013113.h00v10.005.2013130030234.hdf']
"""
    date_soup = get_soup(os.path.join(BASEURL, product))
    dates = filter_dates(get_dates(date_soup), min_date)
    file_list = [f for n in dates for f in get_hdf_urls(n)]
    return filter_tiles(file_list, tiles)

def fix_date_in_path(path):
    '''Replace periods with dashes in date in path.'''
    date_idx = 1
    path_components = path.split('/')
    path_components[date_idx] = path_components[date_idx].replace('.', '-')
    return '/'.join(path_components)

def drop_collection(path):
    '''Remove collection number from product string in path.'''
    return path.replace('.00%s' % COLLECTION, '', 1)
    
def mirror_file(uri, local_dir, bucket_conn):
    '''Check whether file at uri has already been uploaded to S3.'''
    remote_path = uri.replace(BASEURL, '')
    local_path = os.path.join(local_dir, uri.replace(BASEURL, ''))
    local_path = drop_collection(fix_date_in_path(local_path))
    if not trans.exists_on_s3(fix_date_in_path(remote_path), bucket_conn=bucket_conn):
        local_path = trans.download(uri, local_path)
        remote_path = fix_date_in_path(remote_path)
        trans.upload_to_s3(local_path, remote_path, bucket_conn)
        return remote_path

def main(product="MOD13A1.005", tiles=['all'], 
         min_date=[datetime.date.today() - datetime.timedelta(90)][0].isoformat()):
    '''Get list of all HDF files on MODIS server and filter by tiles and min_date.'''
    tiles = t.tile_set(tiles)
    file_list = get_file_list(product, tiles, min_date)
    b = trans.get_bucket_conn('modisfiles')
    uploaded = [mirror_file(fname, '/tmp/', b) for fname in file_list]
    print 'Uploaded %s files to S3.' % len(uploaded)
