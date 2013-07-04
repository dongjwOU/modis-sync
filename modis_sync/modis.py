import time
import os
import sys
import datetime
import traceback
from  urllib2 import urlopen
from optparse import OptionParser
import ast

from bs4 import BeautifulSoup

import tiles as t
import aws

SATELLITE = 'MOLT'
COLLECTION = 5
BASEURL = 'http://e4ftl01.cr.usgs.gov'
PRODUCT = 'MOD13A1'
STAGINGBUCKET = 'formastaging'
BUCKET = 'modisfiles'

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

def get_hdf_urls(base_url, product, date):
    """Build urls for all HDF files on a given date page 
       (e.g. http://e4ftl01.cr.usgs.gov/MOLT/MOD13A1.005/2013.04.23/)."""
    url = os.path.join(base_url, product, date)
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

def get_dates_list(base_url, product, min_date):
    url = os.path.join(base_url, product)
    date_soup = get_soup(url)
    return filter_dates(get_dates(date_soup), min_date)

def get_file_list(base_url, product, tiles, dates):
    """Extract urls for all MOD13A1 HDF files on NASA site. Filters 
    out old files based on min_date, and only keeps files corresponding
    to elements of the tiles list.

    Produces something like this:
    ['http://e4ftl01.cr.usgs.gov/MOLT/MOD13A1.005/2013.04.23/MOD13A1.A2013113.h00v09.005.2013130030546.hdf',
    'http://e4ftl01.cr.usgs.gov/MOLT/MOD13A1.005/2013.04.23/MOD13A1.A2013113.h00v10.005.2013130030234.hdf']
"""
    file_list = [f for n in dates for f in get_hdf_urls(base_url, product, n)]
    return filter_tiles(file_list, tiles)
    
def fix_date_in_path(path):
    '''Replace periods with dashes in date in path.'''
    date_idx = 1
    path_components = path.split('/')
    path_components[date_idx] = path_components[date_idx].replace('.', '-')
    return '/'.join(path_components)

def drop_collection(path, collection):
    '''Remove collection number from product string in path.'''
    return path.replace('.00%s' % collection, '', 1)
    
def mirror_file(base_url, collection, uri, local_dir, staging_bucket_conn, archive_bucket_conn=None):
    '''Check whether file at uri has already been uploaded to S3.'''
    remote_path = drop_collection(fix_date_in_path(uri.replace(base_url, '')), collection)
    local_path = os.path.join(local_dir, fix_date_in_path(uri.replace(base_url, '')))
    local_path = drop_collection(local_path, collection)
    if not aws.exists_on_s3(remote_path, bucket_conn=staging_bucket_conn):
        if archive_bucket_conn and not aws.exists_on_s3(remote_path, bucket_conn=archive_bucket_conn):
            local_path = aws.download(uri, local_path)
            aws.upload_to_s3(local_path, remote_path, staging_bucket_conn)
            os.remove(local_path)
            return remote_path
    else:
        return

def prep_email_body(product, file_list, uploaded_list, dates_checked):
    subject = "[modis-sync] %s: %s new files acquired" % (product, len(uploaded_list))

    body = "files checked: %i\n" % len(file_list)
    body += "files acquired: %i\n\n" % len(uploaded_list)
    body += "date(s) checked:\n"
    body += "\n".join(dates_checked)
    if any(uploaded_list):
        body += "\n\nUploaded:\n"
        body += "\n".join([i for i in uploaded_list if i])
    
    return subject, body

def send_status_email(address, body, subject):
    '''Use AWS Simple Email Service to send status email.'''
    return aws.send_email(to_email=address, from_email=address, subject=subject, body=body)

def min_date_default(days=90):
    '''Calculate the default minimum date as 90 days before today'''
    return [datetime.date.today() - datetime.timedelta(days)][0].isoformat()

def exception_email():
    '''Construct exceptions email.'''
    exc_type, exc_value, exc_traceback = sys.exc_info()
    error = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
    body = 'Exception:\n\n%s' % error
    subject = '[modis-sync] %s' % exc_type
    return subject, body

def parse_cl():
    
    parser = OptionParser()
    parser.add_option("-d", "--date-filter", default=min_date_default(), help="Filter out dates before date string (e.g. '2008-01-01')", dest="date_filter")
    parser.add_option("-b", "--bucket", default=BUCKET, help="S3 bucket to use for file checking", dest="bucket")
    parser.add_option("-a", "--staging-bucket", default=STAGINGBUCKET, help="S3 bucket to use for staging new files", dest="staging_bucket")
    parser.add_option("-s", "--server", default=BASEURL, help="MODIS http server", dest="server")
    parser.add_option("-e", "--satellite", default=SATELLITE, help="MODIS satellite dataset (MOLA, MOLT or MOTA)", dest="satellite")
    parser.add_option("-p", "--product-prefix", default=PRODUCT, help="MODIS product prefix (e.g. MOD13A1)", dest="product_prefix")
    parser.add_option("-c", "--collection", default=COLLECTION, help="MODIS collection number (e.g. 5)", dest="collection")
    parser.add_option("-m", "--emails", default=[], help="Address(es) for status emails - space-separted string", dest="emails")
    parser.add_option("-t", "--tiles", default=['all'], help="List of tile tuples to process (e.g. '['all']' or '[(28, 8), (8, 6)]'", dest="tiles")

    options, args = parser.parse_args()

    min_date = options.date_filter
    bucket = options.bucket
    staging_bucket = options.staging_bucket
    base_url = '%s/%s/' % (options.server, options.satellite)
    product = '%s.%03d' % (options.product_prefix, options.collection)
    email_list = options.emails.split(' ') if type(options.emails) == str else options.emails
    tile_list = options.tiles if type(options.tiles) == list else ast.literal_eval(options.tiles)

    return base_url, product, tile_list, min_date, bucket, staging_bucket, email_list

def main(base_url, product, tile_list, min_date, bucket, staging_bucket, email_list):
    '''Get list of all HDF files on MODIS server and filter by tiles and min_date.

    Note that 'tiles' must be ['all'] or a list of tuples.'''
    try:
        collection = int(product.split('.')[1])
        
        tiles = t.tile_set(tile_list)

        # Getting available dates
        dates = get_dates_list(base_url, product, min_date)
        
        # Get file list for those dates
        file_list = get_file_list(base_url, product, tiles, dates)
        
        # create bucket connections        
        sb = aws.get_bucket_conn(staging_bucket)
        b = aws.get_bucket_conn(bucket)

        uploaded = [mirror_file(base_url, collection, uri, '/tmp/', sb, b) for uri in file_list]
        print 'Uploaded %s files to S3.' % len(uploaded)
        subject, body = prep_email_body(product, file_list, [i for i in uploaded if i], dates)

    except Exception:
        subject, body = exception_email()

    for address in email_list:
        print 'Sending status email to %s:' % address
        print 'Subject: %s' % subject
        print 'Body:\n%s' % body
        send_status_email(address, body, subject)
    
    return

if __name__ == "__main__":
    main(*parse_cl())
