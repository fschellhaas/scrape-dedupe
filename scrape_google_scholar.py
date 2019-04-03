from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
from bs4 import BeautifulSoup
from shutil import rmtree
import random
import time, datetime
import os
import json

# Chromedriver binary file downloaded from here:
# https://sites.google.com/a/chromium.org/chromedriver/downloads


##
# Config
##

# define the url to scrape
# source_url = 'https://scholar.google.com/scholar?cites=8944513168499971421' # Gaertner et al. (1993)
source_url = 'https://scholar.google.com/scholar?cites=5942917844675151262' # Gaertner & Dovidio (2000)

# id the reference to be included in the name of the results directory
dir_reference = 'gaertner2000'

# the min/max years of results
min_year = 2010
max_year = 2018

# if fan_earlier get all records before the min_year value
# if fan_later get all records after the max_year value
fan_earlier = False
fan_later = True


##
# Functions
##

def get_page_source(url):
  '''
  Get the HTML source from a given url
  @args:
    {str} url: the url whose content we'll fetch
  @returns:
    {str} the html source from the url
  '''
  time.sleep(random.randint(1,5))
  driver.get(url)
  html = driver.page_source
  while True:
    try:
      is_captcha = driver.find_element_by_css_selector('#gs_captcha_ccl,#recaptcha')
    except NoSuchElementException:
      return driver.page_source
    time.sleep(5)
  return html


def is_int(char):
  '''
  Return a bool indicating whether `char` is an int
  @args:
    {str} char: a string character
  @returns:
    {bool} is True if `char` is a number
  '''
  try:
    val = int(char)
    return True
  except Exception:
    return False


def get_year(string):
  '''
  Given a string find the first 4 digit integer and return it
  @args:
    {str} str: a string that should contain a year
  @returns:
    {str} a string that contains a 4 digit year or is empty
  '''
  _year = ''
  for idx, i in enumerate(string):
    if is_int(i):
      _year += i
      if len(_year) == 4:
        return _year
    else:
      _year = ''
  return ''


def clean(string):
  '''
  Clean an input string
  @args:
    {str} str: an input string
  @returns:
    {str} the input string, cleaned
  '''
  return string.replace('\xa0', '').strip()


def spaces(string):
  '''
  Replace invisible whitespace with a simple space
  @args:
    {str} string: an input string
  @returns:
    {str} the string, cleaned
  '''
  string = string.replace(u'\xa0', ' ')
  return string.replace('&nbsp;', ' ')


def parse_html(html):
  '''
  Parse fields of interest from an html content
  @args:
    {str} html: the HTML page source to parse
  @returns:
    {dict} a mapping from metadata field to metadata value
  '''
  soup = BeautifulSoup(html, 'html.parser')
  results = soup.find_all('div', {'class': 'gs_r gs_or gs_scl'})

  if len(results) == 0:
    start = 0
    return False

  for result_idx, result in enumerate(results):
    print('     * parsing result', result_idx)
    
    # get the unique Google identifier for this record
    cid = result['data-cid']
    did = result['data-did']

    assert cid == did

    # title
    try:
      title = result.find('h3', {'class': 'gs_rt'}).find('a').get_text()
    except Exception:
      try:
        title = result.find('h3', {'class': 'gs_rt'}).get_text()
      except Exception:
        title = ''
    # url
    try:
      url = result.find('h3', {'class': 'gs_rt'}).find('a')['href']
    except:
      url = ''
    # author
    try:
      authors = spaces(result.find('div', {'class': 'gs_a'}).get_text()).split(' - ')[0]
    except Exception:
      authors = ''
    # year
    try:
      _year = spaces(result.find('div', {'class': 'gs_a'}).get_text()).split(' - ')[1]
      _year = str(int(_year.strip()[-4:]))
    except:
      try:
        _year = get_year(result.find('div', {'class': 'gs_a'}).get_text())
      except Exception:
        _year = ''
    # source
    try:
      source = spaces(result.find('div', {'class': 'gs_a'}).get_text()).split(' - ')[1]
      source = ''.join(source[:-7])
    except Exception:
      source = ''

    # get a random integer for this outfile
    # filename = str(int(random.random() * 2**128))
    # specify the path on disk where we'll store this file
    out_path = os.path.join(out_dir, cid + '.json')

    # create the file to save on disk
    result = {
      'url': clean(url),
      'authors': clean(authors),
      'title': clean(title),
      'year': clean(_year),
      'source': clean(source),
    }

    # write the file to disk
    with open(out_path, 'w') as json_out:
      json.dump(result, json_out)

  return True


def get_records(start_year = None, end_year = None):
  '''
  Get all the results that exist in a year
  @args:
    {int} start_year: the starting year to use in the query
    {int} end_year: the ending year to use in the query
  '''
  start = 0
  get_more = True
  while get_more:
    print('   * fetching with start value', start)
    url = source_url
    if start_year:
      url += '&as_ylo=' + str(start_year)
    if end_year:
      url += '&as_yhi=' + str(end_year)
    url += '&start=' + str(start)
    html = get_page_source(url)
    get_more = parse_html(html)
    start += 10


def write_log():
  '''
  Generate a log file with the configs used for the web scrape
  '''
  filename = str(out_dir + '/___scrape-log.txt')
  with open(filename, 'w') as out:
    out.write('Timestamp:\t' + str(now_date) + ' / ' + now_time)
    out.write('\n' + 'Source URL:\t' + str(source_url))
    out.write('\n' + 'Lower bound:\t' + str(min_year))
    if fan_earlier:
      out.write(' and earlier')
    out.write('\n' + 'Upper bound:\t' + str(max_year))
    if fan_later:
      out.write(' and later')


if __name__ == '__main__':

  # set time
  now_date = str(datetime.datetime.now().strftime('%Y-%m-%d'))
  now_time = str(datetime.datetime.now().strftime('%H-%M-%S'))

  # define name of directory where results are stored
  out_dir = str('results/' + dir_reference + '_' + now_date + '_' + now_time)

  # make directory for results
  if not os.path.exists(out_dir):
    os.makedirs(out_dir)

  # write log of configs
  write_log()

  driver = webdriver.Chrome('./chromedriver')

  # get all records before the start year
  if fan_earlier:
    print(' * fetching year', min_year, 'and earlier')
    get_records(end_year = min_year)

  # get all records between the start and end years
  vals = list(range(max_year - min_year + 1))

  # if fanning before the start_year, remove first value from the year range
  if fan_earlier:
    del vals[0]

  # if fanning after the end_year, remove last value from the year range
  if fan_later:
    if vals:
      del vals[-1]

  if vals:
    for offset in vals:
      year = min_year + offset 

      print(' * fetching year', year)
      get_records(start_year = year, end_year = year)

  # get all records after the end year
  if fan_later:
    print(' * fetching year', max_year, 'and later')
    get_records(start_year = max_year)

  # quit once done
  driver.quit()