import configparser, requests

import regex as re

from bs4 import BeautifulSoup as bs
from os import path
from time import sleep
from datetime import datetime
from random import randrange

import logging




# Defined hard-coded variables to be added to each paper
journal_full = 'Acaralogia'
journal_short = 'Acaralogia'
article_type = 'article'
doi = ''
import_date = str(datetime.now())
status = 'SCRAPED'

# Load the basic URLs to be scraped
first_url = 'https://www1.montpellier.inra.fr/CBGP/acarologia/contents.php'
base_url = 'https://www1.montpellier.inra.fr/CBGP/acarologia/'

# Load the min and max waiting time
min_time = 1

max_time = 10

def get_issues():

  pages = list()
  try:
    r = requests.get(first_url)

    soup = bs(r.content, 'html.parser')
    try:
      for a in soup.find_all('a'):
          if 'Issue' in a.text:
            print(a['href'])
            pages.append(a['href'])
    except Exception as e:
      logging.error('Something wrong extracting issue links. Error: '
                  '{error_class}; '
                  'Message: {message}'.format(error_class=e.__class__,
                                              message=str(e)))
  except Exception as e:
    logging.error('Something wrong extracting issue. Error: '
                 '{error_class}; '
                 'Message: {message}'.format(error_class=e.__class__,
                                             message=str(e)))
  
  return pages


def get_articles(issue_url):

  articles = list()

  try:
    url = base_url + issue_url

    r = requests.get(url)

    soup = bs(r.content, 'html.parser')
    try:
      for a in soup.find_all('a'):
        if 'article' in a['href']:
            articles.append(a['href'])
    except Exception as e:
      logging.error('Something wrong extracting article links. Error: '
                  '{error_class}; '
                  'Message: {message}'.format(error_class=e.__class__,
                                              message=str(e)))
  except Exception as e:
    logging.error('Something wrong getting article links. Error: '
                 '{error_class}; '
                 'Message: {message}'.format(error_class=e.__class__,
                                             message=str(e)))

  return articles

def get_metadata(article_link):

  article = dict()
  try:
    url = base_url + article_link

    r = requests.get(url)

    soup = bs(r.content, 'html.parser')

    try:
      link = ''
      for a in soup.find_all('a'):
        if 'doi.org' in a.text:
            doi = a['href']

        if 'Download article' in a.text:
            link = a['href']
    except Exception as e:
      logging.error('Something wrong getting doi and/or article link. Error: '
                  '{error_class}; '
                  'Message: {message}'.format(error_class=e.__class__,
                                              message=str(e)))

    try:
      for p in soup.find_all('p'):
          if 'Volume:' in p.text and 'Issue' in p.text:
              year = p.text.split(' - ')[0]
              volume = p.text.split(' Volume: ')[1].split(' ')[0]
              issue = p.text.split(' Issue: ')[1].split(' ')[0]
              pagination = p.text.split(' pages: ')[1]
              start_page = pagination.split('-')[0]
              end_page = pagination.split('-')[1]
    except Exception as e:
      logging.error('Something wrong getting metadata. Error: '
                  '{error_class}; '
                  'Message: {message}'.format(error_class=e.__class__,
                                              message=str(e)))

    try:
      for h3 in soup.find_all('h3'):
          for a in h3.find_all('a'):
              first_surname = a.text.split(',')[0]
              break
          break
    except Exception as e:
      logging.error('Something wrong getting author name. Error: '
                  '{error_class}; '
                  'Message: {message}'.format(error_class=e.__class__,
                                              message=str(e)))

    try:

      article = dict()

      article['journal_full'] = journal_full
      article['journal_short'] = journal_short
      article['journal_scraper'] = 'ACAROLOGIA'
      article['year'] = year if year != None else 'None'
      article['volume'] = volume if volume != None else 'None'
      article['issue'] = issue if issue != None else 'None'
      article['start_page'] = start_page if start_page != None else 'None'
      article['end_page'] = end_page if end_page != None else 'None'
      article['article_type'] = article_type
      article['doi'] = ''
      article['first_surname'] = first_surname
      article['link'] = base_url + link
      article['filename'] = ('journal_short.year.volume.issue.start_page-end_page'
                             .replace('journal_short', journal_short)
                             .replace('year', year)
                             .replace('volume', volume)
                             .replace('issue', issue)
                             .replace('start_page', start_page)
                             .replace('end_page', end_page))
      article['status'] = status
      article['import_date'] = import_date

      return article

    except Exception as e:
      logging.error('Something wrong compiling data for article. Error: '
                  '{error_class}; '
                  'Message: {message}'.format(error_class=e.__class__,
                                              message=str(e)))
  except Exception as e:
    logging.error('Something wrong trying to scrape article. Error: '
                 '{error_class}; '
                 'Message: {message}'.format(error_class=e.__class__,
                                             message=str(e)))


def scrape():

  papers = list()

  logging.info("Scraping issues links...")
  issues = get_issues()

#   for issue in issues:
  issue = issues[0]
  logging.info("Scraping article links...")
  articles = get_articles(issue)

  for article in articles:
    #   print(article)
      logging.info("Scraping article metadata...")
      paper = get_metadata(article)
    #   print(paper)

      if article != None:
        papers.append(paper)
    # break

  return papers