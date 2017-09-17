import urllib
from bs4 import BeautifulSoup
import pandas as pd
import re

COLLEGE_SIMPLY_URL = "http://www.collegesimply.com/guides/admission-data/"

def process_raw_links(tags):
  links = []
  for tag in tags:
      links.append((str(tag.string), "http://www.collegesimply.com/guides/admission-data/" + tag['href']))
  data = pd.DataFrame.from_records(links, columns=['School', 'Link'])
  return data  

def write_out_school_ids(links):
  '''
  Given the data frame of all links scraped from collegesimply.com, generate
  a CSV mapping names to ids to uniquely identify schools.
  '''
  school_ids = pd.DataFrame(data=links['School'])
  school_ids['id'] = range(len(school_ids))
  school_ids.to_csv("../data/school_ids.csv", index=False)
  return school_ids

def create_school_id_map():
  school_ids = pd.read_csv('../data/school_ids.csv', index_col=False)
  return {row.School: row.id for _, row in school_ids.iterrows()}

def get_stats_from_college_simply(ids):
  updated_stats = pd.read_html(COLLEGE_SIMPLY_URL, attrs={'id': 'applicationData'})[0]
  # Remove extra whitespace from some of the items.
  updated_stats['School'] = updated_stats['School'].apply(lambda s: ' '.join(s.split()))
  updated_stats['ID'] = updated_stats['School'].apply(lambda key: ids[key])
  return updated_stats

def get_links_from_college_simply(ids):
  # Scrape links
  html = urllib.request.urlopen(urllib.request.Request(COLLEGE_SIMPLY_URL))
  soup = BeautifulSoup(html, 'html.parser')
  raw_links = process_raw_links(soup.find_all('a', href=re.compile('^/college')))
  # Clean some extra columns.
  links = raw_links[(raw_links['School'] != 'By State') & (raw_links['School'] != 'Find Colleges Nearby')]
  # Remove extra whitespace from some of the items.
  links['School'] = links['School'].apply(lambda s: ' '.join(s.split()))
  links['ID'] = links['School'].apply(lambda key: ids[key])
  return links

def get_full_stats_from_college_simply(ids):
  stats = get_stats_from_college_simply(ids)
  links = get_links_from_college_simply(ids)
  res = stats.merge(links, on='ID')
  assert (res['School_x'] == res['School_y']).all()
  res['School'] = res['School_y']
  del res['School_x']
  del res['School_y']
  return res


def add_ranking_data(ids, data):
  rankings = pd.read_csv('../data/college_stats.csv')
  rankings['College/University'] = rankings['College/University'].apply(lambda s: ' '.join(s.split()))
  # Filter out ranked data for which we have no school ids.
  rankings = rankings[rankings['College/University'].apply(lambda key: key in ids)]
  rankings['ID'] = rankings['College/University'].apply(lambda key: ids[key])
  res = data.merge(rankings, on='ID', how='outer')
  # Assume College/University is redundant
  del res['College/University']
  # Reorder columns
  cols = ["ID","School","School Type","Region","US World Ranking",
          "Acceptance Rate","Average GPA",
          "New EBRW 25th","New EBRW 75th","New Math 25th","New Math 75th",
          "SAT Total 25th","SAT TOTAL 75th","SAT 25","SAT 75",
          "ACT 25","ACT 75","ACT Comp 25th","ACT Comp 75th",
          "Test Optional or Test Flexible", "State","Link"]
  import re
  non_decimal = re.compile(r'[^\d.]+')
  res['Acceptance Rate'] = res['Acceptance Rate'].apply(lambda x: float(non_decimal.sub('', x)) / 100)
  return res[cols].sort_values(['Acceptance Rate'])


if __name__ == '__main__':
	pd.options.mode.chained_assignment = None
	schools = create_school_id_map()
	college_simply = get_full_stats_from_college_simply(schools)
	total_set = add_ranking_data(schools, college_simply)
	total_set.to_csv('../data/final_rankings.csv', index=False)