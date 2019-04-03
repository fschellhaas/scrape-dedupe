from glob import glob
from datasketch import MinHash, MinHashLSH
from nltk import ngrams
from difflib import SequenceMatcher
from random import random
import codecs
import datetime
import json
import os
import sys

##
# Get Data
##

def get_google_vals():
  '''
  Get a list of dictionaries, where each dictionary represents
  a distinct citation in Google Scholar.
  '''

  # get google result list
  google_vals = []
  for i in glob('results/*/*.json'):
    with open(i) as f:
      google_dict = json.load(f)
      google_id = os.path.basename(i).replace('.json', '')
      google_dict['google_id'] = google_id
      google_dict['id'] = google_id
      google_dict['collection'] = 'google'
      google_vals.append(google_dict)

  # dedupe using only unique google ids
  google_vals_by_id = {}
  for i in google_vals:
    google_vals_by_id[i['google_id']] = i

  goog_records = google_vals_by_id.values()

  # if deduping only new google records, retain only new google ids
  if only_process_new_google:
    used_goog_ids = set(open('lists/processed_google_ids.txt').read().split('\n'))
    return [i for i in goog_records if i['google_id'] not in used_goog_ids]
  return goog_records


def get_endnote_vals():
  '''
  Get a list of dictionaries, where each dictionary represents
  a distinct citation in EndNote.
  '''
  endnote_vals = []

  # get endnote result list
  with open('endnote.txt') as f:
    endnote = f.read().split('\n')

  for i in endnote:
    cells = i.split('\t')
    author, year, title, source = cells
    endnote_vals.append({
      'authors': author,
      'year': year,
      'title': title,
      'source': source,
      'id': 'endnote-' + str(random() * 2**64),
      'url': '',
      'collection': 'endnote',
    })
  return endnote_vals


def get_metadata_string(obj):
  '''
  Given an object with 'title' and 'authors' keys, return a string
  that represents the metadata for this object
  '''
  return obj['title'] + '-' + obj['authors']


def override_msg(filename, collection):
  '''
  Generate warning message when user config is overridden to get data
  '''
  msg = '\n\n ! Warning: ' + filename + ' not found on disk. Processing all '
  msg += collection + ' records.\n'
  print(msg)


##
# Prep
##

def prepare_directories():
  '''
  Prepare the directories for .json and .tsv files.
  '''
  for i in ['lists', 'json']:
    if not os.path.exists(i):
      os.makedirs(i)


##
# Find Dupes
##

def find_clusters(arr):
  '''
  `arr` is a list of dictionaries where each dictionary represents
  the content from one Google or EndNote record. Return a list of lists,
  where each sublist contains objects that have been identified as being
  similar to one another by minhashing.
  '''

  arr = list(arr)
  if developing:
    arr = arr[:max_dev_records]

  minhashes = [] # a list of the generated minhashes
  clusters = [] # a list of lists where each sublist is a group of clustered records
  index = MinHashLSH(threshold = threshold, num_perm = n_perms)

  # add all strings to the lsh index
  for idx, i in enumerate(arr):
    print(' indexed', idx + 1, 'of', len(arr))
    metadata_string = get_metadata_string(i)
    m = MinHash(num_perm = n_perms)
    for chars in ngrams(metadata_string, 3):
      window = ''.join(chars)
      m.update(window.encode('utf8'))
    # use the index position of this observation as the key for the obs
    minhashes.append(m)
    index.insert(idx, m)

  # for each string, find those sufficiently similar
  for idx in range(len(arr)):
    print(' querying for', idx + 1)
    # find the `nth` minhash in the minhashes
    matches = index.query(minhashes[idx])
    # build a cluster of the records that match this query + the query itself
    cluster = [arr[j] for j in matches]
    # get a list of `arr` values that are part of this cluster
    clusters.append(cluster)
  return clusters


def identify_diplomats(arr, deduped = None):
  '''
  `arr` is a list of dictionaries where each dictionary represents
  the content from one Google or EndNote record. Return two dictionaries,
  one of which identifies the whitelisted values (diplomats), the other
  of which identifies blacklisted values (dupes). Dictionnaries are read
  from disk when `deduped` is True.
  '''

  blacklist = {} # each key is an id that represents a dupe
  whitelist = {} # each key is an id that represents a diplomat

  # get the clusters
  clusters = find_clusters(arr)

  # iterate over each cluster and mark records sufficiently dissimilar
  # as non-dupes
  multi_clusters = []
  for cluster in clusters:

    # case of a single-value cluster - nothing similar to this value in index
    if len(cluster) == 1:
      obj = cluster[0]
      whitelist[obj['id']] = obj

    # case where pairwise distances need to be computed
    else:
      multi_clusters.append(cluster)

  # identify the total number of 'multiclusters'
  n_multiclusters = len(multi_clusters)

  # prompt the user for input on each cluster
  prompt = get_prompt()

  for cluster_idx, cluster in enumerate(multi_clusters):

    # skip clusters that have already been deduped
    if all([(i['id'] in whitelist) or (i['id'] in blacklist) for i in cluster]):
      continue

    # sort the vals in cluster so that endnote always comes first
    cluster = sort_cluster(cluster)

    # get the list of similarities within this cluster
    sims = []
    for idx, _ in enumerate(cluster):
      if idx + 1 < len(cluster):
        sims.append(get_string_similarity(cluster[idx], cluster[idx+1]))

    # get the prompt to show the user the pairwise similarities
    msg = get_prompt_message(whitelist, cluster, cluster_idx, n_multiclusters, sims)

    # if analyzing exactly two records, one from google and one from endnote,
    # if the the years match, and if the pairwise similarity is >= ceiling,
    # then whitelist endnote and blacklist google
    collections = [i['collection'] for i in cluster]

    if (sorted(collections) == sorted(['google', 'endnote']) and
      cluster[0]['year'] == cluster[1]['year'] and
      sims[0] >= ceiling):

      # whitelist the endnote and blacklist the google val
      for i in cluster:
        if i['collection'] == 'endnote':
          whitelist[i['id']] = i
        elif i['collection'] == 'google':
          blacklist[i['id']] = i
      continue

    # when deduping google vs. endnote and analyzing only records from the same collection,
    # whitelist all records - they were already deduped
    if deduped:
      if len(set(collections)) <= 1:
        for i in cluster:
          whitelist[i['id']] = i
        continue

    # keep prompting until user gives a valid response
    response_valid = False
    while not response_valid:
      user_keys = prompt(msg).strip().lower()

      # if user sent the `a` key, keep all records in cluster
      if user_keys == 'a':
        # vals to whitelist is a list of dictionaries
        vals_to_whitelist = cluster
        vals_to_blacklist = []

      elif any([i in numbers for i in user_keys]):
        # make sure the number(s) provided are valid index positions
        try:
          if any([int(i) > len(cluster) for i in user_keys.split(',')]):
            print('\n ! Warning: Invalid response received. Try again.')
            continue
        except:
          print('\n ! Warning: Invalid response received. Try again.')
          continue

        # the received values were all valid indices
        whitelist_indices = [int(i)-1 for i in user_keys.split(',')]
        blacklist_indices = [i for i in range(len(cluster)) if i not in whitelist_indices]
        vals_to_whitelist = [cluster[i] for i in whitelist_indices]
        vals_to_blacklist = [cluster[i] for i in blacklist_indices]

      else:
        print('\n ! Warning: Invalid response received. Try again.')
        continue

      # if there are goog and endnote candidates, ensure the user whitelisted
      # at least one endnote record
      has_goog = any([j for j in cluster if j['collection'] == 'google'])
      has_endnote = any([j for j in cluster if j['collection'] == 'endnote'])
      endnote_whitelisted = any([j for j in vals_to_whitelist if j['collection'] == 'endnote'])

      if has_goog and has_endnote and not endnote_whitelisted:
        print(' ! Warning: When records are duplicates, EndNote must be retained. Try again.')
        continue

      response_valid = True

      # add all whitelist records
      for i in vals_to_whitelist:
        if i['id'] in blacklist.keys():
          whitelist, blacklist = challenge_before_whitelist(i, whitelist, blacklist)
        else:
          whitelist[i['id']] = i

      # add all blacklist records
      for i in vals_to_blacklist:
        if i['id'] in whitelist.keys():
          whitelist, blacklist = challenge_before_blacklist(i, whitelist, blacklist)
        else:
          blacklist[i['id']] = i
  return whitelist, blacklist


def challenge_before_whitelist(obj, whitelist, blacklist):
  '''Before whitelisting something in blacklist, prompt user for confirmation'''
  prompt = get_prompt()
  validate_msg = ' ! Preparing to whitelist former blacklist value\n'
  validate_msg += obj['id'] + '. press w/b to whitelist/blacklist:\n'
  validate = prompt(validate_msg).lower().strip()
  if validate == 'w':
    delete(obj['id'], blacklist)
    whitelist[obj['id']] = obj
  elif validate == 'b':
    delete(obj['id'], whitelist)
    blacklist[obj['id']] = obj
  return whitelist, blacklist


def challenge_before_blacklist(obj, whitelist, blacklist):
  '''Before blacklisting something in whitelist, prompt user for confirmation'''
  prompt = get_prompt()
  validate_msg = ' ! Preparing to blacklist former whitelist value\n'
  validate_msg += obj['id'] + '. press w/b to whitelist/blacklist:\n'
  validate = prompt(validate_msg).lower().strip()
  if validate == 'w':
    delete(obj['id'], blacklist)
    whitelist[obj['id']] = obj
  elif validate == 'b':
    delete(obj['id'], whitelist)
    blacklist[obj['id']] = obj
  return whitelist, blacklist


def get_prompt():
  '''Return a function that can be used to prompt the user for input'''
  try:
    return input
  except:
    return raw_input


def delete(key, obj):
  '''Try to delete key from obj'''
  try:
    del obj[key]
  except Exception:
    print(' ! Warning: Could not delete', key)


def sort_cluster(cluster):
  '''Sort the values in cluster so EndNote records come first'''
  _sorted = []
  for i in cluster:
    if i['collection'] == 'endnote':
      _sorted.append(i)
  for i in cluster:
    if i['collection'] == 'google':
      _sorted.append(i)
  return _sorted


def get_prompt_message(whitelist, cluster, cluster_idx, n_clusters, sims):
  '''Get a prompt with instructions for the user'''
  msg = '\n------------------------------------------------------------------\n'
  msg += 'Please type a comma-separated list of integers, where each integer\n'
  msg += 'represents the index position of a record to be treated as unique.\n'
  msg += 'Type `a` to treat all as unique.\n'
  msg += '------------------------------------------------------------------\n\n'
  msg += ' * considering cluster ' + str(cluster_idx+1) + ' of ' + str(n_clusters) + '\n'

  # add the string similarity between the members of the cluster
  for idx, _ in enumerate(cluster):
    if idx + 1 < len(cluster):
      msg += ' * similarity between ' + str(idx+1) + ' and ' + str(idx+2)
      msg += ': ' + str(sims[idx]) + '\n\n'

  # check if any of these values have been added to whitelist
  if any([i['id'] in whitelist for i in cluster]):
    matched_vals = []
    for i in cluster:
      if i['id'] in whitelist:
        msg += '\nAttention: The following records have been whitelisted:\n'
        msg += json.dumps(reorder_object_keys(i), indent=4)
        msg += '\n\n'

  for idx, i in enumerate(cluster):
    o = json.dumps(reorder_object_keys(i), indent=4)
    msg += str(idx + 1) + ': ' + o + '\n'
  return msg


def reorder_object_keys(obj):
  '''Specify the order of keys in `obj` and return `obj` with just those keys'''
  keys = ['collection', 'authors', 'year', 'title', 'source', 'url']
  d = {}
  for i in keys:
    d[i] = obj[i]
  return d


def get_string_similarity(obj_a, obj_b):
  '''Given the full record objects for two Google or EndNote results, return
  the similarity between the metadata strings from those objects'''
  a = get_metadata_string(obj_a)
  b = get_metadata_string(obj_b)
  return SequenceMatcher(None, a, b, autojunk=False).ratio()


##
# Build a report
##

def count_file_lines(path):
  '''
  Count the number of lines in a file at `path`. If the file
  doesn't exist, return a 0 as there were no records to save
  for that file.
  '''
  try:
    with codecs.open(path, 'r', 'utf8') as f:
      return len(f.read().rstrip().split('\n'))
  except FileNotFoundError:
    return 0


def build_reports():
  '''
  Build a report that indicates:
    - Records with unique Google IDs retrieved
    - Records retained after deduplication against Google
    - Records retained after deduplication against EndNote
  '''

  google_blacklist_count = count_file_lines(os.path.join('lists', 'google_blacklist.tsv'))
  google_whitelist_count = count_file_lines(os.path.join('lists', 'google_whitelist.tsv'))

  # count the number of google records in the master whitelist
  with codecs.open(os.path.join('lists', 'master_whitelist.tsv'), 'r', 'utf8') as f:
    rows = f.read().rstrip().split('\n')
    google_master_whitelist_vals = []
    for i in rows:
      vendor = i.rstrip().split('\t')[-1]
      if vendor == 'google':
        google_master_whitelist_vals.append(i)

  with codecs.open('lists/deduped_google_records.tsv', 'w', 'utf8') as out:
    out.write('\n'.join(google_master_whitelist_vals))

  with codecs.open('report.txt', mode) as out:
    if only_process_new_google:
      out.write('Database update:\n')
    out.write('Timestamp: ' + str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + '\n')
    out.write('Unique Google IDs retrieved:\t\t' + str(google_blacklist_count + google_whitelist_count) + '\n')
    out.write('Deduplicated against itself:\t\t' + str(google_whitelist_count) + '\n')
    out.write('Deduplicated against EndNote:\t\t' + str(len(google_master_whitelist_vals)) + '\n\n')


##
# Outputs
##

def save_tsv(d, filename):
  '''
  Given a dictionary whose keys are _id attributes of a dict, and whose
  value is the full object that contains _id, write each dict as a row in a
  tsv with `filename`
  '''
  key_list = ['id', 'authors', 'year', 'title', 'source', 'url', 'collection']

  # write tsv
  if d:
    with open(filename, mode) as out:
      for _id in d.keys():
        for key in key_list:
          out.write(d[_id].get(key, '') + '\t')
        out.write('\n')


def get_wb(l, filename, read = None):
  '''
  Given a list of dictionaries (endnote_vals or google_vals) and a filename
  in which the deduped vals from that list of dictionaries should be saved,
  return the whitelisted and blacklisted values from `l`. If read is True,
  fetch whitelist and blacklist from disk instead.
  '''
  # if there are no (new) records, return empty lists
  if len(l) == 0:
    return [], []

  path = 'json/' + filename

  if os.path.exists(path):
    
    # in read-mode, fetch saved values from disk
    if read:
      whitelist, blacklist = json.load(open(path))
      return whitelist, blacklist
    
    # if `l` is comprised of google records and  user wants to process only
    # new google records, fetch the white and blacklists for the new google
    # records, and add those to the existing lists
    if only_process_new_google and l[0]['collection'] == 'google':
      new_white, new_black = identify_diplomats(l)
      with open(path) as out:
        whitelist, blacklist = json.load(out)
      whitelist.update(new_white)
      blacklist.update(new_black)
      with open(path, 'w') as out:
        json.dump([
          dict(whitelist),
          dict(blacklist)
          ], out)
      return new_white, new_black

  # generate whitelist and blacklist
  whitelist, blacklist = identify_diplomats(l)
  with open(path, 'w') as out:
    json.dump([
      dict(whitelist),
      dict(blacklist)
    ], out)
  return whitelist, blacklist


##
# Save used Google IDs
##

def cache_parsed_google_ids():
  '''
  Save/append all Google IDs used in this round of analysis so they do not have to be
  processed again
  '''
  goog_ids = [i['id'] for i in google_vals]
  with open('lists/processed_google_ids.txt', mode) as out:
    out.write('\n'.join(goog_ids) + '\n')


if __name__ == '__main__':
  # global
  threshold = 0.60
  ceiling = 0.85 # auto-whitelist only endnote if similarity with goog record >= ceiling
  n_perms = 256
  developing = False
  max_dev_records = 5000
  dedupe_google = False
  dedupe_endnote = False
  dedupe_endnote_v_google = True
  only_process_new_google = False # set to true to process only new records (requires complete deduping vs. endnote)

  # initialize numbers
  numbers = [str(i + 1) for i in range(9)]

  # prepare assets
  prepare_directories()
  google_vals = get_google_vals() # list of dicts
  endnote_vals = get_endnote_vals() # list of dicts

  # force google vs. google if vals cannot be fetched from disk
  if not os.path.exists('json/google_vals.json'):
    if dedupe_endnote_v_google and (not dedupe_google or
      (dedupe_google and only_process_new_google)):
      override_msg('google_vals.json', 'Google')
      dedupe_google = True
      only_process_new_google = False

  if only_process_new_google:
    # force  google vs. google to process the new google vals
    if not dedupe_google:
      print('\n\n ! Warning: Overriding user settings to first dedupe new Google records against self.\n')
      dedupe_google = True
    # force google vs. endnote if `only_process_new_google` is true
    if not dedupe_endnote_v_google:
      print('\n\n ! Warning: Overriding user settings to dedupe new Google records against EndNote.\n')
      print(' Do NOT partially complete this update, otherwise lists will go out of sync.)\n')
      dedupe_endnote_v_google = True

  # config whether to write or append to lists of records
  if only_process_new_google:
    mode = 'a'
  else:
    mode = 'w'

  # dedupe google vs. google
  if dedupe_google:
    print('\n------------------------------------------------------------------')
    print('Deduping Google vs. Google')
    print('------------------------------------------------------------------\n')
    google_whitelist, google_blacklist = get_wb(google_vals, 'google_vals.json')
    save_tsv(google_whitelist, 'lists/google_whitelist.tsv')
    save_tsv(google_blacklist, 'lists/google_blacklist.tsv')

    # cache the google ids used in the analysis
    cache_parsed_google_ids()

  # force endnote vs. endnote if vals cannot be fetched from disk
  if not os.path.exists('json/endnote_vals.json'):
    if dedupe_endnote_v_google and not dedupe_endnote:
      override_msg('endnote_vals.json', 'EndNote')
      dedupe_endnote = True

  # dedupe endnote vs. endnote
  if dedupe_endnote:
    print('\n------------------------------------------------------------------')
    print('Deduping EndNote vs. EndNote')
    print('------------------------------------------------------------------\n')
    endnote_whitelist, endnote_blacklist = get_wb(endnote_vals, 'endnote_vals.json')
    save_tsv(endnote_whitelist, 'lists/endnote_whitelist.tsv')
    save_tsv(endnote_blacklist, 'lists/endnote_blacklist.tsv')

  # dedupe google vs. endnote
  if dedupe_endnote_v_google:
    if not dedupe_google:
      google_whitelist, google_blacklist = get_wb(google_vals, 'google_vals.json', read = True)
    if not dedupe_endnote:
      endnote_whitelist, endnote_blacklist = get_wb(endnote_vals, 'endnote_vals.json', read = True)
    deduped_google_vals = list(dict(google_whitelist).values())
    deduped_endnote_vals = list(dict(endnote_whitelist).values())
    print('\n------------------------------------------------------------------')
    print('Deduping Google vs. EndNote')
    print('Deduped Google records: ' + str(len(deduped_google_vals)))
    print('Deduped EndNote records: ' + str(len(deduped_endnote_vals)))
    print('------------------------------------------------------------------\n')
    master_whitelist, master_blacklist = identify_diplomats(
      deduped_google_vals + deduped_endnote_vals, deduped = True)
    save_tsv(master_whitelist, 'lists/master_whitelist.tsv')
    save_tsv(master_blacklist, 'lists/master_blacklist.tsv')

    # build the final reports
    if os.path.exists('lists/master_whitelist.tsv') and os.path.exists('lists/master_blacklist.tsv'):
      build_reports()
    else:
      print('\n\n ! Warning: Reports could not be generated because master lists are missing.\n')