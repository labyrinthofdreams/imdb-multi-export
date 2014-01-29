"""The MIT License (MIT)

Copyright (c) 2013 https://github.com/labyrinthofdreams

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE."""
from gevent import monkey
monkey.patch_all()
import csv
import re
import os
import argparse
import functools
import logging
import requests
import gevent
import gevent.pool

session = requests.Session()

logformatter = logging.Formatter('%(asctime)s %(username)s %(url)s %(message)s')
fh = logging.FileHandler('output.log')
fh.setLevel(logging.DEBUG)
fh.setFormatter(logformatter)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(fh)

def parse_args():
    opts = argparse.ArgumentParser()
    opts.add_argument('incsvfile', help='CSV file containing submitters and IMDb rating URLs')
    opts.add_argument('outdir', help='Directory where to save the save files')
    opts.add_argument('--cookies', help='Path to cookies file')
    opts.add_argument('--retries', default=100, help='Number of times to retry download')
    opts.add_argument('--overwrite', action='store_const', const=True, help='Overwrite old files')
    opts.add_argument('--threads', default=3, help='Number of simultaneous downloads')

    args = opts.parse_args()
    return args

def read_cookies(cookie_file):
    imdbcookies = {}
    if cookie_file is None:
        return imdbcookies
    try:
        with open(cookie_file, 'rb') as f:
            data = f.read()
            for item in data.split('; '):
                parts = item.split('=', 1)
                imdbcookies[parts[0]] = parts[1]
        return imdbcookies
    except IOError:
        raise

def read_imdb_profiles(infile):
    profiles = []
    try:
        with open(infile, 'rb') as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            uidrx = re.compile('(ur[0-9]+)')
            for row in reader:
                profile = {
                    'username': row[0].strip(),
                    'url': row[1],
                    'userid': uidrx.search(row[1]).group(1)
                }
                profiles.append(profile)
        return profiles
    except IOError:
        raise

def download_imdb_csv(profile):
    error = None
    resp = None
    try:
        prepared_request = prepare_request(profile)
        print '[START] {0}'.format(profile['username'])
        logger.info('Sending HTTP request', extra=profile)
        resp = session.send(prepared_request)
        if resp.status_code != 200:
            logger.info('Bad HTTP status code: %s', resp.status_code, extra=profile)
            raise Exception("Bad HTTP status code: {0}".format(resp.status_code))
        logger.info('OK', extra=profile)
        print '[OK] [ {0}\t]'.format(profile['username'])

        return (resp.content, profile, error)
    except requests.exceptions.ConnectionError as e:
        logger.info('Connection error: %s', str(e), extra=profile)
        error = 'Connection error (see log for details)'
    except Exception as e:
        logger.info('Unspecified error: %s', str(e), extra=profile)
        error = str(e)
    if error is not None:
        print '[FAIL] [ {0}\t] Reason: {1}'.format(profile['username'], error)
    return (resp, profile, error)

def prepare_request(profile):
    listurl = 'http://www.imdb.com/list/export?list_id=ratings&author_id={0}'\
              .format(profile['userid'])
    logger.info('Preparing request: GET %s', listurl, extra=profile)
    req = requests.Request('GET', listurl)
    return session.prepare_request(req)

def profile_exists(indir, profile):
    return os.path.isfile(os.path.join(indir, profile['username'] + '.csv'))

def _not(func):
    """Wraps function func to return opposite boolean value"""
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        return not result
    return wrapper

if __name__ == '__main__':
    try:
        args = parse_args()

        threads = int(args.threads)
        pool = gevent.pool.Pool(threads)

        imdbcookies = read_cookies(args.cookies)
        session.cookies = requests.utils.cookiejar_from_dict(imdbcookies)

        if not os.path.exists(args.outdir):
            os.makedirs(args.outdir)

        retries = int(args.retries)

        imdb_profiles = read_imdb_profiles(args.incsvfile)

        tries = 0
        while tries <= retries:
            failed = []
            downloaded = []

            if tries > 0:
                print '--- Retry attempt {0} of {1} ---'.format(tries, retries)
            # Remove existing files
            if args.overwrite is None:
                new_imdb_profiles = filter(functools.partial(_not(profile_exists), args.outdir),
                                       imdb_profiles)
            else:
                new_imdb_profiles = imdb_profiles

            if len(new_imdb_profiles) == 0:
                # Nothing to download
                break

            results = pool.map(download_imdb_csv, new_imdb_profiles)
            pool.join()
            for response, profile, error in results:
                try:
                    if error is not None:
                        raise Exception(error)

                    # Save
                    savefile = os.path.join(args.outdir, '{0}.csv'.format(profile['username']))
                    with open(savefile, 'wb') as saveto:
                        saveto.write(response)
                    downloaded.append(profile)
                except Exception as e:
                    failed.append(profile)

            tries += 1

            total = len(imdb_profiles)
            if len(failed) > 0:
                print '--- Failed ({0}/{1})'.format(len(failed), total)
                for profile in failed:
                    print profile['username']
            else:
                skipped = set(p['username'] for p in imdb_profiles) - \
                          set(p['username'] for p in new_imdb_profiles)

                print '--- Skipped ({0}/{1}) ---'.format(len(skipped), total)
                print '--- Downloaded ({0}/{1})'.format(len(downloaded), total)

                break
    except IOError as e:
        print str(e)
    except os.error as e:
        print str(e)