#!/usr/env/python
# -*- coding: utf-8 -*-
'''
Script that identifies articles in a category that are above a threshold
of quality based on quality predictions by ORES.

Copyright (c) 2017 Morten Wang

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
SOFTWARE.
'''

import os
import logging

import MySQLdb

import requests

import time

class Prediction():
    def __init__(self, revision_id, rating, probs):
        '''
        A prediction for a given revision consists of a rating
        (the majority class) and a set of probabilities per class.

        :param revision_id: The revision of this prediction.
        :type revision_id: int

        :param rating: The predicted assessment class
        :type rating: str

        :param probs: A set of probabilities per assessment class.
        :type probs: dict
        '''
        self.rev_id = revision_id
        self.rating = rating
        self.probs = probs
        self.p_above_target = 0.0

class Predictor():
    def __init__(self):
        ## ORES url
        self.ORES_url = "https://ores.wikimedia.org/v2/scores/"
        
        ## We identify ourselves
        self.headers = {
            'User-Agent': 'SuggestBot/1.0',
            'From:': 'morten@cs.umn.edu',
            }
        
        ## The WP 1.0 assessment scale used by ORES, in descending order
        ## Note that we use case-insensitive scoring
        self.wp10 = ['FA', 'GA', 'B', 'C', 'Start', 'Stub']

        ## The number of revisions we will retrieve predictions for
        ## with every request to ORES
        self.iter_size = 50

        ## Max number of attempts we'll make to ORES for a set of predictions
        self.max_url_attempts = 3

        self.lang = 'en'
        self.db_conf = "~/replica.my.cnf"
        self.db_server = "enwiki.labsdb"
        self.db_name = "enwiki_p"
        self.db_conn = None
        self.db_cursor = None

    def db_connect(self):
        '''
        Connect to the database. Returns True if successful.
        '''
        self.db_conn = None
        self.db_cursor = None
        try:
            self.db_conn = MySQLdb.connect(db=self.db_name,
                                           host=self.db_server,
                                           charset='utf8',
                                           use_unicode=True,
                                           read_default_file=os.path.expanduser(self.db_conf))
            self.db_cursor = self.db_conn.cursor(MySQLdb.cursors.SSDictCursor)
        except MySQLdb.Error as e:
            logging.error('Unable to connect to database')
            logging.error('{} : {}'.format(e[0], e[1]))

        if self.db_conn:
            return(True)

        return(False)

    def db_disconnect(self):
        '''Close our database connections.'''
        try:
            self.db_cursor.close()
            self.db_conn.close()
        except:
            pass

        return()

    def get_predictions(self, rev_ids):
        '''
        For the given list of revision IDs, fetch predictions for them,
        returning a dictionary mapping revision ID to Prediction objects.

        :param rev_ids: The revision IDs we are predicting for
        :type rev_ids: list (of ints)
        '''

        ## The mapping dictionary we will return
        predictions = dict()

        # ORES uses "{lang}wiki" as identifiers of the wiki
        langcode = "{}wiki".format(self.lang)
        
        http_session = requests.Session()
        i = 0
        while i < len(rev_ids):
            subset = rev_ids[i:i + self.iter_size]
            # make a request to score the revisions
            url = '{ores_url}{langcode}/wp10/?revids={revids}'.format(
                ores_url=self.ORES_url,
                langcode=langcode,
                revids='|'.join([str(rev_id) for rev_id in subset]))

            logging.debug('Requesting predictions for {n} pages from ORES'.format(n=len(subset)))

            num_attempts = 0
            while num_attempts < self.max_url_attempts:
                r = http_session.get(url,
                                     headers=self.headers)
                num_attempts += 1
                if r.status_code == 200:
                    try:
                        response = r.json()
                        revid_pred_map = response['scores'][langcode]['wp10']['scores']
                        # iterate over returned predictions and store
                        for revid, score_data in revid_pred_map.items():
                            predictions[revid] = Prediction(revid,
                                                           score_data['prediction'],
                                                           score_data['probability'])

                        break
                    except ValueError:
                        logging.warning("Unable to decode ORES response as JSON")
                        logging.warning("url={}".format(url))
                    except KeyError:
                        logging.warning("ORES response keys not as expected")
                        logging.warning("url={}".format(url))

                    # something didn't go right, let's wait and try again
                    time.sleep(0.5)

            # update i to iterate to next batch
            i += self.iter_size

        return(predictions)
    
    def predict(self, category_name, target,
                distance=2):
        '''
        For all the articles in the given category, identify those that
        are at least `distance` number of quality classes away from the
        given target.  `distance` defaults to 2, meaning that if the
        target is "Stub", an article has to be predicted to be at least
        of C-class quality.

        :param category_name: Name of the category (without namespace)
        :type category_name: str

        :param target: Target class (one of: FA, GA, B, C, Start, Stub
        :type target: str

        :param distance: Minimum distance from the target class for
                         an article to be included in the output list.
        :type distance: int
        '''

        # Query to get all articles/talk pages in a given category.
        # Note that this query retrieves titles without namespaces.
        member_query = '''SELECT page_title
                          FROM page
                          JOIN categorylinks
                          ON page_id=cl_from
                          WHERE cl_to=%(category)s
                          AND page_namespace IN (0, 1)'''

        # Query to get the latest revision of a given page by title
        latest_query = '''SELECT page_latest
                          FROM page
                          WHERE page_title=%(title)s
                          AND page_namespace=0'''
        
        ## Mapping from article title to revision ID
        art_rev_map = dict()

        ## Mapping from revision ID to prediction
        rev_pred_map = dict()

        ## Articles that are candidates to be reassessed, meaning their
        ## predicted rating is at least 'distance' away from 'target'
        candidate_map = {}
        
        if not self.db_connect():
            logging.error('unable to connect to database')
            return()
        
        # get all articles/talk pages in the category
        self.db_cursor.execute(member_query,
                               {'category': category_name.replace(' ',
                                                                  '_')})
        done = False
        while not done:
            row = self.db_cursor.fetchone()
            if not row:
                done = True
                continue

            page_title = row['page_title'].decode('utf-8')
            art_rev_map[page_title] = -1

        logging.info('Found {} members of the category'.format(len(art_rev_map)))

        # find the latest revision ID of all the pages (use page_latest
        # in the page table, build a map from page title to revision ID)
        for page_title in art_rev_map.keys():
            self.db_cursor.execute(latest_query,
                                   {'title': page_title})
            for row in self.db_cursor.fetchall():
                art_rev_map[page_title] = str(row['page_latest'])

        # If we cannot find a latest revision for a given page, flag the title
        del_list = []
        for page_title, rev_id in art_rev_map.items():
            if int(rev_id) <= 0:
                logging.warning('could not find a latest revision for {}'.format(page_title))
                del_list.append(page_title)

        for page_title in del_list:
            del(art_rev_map[page_title])
                
        # iterate over groups of revision IDs and retrieve predictions,
        # building a map from revision ID to prediction
        rev_pred_map = self.get_predictions(list(art_rev_map.values()))
       
        # iterate over the (page title, rev ID) map and for each:
        #   check if the prediction is >= distance away
        #   calculate probability article rating is greater than target
        wp10_map = { rating: idx for idx, rating in enumerate(self.wp10)}
        target_idx = wp10_map[target]

        for page_title, rev_id in art_rev_map.items():
            try:
                rev_pred = rev_pred_map[rev_id]
            except KeyError:
                # We don't have predictions for this revision
                continue

            ## Is prediction more than 'distance' away from 'target'?
            if target_idx - wp10_map[rev_pred.rating] >= distance:
                candidate_map[page_title] = rev_pred
                for i in range(0, target_idx):
                    rev_pred.p_above_target += rev_pred.probs[self.wp10[i]]

        return(candidate_map)

    def build_table(self, candidates, target):
        '''
        Build a Wikitable based on the candidate articles, sorted by the
        probability that the article is higher than 'target' class.

        :param candidates: Mapping of article titles to predictions.
        :type candidates: dict

        :param target: Target class
        :type target: str
        '''

        table_start= '''{{|
!  Title
!  Predicted class
!  P(> {})'''.format(target)
        table_content = ''
        table_end = '''
|}'''
        
        ## Sort the candidates based on probability that article is not
        ## in the target class.
        sorted_candidates = sorted(candidates.items(),
                                   key=lambda pred: pred[1].p_above_target,
                                   reverse=True)

        for (page_title, pred) in sorted_candidates:
            table_content = '''{content}
|-
| [[{page_title}]] || {rating} || {prob:.1f}'''.format(
    content=table_content, page_title=page_title.replace('_', ' '),
    rating=pred.rating, prob=100*pred.p_above_target)

        return('{}{}{}'.format(table_start, table_content, table_end))

def main():
    # Parse CLI options
    import argparse;

    cli_parser = argparse.ArgumentParser(
        description="Program to identify candidates for reassessment in a given category"
        )

    # category name, not an option
    cli_parser.add_argument('category', type=str,
                            help='name of category to predict articles for (without "Category:")')


    # target class, one of: FA, GA, B, C, Start, Stub
    cli_parser.add_argument('target', type=str,
                            help="target class, must match one of ORES' assessment classes")

    cli_parser.add_argument("-v", "--verbose", action="store_true",
                            help="write informational output");

    cli_parser.add_argument("-d", "--distance", type=str, default=2,
                            help="minimum distance (in number of classes) between target and predicted class for an article to be considered a candidate, the default of 2 means a target of Stub must be predicted to be C-class or higher")
    
    args = cli_parser.parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.INFO)

    my_pred = Predictor()
    candidates = my_pred.predict(args.category,
                                 args.target,
                                 distance=args.distance)
    print(my_pred.build_table(candidates, args.target))
    
    return()

if __name__ == '__main__':
    main()
