from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import JSONValueProtocol
import json
import re
import itertools

dataset_path = '../dataset/'

def get_json_element(line):
    ''' Had to create this function because my shrinked JSON were dumped as a list,
    not as various JSON Objects as in the original files, so this function
    handles both cases (multiple JSON objects and a LIST of JSON Objects) '''
    line = json.loads(line)
    if isinstance(line, list):
        for real_json_object in line:
            yield real_json_object
    else:
        yield line

BUSINESS_CATEGORIES = {}
MAX_REVIEWS_BY_CAT = {}
class TopReviewerByCategory(MRJob):
    def mapper_user_reviews_by_cat(self, _, line):
        for l in get_json_element(line):
            if 'categories' not in l:
                if l['business_id'] in BUSINESS_CATEGORIES:
                    categories = BUSINESS_CATEGORIES[l['business_id']]
                    for category in categories:
                        if category not in MAX_REVIEWS_BY_CAT:
                            MAX_REVIEWS_BY_CAT[category] = [0,'none']
                        yield [l['user_id']+';'+category, 1]
            else:
                BUSINESS_CATEGORIES[l['business_id']] = l['categories']
        yield ['end',-1]

    def reducer(self, key, values):
        yield ['MAX',tuple([sum(values),key])]

    def reduce_max_reviews_by_category(self, stat, values):
        if values[1] != 'end':
            total_reviews = values[0]
            user_id, category = values[1].split(';')
            if total_reviews > MAX_REVIEWS_BY_CAT[category][0]:
                MAX_REVIEWS_BY_CAT[category] = [values[0],user_id]
        else:
            for k, v in MAX_REVIEWS_BY_CAT.items():
                yield ['MORE REVIEWS BY CATEGORY', [k, v[0], v[1]]]

    def steps(self):
        return [MRStep(mapper=self.mapper_user_reviews_by_cat, reducer=self.reducer),
            MRStep(mapper=self.reduce_max_reviews_by_category)]

if __name__ == '__main__':
    TopReviewerByCategory.run()
