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

REVIEW_STARS = {}
class SimilarUsers(MRJob):
    def mapper_user_business(self, _, line):
        for l in get_json_element(line):
            normalized_stars = float(l['stars'])/5
            REVIEW_STARS[l['user_id']+';'+l['business_id']] = normalized_stars
            yield l['user_id'], l['business_id']

    def reducer_businessess_by_user(self, user_id, business_id):
        yield user_id, list(business_id)

    def mapper_business_reviewed_by_user(self, user_id, business_id_list):
        yield 'businessess_reviewed_by_user', [user_id, business_id_list]

    def reducer_star_similarity(self, _, users_with_businessess):
        for combination in itertools.combinations(users_with_businessess, 2):
            first_user, second_user = combination
            first_user_id, first_users_businessess = first_user
            second_user_id, second_user_businessess = second_user
            same_businessess = (set(first_users_businessess)
                                & set(second_user_businessess))
                                # Set removes duplicates
            numerator = 0
            denominator_a = 0
            denominator_b = 0
            for business in same_businessess:
                numerator += REVIEW_STARS[first_user_id + ';' + business] * REVIEW_STARS[second_user_id + ';' + business]
                denominator_a += REVIEW_STARS[first_user_id + ';' + business]**2
                denominator_b += REVIEW_STARS[second_user_id + ';' + business]**2
            star_coef = float(numerator) / (denominator_a**(1/2) * denominator_b**(1/2))
            if star_coef > 0.5:
                yield [first_user_id, second_user_id], star_coef

    def steps(self):
        return [MRStep(mapper=self.mapper_user_business, reducer=self.reducer_businessess_by_user),
                MRStep(mapper=self.mapper_business_reviewed_by_user, reducer=self.reducer_star_similarity)]

if __name__ == '__main__':
    SimilarUsers.run()
