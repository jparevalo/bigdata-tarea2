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

class SimilarUsers(MRJob):
    def mapper_user_business(self, _, line):
        for l in get_json_element(line):
            yield l['user_id'], l['business_id']

    def reducer_businessess_by_user(self, user_id, business_id):
        yield user_id, list(business_id)

    def mapper_business_reviewed_by_user(self, user_id, business_id_list):
        yield 'businessess_reviewed_by_user', [user_id, business_id_list]

    def reducer_jaccard_similarity(self, _, users_with_businessess):
        for combination in itertools.combinations(users_with_businessess, 2):
            first_user, second_user = combination
            first_user_id, first_users_businessess = first_user
            second_user_id, second_user_businessess = second_user
            same_businessess = (set(first_users_businessess)
                                & set(second_user_businessess))
                                # Set removes duplicates
            intersection = len(same_businessess)
            all_businessess = (set(first_users_businessess
                               + second_user_businessess))
                               # Set removes duplicates
            union = len(all_businessess)
            jaccard_coef = float(intersection)/float(union)
            if jaccard_coef > 0.5:
                yield [first_user_id, second_user_id], jaccard_coef

    def steps(self):
        return [MRStep(mapper=self.mapper_user_business, reducer=self.reducer_businessess_by_user),
                MRStep(mapper=self.mapper_business_reviewed_by_user, reducer=self.reducer_jaccard_similarity)]

if __name__ == '__main__':
    SimilarUsers.run()
