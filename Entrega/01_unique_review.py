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

WORDS = {}
class UniqueWords(MRJob):
    def mapper_words(self, _, line):
        for l in get_json_element(line):
            review = re.sub(r'[^\w]', ' ', l['text'])
            for word in review.strip().split():
                WORDS[word.lower()] = [l['review_id'],review]
                yield [word.lower(), 1]

    def reducer(self, key, values):
        yield ['MAX',[sum(values),key]]

    def mapper_unique_word_in_review(self, stat, values):
        if values[0] == 1:
            yield [WORDS[values[1]], 1]

    def reducer2(self, key, values):
        yield ['MOST UNIQUE REVIEW',[sum(values),key]]

    def reduce_uniquest_review(self, key, values):
        yield [key, max(values)]

    def steps(self):
        return [MRStep(mapper=self.mapper_words, reducer=self.reducer),
            MRStep(mapper=self.mapper_unique_word_in_review, reducer=self.reducer2),
            MRStep(reducer=self.reduce_uniquest_review)]

if __name__ == '__main__':
    UniqueWords.run()
