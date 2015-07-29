"""
Coding challenge solution for Aclima
@author danb

My solution to the given problem is a production ready
deployment of the most powerful map reduce framework
in python.  This solution integrates locally, in hadoop
(and with a bit of tweaking, spark) and even in amazon's
EMR service.  It is extremely easy to schedule as a
cronjob or as a one time run.  With just a trivial amout of
work, this can be used to process any dataset efficiently
and in a distributed manner.

For testing purposes, I have included tests as well in the
/tests directory with a README for instructions on running
them.

from project base, make sure you have a virtualenv set with
requirements.txt pip installed.

run unit tests:
python -m unittest discover

To use a file and use the inline or local runner, do as such
(I have provided a sample file for demonstration)



"""
import string

from mrjob.job import MRJob


class MapReduceException(Exception):
    pass


class TripletFreqCount(MRJob):

    def mapper(self, _, line):
        """
        Generate a key=>count for each tripplet in the line

        In this framework, this mapper acts as a generator for
        the combiner
        """
        # First, tokenize the input on " "
        tokens = ''.join(word.lower() for word in line).split(" ")
        for i in range(len(tokens)):
            try:
                if not any([c in tokens[i] for c in string.punctuation]):
                    # We only want to have no punctuation between the words
                    if len(tokens[i].strip()) == 0 or len(tokens[i+1].strip()) == 0:
                        continue
                    pairing_in_order = [tokens[i].strip(), tokens[i+1].strip()]
                    pairing_in_order.sort()
                    left_word = pairing_in_order[0]
                    right_word = pairing_in_order[1]
                    triplet = self.get_triplet(left_word, right_word)
                    yield (triplet, 1)
            except IndexError:
                # We are at the end, lets move on
                pass

    def get_triplet(self, left_word, right_word):
        return "{} {}".format(left_word, right_word.strip(string.punctuation))

    def combiner(self, triplet, counts):
        """
        The conbiner sums up the counts and generated for the reducer
        """
        yield (triplet, sum(counts))

    def reducer(self, triplet, counts):
        """
        Acts as a generator for the sum of the counts
        """
        total_counts = sum(counts)
        if total_counts > 1:
            # Triplets are defined as appearing more than once
            yield (triplet, total_counts)

if __name__ == '__main__':
    TripletFreqCount.run()
