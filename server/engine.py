import os
#from pyspark.mllib.recommendation import ALS
import numpy as np
from numpy import linalg as LA

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_counts_and_averages(ID_and_ratings_tuple):
    """Given a tuple (movieID, ratings_iterable) 
    returns (movieID, (ratings_count, ratings_avg))
    """
    nratings = len(ID_and_ratings_tuple[1])
    return ID_and_ratings_tuple[0], (nratings, float(sum(x for x in ID_and_ratings_tuple[1]))/nratings)

def cosineSimilarity(vec1, vec2):
  return vec1.dot(vec2) / (LA.norm(vec1) * LA.norm(vec2))

class RecommendationEngine:
    """A movie recommendation engine
    """
    def get_recommend_for_movie_id(self,movie_id):
	"""Calculates the nearest 20 movies for the given Movie ID
	"""
	logger.info("Inside Movie Recommend...")
	#movie_id = 20
	complete_itemFactor = np.asarray(self.product_feature_RDD.lookup(movie_id))[0][0][0]
	complete_sims = self.product_feature_RDD.map(lambda products:(products[1][0][1],\
                                        cosineSimilarity(np.asarray(products[1][0][0]), complete_itemFactor),\
                                               products[0],products[1][1][0],products[1][1][1]))
	complete_sortedSims = complete_sims.filter(lambda r: r[3]>=5).takeOrdered(20, key=lambda x: -x[1])
	return complete_sortedSims
		
    def __init__(self, sc, dataset_path):
        """Init the recommendation engine given a Spark context and a dataset path
        """

        logger.info("Starting up the Recommendation Engine: ")

        self.sc = sc

        # Load ratings data for later use
        logger.info("Loading Model Features...")
        features_file_path = os.path.join(dataset_path,'product_40feature_rating')
        self.product_feature_RDD = self.sc.pickleFile(features_file_path)

        #self.movies_titles_RDD = self.movies_RDD.map(lambda x: (int(x[0]),x[1])).cache()
        # Pre-calculate movies ratings counts
        #self.__count_and_average_ratings()

        # Train the model
        self.rank = 8
        self.seed = 5L
        self.iterations = 10
        self.regularization_parameter = 0.1
        #self.__train_model() 
