from Modules.methods import *
from Modules.config import *
import pickle

import functools

import pandas as pd
from sklearn.base import BaseEstimator
from sklearn.base import TransformerMixin


class BayesianTargetEncoder(BaseEstimator, TransformerMixin):

    """
    Reference: https://www.wikiwand.com/en/Bayes_estimator#/Practical_example_of_Bayes_estimators
    Args:
        columns (list of strs): Columns to encode.
        weighting (int or dict): Value(s) used to weight each prior.
        suffix (str): Suffix used for naming the newly created variables.
    """

    def __init__(self, columns=None, prior_weight=100, suffix='_mean'):
        self.columns = columns
        self.prior_weight = prior_weight
        self.suffix = suffix
        self.prior_ = None
        self.posteriors_ = None

    def fit(self, X, y=None, **fit_params):

        if not isinstance(X, pd.DataFrame):
            raise ValueError('X has to be a pandas.DataFrame')

        if not isinstance(y, pd.Series):
            raise ValueError('y has to be a pandas.Series')

        X = X.copy()

        # Default to using all the categorical columns
        columns = (
            X.select_dtypes(['object', 'category']).columns
            if self.columns is None else
            self.columns
        )

        names = []
        for cols in columns:
            if isinstance(cols, list):
                name = '_'.join(cols)
                names.append('_'.join(cols))
                X[name] = functools.reduce(
                    lambda a, b: a.astype(str) + '_' + b.astype(str),
                    [X[col] for col in cols]
                )
            else:
                names.append(cols)

        # Compute prior and posterior probabilities for each feature
        X = pd.concat((X[names], y.rename('y')), axis='columns')
        self.prior_ = y.mean()
        self.posteriors_ = {}

        for name in names:
            agg = X.groupby(name)['y'].agg(['count', 'mean'])
            counts = agg['count']
            means = agg['mean']
            pw = self.prior_weight
            self.posteriors_[name] = ((pw * self.prior_ + counts * means) / (pw + counts)).to_dict()

        return self

    def transform(self, X, y=None):

        if not isinstance(X, pd.DataFrame):
            raise ValueError('X has to be a pandas.DataFrame')

        for cols in self.columns:

            if isinstance(cols, list):
                name = '_'.join(cols)
                x = functools.reduce(
                    lambda a, b: a.astype(str) + '_' + b.astype(str),
                    [X[col] for col in cols]
                )
            else:
                name = cols
                x = X[name]

            posteriors = self.posteriors_[name]
            X[name + self.suffix] = x.map(posteriors).fillna(self.prior_).astype(float)

        return X


def apply_model_cust_new_b2b(data):
	data['province'] = data['province_x']
	data['volume_sig'] = data['demand']
	data['last_cbp_sig'] = data['cbp_sig']
	data['last_cbp_nbc'] = data['cbp_nbc']
	data['province_name'] = data['province']
	encoder= pickle.load(open("./Modules/data/encoder_model_new_customer_b2b.pkl", 'rb'))
	data_encoded = encoder.transform(data)
	var_x = [
		'district_encoded',
		'segmentsi_encoded',
		'plant_to_distance_sig',
		'province_encoded',
		'material_type_encoded',
		'last_cbp_sig',
		'last_cbp_nbc',
		'cluster_encoded',
		'volume_sig',
		'plant_to_distance_sig_nbc',
		'packaging_mode_encoded',
		'term_of_payment'
	]
	file = open("./Modules/data/model_b2b_new_cust.pkl",'rb')
	cbp_model = pickle.load(file)
	file.close()
	prediction_cbp = cbp_model.predict(data_encoded[var_x])
	return prediction_cbp