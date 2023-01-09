
from itertools import product
import json
import os

import numpy as np
import sparse
from sklearn.naive_bayes import MultinomialNB
import joblib
import jellyfish


def preprocess_symbol_tokens(symbol_dict):
    description = symbol_dict['description'].lower()
    symbol = symbol_dict['symbol'].lower()

    tokens = description.split(' ') + symbol.split(' ')
    return tokens


class SymbolInfoFeatureEngineer:
    def __init__(self, gamma=0.75):
        self.gamma = gamma
        self.symbols_weights_info = {}

    def ingest_one_symbol_info(self, symbol_dict):
        symbol = symbol_dict['symbol']
        token_weights = {}
        for token in preprocess_symbol_tokens(symbol_dict):
            token_weights[token] = 1.
        self.symbols_weights_info[symbol] = token_weights

    def _produce_feature_indices(self):
        feature_set = set()
        for token_weights in self.symbols_weights_info.values():
            for feature in token_weights.keys():
                feature_set.add(feature)
        self.feature2idx = {
            feature: idx
            for idx, feature in enumerate(sorted(feature_set))
        }
        self.idx2feature = {idx: feature for feature, idx in self.feature2idx.items()}

    def _construct_training_data(self):
        X = sparse.DOK((len(self.symbols_weights_info), len(self.feature2idx)))
        Y = []
        for i, (symbol, weights_info) in enumerate(self.symbols_weights_info.items()):
            Y.append(symbol)
            for feature, weight in weights_info.items():
                X[i, self.feature2idx[feature]] = weight
        X = X.to_coo().to_scipy_sparse()
        return X, Y

    def compute_training_data(self):
        self._produce_feature_indices()
        return self._construct_training_data()

    def convert_inputstring_to_X(self, string, max_edit_distance_considered=1):
        tokens = string.lower().split(' ')
        nbfeatures = len(self.feature2idx)
        inputX = np.zeros((1, nbfeatures))
        for token in tokens:
            if token in self.feature2idx.keys():
                inputX[0, self.feature2idx[token]] = 1.
        if max_edit_distance_considered > 0:
            for token, feature in product(tokens, self.feature2idx.keys()):
                if token == feature:
                    continue
                edit_distance = jellyfish.damerau_levenshtein_distance(token, feature)
                if edit_distance <= max_edit_distance_considered:
                    inputX[0, self.feature2idx[feature]] = pow(self.gamma, edit_distance)
        return inputX

    def save_model(self, directory):
        json.dump(self.feature2idx, open(os.path.join(directory, 'feature2idx.json'), 'w'))
        json.dump(self.symbols_weights_info, open(os.path.join(directory, 'symbols_weight_info.json'), 'w'))
        json.dump({'gamma': self.gamma}, open(os.path.join(directory, 'feature_engineer_hyperparameters.json'), 'w'))

    @classmethod
    def load_model(cls, directory):
        feature_engineer_hyperparameters = json.load(
            open(os.path.join(directory, 'feature_engineer_hyperparameters.json'), 'r')
        )
        feature_engineer = SymbolInfoFeatureEngineer(**feature_engineer_hyperparameters)
        feature_engineer.symbols_weights_info = json.load(open(os.path.join(directory, 'symbols_weight_info.json'), 'r'))
        feature_engineer.feature2idx = json.load(open(os.path.join(directory, 'feature2idx.json'), 'r'))
        feature_engineer.idx2feature = {idx: feature for feature, idx in feature_engineer.feature2idx.items()}
        return feature_engineer


class SymbolMultinomialNaiveBayesExtractor:
    def __init__(self, feature_engineer, alpha=1.):
        self.feature_engineer = feature_engineer
        self.alpha = alpha
        self.symbols_weights_info = self.feature_engineer.symbols_weights_info

    def ingest_one_symbol_info(self, symbol_dict):
        self.feature_engineer.ingest_one_symbol_info(symbol_dict)

    def train(self):
        X, Y = self.feature_engineer.compute_training_data()
        self.classifier = MultinomialNB()
        self.classifier.fit(X, Y)

    @property
    def symbols(self):
        try:
            return list(self.classifier.classes_)
        except Exception:
            raise ValueError('Classifier not trained yet!')

    def convert_string_to_X(self, string, max_edit_distance_considered=1):
        return self.feature_engineer.convert_inputstring_to_X(string, max_edit_distance_considered=max_edit_distance_considered)

    def predict_proba(self, string, max_edit_distance_considered=1):
        inputX = self.convert_string_to_X(string, max_edit_distance_considered=max_edit_distance_considered)
        proba = self.classifier.predict_proba(inputX)
        return {
            symbol: prob
            for symbol, prob in zip(self.symbols, proba[0, :])
        }

    def save_model(self, directory):
        self.feature_engineer.save_model(directory)
        json.dump({'alpha': self.alpha}, open(os.path.join(directory, 'hyperparameters.json'), 'w'))
        json.dump(self.symbols, open(os.path.join(directory, 'symbols.json'), 'w'))
        joblib.dump(self.classifier, os.path.join(directory, 'multinomialnb.joblib'))

    @classmethod
    def load_model(cls, directory, feature_engineer=None):
        if feature_engineer is None:
            feature_engineer = SymbolInfoFeatureEngineer.load_model(directory)
        hyperparameters = json.load(open(os.path.join(directory, 'hyperparameters.json'), 'r'))
        mclf = cls(feature_engineer, **hyperparameters)
        mclf.classifier = joblib.load(os.path.join(directory, 'multinomialnb.joblib'))
        return mclf
