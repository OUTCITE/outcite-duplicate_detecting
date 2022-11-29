import sys
from tabulate import tabulate
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.linear_model import PassiveAggressiveClassifier
from sklearn.linear_model import Perceptron
from sklearn.linear_model import RidgeClassifier
from sklearn.linear_model import SGDClassifier
from sklearn.linear_model import LinearRegression
from sklearn.svm import LinearSVC


_datafile = sys.argv[1];


def logisticCombination(coef,intercept,X):
    linearCombination = X.dot(coef.T)+intercept;
    return 1/(1 + np.exp(-linearCombination));


IN = open(_datafile);
X, y, sims = [],[],[];
for line in IN:
    line_ = line.rstrip().split(' ');
    if len(line_) != 12:
        print(len(line_));
    X.append([float(val) for val in line_[1:-1]]);
    y.append(int(line_[-1]));
    sims.append(float(line_[0]));
IN.close();

X    = np.array(X);
y    = np.array(y);
sims = np.array(sims)[:,None];

print(X.shape,y.shape);

classifiers = { 'Logistic Regression':  LogisticRegression(),
                #'Linear SVM':           LinearSVC(),
                #'Passive Aggressive':   PassiveAggressiveClassifier(),
                #'Perceptron':           Perceptron(),
                #'Ridge Classifier':     RidgeClassifier(),
                #'SGD Classifier':       SGDClassifier(),
                #'Linear Regression':    LinearRegression(),
                #'Linear SVC':           LinearSVC()
              };

table = [];

for classifier in classifiers:
    clf = classifiers[classifier];
    clf.fit(X,y);
    table.append([classifier,'','']+[round(val*100,1) for val in np.ravel(clf.coef_/clf.coef_.sum())]);
    table.append([classifier,clf.score(X,y),clf.intercept_[0]]+[val for val in np.ravel(clf.coef_)]);
    combination = logisticCombination(clf.coef_,clf.intercept_,X);
    prediction  = np.ravel(combination > 0.5);
    print('Formula and classifier coming to same conclusions:', (clf.predict(X) == prediction).sum()==X.shape[0]);
    print('Similarity from tuning and from classifier coming to same probabilities:',(np.abs(sims-combination)<0.0001).sum()==X.shape[0]);
    print(clf.coef_);

print(tabulate(table,headers=['classifier','score','offset']+['refstring','matchID','pubnumber','source','title','surname','init','first','editor','publisher']));

