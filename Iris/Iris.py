# import libraries
import pandas as pd
from pandas.plotting import scatter_matrix
import matplotlib.pyplot as plt
from numpy import array
from sklearn import model_selection
from sklearn.metrics import classification_report
from sklearn.metrics import confusion_matrix
from sklearn.metrics import accuracy_score
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.naive_bayes import GaussianNB
from sklearn.svm import SVC

# initialize the names
names = ['sepal-length', 'sepal-width', 'petal-length', 'petal-width', 'class']
# Read the data set
iris = pd.read_csv("E:\\iris\\iris.data", names=names)
# Box and Whisker Plot
# iris.plot(kind='box', subplots=True, layout=(2, 2), sharex=False, sharey=False)
# iris.hist()
# scatter_matrix(iris)
# plt.show()
# modelling starts here
npArray = iris.values
print(npArray)
