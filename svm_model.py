from pyspark import SparkContext
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.regression import LabeledPoint
from sklearn.svm import SVC
from sklearn.feature_extraction.text import CountVectorizer
from nltk.tokenize import RegexpTokenizer
import re
from string import punctuation
import joblib

# Initialize Spark context
sc = SparkContext(appName="SentimentAnalysis")

# Load the dataset
data = sc.textFile("Twitter_Data.csv")
header = data.first()
data = data.filter(lambda x: x != header).map(lambda x: x.split(","))
header_split = header.split(",")
clean_text_index = header_split.index("clean_text")
sentiment_index = header_split.index("sentiment")

# Data preprocessing
def preProcessor(text):
    text = re.sub(r'(http|ftp|https):\/\/([\w\-]+(?:(?:\.[\w\-]+)+))([\w\-\.,@?^=%&:/\+#]*[\w\-\@?^=%&/\+#])?', ' ', text)
    text = re.sub(r'[' + punctuation + ']', ' ', text)
    text = re.sub(r'#(\w + )', ' ', text)
    text = re.sub(r'@(\w + )', ' ', text)
    return text

preprocessed_data = data.map(lambda x: (preProcessor(x[clean_text_index]), x[sentiment_index]))

# Tokenization and feature extraction
tokenizer = RegexpTokenizer(r'\w+')
tokenized_data = preprocessed_data.map(lambda x: (x[0], x[1], tokenizer.tokenize(x[0])))

# Convert to LabeledPoint
labeled_data = tokenized_data.map(lambda x: (1.0 if x[1] == "positive" else 0.0 if x[1] == "neutral" else -1.0, x[2]))

# Combine all tokens for CountVectorizer
all_tokens = labeled_data.flatMap(lambda x: x[1])

# Fit CountVectorizer on all tokens
vectorizer = CountVectorizer()
vectorizer.fit(all_tokens.collect())

# Transform tokenized_data to feature vectors
tf_data = labeled_data.map(lambda x: (x[0], vectorizer.transform(x[1]).toarray().flatten()))

# Convert to LabeledPoint
labeled_points = tf_data.map(lambda x: LabeledPoint(x[0], x[1]))

# Train SVM model
model = SVC()
X_train = labeled_points.map(lambda x: x.features).collect()
y_train = labeled_points.map(lambda x: x.label).collect()
model.fit(X_train, y_train)

# Save the model
joblib.dump(model, "svm_model_and_cv.pkl")

# Stop Spark context
sc.stop()