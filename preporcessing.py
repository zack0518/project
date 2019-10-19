from nltk.tokenize import word_tokenize
import re
from config import coordinates
import nltk
nltk.download('punkt')

pattern1 = re.compile(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
pattern2 = re.compile('@[a-zA-Z0-9__]+')
pattern3 = re.compile('[#@]+')
#continuely punctuation
pattern4 =re.compile('[\(\)*^$\,\!\?\.=:/\&\'\;]+')

#preporcess
#the punctuations, hashtags, at, website，if is retweet, then conbine the retweent with tweet.
def pre(s):
    url = re.findall(pattern1, s)
    at = re.findall(pattern2, s)

    s=s.split()
    st = " ".join(w for w in s if w not in url and w not in at)
    s = word_tokenize(st)
    st=' '.join(w for w in s)
    puns = re.findall(pattern4, st)
    hashtag = re.findall(pattern3, st)
    st=' '.join(w.lower() for w in s if w not in puns and w not in hashtag)
    return st

def get_area(li):
	#li[0]=longtitude
	#li[1]=latitude
	name=' '
	for key, value in coordinates.items():
		if li[0]<=value[3] and li[0]>=value[1] and li[1]<=value[2] and li[1]>=value[0]:
			name = key
	return name

# lemmatizer = nltk.stem.wordnet.WordNetLemmatizer()
# def lemmatize(word):
#     lemma = lemmatizer.lemmatize(word,'v')
#     if lemma == word:
#         lemma = lemmatizer.lemmatize(word,'n')
#     elif lemma==word:
#         lemma=lemmatizer.lemmatize(word,'adj')
#     return lemma