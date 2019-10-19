import json, nltk
# import pymysql
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
import pyLDAvis
import pyLDAvis.sklearn
import sys
import couchdb
# f = open('Arncliffe-sydney-3-2016.txt', 'r')
import preporcessing


def get_topic():
    # print(args)
    # conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='toor', db='twitter', charset='utf8')
    # cursor = conn.cursor()
    # cursor.execute("set names utf8")
    # address = 'http://admin:project@' + ip + ':5984/'
    # couch = couchdb.Server(address)
    # db = couch[name]

    # try:
        print('start tf_vectorizer')
        n_features = 500
        tf_vectorizer = CountVectorizer(max_features=n_features,
                                        stop_words='english')
#        sql = """select t.* from (select processed_text, sa2_code as suburb from original_adelaide_coordinate where lang = 'en' and weekday = """ + args[0] + """ and created_at between '2014-01-01 00:00:00' and '2014-12-31 23:59:59' and sa2_code = '402041047') as t order by suburb"""
#         if args[0] != '-1':
#             sql = """select t.* from (select processed_text, sa2_code as suburb from original_""" + args[6] + """_coordinate where lang = 'en' and weekday = """ + args[0] + """ and created_at between '""" + args[1] + """ """ + args[2] + """' and '""" + args[3] + """ """ + args[4] + """' and sa2_code = '""" + args[5] + """') as t order by suburb"""
#         else:
#             sql = """select t.* from (select processed_text, sa2_code as suburb from original_""" + args[6] + """_coordinate where lang = 'en' and created_at between '""" + args[1] + """ """ + args[2] + """' and '""" + args[3] + """ """ + args[4] + """' and sa2_code = '""" + args[5] + """') as t order by suburb"""
#         cursor.execute(sql)
#         result = cursor.fetchall()
#         text = []
#         for item in result:
#             text.append(item[0])
#         try:
#             cursor.execute(sql)
#             conn.commit()
#         except:
#             conn.rollback()
        text=[]
        print('start getting texts')
        with open("/Users/jiaqili/Desktop/project/melbourne2015-01-01_2015-01-03.json", 'r')as f:
            line = f.readline()
            while line:
                line = line.strip('\n, ')
                if line.startswith('{') and line.endswith('}'):
                    line = json.loads(line)
                    tweet = line['doc']
                    # pred_text=tweet['text'].split()

                    pred_text = preporcessing.pre(tweet['text'])

                    text.append(pred_text)
            # if len(text)<1000:
                line=f.readline()
        print(len(text))
        print('start fit_transform')
        # a=text[0:1000]
        # print(a)
        tf = tf_vectorizer.fit_transform(text)
        print(type(tf))
        # tf = tf_vectorizer.fit_transform([f.read()])

        # n_topics = int(args[7])
        n_topics=10
        print(n_topics)
        print('start lda')
        lda = LatentDirichletAllocation(n_components=n_topics, max_iter=50,
                                        # learning_method='online',
                                        learning_method='batch',
                                        learning_offset=50.,
                                        random_state=0)
        print(type(lda))
        print('something')
        lda.fit(tf)
        print('something')

        def print_top_words(model, feature_names, n_top_words):
            temp = {}
            for topic_idx, topic in enumerate(model.components_):
                key = "Topic %d:" % topic_idx
                value = " ".join([feature_names[i] for i in topic.argsort()[:-n_top_words - 1:-1]])
                temp[key] = value
            return print(temp)
        n_top_words = 10
        print(n_top_words)
        tf_feature_names = tf_vectorizer.get_feature_names()
        print(tf_feature_names)
#        data = pyLDAvis.sklearn.prepare(lda, tf, tf_vectorizer)
#        pyLDAvis.show(data)
        return print_top_words(lda, tf_feature_names, n_top_words)
    # f.close()
    # except:
    #     print('Please try another place')

if __name__=='__main__':
    # name=sys.argv[1]
    # ip=sys.argv[2]
    # print('start')
    # ip='45.113.234.34'
    # name='south_yarra'
    # address='http://admin:project@'+ip+':5984/'
    # couch = couchdb.Server(address)
    # db=couch[name]
    # # update_db(db)
    # get_topic(db)
    print('start')
    get_topic()
    print('done')

