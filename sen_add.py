import couchdb
import sys
import preporcessing
from sentiment_analysis import get_sentiment_scores

def update_db(db):
    for id in db:
        tweet = db[id]
        if 'compound' not in tweet.keys():
            tweet['text']
            if tweet['lang'] == 'en':
                pred_text = preporcessing.pre(tweet['text'])
                sentiment = get_sentiment_scores(pred_text)
                tweet['negative']= sentiment['neg']
                tweet['positive']= sentiment['pos']
                tweet['neu']= sentiment['neu']
                tweet['compound']= sentiment['compound']
                db.save(tweet)

if __name__=='__main__':
    name=sys.argv[1]
    ip=sys.argv[2]
    print('start')
    address='http://admin:project@'+ip+':5984/'
    couch = couchdb.Server(address)
    db=couch[name]
    update_db(db)
    print('done')

