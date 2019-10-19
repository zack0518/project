from tweepy import API
from tweepy import Cursor
from tweepy import OAuthHandler
import logging
import sys
import config
import couchdb
from couchdb import design
import timeFormat
from sentiment_analysis import get_sentiment_scores
from preporcessing import get_area,pre
import socket
import time

# create a view of raw db so that the precessed twitter will not be shown in this view again
# which can prevent one twitter from been processed many times
# 这个view function

city_list=['south_yarra','calton','kew','bundoora','preston','cbd','clayton','doncaster','camberwell','brighton']

def view_unprocessed_raw(db):
    map_fnc = '''function(doc) {
        if (!doc.username) {
            emit(doc._id, null);
        }
    }'''
    view = design.ViewDefinition('original_tweets', 'username_not_used', map_fnc)
    view.sync(db)

# api.friends and api.user_timeline in search API
def get_user_timeline_tweets(db_raw, api,city_name):
    result = db_raw.view('original_tweets/username_not_used')
    for res in result:
        id = res['id']
        tweet = db_raw[id]
        name = tweet['user']['screen_name']
        try:
            for friend in Cursor(api.friends, screen_name=name).items(200):
                friend_id = friend._json['id']
                try:
                    for friend_raw_tweet in Cursor(api.user_timeline, user_id=friend_id).items(200):
                        friend_raw_tweet._json['_id'] = str(friend_raw_tweet._json['id'])
                        friend_raw_tweet._json['username'] = True
                        if city_name in city_list:
                            city_name = 'melbourne'
                        area_name = raw_tweet._json['place']['name']
                        new_dic = {}
                        if raw_tweet._json['lang'] == 'en':
                            time_period = timeFormat.get_period(friend_raw_tweet._json['created_at'])
                            pred_text = pre(raw_tweet._json['text'])
                            sentiment = get_sentiment_scores(pred_text)
                            new_dic = {
                                '_id': friend_raw_tweet._json['_id'],
                                'created_at': friend_raw_tweet._json['created_at'],
                                'text': friend_raw_tweet._json['text'],
                                'user': friend_raw_tweet._json['user'],
                                'geo': friend_raw_tweet._json['geo'],
                                'coordinates': friend_raw_tweet._json['coordinates'],
                                'place': friend_raw_tweet._json['place'],
                                'weekday': time_period[0],
                                'month': time_period[1],
                                'day': time_period[2],
                                'hour': time_period[3],
                                'year': time_period[4],
                                'negative': sentiment['neg'],
                                'positive': sentiment['pos'],
                                'neu': sentiment['neu'],
                                'compound': sentiment['compound']
                            }
                        try:
                            if len(new_dic.keys()) > 0 and area_name.lower() == city_name:
                                db_raw.save(new_dic)
                        except couchdb.http.ResourceConflict:
                            pass
                except:
                    pass
        except:
            pass
        try:
            for raw_tweet in Cursor(api.user_timeline, screen_name=name).items(200):
                # for tweet in tw.Cursor(self.api.search, q=query, lang=lang)
                # '''
                # tweet_temp = {'id': status.id_str, 'user': status._json['user'], 'place': status._json['place'],
                #                     'text': status.text, 'coordinates': status._json['coordinates']}
                # '''
                # '''
                raw_tweet._json['_id'] = str(raw_tweet._json['id'])
                raw_tweet._json['username'] = True
                area_name = raw_tweet._json['place']['name']
                time_period = timeFormat.get_period(tweet['created_at'])
                if city_name in city_list:
                    city_name='melbourne'
                new_dic = {}
                if raw_tweet._json['geo']!='null':
                    get_area(raw_tweet._json['geo']['coordinate'])
                if raw_tweet._json['lang'] == 'en':
                    pred_text = pre(raw_tweet._json['text'])
                    sentiment = get_sentiment_scores(pred_text)
                    new_dic = {
                        '_id': raw_tweet._json['_id'],
                        'created_at': raw_tweet._json['created_at'],
                        'text': raw_tweet._json['text'],
                        'user': raw_tweet._json['user'],
                        'geo': raw_tweet._json['geo'],
                        'coordinates': raw_tweet._json['coordinates'],
                        'place': raw_tweet._json['place'],
                        'weekday': time_period[0],
                        'month': time_period[1],
                        'day': time_period[2],
                        'hour': time_period[3],
                        'year': time_period[4],
                        'negative': sentiment['neg'],
                        'positive': sentiment['pos'],
                        'neu': sentiment['neu'],
                        'compound': sentiment['compound']
                    }
                try:
                    if len(new_dic.keys())>0 and area_name.lower() == city_name:
                        db_raw.save(new_dic)
                except couchdb.http.ResourceConflict:
                    pass
            doc = db_raw.get(id)
            doc['username'] = True
            db_raw.save(doc)
        except:
            pass

if __name__ == '__main__':
    #don't know why need wait for 20s
    logging.info("wait for 20s")
    # time.sleep(20)

    # parameters: city_name+num,
    if len(sys.argv) >= 2:
        city_name = sys.argv[1]
        IPaddress = sys.argv[2]
        auth_index= int(sys.argv[3])
    else:
        print('not enough parameter')
        sys.exit(0)
    # name = socket.gethostname()
    # num=''
    # city_name = 'adelaide'
    # IPaddress = '45.113.234.34'
    # auth_index = 17

    city1=['sydney','adelaide', 'south_yarra', 'calton']
    city2=['melbourne', 'brisbane', 'kew', 'bundoora']
    city3=['perth', 'canberra', 'clayton', 'camberwell']
    city4=['cbd', 'doncaster', 'preston', 'brighton']

    if city_name in city1:
        num = '1'
    if city_name in city2:
        num = '2'
    if city_name in city3:
        num = '3'
    if city_name in city4:
        num = '4'

    consumer_key= config.app_keys_tokens[auth_index]['consumer_key']
    consumer_secret = config.app_keys_tokens[auth_index]['consumer_secret']
    access_token = config.app_keys_tokens[auth_index]['access_token']
    access_secret = config.app_keys_tokens[auth_index]['access_token_secret']

    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    api = API(auth, wait_on_rate_limit=True)

    address = 'http://admin:project@'+ IPaddress +':5984/'
    couch = couchdb.Server(address)

    try:
        db = couch[city_name+num]
        logging.info('successully log into the db '+ city_name+num)
    except Exception:
        logging.error("Raw tweets DB does not exist.")

    while True:
        logging.info("Start get tweet by username")
        view_unprocessed_raw(db)
        get_user_timeline_tweets(db, api,city_name)
        logging.info("No new tweets, wait for 60 seconds")
        time.sleep(60)

