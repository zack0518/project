import couchdb
import sys
import json
import timeFormat
import sentiment_analysis
import preporcessing
import config

def get_area(li):
	#li[0]=longtitude
	#li[1]=latitude
	name=' '
	for key, value in config.coordinates.items():
		if li[0]<=value[3] and li[0]>=value[1] and li[1]<=value[2] and li[1]>=value[0]:
			name = key
	return name

def get_dbname(name):
    city1 = ['sydney', 'adelaide', 'south_yarra', 'calton']
    city2 = ['melbourne', 'brisbane', 'kew', 'bundoora']
    city3 = ['perth', 'canberra', 'clayton', 'camberwell']
    city4 = ['cbd', 'doncaster', 'preston', 'brighton']
    if name in city1:
        num = '1'
    if name in city2:
        num = '2'
    if name in city3:
        num = '3'
    if name in city4:
        num = '4'
    db_name=name+num
    return db_name

def save_(tweet):
    new_dic={}
    time_period = timeFormat.get_period(tweet['created_at'])
    if tweet['lang'] == 'en':
        pred_text = preporcessing.pre(tweet['text'])
        sentiment = sentiment_analysis.get_sentiment_scores(pred_text)
        new_dic = {
            '_id': tweet['id_str'],
            'created_at': tweet['created_at'],
            'text': tweet['text'],
            'user': tweet['user'],
            'geo': tweet['geo'],
            'coordinates': tweet['coordinates'],
            'place': tweet['place'],
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
    return new_dic

if __name__ == '__main__':
    if len(sys.argv) >= 4:
        city_name = sys.argv[1]
        file_path = sys.argv[2]
        IPaddress = sys.argv[3]
    else:
        print('no enough parameters!')
        sys.exit(0)
    city_name='melbourne'
    IPaddress = '45.113.234.34'
    db_name = city_name
    address = 'http://' + 'admin:project@' + IPaddress + ':' + '5984/'
    couchserver = couchdb.Server(address)

    print('---------- Now saving Tweets ----------')
    with open(file_path, 'r')as f:
    # with open('/Users/jiaqili/Desktop/project/melbourne2015-01-01_2015-01-03.json', 'r')as f:
        line = f.readline()
        while line:
            l = line.strip('\n, ')
            if l.startswith('{') and l.endswith('}'):
                l = json.loads(l)
                tweet = l['doc']
                time_period = timeFormat.get_period(tweet['created_at'])
                if tweet["geo"] != 'null':
                    name = get_area(tweet['geo']['coordinates'])
                    if name in config.coordinates.keys() and name not in config.L_name:
                        db_name = name
                if tweet['lang'] == 'en':
                    pred_text = preporcessing.pre(tweet['text'])
                    sentiment = sentiment_analysis.get_sentiment_scores(pred_text)
                    new_dic = {
                        '_id': tweet['id_str'],
                        'created_at': tweet['created_at'],
                        'text': tweet['text'],
                        'user': tweet['user'],
                        'geo': tweet['geo'],
                        'coordinates': tweet['coordinates'],
                        'place': tweet['place'],
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
                    try:
                        dn = get_dbname(db_name)
                        db = couchserver.create(dn)
                    except:
                        db = couchserver[dn]
                    db.save(new_dic)
                except couchdb.http.ResourceConflict:
                    print('Ignored duplicate tweet')
                    pass
            line = f.readline()


