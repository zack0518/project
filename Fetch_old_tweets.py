import urllib.parse
import urllib.request
from datetime import date
import config
import sys
import logging
_opener_installed = False


def fetch_as_file(city, start, end, filename, limit=None, include_docs=True):
    global _opener_installed
    if not _opener_installed:
        # Prepare for the authentication
        manager = urllib.request.HTTPPasswordMgrWithDefaultRealm()
        manager.add_password(None, config.COUCHDB_URL, config.COUCHDB_USERNAME, config.COUCHDB_PASSWORD)
        handler = urllib.request.HTTPBasicAuthHandler(manager)
        opener = urllib.request.build_opener(handler)
        opener.open(config.COUCHDB_URL)
        urllib.request.install_opener(opener)
        _opener_installed = True

    paras = {
        'start_key': [city, start.year, start.month, start.day],
        'end_key': [city, end.year, end.month, end.day],
        'reduce': 'false',
        'include_docs': 'true' if include_docs else 'false'}
    if limit:
        paras['limit'] = limit

    paras = urllib.parse.urlencode(paras).replace('%27', '%22')
    urllib.request.urlretrieve(config.COUCHDB_URL + '?' + paras, filename)


# curl "http://45.113.232.90/couchdbro/twitter/_design/twitter/_view/summary" \
# -G \
# --data-urlencode 'start_key=["perth",2014,1,1]' \
# --data-urlencode 'end_key=["perth",2014,12,31]' \
# --data-urlencode 'reduce=false' \
# --data-urlencode 'include_docs=true' \
# --user "readonly:ween7ighai9gahR6" \
# -o /tmp/twitter.json

def str_trans(str_date):
    l = str_date.split(',')
    if len(l) == 3:
        date_=date(int(l[0]),int(l[1]),int(l[2]))
    return date_

if __name__ == '__main__':
    if len(sys.argv) >= 3:
        start_date = str_trans(sys.argv[1])
        end_date = str_trans(sys.argv[2])
        city_name=sys.argv[3]
    else:
        print('no enough parameters!')
        sys.exit(0)

    fetch_as_file(city_name, start_date, end_date, city_name + str(start_date) +'_'+str(end_date) +'.json', limit=None)
    logging.info('Done')
