import ConfigParser
from contextlib import contextmanager
import mysql.connector
from mysql.connector import errorcode
from textwrap import TextWrapper
import time
import tweepy

class StreamWatcherListener(tweepy.StreamListener):
    status_wrapper = TextWrapper(width=60, initial_indent='    ',
                                 subsequent_indent='    ')
    def on_status(self, status):
        try:
            print self.status_wrapper.fill(status.text)
            print '\n %s  %s  via %s\n' % (status.author.screen_name,
                                           status.created_at, status.source)
        except:
            pass
    def on_error(self, status_code):
        print 'An error has occured! Status code = %s' % status_code
        return True
    def on_timeout(self):
        print 'A timeout has occured.'

def check_settings(appName, configParser):
    if appName == 'twitter':
        keys = [
            'username',
            'consumer_key',
            'consumer_secret',
            'access_token',
            'access_token_secret'
            ]
    elif appName == 'mysql':
        keys = [
            'user',
            'password',
            'host',
            'db_name'
            ]
    else:
        raise ValueError("appName not in list of valid apps")

    for key in keys:
        value = configParser.get(appName, key)
        if not value:
            log(at='validate_env', status='missing', var=key)
            raise ValueError("Missing ENV var: {0}".format(key))
        
    log(at='check_settings', status='ok')    

def log(**kwargs):
    if 'isDebug' in kwargs:
        toDebug = kwargs.pop('isDebug')
        if toDebug:
            print ' '.join( "{0}={1}".format(k,v) for k,v in sorted(kwargs.items()) )
    else:
        print ' '.join( "{0}={1}".format(k,v) for k,v in sorted(kwargs.items()) )

@contextmanager
def measure(**kwargs):
    start = time.time()
    status = {'status': 'starting'}
    log(**dict(kwargs.items() + status.items()))
    try:
        yield
    except Exception, e:
        status = {'status': 'err', 'exception': "'{0}'".format(e)}
        log(**dict(kwargs.items() + status.items()))
        raise
    else:
        status = {'status': 'ok', 'duration': time.time() - start}
        log(**dict(kwargs.items() + status.items()))

def debug_print(text):
    print text

def create_database(cnx, cursor, db_name):
    try:
        cnx.database = db_name 
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            try:
                cursor.execute(
                    "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8mb4'".format(db_name))
            except mysql.connector.Error as err:
                print("Failed creating database: {}".format(err))
                exit(1)
            cnx.database = db_name
        else:
            print(err)
            exit(1)

def create_tables(cnx, cursor, tables):
    for name, ddl in tables.iteritems():
        try:
            print "Creating table {}: ".format(name)
            cursor.execute(ddl)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("     already exists.")
            else:
                print("     " + str(err.msg))
        else:
            print("OK")
            
def main():
    log(at='main')
    main_start = time.time()

    configParser = ConfigParser.RawConfigParser()   
    configParser.read('settings.cfg')

    check_settings('twitter', configParser)
    check_settings('mysql', configParser)
    
    username = configParser.get('twitter', 'username')
    consumer_key = configParser.get('twitter', 'consumer_key')
    consumer_secret = configParser.get('twitter', 'consumer_secret')
    access_key = configParser.get('twitter', 'access_token')
    access_secret = configParser.get('twitter', 'access_token_secret')
    isDebug = configParser.getboolean('twitter', 'debug')

    db_user = configParser.get('mysql', 'user')
    db_pass = configParser.get('mysql', 'password')
    db_host = configParser.get('mysql', 'host')
    db_name = configParser.get('mysql', 'db_name')

    auth = tweepy.OAuthHandler(consumer_key=consumer_key,
        consumer_secret=consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tweepy.API(auth_handler=auth, retry_count=3)
    stream = tweepy.Stream(auth, StreamWatcherListener(), timeout=None)

    cnx = mysql.connector.connect(user=db_user, password=db_pass, host=db_host)
    cursor = cnx.cursor()

    with measure(at='create_database', isDebug=isDebug):
        create_database(cnx, cursor, db_name)

    tables = {}
    tables['python_tweets'] = (
        "CREATE TABLE `python_tweets` ("
        "  `created_at` datetime NOT NULL,"
        "  `text` nvarchar(200) NOT NULL"
        ") ENGINE=InnoDB")
    tables['twitter_hashtags'] = (
        "CREATE TABLE `twitter_hashtags` ("
        "  `tweet_id` bigint NOT NULL,"
        "  `hashtag1` nvarchar(200) NOT NULL,"
        "  `hashtag2` nvarchar(200) NOT NULL"
        ") ENGINE=InnoDB")

    with measure(at='create_tables', isDebug=isDebug):    
        create_tables(cnx, cursor, tables)

    add_tweet = ("INSERT INTO twitter_hashtags "
               "(tweet_id, hashtag1, hashtag2) "
               "VALUES (%s, %s, %s)")

    '''
    query = '#news'
    max_tweets = 50

    searched_tweets = []
    last_id = -1
    while len(searched_tweets) < max_tweets:
        count = max_tweets - len(searched_tweets)
        try:
            new_tweets = api.search(q=query, count=count, max_id=str(last_id - 1))
            if not new_tweets:
                break
            searched_tweets.extend(new_tweets)
            last_id = new_tweets[-1].id
        except tweepy.TweepError as e:
            break

    for tweet in searched_tweets:
        #print tweet.created_at, tweet.text
        #print tweet.place
        if tweet.entities['hashtags']:
            hashtags = []
            for i in range(0, len(tweet.entities['hashtags'])):
                theHashtag = tweet.entities['hashtags'][i]['text']
                otherHashtags = []
                for j in range(0, len(tweet.entities['hashtags'])):
                    if j != i:
                        otherHashtags.append(tweet.entities['hashtags'][j]['text'])
                for otherTag in otherHashtags:
                    data_tweet = (tweet.id, theHashtag, otherTag)
                    cursor.execute(add_tweet, data_tweet)
    '''

    stream.filter(None, ['#'])
    stream.disconnect()
    cnx.commit()
    cursor.close()
    cnx.close()

    log(at='finish', status='ok', duration=time.time() - main_start)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print '\nGoodbye!'
