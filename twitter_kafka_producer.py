__author__ = 'pojha'

__author__ = 'pojha'

import oauth2 as oauth
import urllib2 as urllib
from kafka import KafkaProducer , KafkaClient, SimpleProducer
import json
import tweepy
import sys


api_key = "ULI6uCZ07LS2MMGW7DVhfP9Kh"
api_secret = "JWVLqxIqDzAMDICq7ni5WYm2sm06kNUqI465YE1Y6vhJPuJmOV"
access_token_key = "535814971-cHEHHJz01svTweT1KvF6HuGGtnL45O4WUVXfxgnO"
access_token_secret = "5FOQ8Q38p3C8Qy0CzGwJU73V5BFVVpkqmwTchVxbb3ORa"



_debug = 0


auth = tweepy.OAuthHandler(api_key, api_secret)
auth.set_access_token(access_token_key, access_token_secret)
api = tweepy.API(auth)

oauth_token    = oauth.Token(key=access_token_key, secret=access_token_secret)
oauth_consumer = oauth.Consumer(key=api_key, secret=api_secret)

signature_method_hmac_sha1 = oauth.SignatureMethod_HMAC_SHA1()

http_method = "GET"


http_handler  = urllib.HTTPHandler(debuglevel=_debug)
https_handler = urllib.HTTPSHandler(debuglevel=_debug)

'''
Construct, sign, and open a twitter request
using the hard-coded credentials above.
'''
def twitterreq(url, method, parameters):
  req = oauth.Request.from_consumer_and_token(oauth_consumer,
                                             token=oauth_token,
                                             http_method=http_method,
                                             http_url=url,
                                             parameters=parameters)

  req.sign_request(signature_method_hmac_sha1, oauth_consumer, oauth_token)

  headers = req.to_header()

  if http_method == "POST":
    encoded_post_data = req.to_postdata()
  else:
    encoded_post_data = None
    url = req.to_url()

  opener = urllib.OpenerDirector()
  opener.add_handler(http_handler)
  opener.add_handler(https_handler)

  response = opener.open(url, encoded_post_data)

  return response


class CustomStreamListener(tweepy.StreamListener):
    def on_status(self, status):
        print status.text

    def on_error(self, status_code):
        print >> sys.stderr, 'Encountered error with status code:', status_code
        return True # Don't kill the stream

    def on_timeout(self):
        print >> sys.stderr, 'Timeout...'
        return True # Don't kill the stream

    def on_data(self, data):
        print data
        dataJsonized = json.loads(data)
        producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'))
        producer.send('twitter',dataJsonized)
        return True


if __name__ == '__main__':
  sapi = tweepy.streaming.Stream(auth, CustomStreamListener())
  sapi.filter(track=['restaurant'])

