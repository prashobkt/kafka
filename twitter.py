import twitter

import twitter_config

api = twitter.Api(consumer_key=twitter_config.api_key,
                      consumer_secret=twitter_config.api_secret,
                       access_token_key=twitter_config.access_token,
                      access_token_secret=twitter_config.access_secret
                   )
#print(api.VerifyCredentials())
#statuses = api.GetUserTimeline(screen_name="sachin_rt")
statuses = api.GetSearch(term=["war","ukraine"])

# create a kafka producer

for s in statuses:
    print(s.text)
    #send to kafka