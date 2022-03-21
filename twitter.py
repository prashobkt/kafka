import twitter

import settings

api = twitter.Api(consumer_key=settings.api_key,
                      consumer_secret=settings.api_secret,
                       access_token_key=settings.access_token,
                      access_token_secret=settings.access_secret
                   )
#print(api.VerifyCredentials())
#statuses = api.GetUserTimeline(screen_name="sachin_rt")
statuses = api.GetSearch(term=["war","ukraine"])
for s in statuses:
    print(s.text)