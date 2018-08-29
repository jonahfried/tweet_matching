import pickle
import json
import sys
from tqdm import tqdm


if __name__ == "__main__":
    tweets_by_person = {}
    file = open("tweets_for_jonah.txt")
    TWEET_CAP = 400000
    counter = TWEET_CAP
    for counter, line in enumerate(file):
        if counter < TWEET_CAP*(int(sys.argv[1])-1):
            continue
        if counter > TWEET_CAP*(int(sys.argv[1])):
            break
        tweet = json.loads(line)
        
        tweet.pop("urls", None)
        tweet.pop("created_at", None)
        tweet.pop("source", None)
        tweet.pop("hashtags", None)
        tweet["user"].pop("location", None)
        tweet["user"].pop("lang", None)
        tweet["user"].pop("favourites_count", None)
        tweet["user"].pop("friends_count", None)
        tweet["user"].pop("followers_count", None)
        tweet["user"].pop("geo_enabled", None)
        tweet["user"].pop("listed_count", None)
        tweet["user"].pop("profile_background_color", None)
        tweet["user"].pop("statuses_count", None)
        tweet["user"].pop("url", None)
        tweet["user"].pop("profile_background_image_url", None)
        tweet["user"].pop("profile_background_tile", None)
        tweet["user"].pop("profile_image_url", None)
        tweet["user"].pop("profile_link_color", None)
        tweet["user"].pop("profile_sidebar_fill_color", None)
        tweet["user"].pop("profile_text_color", None)
        tweet["user"].pop("screen_name", None)
        tweet["user"].pop("description", None)
        tweet.pop("user_mentions", None)
        
        
        if tweet["user"]["name"] in tweets_by_person:
            tweets_by_person[tweet["user"]["name"]][tweet["id_str"]] = tweet
        else:
            tweets_by_person[tweet["user"]["name"]] = {}
            tweets_by_person[tweet["user"]["name"]][tweet["id_str"]] = tweet                                        
            
    file.close()

    with open("./by_person/tweets_by_person"+sys.argv[1]+".pkl", "wb") as writer:
        pickle.dump(tweets_by_person, writer)