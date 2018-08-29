import pickle
from tqdm import tqdm
import sys

dummy = {}

for i in range(1, 3):
    print("loading file #"+str(i))
    with open("./by_person/tweets_by_person%d.pkl" % i, "rb") as file:
        tweets_by_person = pickle.load(file)
    for (person, tweets) in tweets_by_person.items():
        if person not in dummy:
            dummy[person] = {}
        dummy[person].update(tweets.items())
        
with open("all_tweets_by_person.pkl", "wb") as file:
    pickle.dump(dummy, file)