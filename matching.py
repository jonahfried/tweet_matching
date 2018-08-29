import gzip
import json
import collections
import math
from datasketch import MinHash, MinHashLSH
import concurrent.futures as cf
import sys
import pandas as pd
import subprocess
import argparse
from tqdm import tqdm

MAX_SEEN_VALUE = .50
MIN_SEEN_VALUE = .01
MATCH_LENIENCY = .3 #.3
LSH_LENIENCY = .6

def get_tweets(SAMPLE_SIZE):
    tweet_file = open("tweets_for_jonah.txt")
    loop_stop = SAMPLE_SIZE
    tweets = []
    for line in tweet_file: 
        loop_stop -= 1
        if loop_stop < 0:
            break
        tweets.append(json.loads(line))
    return tweets

def tweet_to_nameid(tweet):
    return tweet["user"]["name"] + tweet["id_str"]

def set_to_minhash(s):
    m = MinHash(num_perm=128)
    for item in s:
        m.update(item.encode("utf-8"))
    return m
#     encode_pool = cf.ProcessPoolExecutor(max_workers=8)
#     encodings = [encode_pool.submit((lamda i: i.encode("utf-8")), item) for item in s]
#     for encoding in cf.as_completed(encodings):
#             m.update(encoding)
#     return m

def get_words(tweet):
    tweet["words"] = []
    tweet["full_text"] = tweet["full_text"]+"."
    last_ind = 0
    for ind in range(len(tweet["full_text"])):
        if not (tweet["full_text"][ind].isalpha()):
            if last_ind == ind:
                last_ind += 1
                continue
            else:
                tweet["words"].append(tweet["full_text"][last_ind:ind].lower())
                last_ind = ind+1
                
    tweet["word_counts"] = collections.Counter(tweet["words"])
    tweet["minHash"] = set_to_minhash(set(tweet["words"]))
    tweet["nameid"] = tweet_to_nameid(tweet)
    tweet["processed"] = False
    return tweet

# def should_tweet_be_processed(tweet, words_to_remove):
#         for word in words_to_remove:
#             if word in tweet["word_counts"]:
#                 del tweet["word_counts"][word]
#         tweet["square_sum"] = math.sqrt(sum(map((lambda x: x**2), tweet["word_counts"].values())))
#         return ((tweet["square_sum"] != 0), tweet)

def determine_usefulness(word, all_words_seen, total_people):
    return ((all_words_seen[word]/total_people > MAX_SEEN_VALUE) or (all_words_seen[word]/total_people < MIN_SEEN_VALUE))

def build_people_and_find_words(tweets, all_words_seen):
    total_people = len(tweets)
    return list(filter(lambda word:((all_words_seen[word]/total_people > MAX_SEEN_VALUE) or (all_words_seen[word]/total_people < MIN_SEEN_VALUE)) , all_words_seen.keys()))

# cos_dist = sum(ser1*ser2)/(sqrt(sum(ser1^2)) * sqrt(sum(ser2^2)))
def cos_dist(tweet1, tweet2):
    numerator = sum(
        map(
            lambda t: tweet2["word_counts"].get(t[0], 0) * t[1],
            tweet1["word_counts"].items()
        )
    )
    ser1_denominator = tweet1["square_sum"]
    ser2_denominator = tweet2["square_sum"]
#     if (ser1_denominator*ser2_denominator) == 0:
#         print(ser1, ser2)
    return (tweet1["nameid"], tweet2["nameid"], 1 - numerator/(ser1_denominator*ser2_denominator))

def find_sums_for_each_person(tdm):
    square_sums = {tweet["nameid"]:math.sqrt(sum(map((lambda x: x**2), tweet["word_counts"].values()))) for (tweet, _) in potentials}
    return square_sums

def term_frequency(person):
    return .5 + (.5*person/person.max())

def strip_id(user):
    ind = -1
    while not user[ind].isalpha():
        ind -= 1
    return user[:ind+1]

def dict_add_person(person, name, names, full_text):
        node = {"id": name, "group":names[strip_id(name)], "full_text":full_text}
        links = []
        for (relation, weight) in person.items():
            if (weight < MATCH_LENIENCY) and (relation != name) : 
                links.append({"source":name, "target":relation, "value":weight})
        return (node, links)
    
def output_to_json(write_path, similarities, tweet_data):
    nodes_and_links = {}
    nodes_and_links["nodes"] = []
    nodes_and_links["links"] = []
    
    with cf.ProcessPoolExecutor(max_workers=8) as executor:
        uniques = set(executor.map(strip_id, similarities.keys()))
    names = dict(zip(uniques, range(len(uniques))))
    json_pool = cf.ProcessPoolExecutor(max_workers=8)
    parsed_results = [json_pool.submit(dict_add_person, similarities[person], person, names, tweet_data[person]["full_text"]) for person in similarities]
    for node in cf.as_completed(parsed_results):
        nodes_and_links["nodes"].append(node.result()[0])
        nodes_and_links["links"].append(node.result()[1])
    nodes_and_links["links"] = [link for links in nodes_and_links["links"] for link in links]
    
    
    with open(write_path, "w") as file:
        file.write(json.dumps(nodes_and_links, sort_keys=True, indent=2))

def main(SAMPLE_SIZE, output_type):
    print("Getting tweets...")
    tweets = get_tweets(SAMPLE_SIZE)
    pool = cf.ProcessPoolExecutor()
    tweet_results = [pool.submit(get_words, tweet) for tweet in tweets]
    tweet_data = {tweet.result()["nameid"]:tweet.result() for tweet in cf.as_completed(tweet_results)}

    word_counts = list(map(lambda d: d["word_counts"], tweet_data.values()))
    packages = []
    for i in range(8):
        packages.append(word_counts[((i) * (SAMPLE_SIZE//8)):(i+1) * (SAMPLE_SIZE//8)])

    package_pool = cf.ProcessPoolExecutor(max_workers=8)
    package_results = [package_pool.submit(sum, counts, collections.Counter()) for counts in packages]
    word_sums = [f.result() for f in cf.as_completed(package_results)]

    all_words_seen = sum(word_sums, collections.Counter())
    words_to_remove = build_people_and_find_words(tweets, all_words_seen)

    print("Removing extraneous...")
    tweets_to_remove = []
    for tweet in tqdm(tweet_data.values(), desc="tweets"):
        for word in words_to_remove:
            if word in tweet["word_counts"]:
                del tweet["word_counts"][word]
        tweet["square_sum"] = math.sqrt(sum(map((lambda x: x**2), tweet["word_counts"].values())))
        if tweet["square_sum"] == 0:
            tweets_to_remove.append(tweet["nameid"])

    for nameid in tweets_to_remove:
        del tweet_data[nameid]


    tweets = tweet_data.values()

    print("Preliminary pairing...")
    prelim_data = list(map(lambda d:(d["nameid"], set_to_minhash(d["word_counts"])), tweets))
    prelim_similarities = MinHashLSH(threshold=LSH_LENIENCY, num_perm=128) #.6
    with prelim_similarities.insertion_session() as session:
        for (key, minhash) in prelim_data:
            session.insert(key, minhash)
    pairs_to_check = {}
    for tweet in tqdm(tweets):
        pairs = [match for match in prelim_similarities.query(tweet["minHash"]) if match != tweet["nameid"]]
        if len(pairs) > 0:
            pairs_to_check[tweet["nameid"]] = pairs
            for pair in pairs:
                if pair not in pairs_to_check:
                    pairs_to_check[pair] = []

    print("Pairing...")            
    distance_pool = cf.ProcessPoolExecutor(max_workers=8)
    future_results = []
    similarities = {}
    for (person, potentials) in tqdm(pairs_to_check.items(), desc="prep"):
        if person in tweet_data:
            tweet_data[person]["processed"] = True
            similarities[person] = {}
            for relation in potentials:
                if not tweet_data.get(relation, {"processed":True})["processed"]:
                    future_results.append(distance_pool.submit(cos_dist, tweet_data[person], tweet_data[relation]))

    for comparison in tqdm(cf.as_completed(future_results), desc="futures"):
        result = comparison.result()
        similarities[result[0]][result[1]] = result[2]
        
    for (person, comparisons) in similarities.items():
        for (relation, weight) in comparisons.items():
            if relation not in similarities:
                similarities[relation] = {}
            if person not in similarities[relation]:
                similarities[relation][person] = weight
            
    print("Outputting...")
    if output_type == "csv":
        similarity_frame = pd.DataFrame(similarities)
        similarity_frame.to_csv("./similarity_matrix.csv", na_rep=1)
    elif output_type == "json":
        output_to_json("./writeTest.json", similarities, tweet_data)
    elif output_type == "csv+json":
        similarity_frame = pd.DataFrame(similarities)
        similarity_frame.to_csv("./similarity_matrix.csv", na_rep=1)
        output_to_json("./writeTest.json", similarities, tweet_data)
    elif output_type == "none":
        print("Did not write data.")
    print("Completed.")
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Specify how to handle data")
    parser.add_argument("-n", type=int, nargs="?", const=1000, dest="SAMPLE_SIZE")
    parser.add_argument("-t", type=str, nargs="?", const="csv", dest="output_type")
    args = parser.parse_args(sys.argv[1:])
    main(args.SAMPLE_SIZE, args.output_type)