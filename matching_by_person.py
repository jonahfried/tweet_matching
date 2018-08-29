import pickle
import json
import collections
import math
from datasketch import MinHash, MinHashLSH
import concurrent.futures as cf
import sys
import pandas as pd
import subprocess
import random
import argparse
from tqdm import tqdm

MAX_SEEN_VALUE = .50
MIN_SEEN_VALUE = .01
MATCH_LENIENCY = .6

def get_tweets():#SAMPLE_SIZE):
#     with open("./all_tweets_by_person.pkl", "rb") as file:
#         personal_info = pickle.load(file)
#     return {key:{"full_text":value} for (key, value) in personal_info.items()}
    sub_people = {}
    for i in range(1, 3):
        print("loading part"+str(i))
        with open("./by_person/tweets_by_person"+str(i)+".pkl", "rb") as file:
            personal_info = pickle.load(file)
        people_text = {
            (name+str(i)):{"full_text":" ".join(map(lambda t:t["full_text"], tweet_texts))}
            for (name, tweet_texts)
            in personal_info.items()
        }
        sub_people.update(people_text)
    return sub_people
     

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

def get_words(name, tweet):
    words = []
    tweet["full_text"] = tweet["full_text"]+"."
    last_ind = 0
    for ind in range(len(tweet["full_text"])):
        if not (tweet["full_text"][ind].isalpha()):
            if last_ind == ind:
                last_ind += 1
                continue
            else:
                words.append(tweet["full_text"][last_ind:ind].lower())
                last_ind = ind+1
                
    tweet["word_counts"] = collections.Counter(words)
    tweet["minHash"] = set_to_minhash(set(words))
    tweet["nameid"] = name
    tweet["processed"] = False
    return tweet

def build_people_and_find_words(total_people, all_words_seen):
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

def dict_add_person(person, name, counter, full_text):
        node = {"id": name, "group":counter}#, "full_text":full_text}
        links = []
        for (relation, weight) in person.items():
            if (weight < MATCH_LENIENCY) and (relation != name) : 
                links.append({"source":name, "target":relation, "value":weight})
        return (node, links)
    
def output_to_json(write_path, similarities, tweet_data):
    nodes_and_links = {}
    nodes_and_links["nodes"] = []
    nodes_and_links["links"] = []
    
    json_pool = cf.ProcessPoolExecutor(max_workers=8)
    parsed_results = [
        json_pool.submit(dict_add_person, similarities[person], person, counter, tweet_data[person]["full_text"]) 
        for (counter, person) 
        in enumerate(similarities)
    ]
    for node in cf.as_completed(parsed_results):
        nodes_and_links["nodes"].append(node.result()[0])
        nodes_and_links["links"].append(node.result()[1])
    nodes_and_links["links"] = [link for links in nodes_and_links["links"] for link in links]
    
    
    with open(write_path, "w") as file:
        file.write(json.dumps(nodes_and_links, sort_keys=True, indent=2))

        
        
def remove_extraneous(tweet, words_to_remove):
    for word in words_to_remove:
        del tweet["word_counts"][word]
    return (len(tweet["word_counts"].keys()) != 0, tweet["nameid"], tweet)
        
def main(output_type): #SAMPLE_SIZE, output_type):
    print("Getting tweets...")
    tweets = get_tweets()#SAMPLE_SIZE)
    SAMPLE_SIZE = len(tweets)
    pool = cf.ProcessPoolExecutor()
    tweet_results = [pool.submit(get_words, name, tweet) for (name, tweet) in tweets.items()]
    tweet_data = {tweet.result()["nameid"]:tweet.result() for tweet in cf.as_completed(tweet_results)}
    del tweets
    
    print("Summing Counts...")
    word_counts = list(map(lambda d: d["word_counts"], tweet_data.values()))
    packages = []
    for i in tqdm(range(8), desc="breaking_up"):
        packages.append(word_counts[((i) * (SAMPLE_SIZE//8)):(i+1) * (SAMPLE_SIZE//8)])

    package_pool = cf.ProcessPoolExecutor(max_workers=8)
    package_results = [package_pool.submit(sum, counts, collections.Counter()) for counts in packages]
    word_sums = [f.result() for f in cf.as_completed(package_results)]

    all_words_seen = sum(word_sums, collections.Counter())
    words_to_remove = build_people_and_find_words(SAMPLE_SIZE, all_words_seen)
    
    for word in words_to_remove:
        del all_words_seen[word]
    
    print("Removing extraneous and square summing...")
    tweets_to_remove = []
    for tweet in tqdm(tweet_data.values(), desc="tweet loop"):
        for word in tqdm(words_to_remove, desc="word loop"):
            if word in tweet["word_counts"]:
                del tweet["word_counts"][word]
        if len(tweet["word_counts"].keys()) == 0:
            tweets_to_remove.append(tweet["nameid"])
            
            
#     removal_pool = cf.ProcessPoolExecutor(max_workers=8)
#     removal_results = [removal_pool.submit(remove_extraneous, tweet, words_to_remove) for tweet in tweet_data.values()]
#     tweet_data = {}
#     for finished in cf.as_completed(removal_results):
#         result = finished.result()
#         if result[0]:
#             tweet_data[result[1]] = result[2]
        

    for nameid in tweets_to_remove:
        del tweet_data[nameid]

#     sample_size = len(tweet_data)    
#     idf = sum(map(lambda d:collections.Counter(d["word_counts"].keys()), tweet_data.values()), collections.Counter())
#     for key in idf:
#         idf[key] = sample_size/idf[key]
        
    for tweet in tqdm(tweet_data.values(), desc="square sum"):
#         for word in tweet["word_counts"]:
#             tweet["word_counts"][word] = tweet["word_counts"][word] * idf[word]
        tweet["square_sum"] = math.sqrt(sum(map((lambda x: x**2), tweet["word_counts"].values())))
    
    print("Preliminary pairing...")
    prelim_data = list(map(lambda d:(d["nameid"], set_to_minhash(d["word_counts"])), tweet_data.values()))
    prelim_similarities = MinHashLSH(threshold=0.3, num_perm=128)
    with prelim_similarities.insertion_session() as session:
        for (key, minhash) in prelim_data:
            session.insert(key, minhash)
    pairs_to_check = {}
    for tweet in tqdm(tweet_data.values()):
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
#     for person in tweet_data:
#         similarities[person] = {}
#         for relation in tweet_data:
#             if person != relation:
#                 future_results.append(distance_pool.submit(cos_dist, tweet_data[person], tweet_data[relation]))
    for (person, potentials) in tqdm(pairs_to_check.items(), desc="person"):
        if person in tweet_data:
            tweet_data[person]["processed"] = True
            similarities[person] = {}
            for relation in tqdm(potentials, desc="submitting"):
                if not tweet_data.get(relation, {"processed":True})["processed"]:
                    future_results.append(distance_pool.submit(cos_dist, tweet_data[person], tweet_data[relation]))

    for comparison in cf.as_completed(future_results):
        result = comparison.result()
        similarities[result[0]][result[1]] = result[2]
        
#     for (person, comparisons) in similarities.items():
#         for (relation, weight) in comparisons.items():
#             if relation not in similarities:
#                 similarities[relation] = {}
#             if person not in similarities[relation]:
#                 similarities[relation][person] = weight
            
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
#     parser.add_argument("-n", type=int, nargs="?", const=1000, dest="SAMPLE_SIZE")
    parser.add_argument("-t", type=str, nargs="?", const="csv", dest="output_type")
    args = parser.parse_args(sys.argv[1:])
#     main(args.SAMPLE_SIZE, args.output_type)
    main(args.output_type)