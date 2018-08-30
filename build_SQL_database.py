import sqlite3

def connect_to_database():
    return sqlite3.connect("tweets.db")

def add_person(executor, person, tweets):
    for (tweet_id, tweet) in tweets.items():
        executor.execute("INSERT INTO tweets VALUES(?, ?, ?)", (tweet_id, tweet["full_text"], person))

if __name__ == "__main__":   
    conn = connect_to_database()
    executor = conn.cursor()
    
    # executor.execute(''' CREATE TABLE people 
    #                     (name text) ''')
    executor.execute(''' CREATE TABLE tweets 
                        (id INTEGER, full_text text, user_name text) ''')
    conn.commit()
    conn.close()
    
    # dummy = {8383:{"full_text":"hello world"}}
    # add_person_if_needed(executor, "Dummy", dummy)
    # conn.commit()
    # executor.execute("SELECT * FROM people")
    # people_result = executor.fetchone()
    # if people_result != None:
    #     for person in people_result:
    #         executor.execute("SELECT full_text FROM tweets WHERE tweeter_name==?", (person,))
    #         tweet_results = executor.fetchone()
    #         if tweet_results != None:
    #             for tweet in tweet_results:
    #                 print(tweet)