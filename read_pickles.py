import pickle
from build_SQL_database import *

if __name__ == "__main__":
    conn = connect_to_database()
    executor = conn.cursor()
    for i in range(1, 6):
        print("loading file #"+str(i))
        with open("./by_person/tweets_by_person%d.pkl" % i, "rb") as file:
            tweets_by_person = pickle.load(file)
        for (person, tweets) in tweets_by_person.items():
            add_person(executor, person, tweets)
        conn.commit()
    conn.close()