import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3

DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
USER_ID = "mbbuybxlx3nwctj13qreqjho3"
TOKEN = "BQDsHDKUll7S2n5gCWudE1hOa3Zr4JHJWTIv0Z8St5TdTatmu_5iOIsXnyiEbY-Rm-TQrGKaDBA-Ce0OWTExgzMYDTAerTrlSUMRUOsW8t1bRzGAlSZVQkL14UNm1RRrfXQBGdLhwVEPyTABXp0NDYYuFu59eq66t67jDPuZ"

def check_if_valid_date(df: pd.DataFrame) -> bool:
    #Check if databframe is empty
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False
    
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary key check is violated")

    if df.isnull().values.any():
        raise Exception("Null value found")

    return True

if __name__ == "__main__":
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN}"
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=50)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get(f"https://api.spotify.com/v1/me/player/recently-played?after={yesterday_unix_timestamp}", headers=headers)
    data = r.json()

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []
   
    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    song_dict = {
        "song_name": song_names,
        "artist_name": artist_names,
        "played_at": played_at_list,
        "timestamp": timestamps
    }

    song_df = pd.DataFrame(song_dict, columns=["song_name", "artist_name", "played_at", "timestamp"])
    
    #Validate
    if check_if_valid_date(song_df):
        print("Data valid, proceed to Load stage")

    #Load

    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('my_played_tracks.sqlite')
    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """

    cursor.execute(sql_query)
    print("Opened database successfully")

    try:
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database")


    conn.close()
    print("Closed database successfully")