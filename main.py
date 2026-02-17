from datetime import date
from pathlib import Path
import csv
import youtube as yt

################################################################
API_KEY = "AIzaSy...Fbyfw0"
VIDEO_URL = "https://www.youtube.com/watch?v=Be4Mf1Eq8Fs"
################################################################


BASE_DIR = Path().resolve()


api = yt.YoutubeAPI(api_key=API_KEY)

with yt.DatabaseConnection(db_path=BASE_DIR / 'data' / 'sqlite.db') as db:
    api.quota = db.get_quota()
    video_id = yt.get_video_id(VIDEO_URL)
    try:
        video = api.get_video(video_id)
        db.save(video)
        for thread_count, thread in enumerate(api.get_comment_threads(video_id), start=1):
            print(f'  Thread {thread_count:5d}   Quota {api.quota: 5d}  ', end='\r')
            db.save(thread)
            db.save(thread.snippet.topLevelComment)
        print()
    except Exception as e:
        print()
        print(f'  Exception: {e}')
    db.set_quota(date.today(), api.quota)
