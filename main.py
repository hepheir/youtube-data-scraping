from datetime import date
from pathlib import Path
from googleapiclient.errors import HttpError
import csv
import youtube as yt

################################################################
API_KEY = "AIzaSy...Fbyfw0"

VIDEO_URLS = """

# 한 줄에 대충 URL 하나 씩 넣기

https://youtu.be/gHEh2ocSIlU?si=dvs9A8iEig15Xs8D
https://youtu.be/CQPlUaY0F3U?si=xkjiBnLTxxfKccg-
https://www.youtube.com/shorts/lyhSBLDOamE
https://www.youtube.com/watch?v=uF6rlvyoszc
http://youtube.com/shorts/dLiR9U4tktk?si=DfI5QMkdsaIxp676

https://www.youtube.com/watch?v=Be4Mf1Eq8Fs

"""
################################################################


BASE_DIR = Path().resolve()


api = yt.YoutubeAPI(api_key=API_KEY)

# 줄 마다 URL 하나 씩 추출해서 리스트로 만들기.
video_url_list = []

for video_url in VIDEO_URLS.splitlines():
    video_url = video_url.strip()
    if not video_url or video_url.startswith('#'):
        continue
    video_url_list.append(video_url)


with yt.DatabaseConnection(db_path=BASE_DIR / 'data' / 'sqlite.db') as db:
    api.quota = db.get_quota(date.today(), default=10000)
    for video_count, video_url in enumerate(video_url_list, start=1):
        video_id = yt.get_video_id(video_url)
        try:
            print(f'Video {video_id}   Quota {api.quota: 5d}   {video_url} ')

            if db.has_video(video_id):
                print(f"Skipping... (Video {video_id} already exists in database.)")
                continue

            video = api.get_video(video_id)
            db.insert_video(video)

            for comment_count, comment in enumerate(api.get_comments(video_id), start=1):
                print(f'  Comment {comment_count:5d}   Quota {api.quota: 5d}  ', end='\r')
                db.insert_comment(comment)

            print()
            print(f'Done video ({video_count}/{len(video_url_list)})')
        except HttpError as e:
            print(f'  HttpError: {e}')
            print()
            print(f'Deleting video and comments : video {video_id}')
            db.delete_video(video_id)
            db.delete_video_comments(video_id)
    db.set_quota(date.today(), api.quota)


with yt.DatabaseConnection() as db:
    csv_saving_options = {
        'index': False,
        'quoting': csv.QUOTE_NONE,
        'escapechar': '\\',
    }

    videos_csv_path = BASE_DIR / 'data' / 'videos.csv'
    db.videos_to_dataframe().to_csv(videos_csv_path, **csv_saving_options)
    print(f'Saved videos to {videos_csv_path}')

    comments_csv_path = BASE_DIR / 'data' / 'comments.csv'
    db.comments_to_dataframe().to_csv(comments_csv_path, **csv_saving_options)
    print(f'Saved comments to {comments_csv_path}')
