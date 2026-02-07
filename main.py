from pathlib import Path
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
    for video_count, video_url in enumerate(video_url_list, start=1):
        video_id = yt.get_video_id(video_url)

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


with yt.DatabaseConnection() as db:
    db.videos_to_dataframe().to_csv(BASE_DIR / 'data' / 'videos.csv', index=False)
    db.comments_to_dataframe().to_csv(BASE_DIR / 'data' / 'comments.csv', index=False)
