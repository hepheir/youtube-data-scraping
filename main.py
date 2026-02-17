from pathlib import Path
import youtube as yt

################################################################
API_KEY = "AIzaSy...Fbyfw0"
VIDEO_URL = "https://www.youtube.com/watch?v=Be4Mf1Eq8Fs"
################################################################


BASE_DIR = Path().resolve()

yt.update_video_data(db_path=BASE_DIR / 'data/sqlite.db',
                     video_url=VIDEO_URL,
                     api_key=API_KEY)
