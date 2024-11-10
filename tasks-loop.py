from celery import Celery
from celery.schedules import crontab, timedelta, solar, schedule
import time
import vlc

from datetime import datetime, timedelta
import requests
import os
import threading
import logging

logging.basicConfig(level=logging.INFO)

urls = [
    "https://files.testfile.org/Video%20MP4%2FRiver%20-%20testfile.org.mp4",
    "https://files.testfile.org/Video%20MP4%2FRiver%20-%20testfile.org.mp4",
    "https://files.testfile.org/Video%20MP4%2FRiver%20-%20testfile.org.mp4"
]

files = []


class Player:
    def __init__(self):
        self.player = vlc.Instance()

    def addPlayList(self, localPaths):
        self.mediaList = self.player.media_list_new()
        # self.player.audio_output_device_enum()

        for p in localPaths:
            path = os.path.join(os.getcwd(), p)
            self.mediaList.add_media(path)

        self.listPlayer = self.player.media_list_player_new()
        self.listPlayer.set_media_list(self.mediaList)

        # self.listPlayer.set_playback_mode(vlc.PlaybackMode(1))

    def play(self):
        self.listPlayer.play()
        logging.info("Vedio playing...")

    def stop(self, file_name="file.mp4"):
        self.listPlayer.stop()
        remove.apply_async((files, ), countdown=3)

    def is_playing(self):
        return self.listPlayer.is_playing()


app = Celery(
    'tasks', broker='redis://127.0.0.1:6379/0', backend='redis://127.0.0.1:6379/0')

app.conf.beat_schedule = {
    "download-remove-at-particul-time": {
        "task": "tasks.download_remove",
        "schedule": crontab(minute=43, hour=15, day_of_week="tuesday"),
        # "args": ("https://files.testfile.org/Video%20MP4%2FRiver%20-%20testfile.org.mp4", ),
        "args": (urls, ),
    },
}

app.conf.timezone = "Asia/Dhaka"


@app.task(bind=True, max_retries=5)
def download_remove(self, urls, file_name="file"):
    for index, url in enumerate(urls):
        file_name += f"{index}." + url.split(".")[-1]
        r = requests.get(url)
        if r.status_code != 200:
            raise self.retry(countdown=10)

        f = open(file_name, 'wb')
        for chunk in r.iter_content(chunk_size=512 * 1024):
            if chunk:
                f.write(chunk)
        f.close()

        files.append(file_name)

    logging.info("Download Complete!")

    play_video.apply_async(
        (files, file_name, ), countdown=1)

    return "Download Complete!"


@app.task
def play_video(video_url, file_name="file.mp4"):
    player = Player()
    player.addPlayList(video_url)
    player.play()

    timer = threading.Timer(30, player.stop, [file_name])
    timer.start()


@app.task(bind=True, max_retries=30)
def remove(self, file_name="file.mp4"):
    for f in files:
        os.remove(f)

    return "File removed"

# *_on_commit - celery will execute the task after the transaction is committed
