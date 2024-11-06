from celery import Celery
from celery.schedules import crontab, timedelta, solar, schedule
import time
import vlc

from datetime import datetime, timedelta
import requests
import os

from .player import Player


# class Player:

#    def __init__(self):
#         self.player = vlc.Instance()

#     def addPlayList(self, localPath):
#         self.mediaList = self.player.media_list_new()
#         path = os.path.join(os.getcwd(), localPath)
#         self.mediaList.add_media(path)
#         self.listPlayer = self.player.media_list_player_new()
#         self.listPlayer.set_media_list(self.mediaList)
#         self.listPlayer.set_playback_mode(vlc.PlaybackMode(1))

#     def play(self):
#         self.listPlayer.play()

#     def stop(self):
#         self.listPlayer.stop()


app = Celery(
    'tasks', broker='redis://127.0.0.1:6379/0', backend='redis://127.0.0.1:6379/0')

app.conf.beat_schedule = {
    "download-remove-at-particul-time": {
        "task": "tasks.download_remove",
        "schedule": crontab(minute=43, hour=15, day_of_week="tuesday"),
        "args": ("https://files.testfile.org/Video%20MP4%2FRiver%20-%20testfile.org.mp4", ),
    },
    # "remove-at-particul-time": {
    #     "task": "tasks.remove",
    #     "schedule": crontab(minute=44, hour=12, day_of_week="tuesday"),
    #     "args": ("file.mp4", ),
    # }
}

app.conf.timezone = "Asia/Dhaka"


@app.task(bind=True, max_retries=5)
def download_remove(self, url, file_name="file"):
    file_name += "." + url.split(".")[-1]
    r = requests.get(url)
    if r.status_code != 200:
        raise self.retry(countdown=10)

    f = open(file_name, 'wb')
    for chunk in r.iter_content(chunk_size=512 * 1024):
        if chunk:
            f.write(chunk)
    f.close()

    # remove.apply_async((file_name, ), countdown=10)
    stop_time = datetime.now() + timedelta(seconds=20)
    play_video.apply_async(
        (url, stop_time, file_name, ))

    return "Download Complete!"


@app.task(bind=True, max_retries=30)
def remove(self, file_name="file.mp4"):
    os.remove(file_name)

    return "File removed"


@app.task
def play_video(video_url, stopping_time, file_name="file.mp4"):
    player = Player()
    player.addPlayList(video_url)
    player.play()

    current_time = datetime.now().strftime("%Y-%m-%d %H:%M")
    while True:
        time.sleep(4)
        if current_time == stopping_time:
            stop_video.apply_async((player, file_name,))
            break

    return "Playing..."


@app.task
def stop_video(player, file_name="file.mp4"):
    player.stop()
    remove.apply_async((file_name, ), countdown=10)

    return "Stopped and Deleted"


# TODO 1. Celery periodic task in depth
# TODO 2. Error handling
# TODO 3. Download large files in python
# TODO 4. Download and remove file at a particular time with Celery

# ? NOTES:
# *_on_commit - celery will execute the task after the transaction is committed
