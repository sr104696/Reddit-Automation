"""
Livestream - Modified for Antigravity Integration
=================================================
Includes automated triggering of Stage 2 (Extractor) and post history logging.
"""

import json
import logging
import os
import time
import requests
from argparse import Namespace
from typing import Any, Dict, Generator, List, Literal, Tuple, Union

from colorama import Fore, Style
from halo import Halo
from praw import Reddit
from praw.models import Redditor, Subreddit

from urs.praw_scrapers.live_scrapers.utils.DisplayStream import DisplayStream
from urs.praw_scrapers.live_scrapers.utils.StreamGenerator import StreamGenerator
from urs.praw_scrapers.utils.Validation import Validation
from urs.utils.DirInit import InitializeDirectory
from urs.utils.Global import date
from urs.utils.Titles import PRAWTitles

# --- INTEGRATION LOGIC ---
EXTRACTOR_URL = os.getenv("EXTRACTOR_API_URL", "http://extractor_api:8000")
POST_HISTORY_PATH = os.getenv("POST_HISTORY_LOG", "post_history.txt")

def trigger_extractor(obj: Dict[str, Any]):
    """
    Triggers the Stage 2 Extractor API for deep-dive scraping.
    """
    try:
        # Extract target (subreddit or author)
        target = ""
        is_user = False
        
        if obj.get("type") == "submission":
            target = obj.get("subreddit", {}).get("display_name", "")
            # Log to post_history.txt
            with open(POST_HISTORY_PATH, "a") as f:
                f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {obj.get('url')}\n")
        elif obj.get("type") == "comment":
            target = obj.get("author", "").replace("u/", "")
            is_user = True
            
        if target:
            payload = {"target": target, "is_user": is_user, "limit": 50}
            requests.post(f"{EXTRACTOR_URL}/trigger", json=payload, timeout=5)
            logging.info(f"Triggered deep-dive for {target}")
    except Exception as e:
        logging.warning(f"Failed to trigger Extractor API: {e}")

# --- END INTEGRATION LOGIC ---

class SaveStream:
    """
    Methods for saving the livestream to file.
    """

    @staticmethod
    def _create_skeleton(args: Namespace) -> Dict[str, Any]:
        skeleton = {"livestream_settings": {}, "livestream_metadata": {}, "data": []}
        skeleton["livestream_settings"]["included_reddit_objects"] = (
            "submissions" if args.stream_submissions else "comments"
        )
        if args.live_subreddit:
            skeleton["livestream_settings"]["subreddit"] = args.live_subreddit
        elif args.live_redditor:
            skeleton["livestream_settings"]["redditor"] = args.live_redditor
        return skeleton

    @staticmethod
    def _make_livestream_dir(split_stream_info: List[str]) -> str:
        if split_stream_info[0] == "r":
            sub_directory = "subreddits"
        elif split_stream_info[0] == "u":
            sub_directory = "redditors"
        stream_directory = f"../scrapes/{date}/livestream/{sub_directory}"
        InitializeDirectory.create_dirs(stream_directory)
        return stream_directory

    @staticmethod
    def _get_temp_filename(stream_info: str) -> str:
        split_stream_info = stream_info.split(" ")[1].split("/")
        filename = split_stream_info[1] + ".json"
        stream_directory = SaveStream._make_livestream_dir(split_stream_info)
        stream_path = stream_directory + "/" + filename
        logging.info(f"Writing stream to temporary file: {stream_path}.")
        return stream_path

    @staticmethod
    def _create_temp_file(skeleton: Dict[str, Any], stream_path: str) -> None:
        if not os.path.isfile(stream_path):
            with open(stream_path, "w", encoding="utf-8") as new_file:
                json.dump(skeleton, new_file)

    @staticmethod
    def _rename(
        duration: str, object_info: str, start_stream: str, stream_path: str
    ) -> None:
        split_stream_path = stream_path.split(".")
        new_filename = f"..{split_stream_path[-2]}-{object_info}-{start_stream.replace(':', '_')}-{duration.replace(':', '_')}.{split_stream_path[-1]}"
        logging.info(f"Renaming livestream file to: {new_filename}.")
        os.rename(stream_path, new_filename)

    @staticmethod
    def write(
        args: Namespace,
        generator: Generator[Dict[str, Any], None, None],
        object_info: str,
        stream_info: str,
    ) -> str:
        skeleton = SaveStream._create_skeleton(args)
        stream_path = SaveStream._get_temp_filename(stream_info)
        SaveStream._create_temp_file(skeleton, stream_path)

        with open(stream_path, "r+", encoding="utf-8") as existing_file:
            stream_data = json.load(existing_file)
            start_stream = time.mktime(time.localtime())
            try:
                logging.info("")
                logging.info("STREAMING...")
                logging.info("")

                for obj in generator:
                    DisplayStream.display(obj)
                    stream_data["data"].append(obj)
                    
                    # Trigger Extractor API
                    trigger_extractor(obj)

                    existing_file.seek(0)
                    existing_file.truncate()
                    json.dump(stream_data, existing_file)

            except KeyboardInterrupt:
                end_stream = time.mktime(time.localtime())
                duration = time.strftime(
                    "%H:%M:%S", time.gmtime(end_stream - start_stream)
                )
                stream_statistics = (
                    f"Streamed {object_info} submitted {stream_info} for {duration}."
                )
                print("\n\n")
                Halo().info(Fore.YELLOW + Style.BRIGHT + "ABORTING LIVESTREAM.")
                logging.info("ABORTING LIVESTREAM.")
                logging.info("")
                Halo().info(stream_statistics)
                print()
                stream_data["livestream_metadata"]["stream_duration"] = duration
                stream_data["livestream_metadata"]["stream_end"] = time.strftime(
                    "%H:%M:%S", time.localtime(end_stream)
                )
                stream_data["livestream_metadata"]["stream_start"] = time.strftime(
                    "%H:%M:%S", time.localtime(start_stream)
                )
                existing_file.seek(0)
                existing_file.truncate()
                json.dump(stream_data, existing_file, indent=2)

        save_spinner = Halo().start("Saving livestream.")
        SaveStream._rename(
            duration,
            object_info,
            time.strftime("%H:%M:%S", time.localtime(start_stream)),
            stream_path,
        )
        save_spinner.info(
            Fore.GREEN + Style.BRIGHT + "Livestream has been saved to file."
        )
        logging.info("Livestream has been saved to file.")
        logging.info("")
        print()
        return stream_statistics


class Livestream:
    """
    Methods for livestreaming a Subreddit or Redditor's new comments or submissions.
    """

    @staticmethod
    def _set_info_and_object(
        args: Namespace, reddit: Reddit
    ) -> Tuple[Union[Redditor, Subreddit], str]:
        if args.live_subreddit:
            PRAWTitles.lr_title()
            Validation.validate([args.live_subreddit], reddit, "subreddit")
            initial_message = (
                f"Initializing Subreddit livestream for r/{args.live_subreddit}."
            )
            stream_info = f"in r/{args.live_subreddit}"
            reddit_object = reddit.subreddit(args.live_subreddit)
        elif args.live_redditor:
            PRAWTitles.lu_title()
            Validation.validate([args.live_redditor], reddit, "redditor")
            initial_message = (
                f"Initializing Redditor livestream for u/{args.live_redditor}."
            )
            stream_info = f"by u/{args.live_redditor}"
            reddit_object = reddit.redditor(args.live_redditor)
        Halo().info(Fore.CYAN + Style.BRIGHT + initial_message)
        logging.info(initial_message + "..")
        Halo().info("New entries will appear when posted to Reddit.")
        return reddit_object, stream_info

    @staticmethod
    def _stream_switch(
        args: Namespace, reddit_object: Union[Redditor, Subreddit]
    ) -> Tuple[
        Generator[Dict[str, Any], None, None], Literal["comments", "submissions"]
    ]:
        if args.stream_submissions:
            Halo().info(Fore.BLUE + Style.BRIGHT + "Displaying submissions.")
            object_info = "submissions"
            generator = StreamGenerator.stream_submissions(reddit_object.stream)
        else:
            Halo().info(Fore.BLUE + Style.BRIGHT + "Displaying comments.")
            object_info = "comments"
            generator = StreamGenerator.stream_comments(reddit_object.stream)
        print()
        return generator, object_info

    @staticmethod
    def _no_save_stream(
        generator: Generator[Dict[str, Any], None, None],
        object_info: str,
        stream_info: str,
    ) -> str:
        start_stream = time.time()
        try:
            logging.info("")
            logging.info("STREAMING...")
            logging.info("")
            for obj in generator:
                DisplayStream.display(obj)
                # Also trigger here for no-save mode
                trigger_extractor(obj)
        except KeyboardInterrupt:
            duration = time.strftime(
                "%H:%M:%S", time.gmtime(time.time() - start_stream)
            )
            stream_statistics = (
                f"Streamed {object_info} submitted {stream_info} for {duration}."
            )
            print("\n\n")
            Halo().info(Fore.YELLOW + Style.BRIGHT + "ABORTING LIVESTREAM.")
            logging.info("ABORTING LIVESTREAM.")
            logging.info("")
            Halo().info(stream_statistics)
            print()
        return stream_statistics

    @staticmethod
    def stream(args: Namespace, reddit: Reddit) -> None:
        reddit_object, stream_info = Livestream._set_info_and_object(args, reddit)
        generator, object_info = Livestream._stream_switch(args, reddit_object)
        stream_statistics = (
            Livestream._no_save_stream(generator, object_info, stream_info)
            if args.nosave
            else SaveStream.write(args, generator, object_info, stream_info)
        )
        logging.info(stream_statistics)
        logging.info("")
