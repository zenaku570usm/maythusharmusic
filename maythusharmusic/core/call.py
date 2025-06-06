import asyncio
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Union, Dict, Any

from ntgcalls import TelegramServerError
from pyrogram import Client
from pyrogram.errors import FloodWait, ChatAdminRequired
from pyrogram.types import InlineKeyboardMarkup
from pytgcalls import PyTgCalls
from pytgcalls.exceptions import NoActiveGroupCall
from pytgcalls.types import (
    AudioQuality,
    ChatUpdate,
    MediaStream,
    StreamEnded,
    Update,
    VideoQuality,
)

import config
from strings import get_string
from maythusharmusic import LOGGER, YouTube, app
from maythusharmusic.misc import db
from maythusharmusic.utils.database import (
    add_active_chat,
    add_active_video_chat,
    get_lang,
    get_loop,
    group_assistant,
    is_autoend,
    music_on,
    remove_active_chat,
    remove_active_video_chat,
    set_loop,
)
from maythusharmusic.utils.exceptions import AssistantErr
from maythusharmusic.utils.formatters import (
    check_duration,
    seconds_to_min,
    speed_converter,
)
from maythusharmusic.utils.inline.play import stream_markup
from maythusharmusic.utils.stream.autoclear import auto_clean
from maythusharmusic.utils.thumbnails import get_thumb
from maythusharmusic.utils.errors import capture_internal_err, send_large_error


autoend: Dict[int, datetime] = {}
counter: Dict[int, Dict[str, Any]] = {}


def dynamic_media_stream(path: str, video: bool = False, ffmpeg_params: str = None) -> MediaStream:
    return MediaStream(
        audio_path=path,
        media_path=path,
        audio_parameters=AudioQuality.STUDIO if video else AudioQuality.STUDIO,
        video_parameters=VideoQuality.HD_720p if video else VideoQuality.HD_720p,
        video_flags=(MediaStream.Flags.AUTO_DETECT if video else MediaStream.Flags.IGNORE),
        ffmpeg_parameters=ffmpeg_params,
    )


async def _clear_(chat_id: int) -> None:
    """Clear the queue and remove active chat."""
    if chat_id in db:
        popped = db.pop(chat_id)
        if popped:
            await auto_clean(popped)
    
    db[chat_id] = []
    await remove_active_video_chat(chat_id)
    await remove_active_chat(chat_id)
    await set_loop(chat_id, 0)


class Call:
    def __init__(self):
        self.userbot1 = Client("maythusharmusic1", config.API_ID, config.API_HASH, session_string=config.STRING1) if config.STRING1 else None
        self.one = PyTgCalls(self.userbot1) if self.userbot1 else None

        self.userbot2 = Client("maythusharmusic2", config.API_ID, config.API_HASH, session_string=config.STRING2) if config.STRING2 else None
        self.two = PyTgCalls(self.userbot2) if self.userbot2 else None

        self.userbot3 = Client("maythusharmusic3", config.API_ID, config.API_HASH, session_string=config.STRING3) if config.STRING3 else None
        self.three = PyTgCalls(self.userbot3) if self.userbot3 else None

        self.userbot4 = Client("maythusharmusic4", config.API_ID, config.API_HASH, session_string=config.STRING4) if config.STRING4 else None
        self.four = PyTgCalls(self.userbot4) if self.userbot4 else None

        self.userbot5 = Client("maythusharmusic5", config.API_ID, config.API_HASH, session_string=config.STRING5) if config.STRING5 else None
        self.five = PyTgCalls(self.userbot5) if self.userbot5 else None

    @capture_internal_err
    async def pause_stream(self, chat_id: int) -> None:
        """Pause the ongoing stream."""
        assistant = await group_assistant(self, chat_id)
        await assistant.pause_stream(chat_id)

    @capture_internal_err
    async def resume_stream(self, chat_id: int) -> None:
        """Resume the paused stream."""
        assistant = await group_assistant(self, chat_id)
        await assistant.resume_stream(chat_id)

    @capture_internal_err
    async def mute_stream(self, chat_id: int) -> None:
        """Mute the ongoing stream."""
        assistant = await group_assistant(self, chat_id)
        await assistant.mute_stream(chat_id)

    @capture_internal_err
    async def unmute_stream(self, chat_id: int) -> None:
        """Unmute the ongoing stream."""
        assistant = await group_assistant(self, chat_id)
        await assistant.unmute_stream(chat_id)

    @capture_internal_err
    async def stop_stream(self, chat_id: int) -> None:
        """Stop the ongoing stream and clean up."""
        assistant = await group_assistant(self, chat_id)
        await _clear_(chat_id)
        
        try:
            await assistant.leave_group_call(chat_id)
        except (NoActiveGroupCall, Exception) as e:
            LOGGER(__name__).warning(
                f"Failed to leave group call for chat {chat_id}: {str(e)}"
            )

    @capture_internal_err
    async def force_stop_stream(self, chat_id: int) -> None:
        """Force stop the stream and clean up."""
        assistant = await group_assistant(self, chat_id)
        
        try:
            if chat_id in db and db[chat_id]:
                db[chat_id].pop(0)
        except (IndexError, KeyError):
            pass
            
        await remove_active_video_chat(chat_id)
        await remove_active_chat(chat_id)
        
        try:
            await assistant.leave_group_call(chat_id)
        except (NoActiveGroupCall, Exception):
            pass

    @capture_internal_err
    async def skip_stream(
        self,
        chat_id: int,
        link: str,
        video: Union[bool, str] = None,
        image: Union[bool, str] = None,
    ) -> None:
        """Skip to the next stream."""
        assistant = await group_assistant(self, chat_id)
        stream = dynamic_media_stream(path=link, video=bool(video))
        await assistant.change_stream(chat_id, stream)

    @capture_internal_err
    async def vc_users(self, chat_id: int) -> List[int]:
        """Get list of users in voice chat."""
        assistant = await group_assistant(self, chat_id)
        participants = await assistant.get_participants(chat_id)
        return [p.user_id for p in participants if not p.is_muted]

    @capture_internal_err
    async def change_volume(self, chat_id: int, volume: int) -> None:
        """Change volume of the ongoing stream."""
        if not 0 <= volume <= 200:
            raise AssistantErr("Volume must be between 0 and 200")
            
        assistant = await group_assistant(self, chat_id)
        await assistant.change_volume(chat_id, volume)

    @capture_internal_err
    async def seek_stream(
        self,
        chat_id: int,
        file_path: str,
        to_seek: str,
        duration: str,
        mode: str,
    ) -> None:
        """Seek to a specific position in the stream."""
        assistant = await group_assistant(self, chat_id)
        ffmpeg_params = f"-ss {to_seek} -to {duration}"
        is_video = mode == "video"
        stream = dynamic_media_stream(
            path=file_path,
            video=is_video,
            ffmpeg_params=ffmpeg_params,
        )
        await assistant.change_stream(chat_id, stream)

    @capture_internal_err
    async def speedup_stream(
        self,
        chat_id: int,
        file_path: str,
        speed: float,
        playing: List[Dict[str, Any]],
    ) -> None:
        """Adjust playback speed of the stream."""
        if not playing or not isinstance(playing[0], dict):
            raise AssistantErr("Invalid stream info for speedup")

        if speed <= 0 or speed > 5.0:
            raise AssistantErr("Speed must be between 0.1 and 5.0")

        assistant = await group_assistant(self, chat_id)
        playback_dir = Path("playback") / str(speed)
        playback_dir.mkdir(parents=True, exist_ok=True)
        
        out_file = playback_dir / Path(file_path).name

        if not out_file.exists():
            try:
                vs = str(2.0 / float(speed))
                cmd = [
                    "ffmpeg",
                    "-i", str(file_path),
                    "-filter:v", f"setpts={vs}*PTS",
                    "-filter:a", f"atempo={speed}",
                    "-y", str(out_file),
                ]
                
                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdin=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                _, stderr = await proc.communicate()
                
                if proc.returncode != 0:
                    raise AssistantErr(f"FFmpeg error: {stderr.decode()}")
                    
            except Exception as e:
                raise AssistantErr(f"Speed adjustment failed: {str(e)}")

        dur = await asyncio.get_event_loop().run_in_executor(
            None, check_duration, str(out_file)
        )
        played, con_seconds = speed_converter(playing[0]["played"], speed)
        duration_min = seconds_to_min(dur)
        is_video = playing[0]["streamtype"] == "video"
        ffmpeg_params = f"-ss {played} -to {duration_min}"
        
        stream = dynamic_media_stream(
            path=str(out_file),
            video=is_video,
            ffmpeg_params=ffmpeg_params,
        )

        if chat_id not in db or not db[chat_id] or db[chat_id][0].get("file") != file_path:
            raise AssistantErr("Stream mismatch during speedup")

        await assistant.change_stream(chat_id, stream)
        
        db[chat_id][0].update({
            "played": con_seconds,
            "dur": duration_min,
            "seconds": dur,
            "speed_path": str(out_file),
            "speed": speed,
            "old_dur": playing[0].get("dur"),
            "old_second": playing[0].get("seconds"),
        })

    @capture_internal_err
    async def stream_call(self, link: str) -> None:
        """Test stream in logger group."""
        assistant = await group_assistant(self, config.LOGGER_ID)
        
        try:
            await assistant.join_group_call(
                config.LOGGER_ID,
                MediaStream(link),
            )
            await asyncio.sleep(8)
        finally:
            try:
                await assistant.leave_group_call(config.LOGGER_ID)
            except Exception:
                pass

    @capture_internal_err
    async def join_call(
        self,
        chat_id: int,
        original_chat_id: int,
        link: str,
        video: Union[bool, str] = None,
        image: Union[bool, str] = None,
    ) -> None:
        """Join voice chat and start streaming."""
        assistant = await group_assistant(self, chat_id)
        lang = await get_lang(chat_id)
        _ = get_string(lang)
        
        stream = dynamic_media_stream(path=link, video=bool(video))

        try:
            await assistant.join_group_call(chat_id, stream)
        except NoActiveGroupCall:
            try:
                await self.userbot1.join_chat(chat_id)
                await assistant.join_group_call(chat_id, stream)
            except (ChatAdminRequired, Exception) as e:
                raise AssistantErr(_["call_8"])
        except TelegramServerError:
            raise AssistantErr(_["call_10"])
        except Exception as e:
            raise AssistantErr(f"Unable to join group call: {str(e)}")

        await add_active_chat(chat_id)
        await music_on(chat_id)
        
        if video:
            await add_active_video_chat(chat_id)

        if await is_autoend():
            counter[chat_id] = {}
            users = len(await assistant.get_participants(chat_id))
            if users == 1:
                autoend[chat_id] = datetime.now() + timedelta(minutes=1)

    @capture_internal_err
    async def play(self, client: PyTgCalls, chat_id: int) -> None:
        """Play next track in queue."""
        try:
            check = db.get(chat_id, [])
            loop = await get_loop(chat_id)
            
            if loop == 0:
                if check:
                    popped = check.pop(0)
                    await auto_clean(popped)
            else:
                await set_loop(chat_id, loop - 1)
                
            if not check:
                await _clear_(chat_id)
                await client.leave_group_call(chat_id)
                return
                
        except Exception as e:
            LOGGER(__name__).error(f"Play error: {str(e)}")
            await _clear_(chat_id)
            try:
                await client.leave_group_call(chat_id)
            except Exception:
                pass
            return

        queued = check[0]["file"]
        language = await get_lang(chat_id)
        _ = get_string(language)
        title = check[0]["title"].title()
        user = check[0]["by"]
        original_chat_id = check[0]["chat_id"]
        streamtype = check[0]["streamtype"]
        videoid = check[0]["vidid"]
        db[chat_id][0]["played"] = 0

        # Restore original duration if speed was changed
        if "old_dur" in check[0]:
            db[chat_id][0]["dur"] = check[0]["old_dur"]
            db[chat_id][0]["seconds"] = check[0]["old_second"]
            db[chat_id][0]["speed_path"] = None
            db[chat_id][0]["speed"] = 1.0

        video = streamtype == "video"

        try:
            if "live_" in queued:
                n, link = await YouTube.video(videoid, True)
                if n == 0:
                    return await app.send_message(
                        original_chat_id,
                        text=_["call_6"],
                    )

                await client.change_stream(
                    chat_id,
                    dynamic_media_stream(link, video=video),
                )

                img = await get_thumb(videoid)
                button = stream_markup(_, chat_id)
                run = await app.send_photo(
                    chat_id=original_chat_id,
                    photo=img,
                    caption=_["stream_1"].format(
                        f"https://t.me/{app.username}?start=info_{videoid}",
                        title[:23],
                        check[0]["dur"],
                        user,
                    ),
                    reply_markup=InlineKeyboardMarkup(button),
                )
                db[chat_id][0]["mystic"] = run
                db[chat_id][0]["markup"] = "tg"

            elif "vid_" in queued:
                mystic = await app.send_message(
                    original_chat_id,
                    _["call_7"],
                )
                
                try:
                    file_path, direct = await YouTube.download(
                        videoid,
                        mystic,
                        videoid=True,
                        video=video,
                    )
                except Exception:
                    return await mystic.edit_text(
                        _["call_6"],
                        disable_web_page_preview=True,
                    )

                await client.change_stream(
                    chat_id,
                    dynamic_media_stream(file_path, video=video),
                )

                img = await get_thumb(videoid)
                button = stream_markup(_, chat_id)
                await mystic.delete()
                
                run = await app.send_photo(
                    chat_id=original_chat_id,
                    photo=img,
                    caption=_["stream_1"].format(
                        f"https://t.me/{app.username}?start=info_{videoid}",
                        title[:23],
                        check[0]["dur"],
                        user,
                    ),
                    reply_markup=InlineKeyboardMarkup(button),
                )
                db[chat_id][0]["mystic"] = run
                db[chat_id][0]["markup"] = "stream"

            elif "index_" in queued:
                await client.change_stream(
                    chat_id,
                    dynamic_media_stream(videoid, video=video),
                )

                button = stream_markup(_, chat_id)
                run = await app.send_photo(
                    chat_id=original_chat_id,
                    photo=config.STREAM_IMG_URL,
                    caption=_["stream_2"].format(user),
                    reply_markup=InlineKeyboardMarkup(button),
                )
                db[chat_id][0]["mystic"] = run
                db[chat_id][0]["markup"] = "tg"

            else:
                await client.change_stream(
                    chat_id,
                    dynamic_media_stream(queued, video=video),
                )

                if videoid == "telegram":
                    button = stream_markup(_, chat_id)
                    run = await app.send_photo(
                        chat_id=original_chat_id,
                        photo=(
                            config.TELEGRAM_AUDIO_URL
                            if streamtype == "audio"
                            else config.TELEGRAM_VIDEO_URL
                        ),
                        caption=_["stream_1"].format(
                            config.SUPPORT_CHAT,
                            title[:23],
                            check[0]["dur"],
                            user,
                        ),
                        reply_markup=InlineKeyboardMarkup(button),
                    )
                    db[chat_id][0]["mystic"] = run
                    db[chat_id][0]["markup"] = "tg"

                elif videoid == "soundcloud":
                    button = stream_markup(_, chat_id)
                    run = await app.send_photo(
                        chat_id=original_chat_id,
                        photo=config.SOUNCLOUD_IMG_URL,
                        caption=_["stream_1"].format(
                            config.SUPPORT_CHAT,
                            title[:23],
                            check[0]["dur"],
                            user,
                        ),
                        reply_markup=InlineKeyboardMarkup(button),
                    )
                    db[chat_id][0]["mystic"] = run
                    db[chat_id][0]["markup"] = "tg"

                else:
                    img = await get_thumb(videoid)
                    button = stream_markup(_, chat_id)
                    
                    try:
                        run = await app.send_photo(
                            chat_id=original_chat_id,
                            photo=img,
                            caption=_["stream_1"].format(
                                f"https://t.me/{app.username}?start=info_{videoid}",
                                title[:23],
                                check[0]["dur"],
                                user,
                            ),
                            reply_markup=InlineKeyboardMarkup(button),
                        )
                    except FloodWait as e:
                        await asyncio.sleep(e.value)
                        run = await app.send_photo(
                            chat_id=original_chat_id,
                            photo=img,
                            caption=_["stream_1"].format(
                                f"https://t.me/{app.username}?start=info_{videoid}",
                                title[:23],
                                check[0]["dur"],
                                user,
                            ),
                            reply_markup=InlineKeyboardMarkup(button),
                        )
                        
                    db[chat_id][0]["mystic"] = run
                    db[chat_id][0]["markup"] = "stream"

        except Exception as e:
            LOGGER(__name__).error(f"Stream error: {str(e)}")
            await app.send_message(
                original_chat_id,
                text=_["call_6"],
            )

    async def start(self) -> None:
        """Start all PyTgCalls clients."""
        LOGGER(__name__).info("Starting PyTgCalls Clients...")
        
        clients = [
            (self.one, config.STRING1),
            (self.two, config.STRING2),
            (self.three, config.STRING3),
            (self.four, config.STRING4),
            (self.five, config.STRING5),
        ]
        
        for client, string in clients:
            if string:
                try:
                    await client.start()
                except Exception as e:
                    LOGGER(__name__).error(f"Failed to start client: {str(e)}")

    @capture_internal_err
    async def ping(self) -> str:
        """Get average ping of all active clients."""
        pings = []
        clients = [
            self.one,
            self.two,
            self.three,
            self.four,
            self.five,
        ]
        
        for client in clients:
            if client:
                try:
                    pings.append(await client.ping)
                except Exception:
                    continue
                    
        return (
            str(round(sum(pings) / len(pings), 3))
            if pings
            else "0.0"
        )

    @capture_internal_err
    async def decorators(self) -> None:
        """Register update handlers for all clients."""
        CRITICAL_FLAGS = (
            ChatUpdate.Status.KICKED |
            ChatUpdate.Status.LEFT_GROUP |
            ChatUpdate.Status.CLOSED_VOICE_CHAT |
            ChatUpdate.Status.DISCARDED_CALL |
            ChatUpdate.Status.BUSY_CALL
        )

        async def unified_update_handler(client: PyTgCalls, update: Update) -> None:
            try:
                if isinstance(update, ChatUpdate):
                    if update.status & CRITICAL_FLAGS:
                        await self.stop_stream(update.chat_id)
                        return

                elif isinstance(update, StreamEnded):
                    assistant = await group_assistant(self, update.chat_id)
                    await self.play(assistant, update.chat_id)

            except Exception as e:
                LOGGER(__name__).error(f"Update handler error: {str(e)}")
                await send_large_error(
                    str(e),
                    "Stream Update Error",
                    f"update_error_{getattr(update, 'chat_id', 'unknown')}",
                )

        clients = [
            self.one,
            self.two,
            self.three,
            self.four,
            self.five,
        ]
        
        for client in filter(None, clients):
            client.on_update()(unified_update_handler)

    async def cleanup(self) -> None:
        """Cleanup temporary files and resources."""
        try:
            shutil.rmtree("playback", ignore_errors=True)
        except Exception as e:
            LOGGER(__name__).warning(f"Cleanup error: {str(e)}")


Hotty = Call()
