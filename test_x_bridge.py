import pytest
from unittest.mock import MagicMock, patch
import os
import shutil
from echo_bot import build_parser, handle_x_bridge_media, handle_xwitter_media, FILES_DIR


def test_parser_x_bridge_args():
    parser = build_parser()
    
    # Test --x-bridge-api
    args = parser.parse_args(["--x-bridge-api", "http://localhost:8000"])
    assert args.x_bridge_api == "http://localhost:8000"
    assert args.xwitter_api is None

    # Test alias --xbridge-api
    args = parser.parse_args(["--xbridge-api", "http://localhost:8000"])
    assert args.x_bridge_api == "http://localhost:8000"

    # Test alias --x-bridge
    args = parser.parse_args(["--x-bridge", "http://localhost:8000"])
    assert args.x_bridge_api == "http://localhost:8000"


def test_parser_xwitter_arg():
    parser = build_parser()
    args = parser.parse_args(["--xwitter-api", "http://localhost:5000/download"])
    assert args.xwitter_api == "http://localhost:5000/download"
    assert args.x_bridge_api is None


def test_parser_mutually_exclusive():
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args([
            "--xwitter-api", "http://localhost:5000/download",
            "--x-bridge-api", "http://localhost:8000"
        ])


def test_x_bridge_video_stream(tmp_path):
    bot = MagicMock()
    message = MagicMock()
    message.message_id = 99
    message.chat.id = 12345
    message.text = "Check this video: https://x.com/tester/status/123456"

    # Mock response for /extract-links
    mock_extract_resp = MagicMock()
    mock_extract_resp.status_code = 200
    mock_extract_resp.json.return_value = {
        "links": [{"canonical_url": "https://x.com/tester/status/123456"}],
        "total_found": 1,
    }

    # Mock response for /download/stream
    mock_stream_resp = MagicMock()
    mock_stream_resp.status_code = 200
    mock_stream_resp.headers = {"Content-Disposition": 'attachment; filename="tester_cool_video.mp4"'}
    mock_stream_resp.iter_content.return_value = [b"chunk1", b"chunk2"]

    with patch("requests.post") as mock_post:
        def post_side_effect(url, **kwargs):
            if url.endswith("/extract-links"):
                return mock_extract_resp
            elif url.endswith("/download/stream"):
                return mock_stream_resp
            return MagicMock(status_code=404)

        mock_post.side_effect = post_side_effect

        handle_x_bridge_media(
            bot=bot,
            message=message,
            x_bridge_api="http://localhost:8000",
            storage_mode="local",
            s3_storage_manager=None,
        )

        bot.send_chat_action.assert_called_with(12345, "upload_video")
        bot.reply_to.assert_called_with(message, "Media file downloaded and stored successfully")

        # Verify saved file
        expected_file = os.path.join(FILES_DIR, "x_bridge_media", "tester_cool_video.mp4")
        assert os.path.exists(expected_file)
        # Clean up
        if os.path.exists(expected_file):
            os.remove(expected_file)


def test_x_bridge_image_fallback():
    bot = MagicMock()
    message = MagicMock()
    message.message_id = 100
    message.chat.id = 12345
    message.text = "Photo post: https://x.com/artist/status/789012"

    mock_extract_resp = MagicMock()
    mock_extract_resp.status_code = 200
    mock_extract_resp.json.return_value = {
        "links": [{"canonical_url": "https://x.com/artist/status/789012"}],
        "total_found": 1,
    }

    # Video stream returns 400 No video media found
    mock_stream_resp = MagicMock()
    mock_stream_resp.status_code = 400
    mock_stream_resp.json.return_value = {"error_message": "No video media found in this post"}

    # /tweet returns media items
    mock_tweet_resp = MagicMock()
    mock_tweet_resp.status_code = 200
    mock_tweet_resp.json.return_value = {
        "id": "789012",
        "username": "artist",
        "media": [{"url": "http://img.example.com/pic.jpg", "type": "image", "format": "jpg"}],
    }

    mock_img_resp = MagicMock()
    mock_img_resp.status_code = 200
    mock_img_resp.content = b"fake-jpg-binary"

    with patch("requests.post") as mock_post, patch("requests.get") as mock_get:
        def post_side_effect(url, **kwargs):
            if url.endswith("/extract-links"):
                return mock_extract_resp
            elif url.endswith("/download/stream"):
                return mock_stream_resp
            elif url.endswith("/tweet"):
                return mock_tweet_resp
            return MagicMock(status_code=404)

        mock_post.side_effect = post_side_effect
        mock_get.return_value = mock_img_resp

        handle_x_bridge_media(
            bot=bot,
            message=message,
            x_bridge_api="http://localhost:8000",
            storage_mode="local",
            s3_storage_manager=None,
        )

        bot.reply_to.assert_called_with(message, "Media file downloaded and stored successfully")

        expected_file = os.path.join(FILES_DIR, "x_bridge_media", "artist_789012.jpg")
        assert os.path.exists(expected_file)
        if os.path.exists(expected_file):
            os.remove(expected_file)


def test_x_bridge_s3_storage():
    bot = MagicMock()
    message = MagicMock()
    message.message_id = 101
    message.chat.id = 12345
    message.text = "Video post https://twitter.com/news/status/55555"

    mock_extract_resp = MagicMock(status_code=200)
    mock_extract_resp.json.return_value = {
        "links": [{"canonical_url": "https://x.com/news/status/55555"}],
    }

    mock_stream_resp = MagicMock(status_code=200)
    mock_stream_resp.headers = {"Content-Disposition": 'attachment; filename="news_55555.mp4"'}
    mock_stream_resp.iter_content.return_value = [b"video-bytes"]

    mock_s3 = MagicMock()
    mock_s3.upload_file.return_value = "https://s3.example.com/bucket/x_bridge_media/permanent/news_55555.mp4"

    with patch("requests.post") as mock_post:
        mock_post.side_effect = lambda url, **kwargs: mock_extract_resp if "extract" in url else mock_stream_resp

        handle_x_bridge_media(
            bot=bot,
            message=message,
            x_bridge_api="http://localhost:8000",
            storage_mode="s3",
            s3_storage_manager=mock_s3,
        )

        mock_s3.upload_file.assert_called_once()
        bot.reply_to.assert_called_with(message, "Media file downloaded and stored successfully")
