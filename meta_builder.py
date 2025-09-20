from api import tmdb
from api import tvdb
from api import fanart
from anime import kitsu
import httpx
import asyncio
import urllib.parse
import translator
import math
import json

REQUEST_TIMEOUT = 100
MAX_CAST_SEARCH = 3
TMDB_ERROR_EPISODE_OFFSET = 50

# Load TMDB exceptions
with open("anime/tmdb_exceptions.json", "r", encoding="utf-8") as f:
    TMDB_EXCEPTIONS = json.load(f) 


async def build_metadata(video_id: str, type: str):
    tmdb_id = None

    # IMDb ID 처리
    if isinstance(video_id, str) and 'tt' in video_id:
        tmdb_id = await tmdb.convert_imdb_to_tmdb(video_id)

    # TMDB ID 처리
    if isinstance(video_id, str) and 'tmdb:' in video_id:
        tmdb_id = video_id.replace('tmdb:', '')
    elif isinstance(tmdb_id, str) and 'tmdb:' in tmdb_id:
        tmdb_id = tmdb_id.replace('tmdb:', '')

    async with httpx.AsyncClient(follow_redirects=True, timeout=REQUEST_TIMEOUT) as client:
        if type == 'movie':
            parse_title = 'title'
            default_video_id = video_id
            has_scheduled_videos = False
            tasks = [
                tmdb.get_movie_details(client, tmdb_id),
                fanart.get_fanart_movie(client, tmdb_id)
            ]
        elif type == 'series':
            parse_title = 'name'
            default_video_id = None
            has_scheduled_videos = True
            tasks = [
                tmdb.get_series_details(client, tmdb_id),
                fanart.get_fanart_series(client, tmdb_id)
            ]
        
        data = await asyncio.gather(*tasks)
        tmdb_data, fanart_data = data[0], data[1]
        if not tmdb_data:
            return {"meta": {}}

        title = tmdb_data.get(parse_title, '')
        slug = f"{type}/{title.lower().replace(' ', '-')}-{tmdb_data.get('imdb_id', '').replace('tt', '')}"
        logo = extract_logo(fanart_data, tmdb_data)
        directors, writers = extract_crew(tmdb_data)
        cast = extract_cast(tmdb_data)
        genres = extract_genres(tmdb_data)
        year = extract_year(tmdb_data, type)
        trailers = extract_trailers(tmdb_data)
        rating = f"{tmdb_data.get('vote_average', 0):.1f}" if tmdb_data.get('vote_average') else ''

        meta = {
            "meta": {
                "imdb_id": tmdb_data.get('imdb_id',''),
                "name": title,
                "type": type,
                "cast": cast,
                "country": tmdb_data.get('origin_country', [''])[0],
                "description": tmdb_data.get('overview', ''),
                "director": directors,
                "genre": genres,
                "imdbRating": rating,
                "released": (tmdb_data.get('release_date', 'TBA') if type == 'movie' else tmdb_data.get('first_air_date', 'TBA')) + 'T00:00:00.000Z',
                "slug": slug,
                "writer": writers,
                "year": year,
                "poster": tmdb.TMDB_POSTER_URL + tmdb_data.get('poster_path', ''),
                "background": tmdb.TMDB_BACK_URL + tmdb_data.get('backdrop_path', ''),
                "logo": logo,
                "runtime": (str(tmdb_data.get('runtime','')) + ' min') if type == 'movie' else extract_series_episode_runtime(tmdb_data),
                "id": 'tmdb:' + str(tmdb_data.get('id', '')),
                "genres": genres,
                "releaseInfo": year,
                "trailerStreams": trailers,
                "links": build_links(video_id, title, slug, rating, cast, writers, directors, genres),
                "behaviorHints": {
                    "defaultVideoId": default_video_id,
                    "hasScheduledVideos": has_scheduled_videos
                }
            }
        }

        # 시리즈일 경우 episodes 생성
        if type == 'series':
            meta['meta']['videos'] = await series_build_episodes(
                client,
                video_id,
                tmdb_id,
                tmdb_data.get('seasons', []),
                tmdb_data['external_ids'].get('tvdb_id'),
                tmdb_data.get('number_of_episodes', 0)
            )

        return meta


async def series_build_episodes(client: httpx.AsyncClient, video_id: str, tmdb_id: str, seasons: list, tvdb_series_id: int, tmdb_episodes_count: int) -> list:
    tasks = []
    videos = []

    # TMDB 시즌 상세 정보 가져오기
    for season in seasons:
        tasks.append(tmdb.get_season_details(client, tmdb_id, season['season_number']))

    tmdb_seasons = await asyncio.gather(*tasks)

    # Anime TVDB 매핑
    if isinstance(video_id, str) and (('kitsu' in video_id) or ('mal' in video_id) or video_id in kitsu.imdb_ids_map) and video_id not in TMDB_EXCEPTIONS:
        # TVDB 데이터 사용
        episodes_tasks = []
        abs_episode_count = tmdb_episodes_count + TMDB_ERROR_EPISODE_OFFSET
        total_pages = math.ceil(abs_episode_count / tvdb.EPISODE_PAGE)
        for i in range(max(1, total_pages)):
            episodes_tasks.append(tvdb.get_translated_episodes(client, tvdb_series_id, i))

        translated_episodes = []
        episodes_tasks_result = await asyncio.gather(*episodes_tasks)
        for result in episodes_tasks_result:
            translated_episodes.extend(result['data']['episodes'])

        for episode in translated_episodes:
            video = {
                "name": f"Episodio {episode['number']}" if not episode['name'] else episode['name'],
                "season": episode['seasonNumber'],
                "number": episode['number'],
                "firstAired": episode['aired'] + 'T05:00:00.000Z' if episode['aired'] else None,
                "rating": "0",
                "overview": '' if not episode['overview'] else episode['overview'],
                "thumbnail": tvdb.IMAGE_URL + episode['image'] if episode['image'] else None,
                "id": f"{video_id}:{episode['seasonNumber']}:{episode['number']}",
                "released": episode['aired'] + 'T05:00:00.000Z' if episode['aired'] else None,
                "episode": episode['number'],
                "description": ''
            }

            if episode['seasonNumber'] != 0 and (not episode['name'] or not episode['overview']):
                video['tvdb_id'] = episode['id']

            videos.append(video)

        return await translator.translate_episodes(client, videos)

    # TMDB 에피소드 빌더
    for season in tmdb_seasons:
        for episode_number, episode in enumerate(season.get('episodes', []), start=1):
            videos.append(
                {
                    "name": episode.get('name', ''),
                    "season": episode.get('season_number', 0),
                    "number": episode_number,
                    "firstAired": episode.get('air_date', None) + 'T05:00:00.000Z' if episode.get('air_date') else None,
                    "rating": str(episode.get('vote_average', 0)),
                    "overview": episode.get('overview', ''),
                    "thumbnail": tmdb.TMDB_BACK_URL + episode['still_path'] if episode.get('still_path') else None,
                    "id": f"{video_id}:{episode.get('season_number', 0)}:{episode_number}",
                    "released": episode.get('air_date', None) + 'T05:00:00.000Z' if episode.get('air_date') else None,
                    "episode": episode_number,
                    "description": episode.get('overview', '')
                }
            )

    return videos


def extract_series_episode_runtime(tmdb_data: dict) -> str:
    runtime = 0
    if tmdb_data.get('episode_run_time'):
        runtime = tmdb_data['episode_run_time'][0]
    elif tmdb_data.get('last_episode_to_air'):
        runtime = tmdb_data['last_episode_to_air'].get('runtime', 'N/A')

    return str(runtime) + ' min'


def extract_logo(fanart_data: dict, tmdb_data: dict) -> str:
    logos = tmdb_data.get('images', {}).get('logos', [])
    for logo in logos:
        logo_path = logo.get('file_path', '')
        if logo.get('iso_639_1') == 'ko' and not logo_path.lower().endswith('.svg'):
            return tmdb.TMDB_POSTER_URL + logo_path
    return ''


def extract_cast(tmdb_data: dict):
    cast = []
    for person in tmdb_data.get('credits', {}).get('cast', [])[:MAX_CAST_SEARCH]:
        if person.get('known_for_department') == 'Acting':
            cast.append(person.get('name'))
    return cast


def extract_crew(tmdb_data: dict):
    directors = []
    writers = []
    for person in tmdb_data.get('credits', {}).get('crew', []):
        if person.get('department') == 'Writing' and person.get('name') not in writers:
            writers.append(person.get('name'))
        elif person.get('known_for_department') == 'Directing' and person.get('job', '') == 'Director' and person.get('name') not in directors:
            directors.append(person.get('name'))
    return directors, writers


def extract_genres(tmdb_data: dict) -> list:
    return [genre.get('name') for genre in tmdb_data.get('genres', [])]


def extract_year(tmdb_data: dict, type: str):
    try:
        if type == 'movie':
            return tmdb_data.get('release_date', '').split('-')[0]
        elif type == 'series':
            first_air = tmdb_data.get('first_air_date', '').split('-')[0]
            last_air = tmdb_data.get('last_air_date', '') if tmdb_data.get('status') == 'Ended' else ''
            return f"{first_air}-{last_air}"
    except:
        return ''


def extract_trailers(tmdb_data):
    videos = tmdb_data.get('videos', {"results": []})
    trailers = []
    for video in videos.get('results', []):
        if video.get('type') == 'Trailer' and video.get('site') == 'YouTube':
            trailers.append({"title": video.get('name'), "ytId": video.get('key')})
    return trailers


def build_links(video_id: str, title: str, slug: str, rating: str, cast: list, writers: list, directors: str, genres: list) -> list:
    links = [
        {"name": rating, "category": "imdb", "url": f"https://imdb.com/title/{video_id}"},
        {"name": title, "category": "share", "url": f"https://www.strem.io/s/movie/{slug}"}
    ]

    for genre in genres:
        links.append({"name": genre, "category": "Genres",
                      "url": f"stremio:///discover/https%3A%2F%2FPLACEHOLDER%2Fmanifest.json/movie/top?genre={urllib.parse.quote(genre)}"})

    for actor in cast:
        links.append({"name": actor, "category": "Cast", "url": f"stremio:///search?search={urllib.parse.quote(actor)}"})

    for writer in writers:
        links.append({"name": writer, "category": "Writers", "url": f"stremio:///search?search={urllib.parse.quote(writer)}"})

    for director in directors:
        links.append({"name": director, "category": "Directors", "url": f"stremio:///search?search={urllib.parse.quote(director)}"})

    return links
