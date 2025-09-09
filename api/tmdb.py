from cache import Cache
from datetime import timedelta
import httpx
import os
import asyncio

#from dotenv import load_dotenv
#load_dotenv()

TMDB_POSTER_URL = 'https://image.tmdb.org/t/p/w500'
TMDB_BACK_URL = 'https://image.tmdb.org/t/p/original'
TMDB_API_KEY = os.getenv('TMDB_API_KEY')

# Cache set
tmp_cache = Cache(maxsize=100000, ttl=timedelta(days=7).total_seconds())
tmp_cache.clear()


# Too many requests retry
async def fetch_and_retry(client: httpx.AsyncClient, id: str, url: str, params={}, max_retries=10) -> dict:
    headers = {
        "accept": "application/json"
    }

    for attempt in range(1, max_retries + 1):
        response = await client.get(url, headers=headers, params=params)

        if response.status_code == 200:
            meta_dict = response.json()
            meta_dict['imdb_id'] = id
            tmp_cache.set(id, meta_dict)
            return meta_dict

        elif response.status_code == 429:
            print(response)
            await asyncio.sleep(attempt * 2)

    return {}


# Get from external source id
async def get_tmdb_data(client: httpx.AsyncClient, id: str, source: str) -> dict:
    params = {
        "external_source": source,
        "language": "ko-KR",
        "api_key": TMDB_API_KEY
    }

    url = f"https://api.themoviedb.org/3/find/{id}"
    item = tmp_cache.get(id)

    if item != None:
        return item
    else:
        return await fetch_and_retry(client, id, url, params)
    

# Get movie detail with cast video and images
async def get_movie_details(client: httpx.AsyncClient, id: str) -> dict:
    params = {
        "api_key": TMDB_API_KEY,
        "language": "ko-KR",
        "append_to_response": "credits,videos,images",
        "include_image_language": "it,null"
    }
    url = f"https://api.themoviedb.org/3/movie/{id}"
    return await fetch_and_retry(client, id, url, params=params)


# Get series detail with cast video and images
async def get_series_details(client: httpx.AsyncClient, id: str) -> dict:
    params = {
        "api_key": TMDB_API_KEY,
        "language": "ko-KR",
        "append_to_response": "external_ids,credits,videos,images",
        "include_image_language": "it,null"
    }
    url = f"https://api.themoviedb.org/3/tv/{id}"
    return await fetch_and_retry(client, id, url, params=params)


# Get series detail with cast video and images
async def get_season_details(client: httpx.AsyncClient, season_id: str, season_number) -> dict:
    params = {
        "language": "ko-KR",
        "append_to_response": "external_ids",
        "api_key": TMDB_API_KEY
    }

    url = f"https://api.themoviedb.org/3/tv/{season_id}/season/{season_number}"
    return await fetch_and_retry(client, id, url, params)

# Converting imdb id to tmdb id
async def convert_imdb_to_tmdb(imdb_id: str) -> str:

    tmdb_data = tmp_cache.get(imdb_id)

    if tmdb_data != None:
        return get_id(tmdb_data)
    else:
        async with httpx.AsyncClient(timeout=20) as client:
            tmdb_data = await get_tmdb_data(client, imdb_id, 'imdb_id')
            return get_id(tmdb_data)
        

# Search and parse id
def get_id(tmdb_data: dict) -> str:
    try:
        id = next((v[0]["id"] for v in tmdb_data.values() if v), None)
    except:
        return tmdb_data['imdb_id']
    else:
        return f"tmdb:{id}"
