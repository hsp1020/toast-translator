from cache import Cache
from datetime import timedelta
import httpx
import anime.anime_mapping as anime_mapping

kitsu_addon_url = 'https://anime-kitsu.strem.fun'

# Cache load
mal_cache = Cache(maxsize=float('inf'), ttl=timedelta(days=30).total_seconds())
mal_cache.clear()

# Anime mapping loading
imdb_ids_map = None
imdb_map = None

def load_anime_map():
	global imdb_ids_map, imdb_map
	# Load MAL -> IMDB converter
	imdb_map = anime_mapping.load_mal_map()
	for mal_id, imdb_id in imdb_map.items():
		mal_cache.set(f"mal:{mal_id}", imdb_id)

	# Load season / episode map
	imdb_ids_map = anime_mapping.load_imdb_map()

load_anime_map()

async def convert_to_imdb(mal_id: str, type: str) -> str:
	is_converted = False
	imdb_id = mal_cache.get(mal_id)
	if imdb_id == None:
		async with httpx.AsyncClient(follow_redirects=True, timeout=20) as client:
			response = await client.get(f"{kitsu_addon_url}/meta/{type}/{mal_id.replace(':','%3A')}.json")
			try:
				imdb_id = response.json()['meta']['imdb_id']
				mal_cache.set(mal_id, imdb_id)
				is_converted = True
			except:
				# If imdb_id not found save mal_id as imdb_id (better performance)
				mal_cache.set(mal_id, mal_id)
				return mal_id, is_converted
	else:
		if 'tt' not in imdb_id:
			is_converted = False
		else:
			is_converted = True

	return imdb_id, is_converted