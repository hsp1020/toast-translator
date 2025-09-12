from fastapi import FastAPI, Request, Response
from fastapi.templating import Jinja2Templates
from fastapi.responses import RedirectResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
from datetime import timedelta
from cache import Cache
from anime import kitsu, mal
import meta_merger
import meta_builder
import translator
import asyncio
import httpx
from api import tmdb
import base64
import os

# Settings
translator_version = 'v0.1.1'
FORCE_PREFIX = False
FORCE_META = False
USE_TMDB_ID_META = True
USE_TMDB_ADDON = False
REQUEST_TIMEOUT = 120
COMPATIBILITY_ID = ['tt', 'kitsu', 'mal']

# Cache set
meta_cache = Cache(maxsize=100000, ttl=timedelta(hours=12).total_seconds())
meta_cache.clear()


# Server start
@asynccontextmanager
async def lifespan(app: FastAPI):
    print('Started')
    yield
    print('Shutdown')

app = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")


stremio_headers = {
    'connection': 'keep-alive', 
    'user-agent': 'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) QtWebEngine/5.15.2 Chrome/83.0.4103.122 Safari/537.36 StremioShell/4.4.168', 
    'accept': '*/*', 
    'origin': 'https://app.strem.io', 
    'sec-fetch-site': 'cross-site', 
    'sec-fetch-mode': 'cors', 
    'sec-fetch-dest': 'empty', 
    'accept-encoding': 'gzip, deflate, br'
}

tmdb_addons_pool = [
    'https://hsptmdb4.duckdns.org/%7B%22provide_imdbId%22%3A%22true%22%2C%22language%22%3A%22ko-KR%22%7D' # Madari    
    'https://tmdb.elfhosted.com/%7B%22provide_imdbId%22%3A%22true%22%2C%22language%22%3A%22ko-KR%22%7D', # Elfhosted
    'https://94c8cb9f702d-tmdb-addon.baby-beamup.club/%7B%22provide_imdbId%22%3A%22true%22%2C%22language%22%3A%22ko-KR%22%7D', # Official
]

tmdb_addon_meta_url = tmdb_addons_pool[0]
cinemeta_url = 'https://v3-cinemeta.strem.io'

def json_response(data):
    response = JSONResponse(data)
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Headers'] = '*'
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    response.headers["Surrogate-Control"] = "no-store"
    return response


@app.get('/', response_class=HTMLResponse)
async def home(request: Request):
    response = templates.TemplateResponse("configure.html", {"request": request})
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    response.headers["Surrogate-Control"] = "no-store"
    return response

@app.get('/{addon_url}/{user_settings}/configure')
async def configure(addon_url):
    addon_url = decode_base64_url(addon_url) + '/configure'
    return RedirectResponse(addon_url)

@app.get('/link_generator', response_class=HTMLResponse)
async def link_generator(request: Request):
    response = templates.TemplateResponse("link_generator.html", {"request": request})
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    response.headers["Surrogate-Control"] = "no-store"
    return response


@app.get('/{addon_url}/{user_settings}/manifest.json')
async def get_manifest(addon_url):
    addon_url = decode_base64_url(addon_url)
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        response = await client.get(f"{addon_url}/manifest.json")
        manifest = response.json()

    is_translated = manifest.get('translated', False)
    if not is_translated:
        manifest['translated'] = True
        manifest['t_language'] = 'ko-KR'
        manifest['name'] += ' 🇮🇹'

        if 'description' in manifest:
            manifest['description'] += f" | Tradotto da Toast Translator. {translator_version}"
        else:
            manifest['description'] = f"Tradotto da Toast Translator. {translator_version}"
    
    if FORCE_PREFIX:
        if 'idPrefixes' in manifest:
            if 'tmdb:' not in manifest['idPrefixes']:
                manifest['idPrefixes'].append('tmdb:')
            if 'tt' not in manifest['idPrefixes']:
                manifest['idPrefixes'].append('tt')

    if FORCE_META:
        if 'meta' not in manifest['resources']:
            manifest['resources'].append('meta')

    return json_response(manifest)


@app.get('/{addon_url}/{user_settings}/catalog/{type}/{path:path}')
async def get_catalog(response: Response, addon_url, type: str, user_settings: str, path: str):
    # Cinemeta last-videos
    if 'last-videos' in path:
        return RedirectResponse(f"{cinemeta_url}/catalog/{type}/{path}")
    
    user_settings = parse_user_settings(user_settings)
    addon_url = decode_base64_url(addon_url)

    async with httpx.AsyncClient(follow_redirects=True, timeout=REQUEST_TIMEOUT) as client:
        response = await client.get(f"{addon_url}/catalog/{type}/{path}")

        try:
            catalog = response.json()
        except:
            print(response.text)
            return json_response({})

        if 'metas' in catalog:
            if type == 'anime':
                await remove_duplicates(catalog)
            tasks = [
                tmdb.get_tmdb_data(client, item.get('imdb_id', item.get('id')), "imdb_id") for item in catalog['metas']
            ]
            tmdb_details = await asyncio.gather(*tasks)
        else:
            return json_response({})

    new_catalog = translator.translate_catalog(catalog, tmdb_details, user_settings['sp'], user_settings['tr'])
    return json_response(new_catalog)


@app.get('/{addon_url}/{user_settings}/meta/{type}/{id}.json')
async def get_meta(request: Request,response: Response, addon_url, type: str, id: str):
    headers = dict(request.headers)
    del headers['host']
    addon_url = decode_base64_url(addon_url)
    global tmdb_addon_meta_url
    async with httpx.AsyncClient(follow_redirects=True, timeout=REQUEST_TIMEOUT) as client:

        # Get from cache
        meta = meta_cache.get(id)

        # Return cached meta
        if meta != None:
            return json_response(meta)

        # Not in cache
        else:
            # Handle imdb ids
            if 'tt' in id:
                tmdb_id = await tmdb.convert_imdb_to_tmdb(id)
                tasks = [
                    client.get(f"{tmdb_addon_meta_url}/meta/{type}/{tmdb_id}.json") if USE_TMDB_ADDON else meta_builder.build_metadata(id, type),
                    client.get(f"{cinemeta_url}/meta/{type}/{id}.json")
                ]
                metas = await asyncio.gather(*tasks)
                
                if USE_TMDB_ADDON:
                    # TMDB addon retry and switch addon
                    for retry in range(6):
                        if metas[0].status_code == 200:
                            tmdb_meta = metas[0].json()
                            break
                        else:
                            index = tmdb_addons_pool.index(tmdb_addon_meta_url)
                            tmdb_addon_meta_url = tmdb_addons_pool[(index + 1) % len(tmdb_addons_pool)]
                            metas[0] = await client.get(f"{tmdb_addon_meta_url}/meta/{type}/{tmdb_id}.json")
                            if metas[0].status_code == 200:
                                tmdb_meta = metas[0].json()
                                break
                else:
                    tmdb_meta = metas[0]

                cinemeta_meta = metas[1].json()
                
                # Not empty tmdb meta
                if len(tmdb_meta.get('meta', [])) > 0:
                    # Not merge anime
                    if id not in kitsu.imdb_ids_map:
                        tasks = []
                        meta, merged_videos = meta_merger.merge(tmdb_meta, cinemeta_meta)
                        tmdb_description = tmdb_meta['meta'].get('description', '')
                        
                        if tmdb_description == '':
                            tasks.append(translator.translate_with_api(client, meta['meta'].get('description', '')))

                        if type == 'series' and (len(meta['meta']['videos']) < len(merged_videos)):
                            tasks.append(translator.translate_episodes(client, merged_videos))

                        translated_tasks = await asyncio.gather(*tasks)
                        for task in translated_tasks:
                            if isinstance(task, list):
                                meta['meta']['videos'] = task
                            elif isinstance(task, str):
                                meta['meta']['description'] = task
                    else:
                        meta = tmdb_meta

                # Empty tmdb_data
                else:
                    if len(cinemeta_meta.get('meta', [])) > 0:
                        meta = cinemeta_meta
                        description = meta['meta'].get('description', '')
                        
                        if type == 'series':
                            tasks = [
                                translator.translate_with_api(client, description),
                                translator.translate_episodes(client, meta['meta']['videos'])
                            ]
                            description, episodes = await asyncio.gather(*tasks)
                            meta['meta']['videos'] = episodes

                        elif type == 'movie':
                            description = await translator.translate_with_api(client, description)

                        meta['meta']['description'] = description
                    
                    # Empty cinemeta and tmdb return empty meta
                    else:
                        return json_response({})
                    
                
            # Handle kitsu and mal ids
            elif 'kitsu' in id or 'mal' in id:
                # Try convert kitsu to imdb
                if 'kitsu' in id:
                    imdb_id, is_converted = await kitsu.convert_to_imdb(id, type)
                else:
                    imdb_id, is_converted = await mal.convert_to_imdb(id.replace('_',':'), type)

                if is_converted:

                    if USE_TMDB_ADDON:
                        tmdb_id = await tmdb.convert_imdb_to_tmdb(imdb_id)
                        # TMDB Addons retry
                        for retry in range(6):
                            response = await client.get(f"{tmdb_addon_meta_url}/meta/{type}/{tmdb_id}.json")
                            if response.status_code == 200:
                                meta = response.json()
                                break
                            else:
                                # Loop addon pool
                                index = tmdb_addons_pool.index(tmdb_addon_meta_url)
                                tmdb_addon_meta_url = tmdb_addons_pool[(index + 1) % len(tmdb_addons_pool)]
                                print(f"Switch to {tmdb_addon_meta_url}")
                    else:
                        meta = await meta_builder.build_metadata(imdb_id, type)

                    if len(meta['meta']) > 0:
                        if type == 'movie':
                            meta['meta']['behaviorHints']['defaultVideoId'] = id
                        elif type == 'series':
                            videos = kitsu.parse_meta_videos(meta['meta']['videos'], imdb_id)
                            meta['meta']['videos'] = videos
                    else:
                        # Get meta from kitsu addon
                        response = await client.get(f"{kitsu.kitsu_addon_url}/meta/{type}/{id.replace(':','%3A')}.json")
                        meta = response.json()
                else:
                    # Get meta from kitsu addon
                    response = await client.get(f"{kitsu.kitsu_addon_url}/meta/{type}/{id.replace(':','%3A')}.json")
                    meta = response.json()

            # Not compatible id -> redirect to original addon
            else:
                return RedirectResponse(f"{addon_url}/meta/{type}/{id}.json")


            meta['meta']['id'] = id
            meta_cache.set(id, meta)
            return json_response(meta)


# Subs redirect
@app.get('/{addon_url}/{user_settings}/subtitles/{path:path}')
async def get_subs(addon_url, path: str):
    addon_url = decode_base64_url(addon_url)
    return RedirectResponse(f"{addon_url}/subtitles/{path}")

# Stream redirect
@app.get('/{addon_url}/{user_settings}/stream/{path:path}')
async def get_subs(addon_url, path: str):
    addon_url = decode_base64_url(addon_url)
    return RedirectResponse(f"{addon_url}/stream/{path}")

# Anime map reloader
@app.get('/map_reload')
async def reload_anime_mapping():
    pass

def decode_base64_url(encoded_url):
    padding = '=' * (-len(encoded_url) % 4)
    encoded_url += padding
    decoded_bytes = base64.b64decode(encoded_url)
    return decoded_bytes.decode('utf-8')


# Anime only
async def remove_duplicates(catalog) -> None:
    unique_items = []
    seen_ids = set()
    
    for item in catalog['metas']:

        if 'kitsu' in item['id']:
            item['imdb_id'], is_converted = await kitsu.convert_to_imdb(item['id'], item['type'])

        elif 'mal_' in item['id']:
            item['imdb_id'], is_converted = await mal.convert_to_imdb(item['id'].replace('_',':'), item['type'])

        if item['imdb_id'] not in seen_ids:
            unique_items.append(item)
            seen_ids.add(item['imdb_id'])

    catalog['metas'] = unique_items


def parse_user_settings(user_settings: str) -> dict:
    settings = user_settings.split(',')
    _user_settings = {}

    for setting in settings:
        key, value = setting.split('=')
        _user_settings[key] = value
    
    return _user_settings


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=int(os.environ.get("PORT", 8080)))
