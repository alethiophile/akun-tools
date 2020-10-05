#!python3

from quart import render_template, request, Response, stream_with_context
from quart_trio import QuartTrio
from .scraper import AkunData, AkunStory, make_filename
import qtoml
from json import dumps
from itertools import chain

from typing import AsyncIterator, Dict, Any

app = QuartTrio(__name__)

@app.route('/')
async def index() -> str:
    return await render_template('client.html')

DOWNLOAD_INT = 1.0

@app.route('/download', methods=['POST'])
async def download_quest() -> Response:
    url = (await request.form)['quest_url']
    json_spacer = ';\n'

    def encode_stream(s: AsyncIterator[str]):
        async def wrapper() -> AsyncIterator[bytes]:
            async for i in s():
                yield i.encode()
        return wrapper

    @stream_with_context
    @encode_stream
    async def flask_stream() -> AsyncIterator[str]:
        data_obj = AkunData(download_delay=DOWNLOAD_INT, vl=2)
        node = await data_obj.get_metadata(url)
        data: Dict[str, Any] = {
            'title': AkunData.get_title(node),
            'author': AkunData.get_author(node),
            'chapter_count': AkunData.get_chapter_count(node)
        }
        data['toml_fn'] = make_filename(data['title']) + '.toml'
        data['html_fn'] = make_filename(data['title']) + '.html'
        data['zip_fn'] = make_filename(data['title']) + '.zip'
        node['original_url'] = url
        data['toml'] = qtoml.dumps(node, encode_none=0) + '\n'
        yield dumps(data) + json_spacer
        del data

        current_toml = ''
        chapnum = 0

        # this is a tricky hack: in order to process each chapter separately
        # for its TOML data while still passing the chapters iterator to
        # gen_html(), we wrap the chapters iterator in this function, which
        # provides all the chapter objects while also doing TOML encoding and
        # passing the results through the nonlocal variable
        async def provide_chapters() -> AsyncIterator[Dict[str, Any]]:
            nonlocal current_toml, chapnum
            async for cl in data_obj.get_chapters(node, url):
                chapnum += 1
                for chapter in cl:
                    current_toml = qtoml.dumps({ 'chapters': [chapter] },
                                               encode_none=0) + '\n'
                    yield chapter

        # story_obj = AkunStory(None, download_delay=DOWNLOAD_INT, vl=2)
        async for html, imgs in AkunStory.gen_html(node, provide_chapters()):
            # this code is guaranteed to run between iterations of
            # provide_chapters(), so TOML data is never lost
            response = {
                'html': html,
                'imgs': imgs,
                'toml': current_toml,
                'chapnum': chapnum,
            }
            if current_toml != '':
                current_toml = ''
            yield dumps(response) + json_spacer
        yield dumps({ 'status': 'done' }) + json_spacer

    rv = Response(flask_stream(),
                  mimetype='text/plain')
    rv.timeout = None  # for large quests this can take a long time
    return rv
