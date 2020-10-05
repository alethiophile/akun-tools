#!/usr/bin/env python3

import asks, trio, re, sys
from bs4 import BeautifulSoup
import zipfile, time, os, datetime
import json, qtoml
import click, magic, math, traceback
from operator import itemgetter
from urllib.parse import urlsplit

from typing import (Tuple, Dict, Optional, Any, Callable, List,
                    AsyncIterator, Set)

import logging
logger = logging.getLogger(__name__)
l_h = logging.StreamHandler(sys.stdout)
l_h.setFormatter(logging.Formatter('%(asctime)s: %(task_name)s: '
                                   '%(levelname)s: %(message)s'))
logger.addHandler(l_h)

class TrioTaskFilter(logging.Filter):
    def filter(self, rec):
        try:
            name = trio.lowlevel.current_task().name
        except RuntimeError:
            name = '(trio not running)'
        rec.task_name = name

        return True
logger.addFilter(TrioTaskFilter())

def make_filename(title: str) -> str:
    title = title.lower().replace(" ", "_")
    return re.sub("[^a-z0-9_]", "", title)

def url_get_hostname(url: str) -> str:
    u = urlsplit(url)
    assert u.hostname is not None
    return u.hostname

class Downloader:
    """This class mediates all downloads by the program and enforces that at least
    download_delay seconds pass between requests to a single host. This is safe
    to use from multiple tasks; the download_retry method will block until the
    necessary wait has elapsed.

    """
    def __init__(self, download_delay: float = 1.0) -> None:
        self.download_delay = download_delay
        self.last_download: Dict[str, float] = {}
        self.locks: Dict[str, trio.CapacityLimiter] = {}
        self.urls_waiting: Dict[str, Set[str]] = {}

    async def download_retry(self, url: str, tries: int = 3
                             ) -> asks.response_objects.Response:
        host = url_get_hostname(url)
        limit = self.locks.setdefault(host, trio.CapacityLimiter(1))
        self.urls_waiting.setdefault(host, set()).add(url)
        logger.debug(f"waiting on limiter for host {host} "
                     f"({len(self.urls_waiting[host])} waiting)")
        try:
            async with limit:
                logger.debug(f"woke up for {url}")
                ld = self.last_download.get(host, 0.0)
                await trio.sleep_until(ld + self.download_delay)
                for i in range(tries):
                    r = await asks.get(url)
                    now = trio.current_time()
                    self.last_download[host] = now
                    try:
                        r.raise_for_status()
                    except Exception as e:
                        if i >= tries - 1:
                            raise
                        logger.debug(f"got {e}, retrying (#{i+1})")
                        err_delay = self.download_delay * (2 ** (i + 2))
                        err_delay = min(err_delay, 30.0)
                        await trio.sleep(err_delay)
                    else:
                        return r
                return r
        except BaseException:
            print(self.urls_waiting)
            raise
        finally:
            self.urls_waiting[host].remove(url)

class AkunData:
    node_url = 'https://fiction.live/api/node/{story_id}'
    story_url = ('https://fiction.live/api/anonkun/chapters'
                 '/{story_id}/{start}/{end}')
    chat_pages_url = 'https://fiction.live/api/chat/pages'
    chat_page_url = 'https://fiction.live/api/chat/page'

    url_re = re.compile(r"https://fiction.live/stories/[^/]+/([^/]+)/?")

    @classmethod
    def id_from_url(cls, url: str) -> str:
        o = cls.url_re.match(url)
        if o is None:
            raise ValueError("bad url {}".format(url))
        return o.group(1)

    def __init__(self, download_delay: float = 0.5, vl: int = 1,
                 output: Callable[[str], None] = print) -> None:
        self.vl = vl
        if not output:
            # a no-op
            output = lambda *args, **kwargs: None
        self.output = output
        self.download_delay = download_delay
        self._url: Optional[str] = None
        self.downloader = Downloader(download_delay=self.download_delay)
        self.story_info: Optional[Dict[str, Any]] = None

    async def get_metadata(self, url: Optional[str] = None) -> Dict[str, Any]:
        if url is None:
            url = self.url

        story_id = self.id_from_url(url)
        mu = self.node_url.format(story_id=story_id)
        logger.debug(f"Metadata URL: {mu}")
        r = await self.downloader.download_retry(mu)
        r.raise_for_status()
        node = r.json()
        return node

    async def get_chapters(self, node: Dict[str, Any],
                           url: str) -> AsyncIterator[List[Dict[str, Any]]]:
        vl, output = self.vl, self.output
        num_chaps = self.get_chapter_count(node)
        logger.info(f"Found {num_chaps} chapters")
        chapter_starts = [i['ct'] for i in node['bm']]
        chapter_starts[0] = 0
        chapter_starts.append(9999999999999999)
        story_id = self.id_from_url(url)
        for i, (s, e) in enumerate([(chapter_starts[i],
                                     chapter_starts[i + 1] - 1)
                                    for i in range(len(node['bm']))]):
            su = self.story_url.format(story_id=story_id,
                                       start=s, end=e)
            if vl == 1:
                print(f"chapter {i+1}/{num_chaps}", end='\r')
            elif vl >= 2:
                print(f"Chapter {i+1}/{num_chaps} URL: {su}")
            # await trio.sleep(self.download_delay)
            r = await self.downloader.download_retry(su)
            yield r.json()

    async def download(self, url: Optional[str] = None) -> None:
        vl, output = self.vl, self.output
        if url is None:
            url = self.url

        node = await self.get_metadata(url)

        if vl >= 3:
            output(str(node))

        story = []
        async for c in self.get_chapters(node, url):
            story.extend(c)
        node['original_url'] = url
        node['chapters'] = story
        self.story_info = node

    async def download_chat(self) -> None:
        sid = self.id_from_url(self.url)
        r = await asks.post(self.chat_pages_url, data={'r': sid})
        comment_count = r.json()['count']
        r = await asks.post(self.chat_page_url,
                            data={
                                'r': sid,
                                'lastCT': int(time.time() * 1000),
                                'firstCT': 0,
                                'cpr': 1,
                                'page': 1
                            })
        o = r.json()
        # print(comment_count, o)
        d = {}
        for i in o:
            d[i['_id']] = i
        print(qtoml.dumps(d))

    def titlefn(self, ext: str = '.toml') -> str:
        fn = make_filename(self.title) + ext
        return fn

    def enc_ext(self, encoder: Any) -> str:
        return ('.toml' if encoder == qtoml else '.json' if encoder == json
                else '')

    @property
    def url(self) -> str:
        if self._url is None:
            assert self.story_info is not None
            self._url = self.story_info['original_url']
        return self._url

    @url.setter
    def url(self, s: str) -> None:
        self._url = s

    @staticmethod
    def get_title(node: Dict[str, Any]) -> str:
        return node['t']

    @property
    def title(self) -> str:
        # return self.story_info['t']
        assert self.story_info is not None
        return self.get_title(self.story_info)

    @staticmethod
    def get_chapter_count(node: Dict[str, Any]) -> int:
        return len(node['bm'])

    @staticmethod
    def get_author(node: Dict[str, Any]) -> str:
        return node['u'][0]['n']

    @property
    def author(self) -> str:
        # return self.story_info['u'][0]['n']
        assert self.story_info is not None
        return self.get_author(self.story_info)

    def write(self, fn: Any = None, encoder: Any = qtoml,
              enc_args: Dict[str, Any] = {'encode_none': 0}) -> None:
        vl, output = self.vl, self.output
        open_file = None
        if self.story_info is None:
            raise ValueError("must download before writing")
        if fn is None:
            ext = self.enc_ext(encoder)
            fn = self.titlefn(ext)
        elif hasattr(fn, 'write'):
            open_file = fn
            try:
                fn = open_file.name
            except AttributeError:
                # this works around a bug in zipfile; if you call open() to
                # create a writable filehandle, it has no name attr
                fn = open_file._zinfo.filename

        if open_file is not None:
            s = encoder.dumps(self.story_info, **enc_args)
            open_file.write(s.encode())
            # encoder.dump(self.story_info, open_file, **enc_args)
        else:
            with open(fn, 'w') as of:
                encoder.dump(self.story_info, of, **enc_args)
        logger.info("Story data written to {}".format(fn))

    def read(self, fn: Any, decoder: Any = None) -> None:
        if hasattr(fn, 'read'):
            open_file = fn
            fn = open_file.name
        elif os.path.exists(fn):
            open_file = None
        else:
            if isinstance(fn, bytes):
                fn = fn.decode()
            file_data = fn
            fn = None
            open_file = None

        if decoder is None:
            if fn is None:
                raise ValueError("must provide decoder for raw data read")
            if fn.endswith('.toml'):
                decoder = qtoml
            elif fn.endswith('.json'):
                decoder = json
            else:
                raise ValueError("Decoder not given and can't infer from "
                                 "filename")

        if open_file is not None:
            self.story_info = decoder.load(open_file)
        elif fn is not None:
            with open(fn, 'r') as inp:
                self.story_info = decoder.load(inp)
        else:
            self.story_info = decoder.loads(file_data)


class AkunStory:
    header_html = """<html>
<head>
<meta charset="UTF-8">
<meta name="Author" content="{author}">
<title>{title}</title>
</head>
<body>
<h1>{title}</h1>
"""

    footer_html = """</body>
</html>
"""

    def get_old_image(self, fn: str) -> Optional[bytes]:
        if self.old_zip is None:
            return None
        try:
            return self.old_zip.read(fn)
        except KeyError:
            nl = self.old_zip.namelist()
            for i in nl:
                if i.startswith(fn):
                    return self.old_zip.read(i)
            return None
        except Exception:  # for file-not-found, etc
            return None

    image_types = { 'image/jpeg': '.jpg', 'image/png': '.png',
                    'image/gif': '.gif' }

    @classmethod
    def get_image_fn(cls, bn: str, data: bytes) -> str:
        logger.debug(f"{bn=} {type(bn)=} {type(data)=}")
        ct = magic.from_buffer(data, mime=True)
        if ct not in cls.image_types:
            raise Exception(f"File '{bn}' has non-image type '{ct}'")
        if not bn.endswith(cls.image_types[ct]):
            bn += cls.image_types[ct]
        return bn

    def __init__(self, inp, images=True, download_delay=0.5, vl=1,
                 output=print) -> None:
        if not output:
            # a no-op
            output = lambda *args, **kwargs: None
        self.info = AkunData(download_delay=download_delay, vl=vl,
                             output=output)
        self.vl, self.output = vl, output
        self.save_images = images

        self.html = ""
        self.images_dl: Dict[str, str] = {}
        self.images_fetched: Set[str] = set()
        self.output_fn: Optional[str] = None
        self.old_zip: Optional[zipfile.ZipFile] = None
        self.out_zip: Optional[zipfile.ZipFile] = None

        if inp is not None:
            if os.path.exists(inp):
                if inp.endswith('.zip'):
                    self.read(inp)
                else:
                    self.info.read(inp)
            else:
                # self.info.download(inp)
                self.info._url = inp

    def _open_out_zip(self, fn: Optional[str]) -> None:
        if self.out_zip is not None:
            return
        if fn is None:
            if self.output_fn:
                fn = self.output_fn
            else:
                fn = make_filename(self.info.title) + '.zip'
                self.output_fn = fn
        if os.path.exists(fn):
            old_fn = fn + '.old'
            os.rename(fn, old_fn)
            if self.old_zip is None:
                self.old_zip = zipfile.ZipFile(old_fn, 'r')

        self.out_zip = zipfile.ZipFile(fn, 'x', zipfile.ZIP_DEFLATED)

    def read(self, fn) -> None:
        self.output_fn = fn
        self.old_zip = zipfile.ZipFile(fn, 'r')
        data_fn = None
        for i in self.old_zip.namelist():
            if i.endswith('.toml') or i.endswith('.json'):
                data_fn = i
                break
        if data_fn:
            with self.old_zip.open(data_fn) as dfh:
                data_bytes = dfh.read()
            decoder = qtoml if data_fn.endswith('.toml') else json
            self.info.read(data_bytes, decoder=decoder)
        else:
            raise ValueError(f"couldn't find metadata in zipfile '{fn}'")

    @staticmethod
    def fix_images(html: str, delete: bool = False) -> Tuple[
            str, Dict[str, str]]:
        soup = BeautifulSoup(html, 'html5lib')
        rv = {}
        for i in soup('img'):
            bn = i['src'].split('/')[-1]
            rv[bn] = i['src']
            if delete:
                i.decompose()
            else:
                i.replace_with(soup.new_tag('img', src=bn))
        return str(soup), rv

    @classmethod
    async def gen_html(cls, node, chapters,
                       delete_images: bool = False) -> AsyncIterator[
                           Tuple[str, Dict[str, str]]]:
        # node = self.info.story_info
        # vl, output = self.vl, self.output

        title = AkunData.get_title(node)
        author = AkunData.get_author(node)
        try:
            chapter_len = len(chapters)
        except TypeError:
            chapter_len = '(unknown)'
        yield (cls.header_html.format(title=title, author=author), {})
        for i, co in enumerate(chapters):
            # yield to let downloads happen
            await trio.sleep(0)
            logger.debug(f"chapters: {i}/{chapter_len}")
            if (co['nt'] == 'chapter' and not
                ('t' in co and co['t'].startswith('#special'))):
                if 't' in co and co['t'] != '':
                    yield (("<h2 class=\"chapter\">{}</h2>\n".
                            format(co['t'])), {})
                else:
                    yield ("<hr>\n", {})
                chap_html, chap_imgs = cls.fix_images(
                    co['b'], delete=delete_images)
                yield (chap_html + "\n", chap_imgs)
                # self.images_dl.update(chap_imgs)
            elif co['nt'] == 'choice':
                try:
                    if 'choices' not in co or 'votes' not in co:
                        continue
                    vc = { i: 0 for i in range(len(co['choices'])) }
                    # if vl >= 2:
                    #     print(co['choices'])
                    for i in co['votes'].values():
                        if type(i) == int:
                            try:
                                vc[i] += 1
                            except KeyError:
                                pass
                        else:
                            for j in i:
                                try:
                                    vc[j] += 1
                                except KeyError:
                                    pass
                    cd = { co['choices'][i]: vc[i] for i in range(len(vc)) }
                    yield ("<hr>\n<ul>\n", {})
                    for i in sorted(cd.items(), key=itemgetter(1),
                                    reverse=True):
                        yield ("<li>{} -- {}</li>\n".format(i[0], i[1]), {})
                    yield ("</ul>\n", {})
                except BaseException:
                    print(co)
                    raise
            else:
                # we do this to ensure we yield at least one result per member
                # of the chapters iterator
                yield ('', {})
        yield (cls.footer_html, {})

    async def collect_html(self, concurrent_images: bool = False) -> None:
        # self.html = ''.join(self.gen_html())
        self.html = ''
        self.images_dl = {}
        assert self.info.story_info is not None
        send_ch: trio.MemorySendChannel
        recv_ch: trio.MemoryReceiveChannel
        send_ch, recv_ch = trio.open_memory_channel(math.inf)
        # if we aren't doing images at all, it's meaningless to do so
        # concurrently
        concurrent_images = concurrent_images and self.save_images

        async def concurrent_fetch():
            async with recv_ch:
                async for val in recv_ch:
                    await self.get_images(val)

        async with trio.open_nursery() as nurs:
            if concurrent_images:
                nurs.start_soon(concurrent_fetch)
            async with send_ch:
                async for html, imgs in self.gen_html(
                        self.info.story_info,
                        self.info.story_info['chapters'],
                        not self.save_images):
                    self.html += html
                    self.images_dl.update(imgs)
                    if concurrent_images and len(imgs) > 0:
                        await send_ch.send(imgs)
            logger.debug("finished processing gen_html results")

    async def get_images(self, imgs: Optional[Dict[str, str]] = None) -> None:
        vl, output = self.vl, self.output
        self._open_out_zip(None)
        assert self.out_zip is not None

        async def fetch_image(bn):
            url = imgs[bn]
            if vl == 1:
                output(f"\r\x1b[KDownloading image {url}",
                       end='')
            elif vl >= 2:
                logger.debug(f"Downloading image {url}")
            idata = self.get_old_image(bn)
            if idata is None:
                logger.debug("No old image data, downloading")
                try:
                    ir = await self.info.downloader.download_retry(url)
                    idata = ir.content
                    if isinstance(idata, bytearray):
                        idata = bytes(idata)
                except Exception as e:  # network errors etc
                    tb = traceback.format_exc()
                    logger.debug(f"error {e} in image download: {tb}")
                    return
            try:
                new_bn = self.get_image_fn(bn, idata)
            except Exception as e:  # failed to get data type
                tb = traceback.format_exc()
                logger.debug(f"error {e} in get_image_fn: {tb}")
                return
            if new_bn != bn:
                self.html = self.html.replace(bn, new_bn)
            # don't bother re-compressing images
            self.out_zip.writestr(new_bn, idata,
                                  zipfile.ZIP_STORED)
            logger.debug(f"Finished downloading image {url}")

        if imgs is None:
            logger.debug("get_images on entire images_dl")
            imgs = self.images_dl
        else:
            logger.debug(f"get_images on {len(imgs)} images")
        async with trio.open_nursery() as nurs:
            for bn in imgs:
                url = imgs[bn]
                if url in self.images_fetched:
                    logger.debug(f"Image at {url} already fetched, skipping")
                    continue
                self.images_fetched.add(url)
                nurs.start_soon(fetch_image, bn)
            logger.debug(f"waiting on {len(imgs)} images")
        logger.debug("get_images finished")
        # if vl >= 1:
        #     output()

    async def write(self, fn=None, data_enc=qtoml,
                    data_enc_args={'encode_none': 0}) -> None:
        vl, output = self.vl, self.output
        assert self.info.story_info is not None
        self.info.story_info['image_urls'] = self.images_dl
        self._open_out_zip(fn)
        assert self.out_zip is not None
        logger.debug(f"doing write to {self.output_fn}")
        with self.out_zip:
            dfn = self.info.titlefn(self.info.enc_ext(data_enc))
            df = self.out_zip.open(dfn, 'w')
            self.info.write(df, data_enc, data_enc_args)
            df.close()
            if self.save_images:
                await self.get_images()
            html_fn = self.info.titlefn('.html')
            self.out_zip.writestr(html_fn, self.html)

@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Be verbose")
@click.pass_context
def scraper(ctx, verbose: bool) -> None:
    ctx.ensure_object(dict)

    vi = 1
    if verbose:
        vi = 2
    ctx.obj['verbose'] = vi
    if vi == 1:
        logger.setLevel(logging.INFO)
    elif vi >= 2:
        logger.setLevel(logging.DEBUG)

async def getinfo_async(vl, download_delay: float, format: str,
                        url: str) -> None:
    """Download story data from anonkun. Writes raw data to disk."""
    s = AkunData(vl=vl, download_delay=download_delay)
    old_fn = None
    pass_url: Optional[str] = url
    if os.path.exists(url):
        s.read(url)
        old_fn = url
        pass_url = None
    await s.download(pass_url)
    if format == 'toml':
        s.write(old_fn)
    elif format == 'json':
        s.write(old_fn, encoder=json, enc_args={'indent': 2})

@scraper.command()
@click.option("-d", "--download-delay", type=float, default=0.5,
              help="Wait this long between requests")
@click.option("--toml", 'format', flag_value='toml', default=True,
              help="Write output as TOML (default)")
@click.option("--json", 'format', flag_value='json',
              help="Write output as JSON")
@click.argument('url')
@click.pass_context
def getinfo(ctx, download_delay: float, format: str, url: str) -> None:
    vl = ctx.obj['verbose']
    trio.run(getinfo_async, vl, download_delay, format, url)

async def download_async(vl, infile: str, images: bool) -> None:
    """Write a zipfile with all story data. Includes an HTML ebook, all referenced
    images, and the raw metadata.

    """
    s = AkunStory(infile, images=images, vl=vl)
    if infile.endswith('.zip'):
        await s.info.download()
    await s.collect_html(concurrent_images=True)
    logger.debug("done collecting HTML")
    await s.write()
    # with open(infile) as inp:
    #     try:
    #         node = json.load(inp)
    #     except json.decoder.JSONDecodeError:
    #         inp.seek(0)
    #         node = qtoml.load(inp)

    # node, story = so['node'], so['story']

@scraper.command()
@click.option("--images/--no-images", default=True,
              help="Include images in output file")
@click.argument('infile')
@click.pass_context
def download(ctx, infile: str, images: bool) -> None:
    vl = ctx.obj['verbose']
    logger.info(f"verbose level: {vl}")
    trio.run(download_async, vl, infile, images)

def zipfile_now():
    """Returns the current time, UTC, """
    # convention is apparently that ZIP timestamp represents local time
    t = datetime.datetime.now()
    return (t.year, t.month, t.day, t.hour, t.minute, t.second)

if __name__ == '__main__':
    scraper(obj={})
