#!/usr/bin/env python3

import requests, re
from bs4 import BeautifulSoup
import io, zipfile, time, os, datetime
# import pytoml as toml
import json, qtoml
import click, magic
from operator import itemgetter
from itertools import chain

def make_filename(title):
    title = title.lower().replace(" ", "_")
    return re.sub("[^a-z0-9_]", "", title)

def download_retry(url, tries=3, download_delay=0.5):
    for i in range(tries):
        r = requests.get(url)
        try:
            r.raise_for_status()
        except Exception:
            if i >= tries - 1:
                raise
            time.sleep(download_delay * (2 ** (i + 2)))
        else:
            return r

class AkunData:
    node_url = 'https://fiction.live/api/node/{story_id}'
    story_url = ('https://fiction.live/api/anonkun/chapters'
                 '/{story_id}/{start}/{end}')
    chat_pages_url = 'https://fiction.live/api/chat/pages'
    chat_page_url = 'https://fiction.live/api/chat/page'

    url_re = re.compile(r"https://fiction.live/stories/[^/]+/([^/]+)/?")

    @classmethod
    def id_from_url(cls, url):
        o = cls.url_re.match(url)
        if o is None:
            raise ValueError("bad url {}".format(url))
        return o.group(1)

    def __init__(self, download_delay=0.5, vl=1, output=print):
        self.vl = vl
        if not output:
            # a no-op
            output = lambda *args, **kwargs: None
        self.output = output
        self.download_delay = download_delay
        self._url = None

    def get_metadata(self, url=None):
        vl, output = self.vl, self.output
        if url is None:
            url = self.url

        story_id = self.id_from_url(url)
        mu = self.node_url.format(story_id=story_id)
        if vl >= 2:
            output(f"Metadata URL: {mu}")
        r = download_retry(mu)
        r.raise_for_status()
        node = r.json()
        return node

    def get_chapters(self, node, url):
        vl, output = self.vl, self.output
        num_chaps = len(node['bm'])
        if vl >= 1:
            output(f"Found {num_chaps} chapters")
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
            time.sleep(self.download_delay)
            r = download_retry(su, download_delay=self.download_delay)
            yield r.json()

    def download(self, url=None):
        vl, output = self.vl, self.output
        if url is None:
            url = self.url

        node = self.get_metadata(url)

        if vl >= 3:
            output(str(node))

        story = list(chain.from_iterable(self.get_chapters(node, url)))
        node['original_url'] = url
        node['chapters'] = story
        self.story_info = node

    def download_chat(self):
        sid = self.id_from_url(self.url)
        r = requests.post(self.chat_pages_url, data={'r': sid})
        comment_count = r.json()['count']
        r = requests.post(self.chat_page_url,
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

    def titlefn(self, ext='.toml'):
        fn = make_filename(self.story_info['t']) + ext
        return fn

    def enc_ext(self, encoder):
        return ('.toml' if encoder == qtoml else '.json' if encoder == json
                else '')

    @property
    def url(self):
        if not self._url:
            self._url = self.story_info['original_url']
        return self._url

    @url.setter
    def url(self, s):
        self._url = s

    @property
    def title(self):
        return self.story_info['t']

    @property
    def author(self):
        return self.story_info['u'][0]['n']

    def write(self, fn=None, encoder=qtoml, enc_args={'encode_none': 0}):
        vl, output = self.vl, self.output
        open_file = None
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
        if vl >= 1:
            output("Story data written to {}".format(fn))

    def read(self, fn, decoder=None):
        if hasattr(fn, 'read'):
            open_file = fn
            fn = open_file.name
        else:
            open_file = None

        if decoder is None:
            if fn.endswith('.toml'):
                decoder = qtoml
            elif fn.endswith('.json'):
                decoder = json
            else:
                raise ValueError("Decoder not given and can't infer from "
                                 "filename")

        if open_file is not None:
            self.story_info = decoder.load(open_file)
        else:
            with open(fn, 'r') as inp:
                self.story_info = decoder.load(inp)


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

    def get_old_image(self, fn):
        try:
            return self.old_zip.read(fn)
        except KeyError:
            nl = self.old_zip.namelist()
            for i in nl:
                if i.startswith(fn):
                    return self.old_zip.read(i)
            raise

    image_types = { 'image/jpeg': '.jpg', 'image/png': '.png',
                    'image/gif': '.gif' }

    @classmethod
    def get_image_fn(cls, bn, data):
        ct = magic.from_buffer(data, mime=True)
        if ct not in cls.image_types:
            raise Exception(f"File '{bn}' has non-image type '{ct}'")
        if not bn.endswith(cls.image_types[ct]):
            bn += cls.image_types[ct]
        return bn

    def __init__(self, inp, images=True, download_delay=0.5, vl=1,
                 output=print):
        if not output:
            # a no-op
            output = lambda *args, **kwargs: None
        self.info = AkunData(download_delay=download_delay, vl=vl,
                             output=output)
        self.vl, self.output = vl, output
        self.save_images = images

        self.html = ""
        self.images_dl = {}
        self.output_fn = None
        self.old_zip = None

        if os.path.exists(inp):
            if inp.endswith('.zip'):
                self.read(inp)
            else:
                self.info.read(inp)
        else:
            self.info.download(inp)

    def read(self, fn):
        self.output_fn = fn
        self.old_zip = zipfile.ZipFile(fn, 'r')
        data_fn = None
        for i in self.old_zip.namelist():
            if i.endswith('.toml') or i.endswith('.json'):
                data_fn = i
                break
        if data_fn:
            self.info.read(self.old_zip.open(data_fn))
        else:
            raise ValueError(f"couldn't find metadata in zipfile '{fn}'")

    def gen_html(self):
        node = self.info.story_info
        vl, output = self.vl, self.output

        title = self.info.title
        author = self.info.author
        html_fn = make_filename(title) + '.html'
        self.html += self.header_html.format(title=title, author=author)
        for co in node['chapters']:
            if (co['nt'] == 'chapter' and not
                ('t' in co and co['t'].startswith('#special'))):
                if 't' in co and co['t'] != '':
                    self.html += ("<h2 class=\"chapter\">{}</h2>\n".
                                  format(co['t']))
                else:
                    self.html += "<hr>\n"
                soup = BeautifulSoup(co['b'], 'html5lib')
                for i in soup('img'):
                    if self.save_images:
                        # if output and vl >= 1:
                        #     output(i['src'])
                        bn = i['src'].split('/')[-1]
                        self.images_dl[bn] = i['src']
                        i.replace_with(soup.new_tag('img', src=bn))
                    else:
                        i.decompose()
                self.html += str(soup) + "\n"
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
                    self.html += "<hr>\n<ul>\n"
                    for i in sorted(cd.items(), key=itemgetter(1),
                                    reverse=True):
                        self.html += "<li>{} -- {}</li>\n".format(i[0], i[1])
                    self.html += "</ul>\n"
                except BaseException:
                    print(co)
                    raise
        self.html += self.footer_html

    def get_images(self):
        vl, output = self.vl, self.output
        for bn in self.images_dl:
            if vl >= 1:
                output(f"\r\x1b[KDownloading image {self.images_dl[bn]}",
                       end='')
            try:
                idata = self.get_old_image(bn)
            except Exception as e:
                if vl >= 2:
                    output(e)
                ir = requests.get(self.images_dl[bn])
                idata = ir.content
            new_bn = self.get_image_fn(bn, idata)
            if new_bn != bn:
                self.html = self.html.replace(bn, new_bn)
            # don't bother re-compressing images
            self.out_zip.writestr(new_bn, idata,
                                  zipfile.ZIP_STORED)
        if vl >= 1:
            output()

    def write(self, fn=None, data_enc=qtoml, data_enc_args={'encode_none': 0}):
        if fn is None:
            if self.output_fn:
                fn = self.output_fn
            else:
                fn = make_filename(self.info.title) + '.zip'
        if os.path.exists(fn):
            old_fn = fn + '.old'
            os.rename(fn, old_fn)
            if self.old_zip is None:
                self.old_zip = zipfile.ZipFile(old_fn, 'r')

        self.info.story_info['image_urls'] = self.images_dl
        self.out_zip = zipfile.ZipFile(fn, 'x', zipfile.ZIP_DEFLATED)
        with self.out_zip:
            dfn = self.info.titlefn(self.info.enc_ext(data_enc))
            df = self.out_zip.open(dfn, 'w')
            self.info.write(df, data_enc, data_enc_args)
            df.close()
            if self.save_images:
                self.get_images()
            html_fn = self.info.titlefn('.html')
            self.out_zip.writestr(html_fn, self.html)

@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Be verbose")
@click.pass_context
def scraper(ctx, verbose):
    ctx.ensure_object(dict)

    vi = 1
    if verbose:
        vi = 2
    ctx.obj['verbose'] = vi

@scraper.command()
@click.option("-d", "--download-delay", type=float, default=0.5,
              help="Wait this long between requests")
@click.option("--toml", 'format', flag_value='toml', default=True,
              help="Write output as TOML (default)")
@click.option("--json", 'format', flag_value='json',
              help="Write output as JSON")
@click.argument('url')
@click.pass_context
def getinfo(ctx, download_delay, format, url):
    """Download story data from anonkun. Writes raw data to disk."""
    vl = ctx.obj['verbose']
    s = AkunData(vl=vl, download_delay=download_delay)
    old_fn = None
    if os.path.exists(url):
        s.read(url)
        old_fn = url
        url = None
    s.download(url)
    if format == 'toml':
        s.write(old_fn)
    elif format == 'json':
        s.write(old_fn, encoder=json, enc_args={'indent': 2})

@scraper.command()
@click.option("--images/--no-images", default=True,
              help="Include images in output file")
@click.argument('infile')
@click.pass_context
def download(ctx, infile, images):
    """Write a zipfile with all story data. Includes an HTML ebook, all referenced
    images, and the raw metadata.

    """
    vl = ctx.obj['verbose']
    s = AkunStory(infile, images, vl)
    if infile.endswith('.zip'):
        s.info.download()
    s.gen_html()
    s.write()
    # with open(infile) as inp:
    #     try:
    #         node = json.load(inp)
    #     except json.decoder.JSONDecodeError:
    #         inp.seek(0)
    #         node = qtoml.load(inp)

    # node, story = so['node'], so['story']

def zipfile_now():
    """Returns the current time, UTC, """
    # convention is apparently that ZIP timestamp represents local time
    t = datetime.datetime.now()
    return (t.year, t.month, t.day, t.hour, t.minute, t.second)

if __name__ == '__main__':
    scraper(obj={})
