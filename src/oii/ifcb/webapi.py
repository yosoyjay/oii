from flask import Flask, request, url_for, abort, session, Response, render_template, render_template_string
from unittest import TestCase
import json
import re
import sys
import os
import tempfile
from io import BytesIO
import shutil
from zipfile import ZipFile, ZIP_DEFLATED
from time import strptime
from io import StringIO
import numpy as np
from skimage.segmentation import find_boundaries
from oii.config import get_config
from oii.times import iso8601, rfc822
import urllib.request, urllib.parse, urllib.error
from oii.utils import order_keys, jsons
from oii.ifcb.formats.adc import read_adc, read_target, ADC
from oii.ifcb.formats.adc import ADC_SCHEMA, TARGET_NUMBER, LEFT, BOTTOM, WIDTH, HEIGHT, STITCHED, SCHEMA_VERSION_2
from oii.ifcb.formats.roi import read_roi, read_rois, ROI, as_pil
from oii.ifcb.formats.hdr import read_hdr, HDR, CONTEXT, HDR_SCHEMA
from oii.ifcb.db import IfcbFeed, IfcbFixity, IfcbAutoclass, IfcbBinProps
from oii.resolver import parse_stream
from oii.ifcb import stitching
from oii.ifcb import represent
from oii.ifcb.stitching import find_pairs, stitch, stitched_box, stitch_raw, list_stitched_targets
from oii.ifcb.joestitch import stitch as joe_stitch
from oii.iopipes import UrlSource, LocalFileSource
from oii.image.pilutils import filename2format, thumbnail
from oii.image import mosaic
from oii.image.mosaic import Tile
from oii.config import get_config
import mimetypes
from zipfile import ZipFile
from PIL import Image
from ImageFilter import FIND_EDGES
import ImageOps
import ImageChops
from werkzeug.contrib.cache import SimpleCache
from lxml import html

# TODO JSON on everything

app = Flask(__name__)
app.debug = True

# importantly, set max-age on static files (e.g., javascript) to something really short
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 30

# string key constants
STITCH='stitch'
NAMESPACE='namespace'
SIZE='size'
SCALE='scale'
PAGE='page'
PID='pid'
CACHE='cache'
CACHE_TTL='ttl'
PSQL_CONNECT='psql_connect'
FEED='feed'
BIN_PROPS='bin_props'
FIXITY='fixity'
PORT='port'
DEBUG='debug'
STATIC='static'
PREVIOUS='previous'
NEXT='next'
START='start'
END='end'
NEAREST='nearest'
LATEST='latest'
FORMAT='format'
RESOLVER='resolver'

# FIXME don't use globals
(rs,binpid2path,pid_resolver,blob_resolver,ts_resolver,fea_resolver,class_scores_resolver) = ({},None,None,None,None,None,None)

def configure(config=None):
    app.config[CACHE] = SimpleCache()
    app.config[STITCH] = True
    app.config[CACHE_TTL] = 120
    app.config[PSQL_CONNECT] = config.psql_connect
    app.config[RESOLVER] = config.resolver
    app.config[FEED] = IfcbFeed(app.config[PSQL_CONNECT])
    app.config[STATIC] = '/static/'
    try:
        if config.debug in ['True', 'true', 'T', 't', 'Yes', 'yes', 'debug']:
            app.debug = True
    except:
        pass
    try:
        app.config[PORT] = int(config.port)
    except:
        app.config[PORT] = 5061

def major_type(mimetype):
    return re.sub(r'/.*','',mimetype)

def minor_type(mimetype):
    return re.sub(r'.*/','',mimetype)

def get_psql_connect(time_series):
    hit = ts_resolver.resolve(time_series=time_series)
    if hit is None:
        app.logger.debug('cannot resolve time series "%s"' % time_series)
        abort(404)
    return '%s dbname=%s' % (app.config[PSQL_CONNECT], hit.dbname)

def get_feed(time_series):
    return IfcbFeed(get_psql_connect(time_series))

def get_bin_props(time_series):
    return IfcbBinProps(get_psql_connect(time_series))

def get_fixity(time_series):
    return IfcbFixity(get_psql_connect(time_series))

def get_autoclass(time_series):
    return IfcbAutoclass(get_psql_connect(time_series))

# simple memoization decorator using Werkzeug's caching support
def memoized(func):
    def wrap(*args):
        cache = app.config[CACHE] # get cache object from config
        # because the cache is shared across the entire app, we need
        # to include the function name in the cache key
        key = ' '.join([func.__name__] + list(map(str,args)))
        result = cache.get(key)
        if result is None: # cache miss
            #app.logger.debug('cache miss on %s' % str(key))
            result = func(*args) # produce the result
            # FIXME accept a TTL argument
            cache.set(key, result, timeout=app.config[CACHE_TTL]) # cache it
        return result
    return wrap

# utilities

def get_target(hit, adc_path=None):
    """Read a single target from an ADC file given the bin PID/LID and target number"""
    if adc_path is None:
        adc_path = resolve_adc(hit.bin_pid)
    prev_pid = None
    this = None
    if app.config[STITCH]:
        for target in list_targets(hit, adc_path=adc_path):
            if target[TARGET_NUMBER] == hit.target_no:
                if prev_pid is not None:
                    target[PREVIOUS] = prev_pid
                this = target
            elif target[TARGET_NUMBER] > hit.target_no and this is not None:
                this[NEXT] = target[PID]
                break
            prev_pid = target[PID]
        return this
    else:
        for target in list_targets(hit, hit.target_no, adc_path=adc_path):
            return target

def max_age(ttl=None):
    if ttl is None:
        return {}
    else:
        return {'Cache-control': 'max-age=%d' % ttl}

def image_response(image,format,mimetype):
    """Construct a Flask Response object for the given image, PIL format, and MIME type."""
    buf = StringIO()
    im = as_pil(image).save(buf,format)
    return Response(buf.getvalue(), mimetype=mimetype)

def resolve_pid(pid):
    return pid_resolver.resolve(pid=pid)

def resolve_file(pid,format):
    return binpid2path.resolve(pid=pid,format=format).value

def resolve_adc(pid):
    return resolve_file(pid,ADC)

def resolve_hdr(pid):
    return resolve_file(pid,HDR)

def resolve_roi(pid):
    return resolve_file(pid,ROI)

def resolve_files(pid,formats):
    return [resolve_file(pid,format) for format in formats]

def parse_params(path, **defaults):
    """Parse a path fragment and convert to dict.
    Slashes separate alternating keys and values.
    For example /a/3/b/5 -> { 'a': '3', 'b': '5' }.
    Any keys not present get default values from **defaults"""
    parts = re.split('/',path)
    d = dict(list(zip(parts[:-1:2], parts[1::2])))
    for k,v in list(defaults.items()):
        if k not in d:
            d[k] = v
    return d

def parse_date_param(sdate):
    try:
        return strptime(sdate,'%Y-%m-%d')
    except:
        pass
    try:
        return strptime(sdate,'%Y-%m-%dT%H:%M:%S')
    except:
        pass
    try:
        return strptime(sdate,'%Y-%m-%dT%H:%M:%SZ')
    except:
        pass
    try:
        return strptime(re.sub(r'\.\d+Z','',sdate),'%Y-%m-%dT%H:%M:%S')
    except:
        app.logger.debug('could not parse date param %s' % sdate)
        abort(400)


def binlid2dict(time_series, bin_lid, format='json'):
    date_format = iso8601
    if format == 'rss':
        date_format = rfc822
    hit = pid_resolver.resolve(pid=bin_lid, time_series=time_series)
    date = date_format(strptime(hit.date, hit.date_format))
    # FIXME fetch metadata?
    return {
        'pid': hit.bin_pid,
        'date': date
        }

def template_response(template, mimetype=None, ttl=None, **kw):
    if mimetype is None:
        (mimetype, _) = mimetypes.guess_type(template)
    if mimetype is None:
        mimetype = 'application/octet-stream'
    return Response(render_template(template,**kw), mimetype=mimetype, headers=max_age(ttl))

def jsonr(obj, ttl=None):
    return Response(jsons(obj), mimetype='application/json', headers=max_age(ttl))

# FIXME need max date, URL prefix
def feed_response(time_series,dicts,format='json'):
    app.logger.debug(dicts)
    if len(dicts) > 0:
        max_date = max([entry['date'] for entry in dicts]) # FIXME doesn't work for RFC822
    else:
        max_date = iso8601() # now
    ns = get_namespace(time_series)
    context = dict(max_date=max_date, namespace=ns, feed=dicts)
    if format == 'json':
        return jsonr(dicts)
    if format == 'html':
        return template_response('feed.html', **context)
    elif format == 'atom':
        #return template_response('feed.atom', mimetype='application/xml+atom', ttl=feed_ttl, **context)
        return template_response('feed.atom', **context)
    elif format == 'rss':
        #return template_response('feed.rss', mimetype='application/xml+rss', ttl=feed_ttl, **context)
        return template_response('feed.rss', **context)

# external access to resolver services
@app.route('/resolve/<name>',methods=['POST'])
def rest_resolve(name):
    context = json.loads(request.data)
    R = rs # FIXME global!
    hit = R[name].resolve(**context)
    if hit is None:
        return jsonr({})
    else:
        return jsonr(hit.bindings)

@app.route('/resolve_all/<name>',methods=['POST'])
def rest_resolve_all(name):
    context = json.loads(request.data)
    R = rs # FIXME global!
    hits = [hit.bindings for hit in R[name].resolve_all(**context)]
    return jsonr(hits)

@app.route('/<time_series>/api/feed/format/<format>')
@app.route('/<time_series>/api/feed/date/<date>')
@app.route('/<time_series>/api/feed/date/<date>/format/<format>')
def serve_feed(time_series,date=None,format='json'):
    if date is not None:
        date = parse_date_param(date)
    # FIXME support formats other than JSON, also use extension
    def feed2dicts():
        # FIXME parameterize by time series!
        for bin_lid in get_feed(time_series).latest_bins(date):
            yield binlid2dict(time_series, bin_lid, format)
    return feed_response(time_series, list(feed2dicts()), format)

@app.route('/<time_series>/api/feed/nearest/<date>')
def serve_nearest(time_series,date):
    if date is not None:
        date = parse_date_param(date)
    # FIXME parameterize by time series!
    for bin_lid in get_feed(time_series).nearest_bin(date):
        d = binlid2dict(time_series, bin_lid)
    return jsonr(d)

# FIXME there's got to be a better way of handling optional parts of URL patterns
@app.route('/<time_series>/api/feed/start/<start>')
@app.route('/<time_series>/api/feed/start/<start>/end/<end>')
@app.route('/<time_series>/api/feed/end/<end>')
@app.route('/<time_series>/api/feed/start/<start>/format/<format>')
@app.route('/<time_series>/api/feed/start/<start>/end/<end>/format/<format>')
@app.route('/<time_series>/api/feed/end/<end>/format/<format>')
def serve_between(time_series,start,end=None,format='json'):
    if start is not None:
        start = parse_date_param(start)
    if end is not None:
        end = parse_date_param(end)
    def doit():
        for bin_lid in get_feed(time_series).between(start,end):
            yield binlid2dict(time_series, bin_lid, format)
    return feed_response(time_series, list(doit()), format)

@app.route('/<time_series>/api/feed/<after_before>/pid/<path:pid>')
@app.route('/<time_series>/api/feed/<after_before>/n/<int:n>/pid/<path:pid>')
def serve_after_before(time_series,after_before,n=1,pid=None):
    if pid is None:
        abort(400)
    if after_before not in ['after', 'before']:
        abort(400)
    hit = resolve_pid(pid)
    if after_before == 'after':
        response = [binlid2dict(time_series, lid) for lid in get_feed(time_series).after(hit.bin_lid,n)]
    else:
        response = [binlid2dict(time_series, lid) for lid in get_feed(time_series).before(hit.bin_lid,n)]
    return jsonr(response)

@app.route('/<time_series>/feed.json')
def serve_json_feed(time_series):
    return serve_feed(time_series,format='json')

@app.route('/<time_series>/feed.rss')
def serve_rss_feed(time_series):
    return serve_feed(time_series,format='rss')

@app.route('/<time_series>/feed.atom')
def serve_atom_feed(time_series):
    return serve_feed(time_series,format='atom')

@app.route('/<time_series>/feed.html')
def serve_html_feed(time_series):
    return serve_feed(time_series,format='html')

def get_volume(time_series):
    # FIXME parameterize by time series!
    return get_fixity(time_series).summarize_data_volume()

@app.route('/<time_series>/api/volume')
def serve_volume(time_series):
    return jsonr(get_volume(time_series))

def get_namespace(time_series):
    hit = ts_resolver.resolve(time_series=time_series)
    return hit.namespace

@app.route('/<time_series>/api/autoclass/list_classes')
def autoclass_list_classes(time_series):
    return jsonr(get_autoclass(time_series).list_classes())

@app.route('/<time_series>/api/autoclass/count_by_day/<class_label>/start/<start>/end/<end>')
def autoclass_count_by_day(time_series,class_label,start=None,end=None):
    if start is not None:
        start = parse_date_param(start)
    if end is not None:
        end = parse_date_param(end)
    return jsonr(list(get_autoclass(time_series).rough_count_by_day(class_label,start,end)))

@app.route('/<time_series>/api/autoclass/rois_of_class/<class_label>')
@app.route('/<time_series>/api/autoclass/rois_of_class/<class_label>/threshold/<float:threshold>')
@app.route('/<time_series>/api/autoclass/rois_of_class/<class_label>/start/<start>')
@app.route('/<time_series>/api/autoclass/rois_of_class/<class_label>/start/<start>/end/<end>')
@app.route('/<time_series>/api/autoclass/rois_of_class/<class_label>/end/<end>')
@app.route('/<time_series>/api/autoclass/rois_of_class/<class_label>/threshold/<float:threshold>/start/<start>')
@app.route('/<time_series>/api/autoclass/rois_of_class/<class_label>/threshold/<float:threshold>/start/<start>/end/<end>')
@app.route('/<time_series>/api/autoclass/rois_of_class/<class_label>/threshold/<float:threshold>/end/<end>')
@app.route('/<time_series>/api/autoclass/rois_of_class/<class_label>/threshold/<float:threshold>/start/<start>/end/<end>/page/<int:page>')
def autoclass_rois_of_class(time_series,class_label,start=None,end=None,threshold=0.0,page=1):
    if start is not None:
        start = parse_date_param(start)
    if end is not None:
        end = parse_date_param(end)
    ns = get_namespace(time_series)
    def doit():
        roi_lids = get_autoclass(time_series).rois_of_class(class_label,start,end,threshold,page)
        for roi_lid in roi_lids:
            if roi_lid is None:
                yield None
            else:
                yield ns + roi_lid
    result = list(doit())
    if result == [None]:
        return jsonr('end')
    return jsonr(result)

### mosaicing

@app.route('/<time_series>/api/mosaic/pid/<path:pid>')
def serve_mosaic(time_series=None,pid=None):
    """Serve a mosaic with all-default parameters"""
    hit = pid_resolver.resolve(pid=pid)
    if hit.extension == 'html': # here we generate an HTML representation with multiple pages
        return template_response('mosaics.html',hit=hit)
    else:
        return serve_mosaic_image(time_series,pid) # default mosaic image

@memoized
def get_sorted_tiles(bin_pid): # FIXME support multiple sort options
    adc_path = resolve_adc(bin_pid)
    hit = resolve_pid(bin_pid)
    # read ADC and convert to Tiles in size-descending order
    def descending_size(t):
        (w,h) = t.size
        return 0 - (w * h)
    # using read_target means we don't stitch. this is simply for performance.
    tiles = [Tile(t, (t[HEIGHT], t[WIDTH])) for t in list_targets(hit, adc_path=adc_path)]
    # FIXME instead of sorting tiles, sort targets to allow for non-geometric sort options
    tiles.sort(key=descending_size)
    return tiles

@memoized
def get_mosaic_layout(pid, scaled_size, page):
    tiles = get_sorted_tiles(pid)
    # perform layout operation
    return mosaic.layout(tiles, scaled_size, page, threshold=0.05)

def layout2json(layout, scale):
    """Doesn't actually produce JSON but rather JSON-serializable representation of the tiles"""
    for t in layout:
        (w,h) = t.size
        (x,y) = t.position
        yield dict(pid=t.image['pid'], width=w*scale, height=h*scale, x=x*scale, y=y*scale)

@app.route('/<time_series>/api/mosaic/<path:params>/pid/<path:pid>')
def serve_mosaic_image(time_series=None, pid=None, params='/'):
    """Generate a mosaic of ROIs from a sample bin.
    params include the following, with default values
    - series (mvco) - time series (FIXME: handle with resolver, clarify difference between namespace, time series, pid, lid)
    - size (1024x1024) - size of the mosaic image
    - page (1) - page. for typical image sizes the entire bin does not fit and so is split into pages.
    - scale - scaling factor for image dimensions """
    # parse params
    params = parse_params(params, size='1024x1024',page=1,scale=1.0)
    (w,h) = tuple(map(int,re.split('x',params[SIZE])))
    scale = float(params[SCALE])
    page = int(params[PAGE])
    # parse pid/lid
    hit = pid_resolver.resolve(pid=pid)
    # perform layout operation
    scaled_size = (int(w/scale), int(h/scale))
    layout = get_mosaic_layout(hit.bin_pid, scaled_size, page)
    # serve JSON on request
    if hit.extension == 'json':
        return jsonr(list(layout2json(layout, scale)))
    # resolve ROI file
    roi_path = resolve_roi(hit.bin_pid)
    # read all images needed for compositing and inject into Tiles
    with open(roi_path,'rb') as roi_file:
        for tile in layout:
            target = tile.image
            # FIXME use fast stitching
            tile.image = as_pil(get_fast_stitched_roi(hit.bin_pid, target[TARGET_NUMBER]))
    # produce and serve composite image
    mosaic_image = thumbnail(mosaic.composite(layout, scaled_size, mode='L', bgcolor=160), (w,h))
    (pil_format, mimetype) = image_types(hit)
    return image_response(mosaic_image, pil_format, mimetype)
    
@app.route('/<time_series>/api/blob/pid/<path:pid>')
def serve_blob(time_series,pid):
    """Serve blob zip or image"""
    pid_hit = pid_resolver.resolve(pid=pid)
    hit = blob_resolver.resolve(pid=pid,time_series=time_series)
    if hit is None:
        abort(404)
    zip_path = hit.value
    if hit.target is None: # bin, not target?
        if hit.extension != 'zip':
            abort(404)
        # the zip file is on disk, stream it directly to client
        return Response(file(zip_path), direct_passthrough=True, mimetype='application/zip', headers=max_age())
    else: # target, not bin
        blobzip = ZipFile(zip_path)
        png = blobzip.read(hit.lid+'.png')
        blobzip.close()
        # now determine PIL format and MIME type
        (pil_format, mimetype) = image_types(hit)
        if pid_hit.product == 'blob' and mimetype == 'image/png':
            return Response(png, mimetype='image/png', headers=max_age())
        else:
            # FIXME support more imaage types
            blob_image = Image.open(StringIO(png))
            if pid_hit.product == 'blob_outline':
                blob = np.asarray(blob_image.convert('L'))
                blob_outline = find_boundaries(blob)
                roi = np.asarray(get_stitched_roi(hit.bin_pid, int(hit.target)))
                blob = np.dstack([roi,roi,roi])
                blob[blob_outline] = [255,0,0]
                blob_image = Image.fromarray(blob,'RGB')
            return image_response(blob_image, pil_format, mimetype)

@app.route('/<time_series>/api/features/pid/<path:pid>')
def serve_features(time_series, pid):
    hit = fea_resolver.resolve(pid=pid,time_series=time_series)
    if hit is None:
        abort(404)
    return Response(file(hit.value), direct_passthrough=True, mimetype='text/csv', headers=max_age())

@app.route('/<time_series>/api/class_scores/pid/<path:pid>')
def serve_class_scores(time_series, pid):
    hit = class_scores_resolver.resolve(pid=pid,time_series=time_series)
    if hit is None:
        abort(404)
    csv_out = '\n'.join(represent.class_scoresmat2csv(hit.value, hit.bin_pid))
    return Response(csv_out + '\n', mimetype='text/plain', headers=max_age())

@app.route('/<time_series>/api/<path:ignore>')
def api_error(time_series,ignore):
    abort(404)

@app.route('/')
@app.route('/<time_series>')
@app.route('/<time_series>/')
@app.route('/<time_series>/dashboard')
@app.route('/<time_series>/dashboard/')
@app.route('/<time_series>/dashboard/<path:pid>')
def serve_timeseries(time_series='mvco', pid=None):
    template = dict(static=app.config[STATIC])
    if pid is not None:
        hit = pid_resolver.resolve(pid=pid)
        template['pid'] = hit.bin_pid
        template['time_series'] = hit.time_series
    else:
        hit = ts_resolver.resolve(time_series=time_series)
        if hit is None:
            abort(404)
        template['time_series'] = time_series
    template['base_url'] = hit.base_url
    template['page_title'] = html.fromstring(hit.title).text_content()
    template['title'] = hit.title
    template['all_series'] = [(h.time_series,h.name) for h in all_series.resolve_all()]
    return template_response('timeseries.html', **template)

@app.route('/api')
@app.route('/api.html')
def serve_doc():
    template = dict(static=app.config[STATIC])
    return template_response('api.html', **template)

@app.route('/about')
@app.route('/about.html')
def serve_about():
    template = dict(static=app.config[STATIC])
    return template_response('help.html', **template)

@app.route('/<path:pid>')
def resolve(pid):
    """Resolve a URL to some data endpoint in a time series, including bin and target metadata endpoints,
    and image endpoints"""
    # use the PID resolver (which also works for LIDs)
    hit = resolve_pid(pid)
    if hit is None:
        abort(404)
    # construct the namespace from the configuration and time series ID
    try:
        hit.date = iso8601(strptime(hit.date, hit.date_format))
    except:
        abort(404) # if the name is malformed, then it there's no resource to serve
    # determine extension
    if hit.extension is None: # default is .rdf
        hit.extension = 'rdf'
    # determine MIME type
    filename = '%s.%s' % (hit.lid, hit.extension)
    (mimetype, _) = mimetypes.guess_type(filename)
    if mimetype is None:
        mimetype = 'application/octet-stream'
    # is this request for a product?
    if hit.product is not None:
        if re.match(r'blob.*',hit.product):
            return serve_blob(hit.time_series,hit.pid)
        if re.match(r'features',hit.product):
            return serve_features(hit.time_series,hit.pid)
        if re.match(r'class_scores',hit.product):
            return serve_class_scores(hit.time_series,hit.pid)
    # is the request for a single target?
    if hit.target is not None:
        hit.target_no = int(hit.target) # parse target number
        if major_type(mimetype) == 'image': # need an image?
            mask = False
            if hit.product == 'stitch2':
                return serve_roi(hit, mask=None, stitch_version=2)
            if hit.product == 'mask':
                mask = True
            return serve_roi(hit, mask=mask) # serve it, or its mask
        else:  # otherwise serve metadata
            hit.target_pid = hit.namespace + hit.lid # construct target pid
            return serve_target(hit,mimetype)
    else: # nope, it's for a whole bin
        return serve_bin(hit,mimetype)
    # nothing recognized, so return Not Found
    abort(404)
# FIXME control flow in above is convoluted

@memoized
def read_targets(adc_path, target_no=1, limit=-1, schema_version=SCHEMA_VERSION_2):
    return list(read_adc(LocalFileSource(adc_path), target_no, limit, schema_version=schema_version))

def add_bin_pid(targets, bin_pid):
    for target in targets:
        # add a binID and pid what are the right keys for these?
        target['binID'] = '%s' % bin_pid
        target['pid'] = '%s_%05d' % (bin_pid, target[TARGET_NUMBER])
    return targets

def list_targets(hit, target_no=1, limit=-1, adc_path=None, stitch_targets=None):
    if stitch_targets is None:
        stitch_targets = app.config[STITCH]
    if adc_path is None:
        adc_path = resolve_adc(hit.bin_pid)
    targets = read_targets(adc_path, target_no, limit, hit.schema_version)
    if stitch_targets:
        targets = list_stitched_targets(targets)
    targets = add_bin_pid(targets, hit.bin_pid)
    return targets

def bin2csv_response(hit,targets):
    csv_out = '\n'.join(represent.bin2csv(targets, hit.schema_version))
    return Response(csv_out + '\n', mimetype='text/plain', headers=max_age())

def serve_bin(hit,mimetype):
    """Serve a sample bin in some format"""
    # for raw files, simply pass the file through
    if hit.extension == ADC:
        return Response(file(resolve_adc(hit.bin_pid)), direct_passthrough=True, mimetype='text/csv')
    elif hit.extension == ROI:
        return Response(file(resolve_roi(hit.bin_pid)), direct_passthrough=True, mimetype='application/octet-stream')
    # at this point we need to resolve the HDR file
    hdr_path = resolve_hdr(hit.bin_pid)
    if hit.extension == HDR:
        return Response(file(hdr_path), direct_passthrough=True, mimetype='text/plain')
    props = read_hdr(LocalFileSource(hdr_path))
    bin_props = get_bin_props(hit.time_series).get_props(hit.bin_lid)
    context = props[CONTEXT]
    del props[CONTEXT]
    # sort properties according to their order in the header schema
    props = [(k,props[k]) for k,_ in HDR_SCHEMA if k in props]
    # then add database props
    props += [(k,bin_props[k]) for k in sorted(bin_props.keys())]
    # get a list of all targets, taking into account stitching
    if hit.product != 'short':
        targets = list_targets(hit)
        target_pids = ['%s_%05d' % (hit.bin_pid, target['targetNumber']) for target in targets]
    else:
        targets = []
        target_pids = []
    template = dict(hit=hit,context=context,properties=props,targets=targets,target_pids=target_pids,static=app.config[STATIC])
    if minor_type(mimetype) == 'xml':
        return template_response('bin.xml', **template)
    elif minor_type(mimetype) == 'rdf+xml':
        return template_response('bin.rdf', mimetype='text/xml', **template)
    elif minor_type(mimetype) == 'csv':
        return bin2csv_response(hit, targets)
    elif mimetype == 'text/html':
        return template_response('bin.html', **template)
    elif mimetype == 'application/json':
        properties = dict(props)
        properties['context'] = context
        properties['targets'] = targets
        properties['date'] = hit.date
        if hit.product == 'short':
            del properties['targets']
        if hit.product == 'medium':
            properties['targets'] = target_pids
        return jsonr(properties)
    elif mimetype == 'application/zip':
        return Response(bin_zip(hit,targets,template), mimetype=mimetype, headers=max_age())
    else:
        abort(404)

def bin_zip(hit,targets,template):
    (hdr_path, adc_path, roi_path) = resolve_files(hit.bin_pid, (HDR, ADC, ROI))
    buffer = BytesIO()
    represent.bin_zip(hit, hdr_path, adc_path, roi_path, buffer)
    return buffer.getvalue()

def serve_target(hit,mimetype):
    target = get_target(hit) # read the target from the ADC file
    if target is None:
        abort(404)
    properties = target
    # sort the target properties according to the order in the schema
    schema_keys = [k for k,_ in ADC_SCHEMA[hit.schema_version]]
    target = [(k,target[k]) for k in order_keys(target, schema_keys)]
    # now populate the template appropriate for the MIME type
    template = dict(hit=hit,target=target,properties=properties,static=app.config[STATIC])
    if minor_type(mimetype) == 'xml':
        return template_response('target.xml', **template)
    elif minor_type(mimetype) == 'rdf+xml':
        return template_response('target.rdf', mimetype='text/xml', **template)
    elif mimetype == 'text/html':
        return template_response('target.html', **template)
    elif mimetype == 'application/json':
        return jsonr(dict(target))

def image_types(hit):
    # now determine PIL format and MIME type
    filename = '%s.%s' % (hit.lid, hit.extension)
    pil_format = filename2format(filename)
    (mimetype, _) = mimetypes.guess_type(filename)
    return (pil_format, mimetype)

def get_stitched_roi(bin_pid, target_no, mask=False, stitch_version=1):
    return get_roi_image(bin_pid, target_no, mask=mask, stitch_version=stitch_version)

def get_fast_stitched_roi(bin_pid, target_no):
    return get_roi_image(bin_pid, target_no, True)

def get_roi_image(bin_pid, target_no, fast_stitching=False, mask=False, stitch_version=1):
    """Serve a stitched ROI image given the output of the pid resolver"""
    # resolve the ADC and ROI files
    hit = resolve_pid(pid=bin_pid)
    schema_version = hit.schema_version
    (adc_path, roi_path) = resolve_files(bin_pid, (ADC, ROI))
    return get_roi_image_from_files(schema_version, adc_path, roi_path, bin_pid, target_no, fast_stitching, mask, stitch_version)

def get_roi_image_from_files(schema_version, adc_path, roi_path, bin_pid, target_no, fast_stitching=False, mask=False, stitch_version=1):
    to_stitch = app.config[STITCH]
    if to_stitch:
        offset=max(1,target_no-1)
        limit=3 # read three targets, in case we need to stitch
    else:
        offset=target_no
        limit=1 # just read one
    targets = list(read_targets(adc_path, offset, limit, schema_version)) 
    if len(targets) == 0: # no targets? return Not Found
        return None
    # open the ROI file as we may need to read more than one
    with open(roi_path,'rb') as roi_file:
        if to_stitch:
            pairs = list(find_pairs(targets)) # look for stitched pairs
        else:
            pairs = targets
        roi_image = None
        if len(pairs) >= 1: # found one?
            (a,b) = pairs[0] # split pair
            if b[TARGET_NUMBER] == target_no: # second of a pair?
                return None
            images = list(read_rois((a,b),roi_file=roi_file)) # read the images
            if mask:
                roi_image = stitching.mask((a,b))
            elif fast_stitching:
                roi_image = stitch_raw((a,b), images, background=180)
            elif stitch_version == 1:
                (roi_image, mask) = stitch((a,b), images) # stitch them
            elif stitch_version == 2:
                (roi_image, mask) = joe_stitch((a,b), images)
        else:
            # now check that the target number is correct
            for target in targets:
                if target[TARGET_NUMBER] == target_no:
                    images = list(read_rois([target],roi_file=roi_file)) # read the image
                    roi_image = images[0]
        return roi_image

def serve_roi(hit,mask=False, stitch_version=1):
    """Serve a stitched ROI image given the output of the pid resolver"""
    roi_image = get_stitched_roi(hit.bin_pid, hit.target_no, mask=mask, stitch_version=stitch_version)
    if roi_image is None:
        abort(404)
    # now determine PIL format and MIME type
    (pil_format, mimetype) = image_types(hit)
    # return the image data
    return image_response(roi_image,pil_format,mimetype)

app.secret_key = os.urandom(24)

if __name__=='__main__':
    """First argument is a config file which must at least have psql_connect in it
    to support feed arguments. Filesystem config is in the resolver."""
    if len(sys.argv) > 1:
        configure(get_config(sys.argv[1]))
    else:
        configure()
else:
    configure(get_config(os.environ['IFCB_CONFIG_FILE']))

# FIXME don't use globals
# FIXME do this in config
rs = parse_stream(app.config[RESOLVER])
binpid2path = rs['binpid2path']
pid_resolver = rs['pid']
blob_resolver = rs['mvco_blob']
fea_resolver = rs['features']
class_scores_resolver = rs['class_scores']
ts_resolver = rs['time_series']
all_series = rs['all_series']

if __name__=='__main__':
#    print blob_resolver.resolve(pid='http://demi.whoi.edu:5062/mvco/IFCB5_2012_243_142205_00179_blob.png')
#    print blob_resolver.resolve(pid='http://demi.whoi.edu:5062/Healy1101/IFCB8_2011_210_011714_00005_blob.png')
    app.run(host='0.0.0.0',port=app.config[PORT])
