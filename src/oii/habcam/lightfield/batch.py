import sys
import os
import re
import urllib2 as urllib
import json
import logging

import numpy as np
from skimage import img_as_float
from skimage.io import imread, imsave
from skimage.transform import resize

from oii import resolver
from oii.procutil import Process
from oii.image.demosaic import demosaic

DATA_NAMESPACE='http://molamola.whoi.edu:7070/'
RESOLVER='./oii/habcam/image_resolver.xml'
SCRATCH='/scratch3/ic201301'
PATTERN='rggb' # v4
IC_EXEC='/home/jfutrelle/honig/ic-build/multi-image-correction/illum_correct_average'

NUM_LEARN=1600
NUM_PROCS=12
NUM_THREADS=24

DEFAULT_IC_CONFIG = {
    'delta': 0.1,
    'min_height': 1.0,
    'max_height': 4.0,
    'img_min': 0.3,
    'img_max': 3.1,
    'smooth': 32,
    'num_threads': NUM_THREADS
}
lgfmt = '%(asctime)-15s %(message)s'
logging.basicConfig(format=lgfmt,stream=sys.stdout,level=logging.DEBUG)

def mkdirs(d):
    """Create directories"""
    if not os.path.exists(d):
        os.makedirs(d)
        logging.info('created directory %s' % d)
    else:
        logging.info('directory %s exists' % d)
    return d

def scratch(bin_lid,suffix=''):
    """Compute path to scratch space"""
    return os.path.join(SCRATCH,bin_lid,suffix)

def list_images(bin_lid):
    """List all images in a bin by LID"""
    bin_url = DATA_NAMESPACE + bin_lid + '.json'
    logging.info('listing images for %s' % bin_lid)
    ds = json.loads(urllib.request.urlopen(bin_url).read())
    for d in ds:
        yield d['imagename']

def metadata2eic(url):
    """Convert bin image metadata to CSV"""
    logging.info('fetching image metadata from %s' % url)
    ds = json.loads(urllib.request.urlopen(url).read())
    fields = ['imagename','alt','pitch','roll']
    for d in ds:
        yield list(map(str,[d[k] for k in fields]))

def fetch_eic(bin_lid,suffix='',tmp=None):
    """Fetch bin image metadata and write CSV to file"""
    if tmp is None:
        tmp = mkdirs(os.path.join(scratch(bin_lid),'tmp'))
    bin_url = DATA_NAMESPACE + bin_lid + '.json'
    eic = os.path.join(tmp,bin_lid + '.eic')
    with open(eic,'w') as fout:
        for tup in metadata2eic(bin_url):
            tup[0] = remove_extension(tup[0]) + suffix
            print(' '.join(tup), file=fout)
    return eic

def remove_extension(p):
    return re.sub(r'\.[a-zA-Z]+$','',p)

def change_extension(p,ext):
    return remove_extension(p) + '.%s' % ext

def as_tiff(imagename):
    """Change extension on imagename to 'tif'"""
    return change_extension(imagename,'tif')

def get_tenmin(bin_lid):
    """Find ten minute directory of TIFFs"""
    # now resolve one of the image files
    logging.info('searching for ten minute directory for %s' % bin_lid)
    for imagename in list_images(bin_lid):
        break
    resolvers = resolver.parse_stream(RESOLVER)
    hit = resolvers['image'].resolve(pid=as_tiff(imagename))
    return re.sub(r'/[^/]+$','/',hit.value)

def split(bin_lid):
    """Split TIFFs into L and R"""
    resolvers = resolver.parse_stream(RESOLVER)
    suffixes = ['_cfa_' + camera for camera in 'LR']
    outdirs = [scratch(bin_lid,bin_lid + suffix) for suffix in suffixes]
    for od in outdirs:
        mkdirs(od)
    imagenames = list(list_images(bin_lid))
    (h,w)=(None,None)
    tiff = None
    # read an image to determine h,w
    for imagename in imagenames:
        for outdir,suffix in zip(outdirs,suffixes):
            LRout = os.path.join(outdir,remove_extension(imagename) + suffix + '.tif')
            if h is None:
                if tiff is None:
                    tiff = as_tiff(imagename)
                    cfain = resolvers['image'].resolve(pid=as_tiff(imagename)).value
                    (h,w) = imread(cfain,plugin='freeimage').shape
    # now fork
    pids = []
    for n in range(NUM_PROCS):
        pid = os.fork()
        if pid == 0:
            for imagename in imagenames[n::NUM_PROCS]:
                tiff = None
                for outdir,suffix,offset in zip(outdirs,suffixes,[0,1]):
                    LRout = os.path.join(outdir,remove_extension(imagename) + suffix + '.tif')
                    if not os.path.exists(LRout):
                        if tiff is None:
                            tiff = as_tiff(imagename)
                            cfain = resolvers['image'].resolve(pid=as_tiff(imagename)).value
                            logging.info('loading %s' % cfain)
                            cfa = imread(cfain,plugin='freeimage')
                            (h,w) = cfa.shape
                    if not os.path.exists(LRout):
                        logging.info('splitting %s -> %s' % (cfain, LRout))
                        half = w / 2
                        off = offset * half
                        imsave(LRout,cfa[:,off:off+half],plugin='freeimage')
            os._exit(0)
        else:
            pids += [pid]
    for pid in pids:
        os.waitpid(pid,0)
        logging.info('joined splitting process %d' % pid)
    return (h,w),outdirs

def learn(bin_lid):
    # provision temp space
    tmp = mkdirs(scratch(bin_lid,'tmp'))
    # find TIFFs
    tenmin = get_tenmin(bin_lid)
    # split TIFFs into L and R
    ((h,w),LR_dirs) = split(bin_lid)
    pids = []
    for LR,LR_dir in zip('LR',LR_dirs):
        pid = os.fork()
        if pid == 0: # subprocess
            # fetch eic
            eic = fetch_eic(bin_lid,suffix='_cfa_%s' % LR)
            # construct param file
            lightmap_dir = mkdirs(scratch(bin_lid,bin_lid + '_lightmap_' + LR))
            lightmap = os.path.join(lightmap_dir,bin_lid+'_lightmap_' + LR)
            if os.path.exists(lightmap):
                logging.info('lightmap exists: %s' % lightmap)
            else: # lightmap doesn't exist
                learn_tmp =  mkdirs(scratch(bin_lid,bin_lid + '_lightmap_tmp_' + LR))
                param = os.path.join(tmp,bin_lid + '_learn.txt')
                # produce param file
                logging.info('writing param file %s' % param)
                with open(param,'w') as fout:
                    print('imagedir %s' % LR_dir, file=fout)
                    print('metafile %s' % re.sub(r'/([^/]+)$',r'/ \1',eic), file=fout)
                    print('tmpdir %s' % learn_tmp, file=fout)
                    print('save %s' % lightmap, file=fout) 
                    for k,v in list(DEFAULT_IC_CONFIG.items()):
                        print('%s %s' % (k,str(v)), file=fout)
                    print('binary_format', file=fout)
                    print('num_to_process %d' % NUM_LEARN, file=fout)
                    print('num_rows %d' % h, file=fout)
                    print('num_cols %d' % (w/2), file=fout)
                    print(PATTERN, file=fout)
                    print('scallop_eic', file=fout)
                    print('learn', file=fout)
                # now learn
                learn = Process('"%s" "%s"' % (IC_EXEC, param))
                for line in learn.run():
                    logging.info(line['message'])
            os._exit(0)
        else:
            pids += [pid]
    # done
    for pid in pids:
        os.waitpid(pid,0)
        logging.info('joined learn process %s' % pid)

def correct(bin_lid):
    # provision temp space
    tmp = mkdirs(scratch(bin_lid,'tmp'))
    # find TIFFs
    tenmin = get_tenmin(bin_lid)
    # split TIFFs into L and R
    ((h,w),LR_dirs) = split(bin_lid)
    for LR,LR_dir in zip('LR',LR_dirs):
        # fetch eic
        eic = fetch_eic(bin_lid,suffix='_cfa_%s' % LR)
        # construct param file
        lightmap_dir = mkdirs(scratch(bin_lid,bin_lid + '_lightmap_' + LR))
        lightmap = os.path.join(lightmap_dir,bin_lid+'_lightmap_' + LR)
        assert os.path.exists(lightmap)
        learn_tmp =  mkdirs(scratch(bin_lid,bin_lid + '_lightmap_tmp_' + LR))
        outdir = mkdirs(scratch(bin_lid,bin_lid + '_cfa_illum_' + LR))
        rgbdir = mkdirs(scratch(bin_lid,bin_lid + '_rgb_illum_' + LR))
        param = os.path.join(tmp,bin_lid + '_learn.txt')
        # produce param file
        logging.info('writing param file %s' % param)
        with open(param,'w') as fout:
            print('imagedir %s' % LR_dir, file=fout)
            print('outdir %s' % outdir, file=fout)
            print('metafile %s' % re.sub(r'/([^/]+)$',r'/ \1',eic), file=fout)
            print('tmpdir %s' % learn_tmp, file=fout)
            print('load %s' % lightmap, file=fout) 
            print('num_to_process -1', file=fout)
            for k,v in list(DEFAULT_IC_CONFIG.items()):
                print('%s %s' % (k,str(v)), file=fout)
            print('binary_format', file=fout)
            print('num_rows %d' % h, file=fout)
            print('num_cols %d' % (w/2), file=fout)
            print(PATTERN, file=fout)
            print('scallop_eic', file=fout)
            print('correct', file=fout)
        # now correct
        logging.info('correcting %s' % bin_lid)
        correct = Process('"%s" "%s"' % (IC_EXEC, param))
        for line in correct.run():
            logging.info(line['message'])
        # now demosaic
        imgs = os.listdir(outdir)
        pids = []
        for n in range(NUM_PROCS):
            pid = os.fork()
            if pid == 0:
                for f in imgs[n::NUM_PROCS]:
                    png = os.path.join(rgbdir,re.sub(r'_[a-zA-Z_.]+$','_rgb_illum_%s.png' % LR,f))
                    p = os.path.join(outdir,f)
                    cfa = img_as_float(imread(p,plugin='freeimage'))
                    rgb = demosaic(cfa,PATTERN)
                    imsave(png,rgb)
                    logging.info('debayered %s' % png)
                    (h,w,_) = rgb.shape
                    aspect = 1. * w / h
                    thumb = os.path.join(rgbdir,re.sub(r'_[a-zA-Z_.]+$','_rgb_illum_%s_thumb.jpg' % LR,f))
                    imsave(thumb,resize(rgb,(480,int(480*aspect))))
                    logging.info('saved thumbnail %s' % thumb)
                os._exit(0)
            else:
                pids += [pid]
        for pid in pids:
            os.waitpid(pid,0)
            logging.info('joined debayering process %d' % pid)

if __name__=='__main__':
    bin_lid = sys.argv[1]
    learn(bin_lid)
    correct(bin_lid)
    
