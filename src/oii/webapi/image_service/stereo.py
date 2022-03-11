import sys
from skimage.io import imsave
from skimage.color import rgb2gray
from oii.image.io import imread
from oii.image.stereo import get_L, get_R, redcyan
from oii.resolver import parse_stream
import fileinput

def get_img(hit):
    if hit is None:
        raise Exception('Not found')
    img = imread(hit.value)
    try:
        if hit.product == 'redcyan':
            return redcyan(rgb2gray(img),None,True)
        if hit.variant is None:
            hit.variant = hit.default_variant
        if hit.variant == 'L':
            return get_L(img)
        elif hit.variant == 'R':
            return get_R(img)
    except AttributeError:
        pass
    return img

def get_resolver(path):
    """assumes there is a resolver called 'image'"""
    resolvers = parse_stream(path)
    return resolvers['image']






