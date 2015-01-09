import os
import json
from zipfile import ZipFile, ZIP_DEFLATED
import shutil
import tempfile

from jinja2 import Environment

from oii.image.io import as_bytes
from oii.csvio import csv_str, csv_quote
from oii.ifcb2.identifiers import PID
from oii.ifcb2.formats.adc import TARGET_NUMBER
from oii.ifcb2.image import read_target_image

from oii.ifcb2.stitching import STITCHED, PAIR
from oii.ifcb2.v1_stitching import stitch

def targets2csv(targets,schema_cols,headers=True):
    """Given targets, produce a CSV representation in the specified schema;
    targets should have had binID and pid added to them (see oii.ifcb2.identifiers)"""
    ks = schema_cols + ['binID','pid','stitched','targetNumber']
    if headers:
        yield ','.join(ks)
    for target in targets:
        # fetch all the data for this row as strings, emit
        yield ','.join(csv_quote(csv_str(target[k])) for k in ks)

BIN_XML_TEMPLATE = """<Bin xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns="http://ifcb.whoi.edu/terms#">
  <dc:identifier>{{pid}}</dc:identifier>
  <dc:date>{{timestamp}}</dc:date>{% for v in context %}
  <context>{{v}}</context>{% endfor %}{% for k,v in properties %}
  <{{k}}>{{v}}</{{k}}>{% endfor %}{% for target_pid in target_pids %}
  <Target dc:identifier="{{target_pid}}"/>{% endfor %}
</Bin>
"""

def split_hdr(parsed_hdr):
    context = parsed_hdr['context']
    properties = [(k,v) for k,v in parsed_hdr.items() if k != 'context']
    return context, properties

def _get_bin_template_bindings(pid,hdr,targets,timestamp):
    """pid should be the bin pid (with namespace)
    timestamp should be a text timestamp in iso8601 format
    hdr should be the result of calling parse_hdr on a header file
    targets should be a list of target dicts with target pids"""
    context, properties = split_hdr(hdr)
    target_pids = [target[PID] for target in targets]
    return dict(pid=pid,timestamp=timestamp,context=context,properties=properties,target_pids=target_pids)

def bin2xml(pid,hdr,targets,timestamp):
    """pid should be the bin pid (with namespace)
    timestamp should be a text timestamp in iso8601 format
    hdr should be the result of calling parse_hdr on a header file
    targets should be a list of target dicts with target pids"""
    bindings = _get_bin_template_bindings(pid,hdr,targets,timestamp)
    return Environment().from_string(BIN_XML_TEMPLATE).render(**bindings)

BIN_RDF_TEMPLATE = """<rdf:RDF xmlns:dcterms="http://purl.org/dc/terms/" xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns="http://ifcb.whoi.edu/terms#">
  <Bin rdf:about="{{pid}}">
    <dc:date>{{timestamp}}</dc:date>{% for v in context %}
    <context>{{v}}</context>{% endfor %}{% for k,v in properties %}
    <{{k}}>{{v}}</{{k}}>{% endfor %}
    <dcterms:hasPart>
      <rdf:Seq rdf:about="{{pid}}/targets">{% for target_pid in target_pids %}
        <rdf:li>
          <Target rdf:about="{{target_pid}}"/>
        </rdf:li>{% endfor %}
      </rdf:Seq>
    </dcterms:hasPart>
  </Bin>
</rdf:RDF>"""

def bin2rdf(pid,hdr,targets,timestamp):
    """pid should be the bin pid (with namespace)
    timestamp should be a text timestamp in iso8601 format
    hdr should be the result of calling parse_hdr on a header file
    targets should be a list of target dicts with target pids"""
    bindings = _get_bin_template_bindings(pid,hdr,targets,timestamp)
    return Environment().from_string(BIN_RDF_TEMPLATE).render(**bindings)

def bin2dict_short(pid,hdr,timestamp):
    context, rep = split_hdr(hdr)
    rep = dict(rep)
    rep['context'] = context
    rep['date'] = timestamp
    rep['pid'] = pid
    return rep

def bin2dict_medium(pid,hdr,targets,timestamp):
    rep = bin2dict_short(pid,hdr,timestamp)
    rep['targets'] = [target[PID] for target in targets]
    return rep

def bin2dict(pid,hdr,targets,timestamp):
    rep = bin2dict_short(pid,hdr,timestamp)
    rep['targets'] = list(targets)
    return rep

def bin2json_short(pid,hdr,timestamp):
    return json.dumps(bin2dict_short(pid,hdr,timestamp))

def bin2json_medium(pid,hdr,targets,timestamp):
    return json.dumps(bin2dict_medium(pid,hdr,targets,timestamp))

def bin2json(pid,hdr,targets,timestamp):
    return json.dumps(bin2dict(pid,hdr,targets,timestamp))

def bin2zip(parsed_pid,canonical_pid,targets,hdr,timestamp,roi_path,outfile):
    bin_lid = parsed_pid['bin_lid']
    adc_cols = parsed_pid['adc_cols'].split(' ')
    targets = list(targets)
    with tempfile.SpooledTemporaryFile() as temp:
        z = ZipFile(temp,'w',ZIP_DEFLATED)
        csv_out = '\n'.join(targets2csv(targets, adc_cols))+'\n'
        z.writestr(bin_lid + '.csv', csv_out)
        xml_out = bin2xml(canonical_pid,hdr,targets,timestamp)
        z.writestr(bin_lid + '.xml', xml_out)
        with open(roi_path,'rb') as roi_file:
            for target in targets:
                if STITCHED in target and target[STITCHED] != 0:
                    subRois = [read_target_image(t,file=roi_file) for t in target[PAIR]]
                    im,_ = stitch(target[PAIR], subRois)
                else:
                    im = read_target_image(target, file=roi_file)
                target_lid = os.path.basename(target['pid'])
                z.writestr(target_lid + '.png', as_bytes(im, mimetype='image/png'))
        z.close()
        temp.seek(0)
        shutil.copyfileobj(temp, outfile)

# individual target representations

TARGET_XML_TEMPLATE="""<Target xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns="http://ifcb.whoi.edu/terms#" number="{{targetNumber}}">
  <dc:identifier>{{pid}}</dc:identifier>
  <dc:date>{{timestamp}}</dc:date>
  {% for k,v in target %}
  <{{k}}>{{v}}</{{k}}>
  {% endfor %}
  <dcterms:hasFormat>{{pid}}.png</dcterms:hasFormat>
  <dcterms:isPartOf>{{bin_pid}}</dcterms:isPartOf>
</Target>
"""

TARGET_RDF_TEMPLATE="""<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns="http://ifcb.whoi.edu/terms#">
<Target rdf:about="{{pid}}">{% for k,v in target %}
  <{{k}}>{{v}}</{{k}}>{% endfor %}
  <binID>{{bin_pid}}</binID>
  <dcterms:hasFormat>{{pid}}.png</dcterms:hasFormat>
</Target>
</rdf:RDF>
"""

def _target2metadata(pid, target, timestamp, bin_pid, template):
    bindings = {
        'pid': pid,
        'target': target.items(), # FIXME sort according to ADC schema using oii.utils.order_keys
        'targetNumber': target['targetNumber'],
        'bin_pid': bin_pid
    }
    return Environment().from_string(template).render(**bindings)

def target2xml(pid, target, timestamp, bin_pid):
    return _target2metadata(pid, target, timestamp, bin_pid, TARGET_XML_TEMPLATE)

def target2rdf(pid, target, timestamp, bin_pid):
    return _target2metadata(pid, target, timestamp, bin_pid, TARGET_RDF_TEMPLATE)

