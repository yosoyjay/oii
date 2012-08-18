from oii.csvio import read_csv

"""A sample represents a set of data files generated by the instrument in the course of
processing a seawater sample. This is not exactly true; some seawater samples are split
across multiple sets of files.

There are three files per set:

hdr - a header file containing metadata about the sample and instrument
adc - a CSV file containing metadata for each trigger
roi - a binary file containing image data for each ROI"""

ADC = 'adc'

# adc columns, 0-based. "x" is horizontal, "y" is vertical, x left to right, y bottom to top
TRIGGER = 'trigger'
PROCESSING_END_TIME = 'processingEndTime'
FLUORESENCE_HIGH = 'fluorescenceHigh'
FLUORESCENCE_LOW = 'fluorescenceLow'
SCATTERING_HIGH = 'scatteringHigh'
SCATTERING_LOW = 'scatteringLow'
COMPARATOR_PULSE = 'comparatorPulse'
TRIGGER_OPEN_TIME = 'triggerOpenTime'
FRAME_GRAB_TIME = 'frameGrabTime'
# location of ROI in camera field in pixels
BOTTOM = 'bottom'
LEFT = 'left'
# ROI extent in pixels
HEIGHT = 'height'
WIDTH = 'width'
# ROI byte offset
BYTE_OFFSET = 'byteOffset'
VALVE_STATUS = 'valveStatus'
PMTA = 'pmtA'
PMTB = 'pmtB'
PMTC = 'pmtC'
PMTD = 'pmtD'
PEAKA = 'peakA'
PEAKB = 'peakB'
PEAKC = 'peakC'
PEAKD = 'peakD'
TIME_OF_FLIGHT = 'timeOfFlight'
GRAB_TIME_START = 'grabTimeStart'
GRAB_TIME_END = 'grabTimeEnd'
COMPARATOR_OUT = 'comparatorOut'
START_POINT = 'startPoint'
SIGNAL_STRENGTH = 'signalStrength'

SCHEMA_VERSION_1 = 'v1'
SCHEMA_VERSION_2 = 'v2'

ADC_SCHEMA = {
SCHEMA_VERSION_1: [(TRIGGER, int),
          (PROCESSING_END_TIME, float),
          (FLUORESENCE_HIGH, float),
          (FLUORESCENCE_LOW, float),
          (SCATTERING_HIGH, float),
          (SCATTERING_LOW, float),
          (COMPARATOR_PULSE, float),
          (TRIGGER_OPEN_TIME, float),
          (FRAME_GRAB_TIME, float),
          (BOTTOM, int),
          (LEFT, int),
          (HEIGHT, int),
          (WIDTH, int),
          (BYTE_OFFSET, int),
          (VALVE_STATUS, float)],
SCHEMA_VERSION_2: [(TRIGGER, int),
              (PROCESSING_END_TIME, float),
              (PMTA, float),
              (PMTB, float),
              (PMTC, float),
              (PMTD, float),
              (PEAKA, float),
              (PEAKB, float),
              (PEAKC, float),
              (PEAKD, float),
              (TIME_OF_FLIGHT, float),
              (GRAB_TIME_START, float),
              (FRAME_GRAB_TIME, float), # assuming this is equivalent to "grab time end"
              (BOTTOM, int),
              (LEFT, int),
              (HEIGHT, int),
              (WIDTH, int),
              (BYTE_OFFSET, int),
              (COMPARATOR_OUT, float),
              (START_POINT, int),
              (SIGNAL_STRENGTH, int),
              (VALVE_STATUS, int)] # assuming this is equivalent to "status"
}

TARGET_NUMBER = 'targetNumber' # 1-based index of ROI in bins
BIN_ID = 'binID' # bin ID
TARGET_ID = 'targetID' # target ID
PID = 'pid'
STITCHED = 'stitched'

def read_adc(source, target_no=1, limit=-1, schema_version=SCHEMA_VERSION_1):
    """Convert ADC data in its native format to dictionaries representing each target.
    Read starting at the specified target number (default 1)"""
    target_number = target_no
    for row in read_csv(source, ADC_SCHEMA[schema_version], target_no-1, limit):
        target_number += 1
        # skip 0x0 targets
        if row[WIDTH] * row[HEIGHT] > 0:
            # add target number
            row[TARGET_NUMBER] = target_number
            yield row

def read_target(source, target_no, schema_version=SCHEMA_VERSION_1):
    for target in read_adc(source, target_no-1, limit=1, schema_version=schema_version):
        return target
    raise KeyError('ADC data not found')
