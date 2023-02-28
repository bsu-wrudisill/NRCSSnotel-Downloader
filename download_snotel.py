import requests
import subprocess
import pandas as pd
import numpy as np
import logging
from sqlalchemy import create_engine
import sys
import traceback


# ------------ Main -------------#
# User parameters
new_columns_names = ['Date', 'SWE', 'AccPrecip', 'TmaxF', 'TminF', 'TavgF', 'IncPrecip']
NoDataValue = np.nan # this is a 'global'

# NRCS Snotel Web URL
target_url = "https://wcc.sc.egov.usda.gov/reportGenerator/view_csv/customSingleStationReport/daily/{}:{}:SNTL%7Cid=%22%22%7Cname/POR_BEGIN,POR_END/WTEQ::value,PREC::value,TMAX::value,TMIN::value,TAVG::value,PRCP::value"


# NRCS Snotel daily reporting column headers
required_columns = ['Date',
                    'Snow Water Equivalent (in) Start of Day Values',
                    'Precipitation Accumulation (in) Start of Day Values',
                    'Air Temperature Maximum (degF)',
                    'Air Temperature Minimum (degF)',
                    'Air Temperature Average (degF)',
                    'Precipitation Increment (in)']
# NRCS Snotel daily reporting row start of data
first_data_row = 59 + 5  # this could change if they update the website
column_names = 58 + 5


# ------------ Functions -------------#
def passfail(func):
	def wrapped_func(*args, **kwargs):
		try:
			output = func(*args)
			message = "{} success".format(str(func))
			return (True,output)
		except Exception as e:
			trace_string =  traceback.format_exc()
			error_message = "{} lead to the following Error: {}\n {}".format(str(func), e, trace_string)
			return (False, error_message)
	return wrapped_func


# operates on a single item
def nan_or_float(x):
    if x == ' ':
        y = NoDataValue
    if x == '':
        y = NoDataValue
    else:
        try:
            y = float(x)
        except:
            raise Exception
    return y

def water_year(date):
    if (date.dayofyear > 274) and (date.dayofyear < 365):
        wy = date.year + 1
    else:
        wy = date.year
    return wy

# pandas.core.series.Series operations
# these aren't used
def test_increasing(series):
    check = not (df.Pinc.diff() > 0).any()
    df.SWE.diff()
    assert check  # this is just to raise the e


def test_negatives(series):
    pass


# ------- Main Function  ----------#
@passfail
def snotelLogger(identifier):
    number, state = identifier
    logger.info('beginning download .... {}{}'.format(number, state))

    # ------------ Download -----------#
    response = requests.get(target_url.format(number, state))
    data = response.text
    data_list = [x.split(',') for x in data.split('\n')] # yikes that's ugly


    # ------ Check that the download was successful ------#
    assert 'ERROR' not in data, 'download failed'

    for column in required_columns:
        assert column in data_list[column_names], 'check column_names = {}'.format(57)

    # ------- Create a Pandas Data frame ------#
    df = pd.DataFrame(data_list[first_data_row:-1])  # omit the last row??
    df.columns = new_columns_names

    df.Date = pd.to_datetime(df.Date)
    df.set_index(df.Date, inplace=True)

    # check that there are no missing date values
    start_date = df.Date.iloc[0]
    end_date = df.Date.iloc[-1]
    date_range = pd.date_range(start_date, end_date, freq='D')
    assert len(df.Date) == len(date_range), 'the dates/times do not match'

    # convert data to floats. if there is no data, convert to np.nan
    for var in new_columns_names:
        if var not in 'Date':
            df[var] = list(map(nan_or_float, df[var]))

    del df['Date']
    return df

@passfail
def readNRCS(state):
    baseurl = "https://wcc.sc.egov.usda.gov/nwcc/yearcount?network=sntl&state={}&counttype=statelist"
    dflist = pd.read_html(baseurl.format(state))

    df = dflist[1] # the first 0-2 columns are junk

    def getid(name):
        return name.split('(')[1].split(')')[0]
    def getname(name):
        return name.split('(')[0].strip()
    def gethucname(huc):
        return huc.split('(')[0].strip()
    def gethucid(huc):
        return huc.split('(')[1].split(')')[0]

    df['site_id'] = list(map(getid, df['site_name']))
    df['site_name'] = list(map(getname, df['site_name']))
    df['huc_name'] = list(map(gethucname, df['huc']))
    df['huc_id'] = list(map(gethucid, df['huc']))
    del df['huc']
    return df


def get_logger():
    log_file_info  = 'info.log'
    log_file_error = 'error.log'
    log           = logging.getLogger(__name__)
    log_formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    # comment this to suppress console output
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(log_formatter)
    log.addHandler(stream_handler)

    file_handler_info = logging.FileHandler(log_file_info, mode='w')
    file_handler_info.setFormatter(log_formatter)
    file_handler_info.setLevel(logging.INFO)
    log.addHandler(file_handler_info)

    file_handler_error = logging.FileHandler(log_file_error, mode='w')
    file_handler_error.setFormatter(log_formatter)
    file_handler_error.setLevel(logging.ERROR)
    log.addHandler(file_handler_error)
    log.setLevel(logging.INFO)
    return log
#
if __name__ == '__main__':
    # read the primary nrcs web portal table to get a list of all stations. log to the database
    logger = get_logger()

    # !!!!!!!!!!!!! CHANGE ME !!!!!!!!!!!!!!!!!
    # the 'state' should be a string abbreviation for the desired US state. 
    # make sure that the state you are interested in in fact has snotel stations ....
    # this example is for "colorado"
    state = 'CO'
    #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    
    engine = create_engine('sqlite:///%s_snotel.db'%(state), echo=False)
    success, result= readNRCS(state)

    if not success:
        logger.critical('failure: {}'.format(result))
        sys.exit()
    else:
        main_df = result
    # main_df.to_csv("CO_snotel.csv")

    main_df.to_sql('main', con = engine, if_exists='append')
    logger.info('Begin download of {} SNOTEL data'.format(state))

    # go through site created in the main_df, download and parse, add to database
    for i in main_df.site_id:
        success, result = snotelLogger((i, state))
        if not success:
            logger.info('{} FAILURE'.format(i))
            logger.error('{} FAILURE'.format(i))
            logger.error('failure: {}'.format(result))
            continue
        else:
            df = result
            df.to_sql(str(i), con=engine, if_exists='append')


    #-------- DATA CHECKING --------


