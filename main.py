import pandas as pd
from openclean.pipeline import stream
from openclean.cluster.key import KeyCollision
from openclean.function.value.key.fingerprint import Fingerprint
from openclean_geo.address.usstreet import USStreetNameKey
from openclean_geo.address.usstreet import StandardizeUSStreetName
from openclean.function.eval.domain import Lookup
from openclean.function.eval.base import Col
from geojson import Point, MultiPolygon, Feature, Polygon
from turfpy.measurement import boolean_point_in_polygon
import json
import time


# define the global varaibles
borough_zipcode_df = pd.read_csv('./data/borough_zipcode.csv')
borough_name_mapping = {'Manhattan': 'MANHATTAN', 'Staten': 'STATEN ISLAND', 'Bronx': 'BRONX', 'Queens': 'QUEENS',
                        'Brooklyn': 'BROOKLYN'}
zipcodestr_to_borough_dict = dict()
for index, row in borough_zipcode_df.iterrows():
    zipcodestr_to_borough_dict[str(row['ZIP_CODE'])] = borough_name_mapping[row['BOROUGH']]
zipcodes_list = list(zipcodestr_to_borough_dict.keys())


# Here we define the mapping of column name for the 10 datasets
column_name_mapping_gcaa = {
    'Borough': 'BOROUGH',
    'Zip Code': 'ZIP CODE',
    'Latitude': "LATITUDE",
    'Longitude': 'LONGITUDE',
    'Address': 'STREET NAME'
}

column_name_mapping_4ph8 = {
    'BOROUGH': 'BOROUGH',
    'ZIP CODE': 'ZIP CODE',
    'LATITUDE': 'LATITUDE',
    'LONGITUDE': 'LONGITUDE',
    'ADDRESS': 'STREET NAME'
}

column_name_mapping_wcy4 = {
    'BOROUGH': 'BOROUGH',
    'ZIP': 'ZIP CODE',
    'LATITUDE': 'LATITUDE',
    'LONGITUDE': 'LONGITUDE',
    'ADDRESS': 'STREET NAME'
}

column_name_mapping_pn8j = {
    'BORO': 'BOROUGH',
    'ZIPCODE': 'ZIP CODE',
    'Latitude': 'LATITUDE',
    'Longitude': 'LONGITUDE',
    'STREET': 'STREET NAME'
}

column_name_mapping_2jhb = {
    'BOROUGH': 'BOROUGH',
    'Postcode': 'ZIP CODE',
    'Latitude': 'LATITUDE',
    'Longitude': 'LONGITUDE',
    'Street': 'STREET NAME'
}

column_name_mapping_m9mq = {
    'Borough': 'BOROUGH',
    'Zip Code': 'ZIP CODE',
    'latitude': 'LATITUDE',
    'longitude': 'LONGITUDE',
    'address': 'STREET NAME'
}

column_name_mapping_wt5b = {
    'borough': 'BOROUGH',
    'Zip': 'ZIP CODE',
    'Latitude': 'LATITUDE',
    'Longitude': 'LONGITUDE',
    'address': 'STREET NAME'
}

column_name_mapping_k24g = {
    'BOROUGH': 'BOROUGH',
    'ZIP': 'ZIP CODE',
    'LATITUDE': 'LATITUDE',
    'LONGITUDE': 'LONGITUDE',
    'STREET_NAME': 'STREET NAME'
}

column_name_mapping_2q9a = {
    'BOROUGH': 'BOROUGH',
    'Zip Code': 'ZIP CODE',
    'LATITUDE': 'LATITUDE',
    'LONGITUDE': 'LONGITUDE',
    'Street Name': 'STREET NAME'
}

column_name_mapping_gqsg = {
    'Borough': 'BOROUGH',
    'Zip Code': 'ZIP CODE',
    'Latitude': 'LATITUDE',
    'Longitude': 'LONGITUDE',
    'Address': 'STREET NAME'
}

filename_map = {
    'data-cityofnewyork-us.2pg3-gcaa.csv': column_name_mapping_gcaa,
    'data-cityofnewyork-us.3ub5-4ph8.csv': column_name_mapping_4ph8,
    'data-cityofnewyork-us.5ziv-wcy4.csv': column_name_mapping_wcy4,
    'data-cityofnewyork-us.43nn-pn8j.csv': column_name_mapping_pn8j,
    'data-cityofnewyork-us.bty7-2jhb.csv': column_name_mapping_2jhb,
    'data-cityofnewyork-us.dpm2-m9mq.csv': column_name_mapping_m9mq,
    'data-cityofnewyork-us.fp78-wt5b.csv': column_name_mapping_wt5b,
    'data-cityofnewyork-us.gjm4-k24g.csv': column_name_mapping_k24g,
    'data-cityofnewyork-us.ipu4-2q9a.csv': column_name_mapping_2q9a,
    'data-cityofnewyork-us.p6bh-gqsg.csv': column_name_mapping_gqsg
}


# create dictionary for fixing streetnames
def create_street_dict(cluster, STREET_MAPPING):
    for i in range(len(cluster)):
        sorted_cluster = sorted(cluster[i].items(), key=lambda x: x[1])
        for j in range(len(sorted_cluster)):
            if j == 0:
                value = sorted_cluster[j][0]
            else:
                STREET_MAPPING[sorted_cluster[j][0]] = value
    return STREET_MAPPING


# function for fixing street typos in columns using openclean
def fix_typos(df, column_name):
    df[column_name] = df[column_name].apply(lambda x: str(x).upper())
    ds = stream(df)
    f = StandardizeUSStreetName(characters='upper', alphanum=True, repeated=False)
    ds = ds.update(column_name, f)
    street_values = ds.distinct(column_name)
    street_clusters = KeyCollision(func=USStreetNameKey()).clusters(street_values)
    if len(street_clusters) == 0:
        print('No clusters remaining')
        return ds
    else:
        STREET_MAPPING = {}
        STREET_MAPPING = create_street_dict(street_clusters, STREET_MAPPING)
        ds = ds.update(column_name, Lookup(column_name, STREET_MAPPING, default=Col(column_name)))
        street_values = ds.distinct(column_name)
        street_clusters = KeyCollision(func=USStreetNameKey()).clusters(street_values)
        clusters_print(street_clusters, 10)
        print('No clusters remaining')
        return ds


# function for fixing clusters
def clusters_print(clusters, k=5):
    clusters = sorted(clusters, key=lambda x: len(x), reverse=True)
    val_count = sum([len(c) for c in clusters])
    print('Total number of clusters is {} with {} values'.format(len(clusters), val_count))
    for i in range(min(k, len(clusters))):
        print('\nCluster {}'.format(i + 1))
        for key, cnt in clusters[i].items():
            if key == '':
                key = "''"
            print('  {} (x {})'.format(key, cnt))


def fix_borough(df):
    borough_standandization_map = {
        'Manhattan': 'MANHATTAN',
        'MANHATTAN': 'MANHATTAN',
        'manhattan': 'MANHATTAN',
        'Bronx': 'BRONX',
        'BRONX': 'BRONX',
        'Brooklyn': 'BROOKLYN',
        'BROOKLYN': 'BROOKLYN',
        'Queens': 'QUEENS',
        'QUEENS': 'QUEENS',
        'Staten Island': 'STATEN ISLAND',
        'STATEN ISLAND': 'STATEN ISLAND',
        'NEW YORK': 'MANHATTAN',
        '': ''
    }

    if 'BOROUGH' in df.columns.tolist():
        df['BOROUGH'] = df['BOROUGH'].map(lambda x: borough_standandization_map.get(x, ''))
    elif 'Borough' in df.columns.tolist():
        df['Borough'] = df['Borough'].map(lambda x: borough_standandization_map.get(x, ''))


# drop column not in the range of nyc
def fix_latitude(df):
    df['LATITUDE'] = pd.to_numeric(df['LATITUDE'], downcast="float")
    if len(df.loc[~((df['LATITUDE'] >= 40.4961154) & (df['LATITUDE'] <= 40.91553278)), 'LATITUDE']) > 0:
        df.loc[~((df['LATITUDE'] >= 40.4961154) & (df['LATITUDE'] <= 40.91553278)), 'LATITUDE'] = -1


# drop column not in the range of nyc
def fix_longitude(df):
    df['LONGITUDE'] = pd.to_numeric(df['LONGITUDE'], downcast="float")
    if len(df.loc[~((df['LONGITUDE'] >= -74.25559136) & (df['LONGITUDE'] <= -73.70000906)), 'LONGITUDE']) > 0:
        df.loc[~((df['LONGITUDE'] >= -74.25559136) & (df['LONGITUDE'] <= -73.70000906)), 'LONGITUDE'] = -1


def fix_zipcode(df):
    # if zip code is float, transform to str
    if df['ZIP CODE'].dtype == 'float64':
        df['ZIP CODE'] = df['ZIP CODE'].apply(str)
        df['ZIP CODE'] = df['ZIP CODE'].apply(lambda x: x.split('.')[0])

    df['ZIP CODE'] = df['ZIP CODE'].apply(str)

    df_zipcode = df[['ZIP CODE']]
    df_notin = df_zipcode[~df_zipcode['ZIP CODE'].isin(zipcodes_list)]
    check_list = list(df_notin['ZIP CODE'].unique())
    array_notin = df_notin['ZIP CODE'].unique()

    def updateZipcode(zipcode):
        if zipcode in array_notin:
            return ''
        else:
            return zipcode

    df['ZIP CODE'] = df['ZIP CODE'].apply(updateZipcode)
    print(df_notin['ZIP CODE'].unique())
    for i in check_list:
        print(df.loc[df['ZIP CODE'] == i].value_counts())


# 1 -> no missing value
# 0 -> has missing value
def check_missing_value(df, column_name):
    # check empty string, None, and Nan
    empty_str = len(df[df[column_name] == ''])
    nan_val = len(df[pd.isna(df[column_name]) == True])
    if not empty_str and not nan_val:
        print("no missing value on " + column_name)
        return 1
    else:
        print("has {} missing value on {}".format((empty_str + nan_val), column_name))
        return 0


def fix_latlong_datatype(df):
    for i in df.index:
        lat = df['LATITUDE'][i]
        if isinstance(lat, str):
            lat = lat.strip(',')
        try:
            df['LATITUDE'][i] = float(lat)
        except:
            df['LATITUDE'][i] = float(-1)

    # remove string values from Longitude
    for i in df.index:
        lon = df['LONGITUDE'][i]
        if isinstance(lon, str):
            lon = lon.strip(',')
        try:
            df['LONGITUDE'][i] = float(lon)
        except:
            df['LONGITUDE'][i] = float(-1)


def loadJson(filename):
    with open(filename, 'r', encoding='utf8') as fp:
        json_data = json.load(fp)
        coordinate_for_each_zip_map = {}
        for line in json_data['features']:
            # print(line['geometry']['coordinates'])
            zip_str = line["properties"]["zcta"]
            coordinates = line['geometry']['coordinates']
            coordinates = coordinates[0]
            # print(type(coordinates))
            list_of_polygons = []
            for polygon in coordinates:
                # print(len(polygon))
                list_of_tuple = []
                for longlat in polygon:
                    list_of_tuple.append((longlat[0], longlat[1]))
                # print(list_of_tuple)
                tuple_of_list_of_tuple = (list_of_tuple,)
                list_of_polygons.append(tuple_of_list_of_tuple)
            # print(list_of_polygons)
            coordinate_for_each_zip_map[zip_str] = list_of_polygons
        return coordinate_for_each_zip_map


zip_codes = loadJson('./data/nyu-2451-34509-geojson.json')


def convert_coordinates_to_zips(series):
    # point = Feature(geometry=Point([-73.97617, 40.68358]))
    lat = float(series['LATITUDE'])
    lon = float(series['LONGITUDE'])
    zipcode = series['ZIP CODE']
    if zipcode:
        return zipcode

    if lat == 0 or lon == 0 or lat == -1.0 or lat == -1.0:
        print('None')
        return ""
    point = Feature(geometry=Point([lon, lat]))
    for k, v in zip_codes.items():
        polygon = Feature(geometry=MultiPolygon(v))
        res = boolean_point_in_polygon(point, polygon)
        if res:
            print(k)
            return k
    print('None')
    return ""


def fix_missing_value_zipcode(df):
    df['ZIP CODE'] = df.apply(convert_coordinates_to_zips, axis=1)


def findBorough(zipcode):
    return zipcodestr_to_borough_dict.get(str(zipcode), '')


def fillBoroughByZipcode(series):
    zipcode = series['ZIP CODE']
    borough = series['BOROUGH']
    if borough:
        return borough
    if not zipcode:
        return borough

    return findBorough(zipcode)


def fix_missing_value_borough(df):
    df['BOROUGH'] = df.apply(fillBoroughByZipcode, axis=1)


if __name__ == '__main__':
    for filename in filename_map.keys():
        df = pd.read_csv(filename)
        column_name_mapping = filename_map.get(filename)
        # print data type
        print(df.dtypes)

        # Start data cleaning here
        # - Part 1: fix all columns
        # Since the data types are not consistent for latitude and longitude, so we first fix the data type for them
        df = df.rename(columns=column_name_mapping)

        # This method is exclusively applied to one dataset named 2pg3-gcaa, since the latitude and longitude
        fix_latlong_datatype(df)

        fix_borough(df)
        fix_latitude(df)
        fix_longitude(df)
        fix_zipcode(df)
        fix_typos(df, 'STREET NAME')

        # Part 2: check missing values and fill in
        res_missing_value_zipcode = check_missing_value(df, 'ZIP CODE')
        res_missing_value_borough = check_missing_value(df, 'BOROUGH')
        check_missing_value(df, 'LATITUDE')
        check_missing_value(df, 'LONGITUDE')

        # Fill in zip code by coordinates
        if res_missing_value_zipcode == 0:  # 0 means it has some missing values on that column
            fix_missing_value_zipcode(df)

        # Fill in borough by zip code
        if res_missing_value_borough == 0:
            fix_missing_value_borough(df)

        # print(df)
        df.to_csv(filename + '_out.csv')



