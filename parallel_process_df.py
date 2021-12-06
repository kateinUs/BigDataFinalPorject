import numpy as np
import json
from turfpy.measurement import boolean_point_in_polygon
from geojson import Point, MultiPolygon, Feature, Polygon
import time
from multiprocessing import Pool
import pandas as pd


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


zip_codes = loadJson('nyu-2451-34509-geojson.json')


def convert_coordinates_to_zips(coordinate):
    # point = Feature(geometry=Point([-73.97617, 40.68358]))
    if coordinate[0] == '0' or coordinate[1] == '0':
        print('None')
        return "None"
    point = Feature(geometry=Point(coordinate))
    for k, v in zip_codes.items():
        polygon = Feature(geometry=MultiPolygon(v))
        res = boolean_point_in_polygon(point, polygon)
        if res:
            print(k)
            return k
    print('None')
    return "None"


def apply_f(df):
    return df.apply(lambda series: convert_coordinates_to_zips(
        [float(series['LONGITUDE']), float(series['LATITUDE'])]), axis=1)


def init_process(global_vars):
    global a
    a = global_vars


if __name__ == '__main__':
    df = pd.read_csv('geo_coordinates.csv')
    # df = df.iloc[:1000]

    t1 = time.time()

    df_parts = np.array_split(df, 20)
    with Pool(processes=8) as pool:
        result_parts = pool.map(apply_f, df_parts)

    result_parallel = pd.concat(result_parts)
    df['ZIP_CODE'] = result_parallel
    t2 = time.time()
    df.to_csv('./out_test.csv', sep=',', header=True, index=True)
    print("Parallel time =", t2 - t1)
