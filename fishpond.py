import json
import requests
import argparse
import csv
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

NUM_TO_TRY=3

requests.adapters.DEFAULT_RETRIES=3
session = requests.session()
session.keep_alive = False

def coordinate_parser(text):
    coordinate_text = text.strip().split()
    try:
        coordinate = [float(coordinate_text[0]), float(coordinate_text[1])]
    except ValueError:
        coordinate = [float(coordinate_text[0].strip('(').strip(')')), float(coordinate_text[1].strip('(').strip(')'))]
    return coordinate

def shape2polygon(shape):
    if shape[:7] == 'POLYGON':
        shape = shape.split("((")[1].split("))")[0].split(",")
        coordinates = []
        for coordinate in shape:
            coordinates.append(coordinate_parser(coordinate))
        return "Polygon", coordinates
    else:
        shape = shape[len('MULTIPOLYGON ('):-1]
        shapes = shape.split(")),")
        multi_coordinates = []
        for shape in shapes:
            shape = shape[2:].strip().strip("((").strip("))").split(",")
            coordinates = []
            for coordinate in shape:
                coordinates.append(coordinate_parser(coordinate))
            multi_coordinates.append(coordinates)
        return "MultiPolygon", multi_coordinates

def input2feature(input_json):
    feature = {"type": "Feature", "geometry": {"type": "", "coordinates": []}, "properties": {}}
    coordinate_type, coordinates = shape2polygon(input_json["shape"])
    feature["geometry"]["type"] = coordinate_type
    feature["geometry"]["coordinates"].append(coordinates)
    for key in ["dataid", "county", "town", "daun","parcel", "fishfarm", "area", "ISSUE", "remark"]:
        feature["properties"][key] = input_json[key]
    return feature

def response2features(input_list):
    features = []
    for input_json in input_list:
        features.append(input2feature(input_json))
    return features

def get_features(area, county, town, daun, parcel):
    url = f"https://www.sfeamap.org.tw:443/SfeaMap_API/api/getDataSearch?area={area}&type=Parcel&county={county}&town={town}&daun={daun}&parcel={parcel}"
    num_to_try = NUM_TO_TRY
    while num_to_try > 0:
        r = session.get(url)
        if r.status_code == 200:
            response = json.loads(r.text)
            if len(response['results']):
                return response2features(response['results'][0]["fishfarmList"])
            else:
                return []
        num_to_try -= 1
    print(f"Request error occur : area={area}, county={county}, town={town}, daun={daun}, parcel={parcel}")
    return []

def main(input_file, output_file, county, town, thread, daun_column_num, parcel_column_num):
    output_json = {"type": "FeatureCollection", "features": []}
    request_dict = {}
    with open(input_file) as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        next(reader, None)
        for row in reader:
            daun = row[daun_column_num]
            parcel = row[parcel_column_num].strip('"')
            if len(daun) and len(parcel):
                for area in ["先行區", "優先區", "關注減緩區"]:
                    request_dict[f"{area}_{daun}_{parcel}"] = [area, daun, parcel]

    if thread == 1:
        for key, value in tqdm(request_dict.items()):
            features = get_features(value[0], county, town, value[1], value[2])
            output_json['features'] += features
    else:
        with ThreadPoolExecutor(max_workers=thread) as executor:
            futures = []
            for key, value in request_dict.items():
                future = executor.submit(get_features, value[0], county, town, value[1], value[2])
                futures.append(future)
            
            pbar = tqdm(total=len(futures))
            for future in as_completed(futures):
                output_json['features'] += future.result()
                pbar.update(1)
    
    with open(output_file, 'w') as f:
        json.dump(output_json, f)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('input', type=str, help='Path to input csv file')
    parser.add_argument('output', type=str, help='Path to output json file')
    parser.add_argument('daun_column_num', type=int, help='The column number of daun in input csv')
    parser.add_argument('parcel_column_num', type=int, help='The column number of parcel in input csv')
    parser.add_argument('county', type=str, help='County of the input csv')
    parser.add_argument('town', type=str, help='Town of the input csv')
    parser.add_argument('--thread', default=1, type=int, help='Number of threads')

    args = parser.parse_args()
    main(args.input, args.output, args.county, args.town, args.thread, args.daun_column_num, args.parcel_column_num)
