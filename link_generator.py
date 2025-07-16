# -*- coding: utf-8 -*-
'''
Multi-source downloader for Earth observation datasets based on a user-provided GeoJSON AOI.
Supports GSW, GFC, GFC_FCS30D, ESRI, GHS_BU_R2023A, FROM_GLC (2010, 2015, 2017), GISD30, WSF, and GFC_TreeCover2000.
'''

import os
import json
import pandas as pd
import geopandas as gpd
import requests
from shapely.geometry import shape, box, Polygon
from collections import defaultdict

# ----------------------------- GeoJSON Bounding Box ----------------------------- #
def get_bounds_from_geojson(geojson_str):
    try:
        geojson_data = json.loads(geojson_str)
        geom = shape(geojson_data['features'][0]['geometry']) if 'features' in geojson_data else shape(geojson_data['geometry'])
        min_lon, min_lat, max_lon, max_lat = geom.bounds
        return min_lat, max_lat, min_lon, max_lon
    except Exception as e:
        print(f"Error parsing GeoJSON: {e}")
        return None

# ----------------------------- GLC_FCS30D_UNOFFICIAL ----------------------------- #
def adjust_to_nearest_10(value, is_min=True):
    if is_min:
        return max(-180, (value // 10) * 10)
    else:
        return min(180, ((value + 9) // 10) * 10)

def get_links_glc_fcs30d_unofficial(min_lat, max_lat, min_lon, max_lon):
    link_tmp = r"https://zenodo.org/records/8239305/files/GLC_FCS30D_19852022maps_{left_left_top}-{right_left_top}.zip?download=1"
    lon_min = adjust_to_nearest_10(min_lon, is_min=True)
    lon_max = adjust_to_nearest_10(max_lon, is_min=False)

    for lon in range(int(lon_min), int(lon_max)+1, 10):
        left_left_top, right_left_top = (lon, lon+5) if abs(lon) < abs(lon+5) else (lon+5, lon)
        left_left_top = f"{'E' if left_left_top >= 0 else 'W'}{abs(left_left_top)}"
        right_left_top = f"{'E' if right_left_top >= 0 else 'W'}{abs(right_left_top)}"
        yield link_tmp.format(left_left_top=left_left_top, right_left_top=right_left_top)

# ----------------------------- GSW ----------------------------- #
def _get_left_decval(val):
    val = int(val)
    if val % 10 == 0:
        return val
    if val > 0:
        return abs(val) // 10 * 10
    return -(1 + abs(val) // 10) * 10

def get_links_gsw(year, min_lat, max_lat, min_lon, max_lon):
    link_tmp = r"https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GSWE/YearlyClassification/LATEST/tiles/yearlyClassification{year}/yearlyClassification{year}-{left:0>6}0000-{right:0>6}0000.tif"
    if year == 2021:
        link_tmp = r"https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GSWE/YearlyClassification/LATEST/tiles/yearlyClassification{year}/yearlyClassification{year}_{left:0>6}0000-{right:0>6}0000.tif"

    start_lon, end_lon = _get_left_decval(min_lon), _get_left_decval(max_lon)
    start_lat, end_lat = _get_left_decval(min_lat), _get_left_decval(max_lat)

    for lon in range(start_lon, end_lon + 10, 10):
        for lat in range(start_lat, end_lat + 10, 10):
            left = 4 * (70 - lat) // 10
            right = 4 * (lon + 180) // 10
            yield link_tmp.format(year=year, left=left, right=right)

# ----------------------------- GFC ----------------------------- #
def get_links_gfc(year):
    link_tmpl = r"https://zenodo.org/records/10068479/files/GWL_FCS30_{map_year}.zip?download=1"
    return [link_tmpl.format(map_year=year)]


# ----------------------------- GFC_FCS30D ----------------------------- #
def get_links_gfc_fcs30d(year, min_lat, max_lat, min_lon, max_lon):
    """
    Generates download links for GFC_FCS30D if the user's AOI intersects
    with the dataset's predefined geographical regions for a given year.
    """
    link_tmpl = r"https://zenodo.org/records/10068479/files/GWL_FCS30_{map_year}.zip?download=1"
    
    # Predefined bounding boxes for specific years/regions for this dataset
    predefined_bboxes = {
        2000: {"min_lat":3.5, "max_lat":16.3, "min_lon": 27, "max_lon": 43.3},      # Africa Historical
        2005: {"min_lat":3.5, "max_lat":16.3, "min_lon": 27, "max_lon": 43.3},      # Africa Historical
        2010: {"min_lat":3.5, "max_lat":16.3, "min_lon": 27, "max_lon": 43.3},      # Africa Historical
        2015: {"min_lat":3.5, "max_lat":16.3, "min_lon": 27, "max_lon": 43.3},      # Africa Historical
        2019: {"min_lat":-0.1, "max_lat":18.1, "min_lon": 9.9, "max_lon": 43.3},       # Africa Static
        2022: {"min_lat":3.5, "max_lat":16.3, "min_lon": 27, "max_lon": 43.3},      # Africa 2024 -> map_year 2022
    }
    
    # These regions are global and will always match if the year is selected
    global_coverage_years = [2000, 2005, 2010, 2015, 2019, 2020, 2022]

    map_year = year

    # Special case from the original logic: year 2024 uses the 2022 map
    if year == 2024:
        map_year = 2022
        
    user_aoi = box(min_lon, min_lat, max_lon, max_lat)
    
    # Check for global coverage years first
    if map_year in global_coverage_years:
        # For these years, the data is considered global, so if the year matches, generate the link.
        yield link_tmpl.format(map_year=map_year)
        return

    # Check for intersection with region-specific data
    if map_year in predefined_bboxes:
        data_bbox = box(
            predefined_bboxes[map_year]['min_lon'],
            predefined_bboxes[map_year]['min_lat'],
            predefined_bboxes[map_year]['max_lon'],
            predefined_bboxes[map_year]['max_lat']
        )
        if user_aoi.intersects(data_bbox):
            yield link_tmpl.format(map_year=map_year)
        else:
            print(f"Info: Your AOI does not intersect with the predefined region for GFC_FCS30D in {map_year}.")
    else:
        print(f"Info: GFC_FCS30D data is not available for the map year {map_year} with this logic.")


# ----------------------------- ESRI ----------------------------- #
def get_links_esri(year, min_lat, max_lat, min_lon, max_lon):
    link_tmp = r"https://lulctimeseries.blob.core.windows.net/lulctimeseriesv003/lc{from_year}/{image_name}_{from_year}0101-{to_year}0101.tif"
    from_year = year
    to_year = year + 1

    query_url = (
        f"https://services.arcgis.com/P3ePLMYs2RVChkJx/ArcGIS/rest/services/LULC_Footprints/FeatureServer/0/query"
        f"?f=json&geometry=%7B%22rings%22:[[[{min_lon},{min_lat}],[{min_lon},{max_lat}],"
        f"[{max_lon},{max_lat}],[{max_lon},{min_lat}],[{min_lon},{min_lat}]]],"
        f"%22spatialReference%22:%20%7B%20%22wkid%22:%204326%20%7D%7D&where=1%3D1"
        f"&outFields=*&returnGeometry=false&returnQueryGeometry=true"
        f"&spatialRel=esriSpatialRelIntersects&geometryType=esriGeometryPolygon"
    )

    try:
        response = requests.get(query_url)
        response.raise_for_status()
        features = response.json().get("features", [])
    except Exception as e:
        print(f"Failed to query ESRI service: {e}")
        return

    for feature in features:
        image_name = feature["attributes"]["ImageName"]
        yield link_tmp.format(from_year=from_year, to_year=to_year, image_name=image_name)

# ----------------------------- GHS_BU_R2023A ----------------------------- #
def get_links_ghs_bu_r2023a(year, min_lat, max_lat, min_lon, max_lon, shapefile_path):
    link_tpl = r"https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_BUILT_S_GLOBE_R2023A/GHS_BUILT_S_E2018_GLOBE_R2023A_54009_10/V1-0/tiles/GHS_BUILT_S_E2018_GLOBE_R2023A_54009_10_V1_0_{tid}.zip"

    gdf = gpd.read_file(shapefile_path).to_crs(epsg=4326)
    bbox = box(min_lon, min_lat, max_lon, max_lat)
    filtered_gdf = gdf[gdf.intersects(bbox) & gdf.is_valid]

    for tid in filtered_gdf['tile_id'].tolist():
        yield link_tpl.format(tid=tid)

# ----------------------------- FROM_GLC 2010 (combined asc/desc) ----------------------------- #
def find_paths_rows_of_Landsat_by_range(min_lat, max_lat, min_lon, max_lon, wrs2_desc_path, wrs2_asc_path):
    wrs2_desc = gpd.read_file(wrs2_desc_path)
    wrs2_asc = gpd.read_file(wrs2_asc_path)
    wrs2 = pd.concat([wrs2_desc, wrs2_asc], ignore_index=True)
    wrs2 = wrs2[wrs2.is_valid]

    query_bbox = Polygon([(min_lon, min_lat), (max_lon, min_lat),
                          (max_lon, max_lat), (min_lon, max_lat), (min_lon, min_lat)])
    return wrs2[wrs2.intersects(query_bbox)][['PATH', 'ROW']].drop_duplicates()

def get_path_rows_from_name(name):
    parts = name.split('_')[0][2:]
    return int(parts[:3]), int(parts[3:])

def get_links_from_glc_2010(meta_file, min_lat, max_lat, min_lon, max_lon, wrs2_desc_path, wrs2_asc_path):
    with open(meta_file) as f:
        meta_data = json.load(f)

    metas = defaultdict(list)
    for ele in meta_data:
        if not ele['name'].lower().startswith('l5'):
            continue
        path, row = get_path_rows_from_name(ele['name'])
        metas[f"{path}_{row}"].append(ele)

    paths_rows_df = find_paths_rows_of_Landsat_by_range(min_lat, max_lat, min_lon, max_lon, wrs2_desc_path, wrs2_asc_path)
    link_tmp = "https://data-starcloud.pcl.ac.cn/api/en/resourceFile/download/4/{id}"

    for _, row_info in paths_rows_df.iterrows():
        key = f"{row_info['PATH']}_{row_info['ROW']}"
        if key not in metas:
            print(f"Missing metadata for path/row: {key}")
            continue
        for ele in metas[key]:
            if ele['name'].endswith('.tif.tar.gz'):
                yield link_tmp.format(id=ele['id'])

# ----------------------------- FROM_GLC 2015 ----------------------------- #
def get_links_from_glc_2015(meta_file, min_lat, max_lat, min_lon, max_lon):
    """Generates download links for FROM_GLC 2015 based on a bounding box."""
    def adjust_to_nearest_10(value, is_latitude=True, is_min=True):
        if is_latitude: # Latitude range: -60 to 80
            return max(-60, (value // 10) * 10) if is_min else min(80, ((value + 9) // 10) * 10)
        else: # Longitude range: -180 to 180
            return max(-180, (value // 10) * 10) if is_min else min(180, ((value + 9) // 10) * 10)

    with open(meta_file) as f:
        meta_data = json.load(f)
    
    link_tmp = "https://data-starcloud.pcl.ac.cn/api/en/resourceFile/download/3/{id}"
    lat_min = adjust_to_nearest_10(min_lat, is_latitude=True, is_min=True)
    lat_max = adjust_to_nearest_10(max_lat, is_latitude=True, is_min=False)
    lon_min = adjust_to_nearest_10(min_lon, is_latitude=False, is_min=True)
    lon_max = adjust_to_nearest_10(max_lon, is_latitude=False, is_min=False)
    
    print(f"Adjusted boundary for FROM_GLC 2015: [({lon_min}, {lat_min}) - ({lon_max}, {lat_max})]")
    
    for lat in range(int(lat_max), int(lat_min), -10):
        for lon in range(int(lon_min), int(lon_max), 10):
            lat_label = f"{abs(lat):0>2}{'N' if lat >= 0 else 'S'}"
            lon_label = f"{abs(lon):0>3}{'E' if lon >= 0 else 'W'}"
            name = f"{lon_label}{lat_label}.tif"
            for ele in meta_data:
                if ele['name'] == name:
                    yield link_tmp.format(id=ele['id'])
                    break
            else:
                print(f"No FROM_GLC 2015 data for tile: {name}")

# ----------------------------- FROM_GLC 2017 ----------------------------- #
def get_links_from_glc_2017(meta_file, min_lat, max_lat, min_lon, max_lon):
    """Generates download links for FROM_GLC 2017 by checking tile coordinates."""
    with open(meta_file) as f:
        meta_data = json.load(f)
        
    link_tmp = "https://data-starcloud.pcl.ac.cn/api/en/resourceFile/download/1/{id}"
    for ele in meta_data:
        if not ele['name'].endswith('.tif'):
            continue
        try:
            # Assumes filename format is like 'WSF_Classification_v1_1_lat_lon.tif'
            lat_str, lon_str = ele['name'].split('.')[0].split('_')[-2:]
            lat, lon = int(lat_str), int(lon_str)
            # Check if the tile's bottom-left corner is within the bounding box
            if (min_lon <= lon <= max_lon and min_lat <= lat <= max_lat):
                yield link_tmp.format(id=ele['id'])
        except (ValueError, IndexError):
            # print(f"Could not parse lat/lon from filename: {ele['name']}")
            continue

# ----------------------------- GISD30 ----------------------------- #
def get_links_gisd30(min_lat, max_lat, min_lon, max_lon):
    """Generates download links for GISD30 based on intersecting longitude ranges."""
    map_link_ranges = {
        (-180, -150): "W155_W180",
        (-150, -120): "W125_W150",
        (-120, -90): "W95_W120",
        (-90, -60): "W65_W90",
        (-60, -30): "W35_W60",
        (-30, 0): "W5_W30", 
        (0, 35): "E0_E30", 
        (35, 65): "E35_E60",
        (65, 95): "E65_E90",
        (95, 125): "E95_E120", 
        (125, 155): "E125_E150",
        (155, 180): "E155_E175", 
    }
    link_tmp = r"https://zenodo.org/records/5220816/files/GISD30_1985-2020_{val}.rar?download=1"
    
    unique_links = set()
    for rg, val in map_link_ranges.items():
        if rg[0] <= max_lon and rg[1] >= min_lon:
            unique_links.add(link_tmp.format(val=val))
    
    for link in unique_links:
        yield link

# ----------------------------- WSF ----------------------------- #
def get_links_wsf(year, min_lat, max_lat, min_lon, max_lon, geojson_path):
    """Generates download links for WSF by intersecting with a GeoJSON grid."""
    gdf = gpd.read_file(geojson_path)
    bbox = box(min_lon, min_lat, max_lon, max_lat)
    filtered_gdf = gdf[gdf.intersects(bbox)]
    return filtered_gdf['Download'].tolist()

# ------------------------- GFC_TreeCover2000 ------------------------- #
def get_links_gfc_treecover2000(min_lat, max_lat, min_lon, max_lon):
    """Generates download links for Hansen GFC Tree Canopy Cover 2000."""
    def adjust_to_nearest_10(value, is_latitude=True, is_min=True):
        if is_latitude: # Latitude range: -50 to 80 (based on dataset)
            if is_min:
                return max(-50, (value // 10) * 10)
            else:
                return min(80, ((value + 9) // 10) * 10)
        else: # Longitude range: -180 to 180
            if is_min:
                return max(-180, (value // 10) * 10)
            else:
                return min(180, ((value + 9) // 10) * 10)
    
    link_tmp = r"https://storage.googleapis.com/earthenginepartners-hansen/GFC-2023-v1.11/Hansen_GFC-2023-v1.11_treecover2000_{lat_label}_{lon_label}.tif"
    
    lat_min = adjust_to_nearest_10(min_lat, is_latitude=True, is_min=True)
    lat_max = adjust_to_nearest_10(max_lat, is_latitude=True, is_min=False)
    lon_min = adjust_to_nearest_10(min_lon, is_latitude=False, is_min=True)
    lon_max = adjust_to_nearest_10(max_lon, is_latitude=False, is_min=False)
    
    print(f"Adjusted boundary for GFC TreeCover2000: [({lon_min}, {lat_min}) - ({lon_max}, {lat_max})]")
    
    for lat in range(int(lat_max), int(lat_min), -10):
        for lon in range(int(lon_min), int(lon_max), 10):
            lat_label = f"{abs(lat):0>2}{'N' if lat >= 0 else 'S'}"
            lon_label = f"{abs(lon):0>3}{'E' if lon >= 0 else 'W'}"
            yield link_tmp.format(lat_label=lat_label, lon_label=lon_label)

# ------------------------- PCL Metadata Fetcher (Helper) ------------------------ #
def _fetch_pcl_metadata(save_to, url, referer, dataset_name):
    """Generic function to fetch paginated metadata from data-starcloud.pcl.ac.cn."""
    os.makedirs(os.path.dirname(save_to), exist_ok=True)
    
    cookies = {'i18n_redirected': 'en'}
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Content-Type': 'application/json;charset=UTF-8',
        'Origin': 'https://data-starcloud.pcl.ac.cn',
        'Referer': referer,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    }
    json_data = {'name': ''}
    res = []
    pageNum, pageSize = 1, 100
    while True:
        params = {'pageNum': pageNum, 'pageSize': pageSize}
        try:
            response = requests.post(url, params=params, cookies=cookies, headers=headers, json=json_data)
            response.raise_for_status()
            resp_data = response.json()
            total = resp_data.get('data', {}).get('total', 0)
            print(f"Querying {dataset_name} metadata: Page {params['pageNum']}... Total records: {total}")
            
            if not resp_data.get('success'):
                print(f"API request failed: {resp_data.get('failReason')}")
                break
            
            page_list = resp_data.get('data', {}).get('list', [])
            if not page_list:
                break
                
            res.extend(page_list)
            if pageNum * pageSize >= total:
                break
            pageNum += 1

        except requests.exceptions.RequestException as e:
            print(f"Error fetching metadata: {e}")
            return None
    
    with open(save_to, 'w') as f:
        json.dump(res, f, indent=4)
    print(f"Successfully downloaded metadata for {dataset_name} to {save_to}")
    return res

# ----------------------------- Combined Download Info ----------------------------- #
def get_bounding_box_from_range(min_lat, max_lat, min_lon, max_lon):
    return [[min_lon, max_lat], [min_lon, min_lat], [max_lon, min_lat], [max_lon, max_lat], [min_lon, max_lat]]

def generate_combined_downloadinfo_csv(region, info_list, save_dir=".", **kwargs):
    all_dinfos = []
    for source_name, info_entries in info_list.items():
        if not info_entries: continue

        print(f"\nProcessing source: {source_name}")
        for info in info_entries:
            year = info["year"]
            ranges = info["ranges"]
            links_generated = 0
            
            links_iterator = []
            dinfo_template = {
                "region": region, "year": year, "map_name": source_name, "map_year": info["map_year"],
                "aoi": get_bounding_box_from_range(**ranges)
            }

            if source_name == "GSW":
                links_iterator = get_links_gsw(year, **ranges)
                dinfo_template.update({"type": "URL"})
            elif source_name == "GFC":
                links_iterator = get_links_gfc(info["map_year"])
                dinfo_template.update({"type": "URL_UNARCHIVE"})
            elif source_name == "GFC_FCS30D":
                links_iterator = get_links_gfc_fcs30d(year, **ranges)
                dinfo_template.update({"type": "URL_UNARCHIVE", "map_name": "GWL_FCS30D"}) # Use specific map_name
            elif source_name == "ESRI":
                links_iterator = get_links_esri(year, **ranges)
                dinfo_template.update({"type": "URL"})
            elif source_name == "GHS_BU_R2023A":
                links_iterator = get_links_ghs_bu_r2023a(year, **ranges, shapefile_path=kwargs['ghs_shapefile_path'])
                dinfo_template.update({"type": "URL_UNARCHIVE"})
            elif source_name == "FROM_GLC_2010":
                links_iterator = get_links_from_glc_2010(kwargs['meta_file_2010'], **ranges, wrs2_desc_path=kwargs['wrs2_desc_path'], wrs2_asc_path=kwargs['wrs2_asc_path'])
                dinfo_template.update({"type": "URL_UNARCHIVE", "request_headers": "FROM_GLC_Headers"})
            elif source_name == "FROM_GLC_2015":
                links_iterator = get_links_from_glc_2015(kwargs['meta_file_2015'], **ranges)
                dinfo_template.update({"type": "URL", "request_headers": "FROM_GLC_Headers"})
            elif source_name == "FROM_GLC_2017":
                links_iterator = get_links_from_glc_2017(kwargs['meta_file_2017'], **ranges)
                dinfo_template.update({"type": "URL", "request_headers": "FROM_GLC_Headers"})
            elif source_name == "GISD30":
                links_iterator = get_links_gisd30(**ranges)
                dinfo_template.update({"type": "URL_UNARCHIVE"})
            elif source_name == "WSF":
                geojson_path = kwargs.get(f"wsf_{year}_grid_path")
                if geojson_path and os.path.exists(geojson_path):
                    links_iterator = get_links_wsf(year, **ranges, geojson_path=geojson_path)
                else:
                    print(f"WSF grid file for {year} not found at '{geojson_path}'. Skipping.")
                dinfo_template.update({"type": "URL"})
            elif source_name == "GFC_TreeCover2000":
                links_iterator = get_links_gfc_treecover2000(**ranges)
                dinfo_template.update({"type": "URL"})
            elif source_name == "GLC_FCS30D_UNOFFICIAL":
                links_iterator = get_links_glc_fcs30d_unofficial(**ranges)
                dinfo_template.update({"type": "URL_UNOFFICIAL"})

            for link in links_iterator:
                dinfo = dinfo_template.copy()
                dinfo["url"] = link
                all_dinfos.append(dinfo)
                links_generated += 1
            
            print(f"Found {links_generated} links for {info['map_year']}.")


    if not all_dinfos:
        print("No download links were generated.")
        return

    df = pd.DataFrame(all_dinfos)
    output_path = os.path.join(save_dir, f"MULTISOURCE_{region}_download_links.csv")
    df.to_csv(output_path, index=False)
    print(f"\nGenerated a total of {len(df)} links. Saved to {output_path}")

# ----------------------------- Main ----------------------------- #

def find_closest_year(target_year, available_years):
    """
    Finds the closest year in a list to the target year.
    Prefers the more recent year in case of a tie.
    """
    if not available_years:
        return None
    # The key sorts by absolute difference, then by the negative year (to prefer higher years in ties)
    closest = min(available_years, key=lambda y: (abs(y - target_year), -y))
    return closest

if __name__ == '__main__':
    print("--- Multi-source Dataset Downloader via GeoJSON ---")
    region_name = input("Enter a name for your region (e.g., 'North_Italy'): ") or "custom_AOI"
    
    # --- EDIT 1: Suggest GeoJSON creation tool ---
    print("\n💡 You can create a GeoJSON for your Area of Interest (AOI) at: https://geojson.io/#map=2/0/20")
    print("Paste your GeoJSON content below (press Enter on an empty line to finish):")
    
    geojson_lines = []
    while True:
        line = input()
        if not line.strip():
            break
        geojson_lines.append(line)
    geojson_str = "\n".join(geojson_lines)

    bounds = get_bounds_from_geojson(geojson_str)
    if not bounds:
        exit("Invalid GeoJSON provided. Exiting.")

    min_lat, max_lat, min_lon, max_lon = bounds
    print(f"\nBounding box derived from GeoJSON: MinLon={min_lon}, MinLat={min_lat}, MaxLon={max_lon}, MaxLat={max_lat}")
    
    save_directory = input("Enter the directory to save the final CSV and GeoJSON (default is current directory): ") or "."
    os.makedirs(save_directory, exist_ok=True)

    # --- EDIT 3: Save the provided GeoJSON to a file ---
    geojson_output_path = os.path.join(save_directory, f"{region_name}_aoi.geojson")
    try:
        with open(geojson_output_path, 'w') as f:
            # Pretty-print the JSON to the file
            json.dump(json.loads(geojson_str), f, indent=2)
        print(f"✅ Successfully saved your AOI to {geojson_output_path}")
    except Exception as e:
        print(f"⚠️ Could not save GeoJSON file: {e}")


    while True:
        try:
            years_input = input("Enter the years you want to download (comma-separated, e.g., 2000,2015,2019): ")
            years_list = [int(y.strip()) for y in years_input.split(',')]
            break
        except ValueError:
            print("Invalid input. Please enter years as comma-separated numbers.")

    sources = ["GSW", "GFC", "GFC_FCS30D", "ESRI", "GHS_BU_R2023A", "FROM_GLC_2010",
               "FROM_GLC_2015", "FROM_GLC_2017", "GISD30", "WSF", "GFC_TreeCover2000",
               "GLC_FCS30D_UNOFFICIAL"]

    # --- EDIT 2: Logic for finding the closest available year ---
    SOURCE_YEAR_AVAILABILITY = {
        "GFC_FCS30D": [2000, 2005, 2010, 2015, 2019, 2020, 2024],
        "GHS_BU_R2023A": [2018],
        "FROM_GLC_2010": [2010],
        "FROM_GLC_2015": [2015],
        "FROM_GLC_2017": [2017],
        "GISD30": list(range(1985, 2021)),
        "WSF": [2015, 2019],
        "GFC_TreeCover2000": [2000],
        "GLC_FCS30D_UNOFFICIAL": list(range(1985, 2023)),
    }

    info_list = {src: [] for src in sources}
    
    for source in sources:
        for year in years_list:
            original_year = year
            available_years = SOURCE_YEAR_AVAILABILITY.get(source)

            # If the source has a defined list of years and the requested year is not in it
            if available_years and year not in available_years:
                closest_year = find_closest_year(year, available_years)
                print(f"⚠️ INFO: {source} is not available for {year}. Using closest available year: {closest_year}.")
                year = closest_year
            
            # Skip sources that are still not valid after checking for closest year
            if source == "GSW" and not(1984 <= year <= 2023): # Example valid range
                print(f"Skipping {source} for {year} (year out of known range).")
                continue
            
            # Special map_year assignments
            map_year = year
            if source == "GFC_FCS30D" and year == 2024:
                map_year = 2022
            elif source == "GHS_BU_R2023A":
                map_year = 2018 # This dataset is only for 2018

            info_list[source].append({
                "year": year,
                "map_year": map_year,
                "ranges": {"min_lat": min_lat, "max_lat": max_lat, "min_lon": min_lon, "max_lon": max_lon}
            })
    
    # --- Define paths for all required local dependency files ---
    kwargs = {
        # GHS
        "ghs_shapefile_path": "GHSL_data_54009_shapefile/GHSL2_0_MWD_L1_tile_schema_land.shp",
        # FROM_GLC
        "meta_file_2010": "2010/FROM_GLC_2010_META.json",
        "wrs2_desc_path": "2010/WRS2_descending_0/WRS2_descending.shp",
        "wrs2_asc_path": "2010/WRS2_ascending_0/WRS2_ascending.shp",
        "meta_file_2015": "2015/FROM_GLC_2015_META.json",
        "meta_file_2017": "2017/FROM_GLC_2017_META.json",
        # WSF
        "wsf_2015_grid_path": "WSF/WSF2015_grid.geojson",
        "wsf_2019_grid_path": "WSF/WSF2019_grid.geojson",
    }
    
    # --- Automatically download metadata if missing ---
    if any(info['year'] == 2015 for info in info_list["FROM_GLC_2015"]) and not os.path.exists(kwargs['meta_file_2015']):
        print(f"'{kwargs['meta_file_2015']}' not found. Attempting to download...")
        _fetch_pcl_metadata(kwargs['meta_file_2015'], 'https://data-starcloud.pcl.ac.cn/api/en/resource/3/dir/1714118541720240133', 'https://data-starcloud.pcl.ac.cn/resource/3', "FROM_GLC 2015")

    if any(info['year'] == 2017 for info in info_list["FROM_GLC_2017"]) and not os.path.exists(kwargs['meta_file_2017']):
        print(f"'{kwargs['meta_file_2017']}' not found. Attempting to download...")
        _fetch_pcl_metadata(kwargs['meta_file_2017'], 'https://data-starcloud.pcl.ac.cn/api/en/resource/3/dir/1714157507660038144', 'https://data-starcloud.pcl.ac.cn/resource/1', "FROM_GLC 2017")

    # --- Generate the final CSV file ---
    generate_combined_downloadinfo_csv(
        region=region_name,
        info_list=info_list,
        save_dir=save_directory,
        **kwargs
    )