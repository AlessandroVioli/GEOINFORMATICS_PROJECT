
import os, requests
import json
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon
from collections import defaultdict

wrs2_asce_path = r"/Users/qiong/Library/CloudStorage/OneDrive-PolitecnicodiMilano/CCI HRLC/Phase2/Processing_and_data_descriptions/CCI_HRLC_data_collection/FROM_GLC/2010/WRS2_ascending_0/WRS2_ascending.shp"
wrs2_desc_path = r"/Users/qiong/Library/CloudStorage/OneDrive-PolitecnicodiMilano/CCI HRLC/Phase2/Processing_and_data_descriptions/CCI_HRLC_data_collection/FROM_GLC/2010/WRS2_descending_0/WRS2_descending.shp"
def find_paths_rows_of_Landsat_by_range(min_lat, max_lat, min_lon, max_lon):
    '''根据指定的地理坐标范围（最小纬度、最大纬度、最小经度、最大经度）返回符合条件的 Landsat 路径和行号'''
    # 加载 WRS-2 Shapefile 数据
    wrs2 = gpd.read_file(wrs2_desc_path)
    
    # 创建查询范围的多边形
    query_bbox = Polygon([(min_lon, min_lat), (max_lon, min_lat), (max_lon, max_lat), (min_lon, max_lat)])

    # 使用 GeoPandas 过滤符合条件的路径和行
    # 筛选出与查询范围相交的路径和行
    results = wrs2[wrs2.intersects(query_bbox)]

    # 返回符合条件的路径和行列表
    paths_rows = results[['PATH', 'ROW']].drop_duplicates()
    print(f"Number of path/row: {paths_rows.shape}")

    return paths_rows

def get_path_rows_from_name(name):
    parts = name.split('_')[0][2:]
    path, row = parts[:3], parts[3:]
    return int(path), int(row)

def get_info(dir_id_file, save_to):
    # load dir ids
    dir_ids = []
    with open(dir_id_file) as f:
        dir_ids = json.load(f)
    cookies = {
    'i18n_redirected': 'en',
    'Hm_lvt_d7e39070e21f766786ce73d5692eafb7': '1726665804',
    'HMACCOUNT': '79420EB154010D12',
    'Hm_lpvt_d7e39070e21f766786ce73d5692eafb7': '1726675822',
}

    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json;charset=UTF-8',
        # 'Cookie': 'i18n_redirected=en; Hm_lvt_d7e39070e21f766786ce73d5692eafb7=1726665804; HMACCOUNT=79420EB154010D12; Hm_lpvt_d7e39070e21f766786ce73d5692eafb7=1726675822',
        'Origin': 'https://data-starcloud.pcl.ac.cn',
        'Pragma': 'no-cache',
        'Referer': 'https://data-starcloud.pcl.ac.cn/resource/4',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
    }
    json_data = {
        'name': '',
    }

    res = []
    for dir_id in dir_ids:
        pageNum, pageSize = 1, 100
        while True:
            params = {
                'pageNum': pageNum,
                'pageSize': pageSize,
            }
            response = requests.post(
                f'https://data-starcloud.pcl.ac.cn/api/en/resource/3/dir/{dir_id}',
                params=params,
                cookies=cookies,
                headers=headers,
                json=json_data,
            )
            response.raise_for_status()
            resp_data = response.json()
            print(f"Query: {params['pageNum']} * {params['pageSize']} / {resp_data['data']['total']}")
            if not resp_data['success']:
                print(f"Fail Code: {resp_data['failCode']} and Fail Reason: {resp_data['failReason']}")
                break
            if resp_data['data']['total'] <= 0:
                print(f"No available data: {resp_data}")
                break
            res.extend(resp_data['data']['list'])
            pageNum += 1
    
    # save
    with open(save_to, 'w') as f:
        json.dump(res, f)
    return res

"""
Download link informatin: 
GET https://data-starcloud.pcl.ac.cn/api/en/resourceFile/download/4/{id}
"""
def get_links_for_region(meta_file, min_lat, max_lat, min_lon, max_lon):
    with open(meta_file) as f:
        meta_data = json.load(f)
        
    # process meta data: key is "{path}_{row}"
    metas = defaultdict(list)
    for ele in meta_data:
        if not ele['name'].startswith('L5') and not ele['name'].startswith('l5'): continue
        path, row = get_path_rows_from_name(ele['name'])
        #if f"{path}_{row}" in metas: print(f"Duplicate {path}_{row}: {metas[f"{path}_{row}"]} and {ele}")
        metas[f"{path}_{row}"].append(ele)

    # find paths and rows
    paths_rows_df = find_paths_rows_of_Landsat_by_range(min_lat, max_lat, min_lon, max_lon)

    link_tmp = "https://data-starcloud.pcl.ac.cn/api/en/resourceFile/download/4/{id}"
    for _, row_info in paths_rows_df.iterrows():
        path, row = row_info['PATH'], row_info['ROW']
        if f"{path}_{row}" not in metas:
            print(f"Cannot find meta for path={path} and row={row}")
        eles = metas[f"{path}_{row}"]
        for ele in eles:
            if not ele['name'].endswith('.tif.tar.gz'):
                continue
        
            yield link_tmp.format(id=ele['id'])

def get_bounding_box_from_range(min_lat, max_lat, min_lon, max_lon):
    return [
        [min_lon, max_lat],
        [min_lon, min_lat],
        [max_lon, min_lat],
        [max_lon, max_lat],
        [min_lon, max_lat],
    ]
          
def generate_downloadinfo_csv(region:str, info_list: list, meta_file, map_name="FROM_GLC", type="URL_UNARCHIVE", save_dir="."):
    """_summary_

    Args:
        region (str): region name
        info_lsit (list): a list of dict
        map_name (str, optional): _description_. Defaults to "WSF".
        type (str, optional): _description_. Defaults to "URL".
        save_dir (str, optional): path to save. Defaults to ".".
    """
    dinfos = []
    for info in info_list:
        ranges = info["ranges"]
        for link in get_links_for_region(meta_file, **ranges):
            dinfos.append({
                "region": region,
                "year": info["year"],
                "map_name": map_name,
                "map_year": info["map_year"],
                "type": type,
                "url": link,
                "request_headers": 'FROM_GLC_Headers',
                "aoi": get_bounding_box_from_range(**ranges),
            })
        #print(dinfos)
    df = pd.DataFrame(dinfos)
    #print(df)
    df.to_csv(os.path.join(save_dir, f"{map_name}_2010_{region}_download_links.csv"), index=False)


if __name__ == '__main__':
    dir_id_file = "data-starcloud.pcl.ac.cn.4.dir.id"
    meta_file = "FROM_GLC_2010_META.json"
    #downlaod_info = get_info(dir_id_file,meta_file) # execute once to get the meta data
    #print(f"Total download info: {len(downlaod_info)}")
    #exit()
    infos = {
        "Africa": [
            # Historical LC and LCC map: (3.5°N – 16.3°N; 27.0°E – 43.3°E)
            {
                "year": 2010,
                "ranges": {"min_lat":3.5, "max_lat":16.3, "min_lon": 27, "max_lon": 43.3},
                "map_year": 2010,
            },
        ],
        "Siberia": [
            # Historical LC and LCC map: (59.4°N – 73.9°N; 64.8°E – 87.4°E).
            {
                "year": 2010,
                "ranges": {"min_lat":59.4, "max_lat":73.9, "min_lon": 64.8, "max_lon": 87.4},
                "map_year": 2010,
            },
        ],
        "Amazon_Extension": [
            # Static and Historical LC maps
            {
                "year": 2010,
                "ranges": {"min_lat":-24, "max_lat":12, "min_lon": -82, "max_lon": -34},
                "map_year": 2010,
            },
        ],
    }
    for region, info_list in infos.items():
        generate_downloadinfo_csv(region, info_list, meta_file)
    