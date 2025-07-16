
import os, requests
import json
import pandas as pd

def get_info(save_to):
    cookies = {
        'i18n_redirected': 'en',
    }

    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json;charset=UTF-8',
        # 'Cookie': 'i18n_redirected=en',
        'Origin': 'https://data-starcloud.pcl.ac.cn',
        'Referer': 'https://data-starcloud.pcl.ac.cn/resource/3',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }
    json_data = {
        'name': '',
    }

    res = []
    pageNum, pageSize = 1, 100
    while True:
        params = {
            'pageNum': pageNum,
            'pageSize': pageSize,
        }
        response = requests.post(
            'https://data-starcloud.pcl.ac.cn/api/en/resource/3/dir/1714118541720240133',
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
GET https://data-starcloud.pcl.ac.cn/api/en/resourceFile/download/3/{id}
"""
def adjust_to_nearest_10(value, is_latitude=True, is_min=True):
    """
    调整经纬度值到最近的10度整数。
    :param value: 原始经纬度值
    :param is_latitude: 是否是纬度（True 为纬度，False 为经度）
    :param is_min: 是否是最小值（最小值向下调整，最大值向上调整）
    :return: 调整后的经纬度值
    """
    if is_latitude:
        # 纬度范围为 -60 到 80
        if is_min:
            return max(-60, (value // 10) * 10)  # 向下调整
        else:
            return min(80, ((value + 9) // 10) * 10)  # 向上调整
    else:
        # 经度范围为 -180 到 180
        if is_min:
            return max(-180, (value // 10) * 10)  # 向下调整
        else:
            return min(180, ((value + 9) // 10) * 10)  # 向上调整
def get_links_for_region(meta_file, min_lat, max_lat, min_lon, max_lon):
    with open(meta_file) as f:
        meta_data = json.load(f)
    # 调整经纬度到最近的10度整数
    link_tmp = "https://data-starcloud.pcl.ac.cn/api/en/resourceFile/download/3/{id}"
    lat_min = adjust_to_nearest_10(min_lat, is_latitude=True, is_min=True)
    lat_max = adjust_to_nearest_10(max_lat, is_latitude=True, is_min=False)
    lon_min = adjust_to_nearest_10(min_lon, is_latitude=False, is_min=True)
    lon_max = adjust_to_nearest_10(max_lon, is_latitude=False, is_min=False)
    print(f"Boundary for [({min_lon}, {min_lat}) - ({max_lon}, {max_lat})] is [({lon_min}, {lat_min}) - ({lon_max}, {lat_max})]")
    for lat in range(int(lat_max), int(lat_min), -10):
        for lon in range(int(lon_min), int(lon_max), 10):
            # 确定grid的左上角坐标
            lat_label = f"{abs(lat):0>2}{'N' if lat >= 0 else 'S'}"
            lon_label = f"{abs(lon):0>3}{'E' if lon >= 0 else 'W'}"
            name = f"{lon_label}{lat_label}.tif"
            for ele in meta_data:
                if ele['name'] == name:
                    #print(f"link for {name} is {link_tmp.format(id=ele['id'])}")
                    yield link_tmp.format(id=ele['id'])
                    break
            else:
                print(f"No data for {name}")
            
def generate_downloadinfo_csv(region:str, info_list: list, meta_file, map_name="FROM_GLC", type="URL", save_dir="."):
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
            })
        #print(dinfos)
    df = pd.DataFrame(dinfos)
    #print(df)
    df.to_csv(os.path.join(save_dir, f"{map_name}_{info["map_year"]}_{region}_download_links.csv"), index=False)


if __name__ == '__main__':
    #downlaod_info = get_info("FROM_GLC_2015_META.json")
    #print(f"Total download info: {len(downlaod_info)}")
    infos = {
        "Africa": [
            # Historical LC and LCC map: (3.5°N – 16.3°N; 27.0°E – 43.3°E)
            {
                "year": 2015,
                "ranges": {"min_lat":3.5, "max_lat":16.3, "min_lon": 27, "max_lon": 43.3},
                "map_year": 2015,
            },
        ],
        "Siberia": [
            # Historical LC and LCC map: (59.4°N – 73.9°N; 64.8°E – 87.4°E).
            {
                "year": 2015,
                "ranges": {"min_lat":59.4, "max_lat":73.9, "min_lon": 64.8, "max_lon": 87.4},
                "map_year": 2015,
            }
        ],
        "Amazon_Extension": [
            # Static and Historical LC maps
            {
                "year": 2015,
                "ranges": {"min_lat":-24, "max_lat":12, "min_lon": -82, "max_lon": -34},
                "map_year": 2015,
            },
        ],
    }
    for region, info_list in infos.items():
        generate_downloadinfo_csv(region, info_list, "FROM_GLC_2015_META.json")