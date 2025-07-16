
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
            'https://data-starcloud.pcl.ac.cn/api/en/resource/3/dir/1714157507660038144',
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
def get_links_for_region(meta_file, min_lat, max_lat, min_lon, max_lon):
    with open(meta_file) as f:
        meta_data = json.load(f)
        
    link_tmp = "https://data-starcloud.pcl.ac.cn/api/en/resourceFile/download/1/{id}"
    for ele in meta_data:
        if not ele['name'].endswith('.tif'):
            continue
        lat, lon = ele['name'].split('.')[0].split('_')[1:]
        lat, lon = int(lat), int(lon)
        if (min_lon<=lon<=max_lon and min_lat<=lat<=max_lat) or (min_lon<=lon+2<=max_lon and min_lat<=lat<=max_lat) or (min_lon<=lon<=max_lon and min_lat<=lat+2<=max_lat) or (min_lon<=lon+2<=max_lon and min_lat<=lat+2<=max_lat):
            yield link_tmp.format(id=ele['id'])
            
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
    df.to_csv(os.path.join(save_dir, f"{map_name}_{region}_download_links.csv"), index=False)


if __name__ == '__main__':
    meta_file = "FROM_GLC_2017_META.json"
    #downlaod_info = get_info(meta_file)
    #print(f"Total download info: {len(downlaod_info)}")
    infos = {
        "Africa": [
            # Static map: (0.1°S – 18.1°N; 9.9°E – 43.3°E), 
            {
                "year": 2019,
                "ranges": {"min_lat":-0.1, "max_lat":18.1, "min_lon": 9.9, "max_lon": 43.3},
                "map_year": 2017,
            },
        ],
        "Siberia": [
            #Static map: (51.3°N – 75.7°N; 64.4°E – 93.4°E)
            {
                "year": 2019,
                "ranges": {"min_lat":51.3, "max_lat":75.7, "min_lon": 64.4, "max_lon": 93.4},
                "map_year": 2017,
            },
        ],
        "Amazon_Extension": [
            # Static and Historical LC maps
            {
                "year": 2019,
                "ranges": {"min_lat":-24, "max_lat":12, "min_lon": -82, "max_lon": -34},
                "map_year": 2017,
            },
        ],
        "Philippines": [
            # Static map: , 
            {
                "year": 2019,
                "ranges": {"min_lat":4.3833, "max_lat":21.0, "min_lon": 116.0, "max_lon": 127.0},
                "map_year": 2017,
            },
        ],
    }
    for region, info_list in infos.items():
        if region != "Amazon_Extension": continue
        generate_downloadinfo_csv(region, info_list, meta_file)
    