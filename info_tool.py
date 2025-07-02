from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from bs4 import BeautifulSoup
import time
import pandas as pd


def get_coords(http_url):
# 固定长度
    desired_length = 22

    target_substring = "/@"

    # 找到目标子字符串的位置
    start_index = http_url.find(target_substring)

    if start_index != -1:
        # 根据目标子字符串的位置和固定长度进行切片
        extracted_substring = http_url[start_index + len(target_substring): start_index + len(target_substring) + desired_length]
        lat = float((extracted_substring.split(',')[0]))
        lng = float((extracted_substring.split(',')[1]))
    return lat,lng


#判断是否是建筑物
def get_building_type(driver):
    try:
        place_type_XPATH = '//*[@id="QA0Szd"]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]/div/div[1]/div[2]/div/div[2]/span/span/span'
        WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.XPATH, place_type_XPATH)))

        
        
        place_type = driver.find_element(By.XPATH, place_type_XPATH).text
    except:
        place_type = 'nan'
  
    return place_type


# 取得poi名稱
def get_building_name(driver):
    place_name_XPATH = '//*[@id="QA0Szd"]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]/div/div[1]/div[1]/h1'
    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, place_name_XPATH)))

    
    
    place_name = driver.find_element(By.XPATH, place_name_XPATH).text
    place_name = place_name.replace('/', ' ')
    place_name = place_name.replace('|', ' ')
    place_name = place_name.replace('｜', ' ')
    place_name = place_name.replace('*', ' ')
    place_name = place_name.replace('!', ' ')
    place_name = place_name.replace('?', ' ')
    place_name = place_name.replace(':', ' ')

    # Silently get building name

    return place_name

#获得已知poi总数
def get_poi_type_total(driver):
  
    user_ratings_list = [poitype.text for poitype in driver.find_elements(By.XPATH, "//span[@class='bC3Nkc fontBodySmall']")]

    user_ratings_total_element = sum(int(num) for num in user_ratings_list)

    return user_ratings_total_element
    


def get_poi_comment_count(driver):
    try:
        comment_elements = driver.find_elements(By.XPATH, "//span[@class='bC3Nkc fontBodySmall']")
        if comment_elements:
            return sum(int(elem.text) for elem in comment_elements if elem.text.isdigit())
        return 0
    except:
        return 0


def get_poi_individual_comment_count(soup):
    try:
        import re
        
        # 方法1: 在W4Efsd div中查找"评分(评论数)"格式
        w4efsd_divs = soup.find_all("div", class_="W4Efsd")
        for div in w4efsd_divs:
            text = div.get_text().strip()
            # 匹配格式如: "4.2(144)" 或 "4.8(37)"
            match = re.search(r'\d+\.\d+\((\d+)\)', text)
            if match:
                return int(match.group(1))
        
        # 方法2: 查找aria-label中的评论数
        aria_labels = soup.find_all(attrs={"aria-label": True})
        for elem in aria_labels:
            aria_text = elem.get("aria-label", "")
            # 匹配日文格式: "4.2 つ星 クチコミ 144 件"
            match = re.search(r'クチコミ\s*(\d+)\s*件', aria_text)
            if match:
                return int(match.group(1))
        
        # 方法3: 查找包含括号数字的文本
        all_text = soup.get_text()
        match = re.search(r'\((\d+)\)', all_text)
        if match:
            return int(match.group(1))
            
        return 0
    except Exception as e:
        return 0


def get_all_poi_info(driver):
    
    result = {}
    poi_name_list = []
    poi_rating_list = []
    poi_class_list = []
    poi_add_list = []
    poi_comment_list = []

    
        
    ele_class_list = ["Nv2PK.THOPZb.CpccDe", 'Nv2PK.Q2HXcd.THOPZb']
    for class_key in ele_class_list:
   
        poi_frame_list = driver.find_elements(By.CLASS_NAME, class_key)

        if poi_frame_list:

            for poi_frame in poi_frame_list:
                soup = BeautifulSoup(poi_frame.get_attribute('innerHTML'), "html.parser") 
                
                poi_name = get_poi_name(soup)  # 取得 user 名稱 
                #user_profile_url = get_user_profile_url(soup)  # 取得 user 個人檔案的 URL
                rating = get_rating(soup)  # 取得評級
                poi_class, poi_address = get_class_address(soup)
                poi_comment_count = get_poi_individual_comment_count(soup)  # 取得單個POI評論數
                #local_guide, comment_num = get_local_guide_and_comment_num(soup)  # 取得在地嚮導的狀態和評論數
                #comment_text = get_comment_text(soup)# 取得評論內文
                poi_name_list.append(poi_name)
                poi_rating_list.append(rating)
                poi_class_list.append(poi_class)
                poi_add_list.append(poi_address)
                poi_comment_list.append(poi_comment_count)
        else:
            continue
    
    #poi_frame_list = driver.find_elements(By.CSS_SELECTOR, ".Nv2PK THOPZb CpccDe, .Nv2PK Q2HXcd THOPZb")
    
    
    # 取得評論進度條
    # Silently extract POI data

    #df = pd.DataFrame(columns = ['name',	'rating',	'class',	'add'])
    
    '''
    result['name'] = poi_name_list
    result['rating'] = poi_rating_list
    result['class'] = poi_class_list
    result['add'] = poi_add_list
    '''
    if len(poi_name_list) > 0:
       df = pd.DataFrame(
                        {'name': poi_name_list, 
                         'rating': poi_rating_list,	
                         'class' : poi_class_list,	
                         'add' : poi_add_list,
                         'comment_count': poi_comment_list
                        })
    else:
        df = pd.DataFrame(
                {'name': ['nan'], 
                    'rating': ['nan'],	
                    'class' : ['nan'],	
                    'add' : ['nan'],
                    'comment_count': [0]
                })
        
    return df
            


# 取得 poi 名稱
def get_poi_name(soup):
    poi_name = soup.find('div', class_ = 'qBF1Pd fontHeadlineSmall').text

    return poi_name
    

# 取得評級
def get_rating(soup):
    try:
        rating = soup.find("span", class_='MW4etd').text
        
    except:
        rating = 'nan'
        
    return rating


# 取得分类
def get_class_address(soup):

    
    class_div = soup.find_all("div",{"class":'W4Efsd'})
    
    try:
        class_list = class_div[2].select("span")[1].text
    except:
        class_list='nan'
    try:
        add_list=class_div[2].select("span")[4].text
    except:
        add_list='nan'
        
    
    return class_list,add_list


'''
def get_rating(soup):
    try:
        rating = soup.find('div', class_ = 'DU9Pgb').find("span", class_="kvMYJc").get("aria-label")
        rating = rating.replace('顆星', '')
        rating = rating.replace(' ', '')
        
    except:
        rating = float('nan')
        
    return rating
'''

    

# 取得評論內文
def get_comment_text(soup):
    comment_text = soup.find('div' ,class_ = 'MyEned').text
    comment_text = comment_text.replace(' ', '')

    return comment_text


