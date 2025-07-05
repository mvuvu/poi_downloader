from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from bs4 import BeautifulSoup
import time
import pandas as pd

def wait_for_coords_url(driver, timeout=20):
    """等待跳转后的 Google Maps URL 出现 /@lat,lng 格式"""
    try:
        WebDriverWait(driver, timeout).until(lambda d: "/@" in d.current_url)
        return driver.current_url
    except Exception as e:
        print("等待跳转失败：", e)
        return None


def get_coords(http_url):
    target_substring = "/@"
    start_index = http_url.find(target_substring)

    if start_index != -1:
        end_index = http_url.find('/', start_index + 2)  # 找到 `/@.../` 的结尾
        if end_index == -1:
            end_index = len(http_url)
        extracted = http_url[start_index + 2 : end_index]
        parts = extracted.split(',')
        if len(parts) >= 2:
            lat = float(parts[0])
            lng = float(parts[1])
            return lat, lng
    return None, None



#判断是否是建筑物
def get_building_type(driver):
    try:
        place_type_XPATH = '//*[@id="QA0Szd"]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]/div/div[1]/div[2]/div/div[2]/span/span/span'
        # 恢复充足等待时间确保元素加载完成
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, place_type_XPATH)))
        place_type = driver.find_element(By.XPATH, place_type_XPATH).text
    except:
        place_type = 'nan'
  
    return place_type



# 取得poi名稱
def get_building_name(driver):
    # 回到老版本简单可靠的策略
    place_name_XPATH = '//*[@id="QA0Szd"]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]/div/div[1]/div[1]/h1'
    xpath_candidates = [
        (place_name_XPATH, 10),  # 主要XPath，使用老版本的充足等待时间
        ('//h1[@data-value]', 5),
        ('//h1[contains(@class, "x3AX1")]', 5),
        ('//div[@data-value]//h1', 3),
        ('//span[@data-value]', 3)
    ]
    
    for xpath, timeout in xpath_candidates:
        try:
            WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.XPATH, xpath)))
            place_name = driver.find_element(By.XPATH, xpath).text
            if place_name and place_name.strip():
                # 清理特殊字符
                place_name = place_name.replace('/', ' ')
                place_name = place_name.replace('|', ' ')
                place_name = place_name.replace('｜', ' ')
                place_name = place_name.replace('*', ' ')
                place_name = place_name.replace('!', ' ')
                place_name = place_name.replace('?', ' ')
                place_name = place_name.replace(':', ' ')
                return place_name.strip()
        except:
            continue
    
    # 如果所有XPath都失败，抛出异常让上层处理
    raise Exception("无法找到地点名称")

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


def get_all_poi_info(driver):
    
    result = {}
    poi_name_list = []
    poi_rating_list = []
    poi_class_list = []
    poi_add_list = []
    poi_comment_list = []

    
        
    ele_class_list = ["Nv2PK.THOPZb.CpccDe", 'Nv2PK.Q2HXcd.THOPZb']
    for class_key in ele_class_list:
   
        poi_frame_list = driver.find_elements(By.CSS_SELECTOR, f"div.{class_key.replace('.', '.')}")

        if poi_frame_list:

            for poi_frame in poi_frame_list:
                soup = BeautifulSoup(poi_frame.get_attribute('innerHTML'), "html.parser") 
                
                poi_name = get_poi_name(soup)  # 取得 user 名稱 
                #user_profile_url = get_user_profile_url(soup)  # 取得 user 個人檔案的 URL
                rating = get_rating(soup)  # 取得評級
                poi_class, poi_address = get_class_address(soup)
                poi_comment_count = get_rating_count(soup)  # 取得單個POI評論數
                #local_guide, comment_num = get_local_guide_and_comment_num(soup)  # 取得在地嚮導的狀態和評論數
                #comment_text = get_comment_text(soup)# 取得評論內文
                poi_name_list.append(poi_name)
                poi_rating_list.append(rating)
                poi_class_list.append(poi_class)
                poi_add_list.append(poi_address)
                poi_comment_list.append(poi_comment_count)
        else:
            continue
    
 
    if len(poi_name_list) > 0:
       df = pd.DataFrame(
                        {'name': poi_name_list, 
                         'rating': poi_rating_list,	
                         'class' : poi_class_list,	
                         'add' : poi_add_list,
                         'comment_count': poi_comment_list
                        })
    else:
        df = None
        
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



def get_rating_count(soup):
    try:
        rating_count = soup.find("span", class_='UY7F9').text
        rating_count = int(rating_count.strip("()"))
    
    except:
        rating_count = 'nan'
        
    return rating_count




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






