from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from info_tool import get_poi_type_total
from tqdm.notebook import tqdm
import time
import math
import os


# click [more] button
def click_on_more_button(driver):
    # waiting for more button to be clicked
    try:

        element = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CLASS_NAME, 'M77dve')))
        
        # find more button and click it
        element.click()
    
    except:
        
        pass




def scroll_poi_section(driver):
    # wait for poi loading 
    poi_section_XPATH = '//*[@id="QA0Szd"]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]'
   
    #WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, poi_section_XPATH)))
    
    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".m6QErb.DxyBCb.kA9KIf.dS8AEf")))
    #time.sleep(10)
    
    # 下滑評論區塊，加載更多評論
    poi_section = driver.find_element(By.CLASS_NAME, 'm6QErb.DxyBCb.kA9KIf.dS8AEf')
    #poi_section = driver.find_elements((By.CSS_SELECTOR, ".m6QErb.DxyBCb.kA9KIf.dS8AEf"))
    poi_total = get_poi_type_total(driver)
    scroll_times = math.ceil(poi_total/10) + 1
    # Calculate scroll times silently
    # google map 最多能顯示 1130 則，無論再怎麼滑動都不會回傳新的評論。
    if scroll_times >= 112:
        scroll_times = 112
  
    # 滑動進度條
    # Silently scroll POI section
    #scroll_process = tqdm(total=scroll_times)
    for i in range(scroll_times):
            
        driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", poi_section)
        time.sleep(2)  
        #scroll_process.update(1)



