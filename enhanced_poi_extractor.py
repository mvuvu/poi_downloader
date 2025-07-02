#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
增强版POI数据提取器
包含完整的POI信息提取功能，不依赖原始info_tool.py
增加了评论数量、营业时间、电话号码等更多有用信息
"""

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
import logging

logger = logging.getLogger(__name__)

class EnhancedPOIExtractor:
    """增强版POI数据提取器"""
    
    def __init__(self):
        self.building_keywords = [
            '建筑物', '建築物', '建造物', 'building',
            'ビル', 'マンション', 'アパート', 
            '住宅', '事務所', 'オフィス',
            '商業施設', '店舗', '複合施設'
        ]
    
    def get_coords(self, url):
        """从URL中提取坐标"""
        try:
            target_substring = "/@"
            start_index = url.find(target_substring)
            
            if start_index != -1:
                # 提取坐标部分
                coord_part = url[start_index + len(target_substring):]
                coords = coord_part.split(',')
                
                if len(coords) >= 2:
                    lat = float(coords[0])
                    lng = float(coords[1])
                    return lat, lng
        except Exception as e:
            logger.warning(f"坐标提取失败: {e}")
        
        return None, None
    
    def get_building_type_robust(self, driver, timeout=10):
        """获取地点类型 - 多策略版本"""
        strategies = [
            # 策略1: 通用样式选择器
            {
                'method': 'css',
                'selector': 'span[class*="fontBodyMedium"]',
                'description': '通用字体样式选择器'
            },
            # 策略2: 通用span选择器
            {
                'method': 'xpath',
                'selector': '//span[contains(@class, "fontBodyMedium") or contains(@class, "place-type")]',
                'description': '通用span选择器'
            },
            # 策略3: 原始XPATH（备用）
            {
                'method': 'xpath',
                'selector': '//*[@id="QA0Szd"]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]/div/div[1]/div[2]/div/div[2]/span/span/span',
                'description': '原始XPATH'
            }
        ]
        
        for strategy in strategies:
            try:
                if strategy['method'] == 'css':
                    element = WebDriverWait(driver, timeout).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, strategy['selector']))
                    )
                else:
                    element = WebDriverWait(driver, timeout).until(
                        EC.presence_of_element_located((By.XPATH, strategy['selector']))
                    )
                
                place_type = element.text.strip()
                if place_type and place_type.lower() not in ['', 'loading', '読み込み中']:
                    return place_type
                    
            except Exception:
                continue
        
        return None
    
    def is_building(self, place_type):
        """判断是否为建筑物类型"""
        if not place_type:
            return False
        
        place_type_lower = place_type.lower()
        
        for keyword in self.building_keywords:
            if keyword.lower() in place_type_lower:
                return True
        
        return False
    
    def get_building_name(self, driver, timeout=15):
        """获取建筑物名称"""
        name_selectors = [
            '//*[@id="QA0Szd"]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]/div/div[1]/div[1]/h1',
            'h1[data-attrid="title"]',
            '.x3AX1-LfntMc-header-title-title',
            'h1'
        ]
        
        for selector in name_selectors:
            try:
                if selector.startswith('/'):
                    element = WebDriverWait(driver, timeout).until(
                        EC.presence_of_element_located((By.XPATH, selector))
                    )
                else:
                    element = WebDriverWait(driver, timeout).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                
                place_name = element.text.strip()
                if place_name:
                    # 清理特殊字符
                    place_name = re.sub(r'[/|｜*!?:]', ' ', place_name)
                    return place_name
                    
            except Exception:
                continue
        
        return "未知建筑"
    
    def get_poi_count(self, driver):
        """获取POI总数"""
        try:
            count_elements = driver.find_elements(By.XPATH, "//span[@class='bC3Nkc fontBodySmall']")
            if count_elements:
                total = sum(int(elem.text) for elem in count_elements if elem.text.isdigit())
                return total
        except Exception:
            pass
        
        return 0
    
    def extract_poi_info(self, soup):
        """从BeautifulSoup对象中提取POI信息"""
        poi_data = {
            'name': 'Unknown',
            'rating': None,
            'review_count': 0,
            'category': 'Unknown',
            'address': 'Unknown',
            'phone': None,
            'website': None,
            'hours': None,
            'price_level': None
        }
        
        try:
            # 提取POI名称
            name_elem = soup.find('div', class_='qBF1Pd fontHeadlineSmall')
            if name_elem:
                poi_data['name'] = name_elem.text.strip()
        except:
            pass
        
        try:
            # 提取评分
            rating_elem = soup.find("span", class_='MW4etd')
            if rating_elem:
                rating_text = rating_elem.text.strip()
                rating_match = re.search(r'(\d+\.?\d*)', rating_text)
                if rating_match:
                    poi_data['rating'] = float(rating_match.group(1))
        except:
            pass
        
        try:
            # 提取评论数量
            review_elem = soup.find("span", class_='UY7F9')
            if review_elem:
                review_text = review_elem.text
                review_match = re.search(r'(\d+)', review_text.replace(',', ''))
                if review_match:
                    poi_data['review_count'] = int(review_match.group(1))
        except:
            pass
        
        try:
            # 提取类别和地址
            info_divs = soup.find_all("div", {"class": 'W4Efsd'})
            if len(info_divs) > 2:
                spans = info_divs[2].select("span")
                if len(spans) > 1:
                    poi_data['category'] = spans[1].text.strip()
                if len(spans) > 4:
                    poi_data['address'] = spans[4].text.strip()
        except:
            pass
        
        try:
            # 提取电话号码
            phone_elem = soup.find('button', {'data-item-id': 'phone:tel'})
            if phone_elem:
                poi_data['phone'] = phone_elem.get('aria-label', '').replace('电话号码: ', '')
        except:
            pass
        
        try:
            # 提取网站
            website_elem = soup.find('a', {'data-item-id': 'authority'})
            if website_elem:
                poi_data['website'] = website_elem.get('href', '')
        except:
            pass
        
        try:
            # 提取营业时间
            hours_elem = soup.find('div', class_='t39EBf GUrTXd')
            if hours_elem:
                poi_data['hours'] = hours_elem.text.strip()
        except:
            pass
        
        try:
            # 提取价格等级 ($ $$ $$$ $$$$)
            price_elem = soup.find('span', class_='mgr77e')
            if price_elem:
                price_text = price_elem.text
                price_count = price_text.count('$') + price_text.count('¥')
                if price_count > 0:
                    poi_data['price_level'] = price_count
        except:
            pass
        
        return poi_data
    
    def get_all_poi_info_enhanced(self, driver):
        """获取所有POI信息 - 增强版"""
        poi_list = []
        
        # 多种POI容器class尝试
        poi_container_classes = [
            "Nv2PK.THOPZb.CpccDe", 
            'Nv2PK.Q2HXcd.THOPZb',
            'Nv2PK THOPZb CpccDe',
            'lI9IFe'  # 新的可能class
        ]
        
        for container_class in poi_container_classes:
            try:
                poi_elements = driver.find_elements(By.CLASS_NAME, container_class.replace(' ', '.'))
                
                if poi_elements:
                    for poi_element in poi_elements:
                        try:
                            soup = BeautifulSoup(poi_element.get_attribute('innerHTML'), "html.parser")
                            poi_info = self.extract_poi_info(soup)
                            
                            # 只添加有效的POI信息
                            if poi_info['name'] != 'Unknown':
                                poi_list.append(poi_info)
                        except Exception as e:
                            logger.warning(f"提取单个POI失败: {e}")
                            continue
                    
                    break  # 找到数据就停止尝试其他class
                    
            except Exception as e:
                logger.warning(f"尝试class {container_class} 失败: {e}")
                continue
        
        if poi_list:
            df = pd.DataFrame(poi_list)
            return df
        else:
            # 返回空DataFrame但包含所有列
            return pd.DataFrame(columns=[
                'name', 'rating', 'review_count', 'category', 'address', 
                'phone', 'website', 'hours', 'price_level'
            ])

# 为了向后兼容，提供独立函数
def get_building_type_robust(driver, timeout=10):
    """向后兼容的函数"""
    extractor = EnhancedPOIExtractor()
    return extractor.get_building_type_robust(driver, timeout)

def is_building(place_type):
    """向后兼容的函数"""
    extractor = EnhancedPOIExtractor()
    return extractor.is_building(place_type)

def safe_get_building_name(driver, timeout=15):
    """向后兼容的函数"""
    extractor = EnhancedPOIExtractor()
    return extractor.get_building_name(driver, timeout)

def safe_get_coords(url):
    """向后兼容的函数"""
    extractor = EnhancedPOIExtractor()
    return extractor.get_coords(url)

def safe_get_all_poi_info(driver):
    """向后兼容的函数"""
    extractor = EnhancedPOIExtractor()
    return extractor.get_all_poi_info_enhanced(driver)

print("✅ 增强版POI提取器加载完成")