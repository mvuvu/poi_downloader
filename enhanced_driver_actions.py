#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
增强版浏览器操作模块
移除对info_tool的依赖，增加更稳定的操作函数
"""

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import math
import logging

logger = logging.getLogger(__name__)

def click_on_more_button(driver, timeout=5):
    """点击'更多'按钮 - 增强版"""
    try:
        # 多种可能的更多按钮选择器
        more_button_selectors = [
            'M77dve',  # 原始class
            'button[aria-label*="更多"]',
            'button[aria-label*="More"]', 
            '.VfPpkd-LgbsSe.VfPpkd-LgbsSe-OWXEXe-dgl2Hf',
            '[data-value="More"]'
        ]
        
        for selector in more_button_selectors:
            try:
                if '.' in selector or '[' in selector:
                    element = WebDriverWait(driver, timeout).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                    )
                else:
                    element = WebDriverWait(driver, timeout).until(
                        EC.element_to_be_clickable((By.CLASS_NAME, selector))
                    )
                
                element.click()
                print(f"  ✅ 成功点击更多按钮 (使用选择器: {selector})")
                time.sleep(1)  # 等待展开
                return True
                
            except Exception:
                continue
        
        print("  ⚠️ 未找到更多按钮")
        return False
        
    except Exception as e:
        logger.warning(f"点击更多按钮失败: {e}")
        return False

def get_poi_count_enhanced(driver):
    """获取POI数量 - 增强版"""
    try:
        # 多种获取POI数量的策略
        count_selectors = [
            "//span[@class='bC3Nkc fontBodySmall']",
            "//span[contains(@class, 'fontBodySmall')]",
            ".bC3Nkc.fontBodySmall",
            "[data-value*='results']"
        ]
        
        for selector in count_selectors:
            try:
                if selector.startswith('//'):
                    elements = driver.find_elements(By.XPATH, selector)
                else:
                    elements = driver.find_elements(By.CSS_SELECTOR, selector)
                
                if elements:
                    total = 0
                    for elem in elements:
                        text = elem.text.strip()
                        if text.isdigit():
                            total += int(text)
                    
                    if total > 0:
                        print(f"  📊 找到 {total} 个POI")
                        return total
                        
            except Exception:
                continue
        
        # 如果无法获取准确数量，返回估算值
        print("  📊 无法获取准确POI数量，使用估算值")
        return 10  # 默认估算值
        
    except Exception as e:
        logger.warning(f"获取POI数量失败: {e}")
        return 10

def scroll_poi_section_enhanced(driver, max_scrolls=20):
    """滚动POI区域 - 增强版"""
    try:
        # 多种POI区域选择器
        poi_section_selectors = [
            'm6QErb.DxyBCb.kA9KIf.dS8AEf',  # 原始class
            '.m6QErb.DxyBCb.kA9KIf.dS8AEf',
            'div[role="main"]',
            '.siAUzd-neVct.siAUzd-EfZnuf-OWXEXe-uT8Y5e',
            'div.m6QErb'
        ]
        
        poi_section = None
        
        # 等待POI区域加载
        for selector in poi_section_selectors:
            try:
                if '.' in selector and not selector.startswith('.'):
                    # 处理多个class的情况
                    classes = selector.split('.')
                    elements = driver.find_elements(By.CLASS_NAME, classes[0])
                    for elem in elements:
                        if all(cls in elem.get_attribute('class') for cls in classes):
                            poi_section = elem
                            break
                elif selector.startswith('.'):
                    poi_section = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                else:
                    poi_section = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CLASS_NAME, selector))
                    )
                
                if poi_section:
                    print(f"  ✅ 找到POI区域 (使用选择器: {selector})")
                    break
                    
            except Exception:
                continue
        
        if not poi_section:
            print("  ❌ 未找到POI区域")
            return False
        
        # 智能计算滚动次数
        poi_count = get_poi_count_enhanced(driver)
        scroll_times = min(math.ceil(poi_count / 10) + 1, max_scrolls)
        
        # 限制最大滚动次数以避免过度滚动
        if scroll_times > 50:
            scroll_times = 50
        
        print(f'🔄 开始滚动POI区域 (预计滚动 {scroll_times} 次)...')
        
        # 执行滚动
        last_height = driver.execute_script("return arguments[0].scrollHeight", poi_section)
        scroll_count = 0
        no_change_count = 0
        
        for i in range(scroll_times):
            try:
                # 滚动到底部
                driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", poi_section)
                time.sleep(1.5)  # 减少等待时间
                
                # 检查是否有新内容加载
                new_height = driver.execute_script("return arguments[0].scrollHeight", poi_section)
                
                if new_height == last_height:
                    no_change_count += 1
                    if no_change_count >= 3:  # 连续3次没有变化就停止
                        print(f"  ⏹️ 内容已全部加载完成 (滚动 {scroll_count + 1} 次)")
                        break
                else:
                    no_change_count = 0
                    last_height = new_height
                
                scroll_count += 1
                
                # 每10次滚动显示进度
                if (i + 1) % 10 == 0:
                    print(f"  🔄 已滚动 {i + 1}/{scroll_times} 次")
                    
            except Exception as e:
                logger.warning(f"滚动失败: {e}")
                break
        
        print(f"  ✅ POI区域滚动完成 (实际滚动 {scroll_count} 次)")
        return True
        
    except Exception as e:
        logger.error(f"滚动POI区域失败: {e}")
        return False

def expand_full_comments(driver):
    """展开评论全文 - 增强版"""
    try:
        print('🔍 查找需要展开的评论...')
        
        # 多种展开按钮选择器
        expand_button_selectors = [
            "w8nwRe.kyuRq",
            ".w8nwRe.kyuRq", 
            "button[aria-label*='展开']",
            "button[aria-label*='Show more']",
            ".VfPpkd-LgbsSe[aria-label*='more']"
        ]
        
        expand_buttons = []
        
        for selector in expand_button_selectors:
            try:
                if '.' in selector and not selector.startswith('.'):
                    buttons = driver.find_elements(By.CLASS_NAME, selector.replace('.', ''))
                else:
                    buttons = driver.find_elements(By.CSS_SELECTOR, selector)
                
                if buttons:
                    expand_buttons = buttons
                    print(f"  ✅ 找到 {len(buttons)} 个展开按钮 (使用选择器: {selector})")
                    break
                    
            except Exception:
                continue
        
        if not expand_buttons:
            print("  ℹ️ 没有需要展开的评论")
            return True
        
        # 滚动到顶部
        try:
            comment_section = driver.find_element(By.CLASS_NAME, 'm6QErb.DxyBCb.kA9KIf.dS8AEf')
            driver.execute_script("arguments[0].scrollTop = 0", comment_section)
            time.sleep(1)
        except:
            pass
        
        # 展开所有评论
        success_count = 0
        for i, button in enumerate(expand_buttons):
            try:
                # 滚动到按钮位置
                driver.execute_script("arguments[0].scrollIntoView(true);", button)
                time.sleep(0.5)
                
                # 点击按钮
                button.click()
                success_count += 1
                time.sleep(0.3)  # 减少等待时间
                
                # 每10个显示进度
                if (i + 1) % 10 == 0:
                    print(f"  🔄 已展开 {i + 1}/{len(expand_buttons)} 个评论")
                    
            except Exception as e:
                logger.warning(f"展开第 {i+1} 个评论失败: {e}")
                continue
        
        print(f"  ✅ 成功展开 {success_count}/{len(expand_buttons)} 个评论")
        return True
        
    except Exception as e:
        logger.error(f"展开评论失败: {e}")
        return False

# 向后兼容的函数名
def scroll_poi_section(driver):
    """向后兼容的函数"""
    return scroll_poi_section_enhanced(driver)

def expand_full_comment(driver):
    """向后兼容的函数"""
    return expand_full_comments(driver)

print("✅ 增强版浏览器操作模块加载完成")