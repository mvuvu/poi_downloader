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
                time.sleep(1)  # 等待展开
                return True
                
            except Exception:
                continue
        
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
                        return total
                        
            except Exception:
                continue
        
        # 如果无法获取准确数量，返回估算值
        return 10  # 默认估算值
        
    except Exception as e:
        logger.warning(f"获取POI数量失败: {e}")
        return 10

def scroll_poi_section_enhanced(driver, max_scrolls=100):
    """滚动POI区域 - 强化版，确保完全加载"""
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
                    break
                    
            except Exception:
                continue
        
        if not poi_section:
            return False
        
        # 获取初始POI数量用于参考
        initial_poi_count = get_poi_count_enhanced(driver)
        
        # 执行智能滚动 - 基于实际内容变化
        last_height = driver.execute_script("return arguments[0].scrollHeight", poi_section)
        stable_count = 0  # 连续稳定计数
        scroll_count = 0
        max_stable_attempts = 8  # 增加稳定尝试次数
        
        # 第一阶段：快速滚动加载大部分内容
        for i in range(max_scrolls):
            try:
                # 滚动到底部
                driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", poi_section)
                
                # 快速等待时间
                if scroll_count < 20:
                    time.sleep(0.8)  # 前期很快
                elif scroll_count < 50:
                    time.sleep(1.2)  # 中期快速
                else:
                    time.sleep(1.8)  # 后期稍慢
                
                # 检查内容变化
                new_height = driver.execute_script("return arguments[0].scrollHeight", poi_section)
                
                if new_height == last_height:
                    stable_count += 1
                    # 减少稳定次数判断
                    if stable_count >= 5:  # 从8次减少到5次
                        # 快速最终确认
                        for final_check in range(2):  # 从3次减少到2次
                            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", poi_section)
                            time.sleep(1.5)  # 从4秒减少到1.5秒
                            
                            final_height = driver.execute_script("return arguments[0].scrollHeight", poi_section)
                            if final_height > new_height:
                                # 还有新内容，重置计数继续
                                stable_count = 0
                                last_height = final_height
                                break
                        else:
                            # 真正到底了
                            break
                else:
                    stable_count = 0
                    last_height = new_height
                
                scroll_count += 1
                
                # 每20次滚动检查是否有"显示更多"按钮
                if scroll_count % 20 == 0:
                    try:
                        show_more_buttons = driver.find_elements(By.XPATH, "//button[contains(text(), '显示更多') or contains(text(), 'Show more')]")
                        for btn in show_more_buttons:
                            if btn.is_displayed():
                                btn.click()
                                time.sleep(2)
                                break
                    except:
                        pass
                    
            except Exception as e:
                logger.warning(f"滚动失败: {e}")
                break
        
        # 第二阶段：验证加载完整性
        final_poi_count = get_poi_count_enhanced(driver)
        
        # 如果实际加载的POI数量明显少于预期，快速补充滚动
        if initial_poi_count > 30 and final_poi_count < initial_poi_count * 0.6:  # 提高阈值
            # 快速补充滚动
            for extra_scroll in range(10):  # 减少补充次数
                driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", poi_section)
                time.sleep(1.5)  # 减少等待时间
                
                newer_height = driver.execute_script("return arguments[0].scrollHeight", poi_section)
                if newer_height > last_height:
                    last_height = newer_height
                    stable_count = 0
                else:
                    stable_count += 1
                    if stable_count >= 3:  # 更快停止
                        break
        
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