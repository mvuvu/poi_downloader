#!/usr/bin/env python3
# 测试重试逻辑

def test_retry_conditions():
    """测试重试条件逻辑"""
    
    # 测试用例1: 酒店页面 - 不应该重试
    result1 = {
        'success': True,
        'result_type': 'hotel_category_page',
        'poi_count': 0,
        'original_address': '東京都世田谷区池尻1丁目6-1',
        'address': '1-chōme-6-1+Ikejiri,+Setagaya+City,+Tokyo+154-0001',
        'is_retry': False
    }
    
    # 测试用例2: 非建筑物 - 应该重试
    result2 = {
        'success': True,
        'result_type': 'not_building',
        'poi_count': 0,
        'original_address': '東京都世田谷区池尻1丁目6-1',
        'address': '1-chōme-6-1+Ikejiri,+Setagaya+City,+Tokyo+154-0001',
        'is_retry': False
    }
    
    # 测试用例3: 建筑物无POI - 不应该重试
    result3 = {
        'success': True,
        'result_type': 'building_no_poi',
        'poi_count': 0,
        'original_address': '東京都世田谷区池尻1丁目6-1',
        'address': '1-chōme-6-1+Ikejiri,+Setagaya+City,+Tokyo+154-0001',
        'is_retry': False
    }
    
    # 测试用例4: 已经是重试任务 - 不应该再次重试
    result4 = {
        'success': True,
        'result_type': 'not_building',
        'poi_count': 0,
        'original_address': '東京都世田谷区池尻1丁目6-1',
        'address': '東京都世田谷区池尻1丁目6-1',
        'is_retry': True
    }
    
    def should_retry(result):
        return (result['success'] and 
                result.get('result_type') == 'not_building' and
                result.get('poi_count', 0) == 0 and 
                result.get('original_address') and 
                result['address'] != result['original_address'] and
                not result.get('is_retry', False))
    
    print("测试重试逻辑:")
    print(f"酒店页面: {should_retry(result1)} (期望: False)")
    print(f"非建筑物: {should_retry(result2)} (期望: True)")
    print(f"建筑物无POI: {should_retry(result3)} (期望: False)")
    print(f"已重试任务: {should_retry(result4)} (期望: False)")

if __name__ == "__main__":
    test_retry_conditions()