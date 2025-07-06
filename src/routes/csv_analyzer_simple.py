import os
import csv
import io
import requests
from PIL import Image
from flask import Blueprint, request, jsonify
from flask_cors import cross_origin
import tempfile
import hashlib
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
import time

csv_analyzer_bp = Blueprint('csv_analyzer', __name__)

# 全局缓存
image_hash_cache = {}
similarity_cache = {}

def get_cache_key(url1, url2):
    """生成缓存键"""
    return f"{min(url1, url2)}_{max(url1, url2)}"
def download_image(url, timeout=10):
    """下载图片并返回PIL Image对象"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        
        # 创建PIL Image对象
        image = Image.open(io.BytesIO(response.content))
        # 转换为RGB模式（如果是RGBA或其他模式）
        if image.mode != 'RGB':
            image = image.convert('RGB')
        return image
    except Exception as e:
        print(f"下载图片失败 {url}: {str(e)}")
        return None
import imagehash

def calculate_image_hash(image, url=None):
    """计算图片的感知哈希值（带缓存）"""
    try:
        # 如果有URL且已缓存，直接返回
        if url and url in image_hash_cache:
            return image_hash_cache[url]
        
        hash_value = imagehash.phash(image)
        
        # 缓存结果
        if url:
            image_hash_cache[url] = hash_value
            
        return hash_value
    except Exception as e:
        print(f"计算图片哈希失败: {str(e)}")
        return None

def image_hash_similarity(hash1, hash2):
    """计算两个哈希值之间的相似度（0-1，1表示完全相同）"""
    if hash1 is None or hash2 is None:
        return 0.0
    # 感知哈希的差异值越小，相似度越高
    # 这里将差异值转换为相似度分数，最大差异为64（对于phash）
    max_hash_diff = 64  # phash的默认哈希大小是8x8=64位
    diff = hash1 - hash2
    similarity = 1 - (diff / max_hash_diff)
    return max(0, similarity)

def parse_price(price_str):
    """解析价格字符串，返回数值"""
    try:
        # 移除货币符号和空格，替换逗号为点
        price_clean = price_str.replace('€', '').replace(',', '.').strip()
        return float(price_clean)
    except:
        return 0.0

def parse_csv_data(csv_content, filename):
    """解析CSV数据"""
    try:
        # 使用StringIO读取CSV内容
        csv_reader = csv.DictReader(io.StringIO(csv_content))
        products = []
        
        for i, row in enumerate(csv_reader):
            try:
                # 解析数据
                product = {
                    'image_url': row.get('small src', '').strip(),
                    'product_url': row.get('research-table-row__link-row-anchor href', '').strip(),
                    'title': row.get('research-table-row__link-row-anchor', '').strip(),
                    'price_without_tax': row.get('research-table-row__item-with-subtitle', '').strip(),
                    'sales_volume': row.get('research-table-row__inner-item', '1').strip(),
                    'last_sold_time': row.get('research-table-row__inner-item (4)', '').strip(),
                    'source_file': filename
                }
                
                # 处理缺失数据
                if not product['sales_volume'] or product['sales_volume'] == '':
                    product['sales_volume'] = '1'
                
                # 计算总销售额
                price = parse_price(product['price_without_tax'])
                volume = int(product['sales_volume']) if product['sales_volume'].isdigit() else 1
                product['total_sales'] = price * volume
                product['price_numeric'] = price
                product['volume_numeric'] = volume
                
                products.append(product)
            except Exception as e:
                print(f"解析CSV文件 {filename} 的第 {i+2} 行失败: {str(e)}") # +2 for header and 0-indexed loop
        
        return products
    except Exception as e:
        print(f"解析CSV文件 {filename} 失败: {str(e)}")
        return []
def calculate_title_similarity(title1, title2):
    """计算标题相似度"""
    try:
        # 转换为小写并分词
        words1 = set(title1.lower().split())
        words2 = set(title2.lower().split())
        
        # 计算交集和并集
        intersection = words1.intersection(words2)
        union = words1.union(words2)
        
        # Jaccard相似度
        if len(union) == 0:
            return 0.0
        
        similarity = len(intersection) / len(union)
        return similarity
    except Exception as e:
        print(f"计算标题相似度失败: {str(e)}")
        return 0.0

def calculate_price_similarity(price1, price2):
    """计算价格相似度"""
    try:
        if price1 == 0 or price2 == 0:
            return 0.0
        
        # 计算价格差异百分比
        price_diff = abs(price1 - price2) / max(price1, price2)
        
        # 转换为相似度（差异越小，相似度越高）
        similarity = max(0, 1 - price_diff)
        return similarity
    except Exception as e:
        print(f"计算价格相似度失败: {str(e)}")
        return 0.0

def quick_filter_by_title_and_price(product1, product2, min_title_similarity=0.3, max_price_diff=0.5):
    """快速过滤：基于标题和价格的早期筛选"""
    try:
        # 标题快速检查
        title_sim = calculate_title_similarity(product1['title'], product2['title'])
        if title_sim < min_title_similarity:
            return False
        
        # 价格快速检查
        price1 = product1.get('price_numeric', 0)
        price2 = product2.get('price_numeric', 0)
        
        if price1 > 0 and price2 > 0:
            price_diff = abs(price1 - price2) / max(price1, price2)
            if price_diff > max_price_diff:
                return False
        
        return True
    except Exception as e:
        print(f"快速过滤失败: {str(e)}")
        return True  # 出错时保守处理，不过滤

def calculate_comprehensive_similarity(product1, product2, img1=None, img2=None):
    """计算综合相似度"""
    try:
        # 图片相似度（权重40%）
        image_similarity = 0.0
        if img1 and img2:
            # 传入URL用于缓存
            url1 = product1.get('image_url', '')
            url2 = product2.get('image_url', '')
            hash1 = calculate_image_hash(img1, url1)
            hash2 = calculate_image_hash(img2, url2)
            image_similarity = image_hash_similarity(hash1, hash2)
        # 标题相似度（权重40%）
        title_similarity = calculate_title_similarity(product1['title'], product2['title'])
        
        # 价格相似度（权重20%）
        price_similarity = calculate_price_similarity(product1['price_numeric'], product2['price_numeric'])
        
        # 综合评分
        comprehensive_score = (
            image_similarity * 0.4 +
            title_similarity * 0.4 +
            price_similarity * 0.2
        )
        
        return {
            'comprehensive_score': comprehensive_score,
            'image_similarity': image_similarity,
            'title_similarity': title_similarity,
            'price_similarity': price_similarity
        }
    except Exception as e:
        print(f"计算综合相似度失败: {str(e)}")
        return {
            'comprehensive_score': 0.0,
            'image_similarity': 0.0,
            'title_similarity': 0.0,
            'price_similarity': 0.0
        }

def find_similar_products_simple(products, similarity_threshold=0.5):
    """找到相似的商品（优化版：缓存+早期过滤+进度显示）"""
    start_time = time.time()
    print(f"开始分析 {len(products)} 个产品...")
    
    # 下载所有图片（增加并发数）
    images = {}
    valid_products = []
    
    print("正在下载图片...")
    with ThreadPoolExecutor(max_workers=20) as executor:  # 增加并发数
        future_to_product = {executor.submit(download_image, product["image_url"]): (i, product) for i, product in enumerate(products) if product["image_url"]}
        completed = 0
        total = len(future_to_product)
        
        for future in concurrent.futures.as_completed(future_to_product):
            idx, product = future_to_product[future]
            completed += 1
            if completed % 10 == 0 or completed == total:
                print(f"图片下载进度: {completed}/{total}")
                
            try:
                image = future.result()
                if image:
                    images[idx] = image
                    valid_products.append((idx, product))
                else:
                    # 即使图片下载失败，也保留产品用于标题和价格比较
                    valid_products.append((idx, product))
            except Exception as exc:
                print(f"图片下载生成异常: {exc}")
                # 即使图片下载失败，也保留产品用于标题和价格比较
                valid_products.append((idx, product))

    download_time = time.time() - start_time
    print(f"图片下载完成，耗时: {download_time:.2f}秒")

    # 综合相似度比较（带早期过滤）
    print("开始相似度分析...")
    similar_groups = {}
    group_id = 0
    processed = set()
    comparisons_made = 0
    comparisons_skipped = 0
    
    total_comparisons = len(valid_products) * (len(valid_products) - 1) // 2
    
    for i, (idx1, product1) in enumerate(valid_products):
        if idx1 in processed:
            continue
            
        current_group = [{'product': product1, 'index': idx1}]
        processed.add(idx1)
        
        for j, (idx2, product2) in enumerate(valid_products[i+1:], i+1):
            if idx2 in processed:
                continue
            
            # 早期过滤：快速检查标题和价格
            if not quick_filter_by_title_and_price(product1, product2):
                comparisons_skipped += 1
                continue
            
            comparisons_made += 1
            
            # 计算综合相似度
            img1 = images.get(idx1)
            img2 = images.get(idx2)
            
            similarity_result = calculate_comprehensive_similarity(product1, product2, img1, img2)
            comprehensive_score = similarity_result['comprehensive_score']
            
            # 特殊规则：如果标题相似度很高（>0.8）且价格相似度也高（>0.8），降低阈值
            if (similarity_result['title_similarity'] > 0.8 and 
                similarity_result['price_similarity'] > 0.8):
                adjusted_threshold = 0.4
            else:
                adjusted_threshold = similarity_threshold
            
            if comprehensive_score >= adjusted_threshold:
                current_group.append({
                    "product": product2, 
                    "index": idx2,
                    "similarity_details": similarity_result
                })
                processed.add(idx2)
                print(f"找到相似产品: {product1['title'][:50]}... <-> {product2['title'][:50]}...")
                print(f"综合相似度: {comprehensive_score:.3f}, 图片: {similarity_result['image_similarity']:.3f}, 标题: {similarity_result['title_similarity']:.3f}, 价格: {similarity_result['price_similarity']:.3f}")
        
        if len(current_group) > 1:
            similar_groups[group_id] = current_group
            group_id += 1
        
        # 进度显示
        if (i + 1) % 10 == 0:
            progress = (i + 1) / len(valid_products) * 100
            print(f"分析进度: {progress:.1f}% ({i + 1}/{len(valid_products)})")
    
    total_time = time.time() - start_time
    print(f"分析完成！总耗时: {total_time:.2f}秒")
    print(f"比较统计: 执行了 {comparisons_made} 次详细比较，跳过了 {comparisons_skipped} 次")
    print(f"效率提升: {comparisons_skipped / (comparisons_made + comparisons_skipped) * 100:.1f}% 的比较被跳过")
    
    return similar_groups, [], []

@csv_analyzer_bp.route("/upload", methods=["POST"])
@cross_origin()
def upload_csv():
    """处理CSV文件上传"""
    try:
        if 'files' not in request.files:
            return jsonify({'error': '没有文件上传'}), 400
        
        files = request.files.getlist('files')
        if not files or all(file.filename == '' for file in files):
            return jsonify({'error': '没有选择文件'}), 400
        
        all_products = []
        
        # 处理每个CSV文件
        for file in files:
            if file and file.filename.endswith('.csv'):
                # 读取文件内容
                content = file.read().decode('utf-8')
                
                # 解析CSV数据
                products = parse_csv_data(content, file.filename)
                all_products.extend(products)
        
        if not all_products:
            return jsonify({'error': '没有有效的产品数据'}), 400
        
        # 进行相似度分析
        similar_groups, _, _ = find_similar_products_simple(all_products)
        
        # 准备返回数据
        result = {
            'total_products': len(all_products),
            'products': all_products,
            'similar_groups': similar_groups,
            'similarity_analysis': {
                'threshold': 0.5,
                'algorithm': 'comprehensive_scoring',
                'weights': {
                    'image_similarity': 0.4,
                    'title_similarity': 0.4,
                    'price_similarity': 0.2
                },
                'special_rules': 'Lower threshold (0.4) for high title+price similarity',
                'groups_found': len(similar_groups),
                'products_in_groups': sum(len(group) for group in similar_groups.values())
            }
        }
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'error': f'处理文件时出错: {str(e)}'}), 500

@csv_analyzer_bp.route('/test', methods=['GET'])
@cross_origin()
def test_endpoint():
    """测试端点"""
    return jsonify({'message': 'CSV分析器API正常工作', 'status': 'ok'})

