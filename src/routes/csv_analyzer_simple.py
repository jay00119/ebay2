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

csv_analyzer_bp = Blueprint('csv_analyzer', __name__)
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

def simple_image_similarity(img1, img2):
    """简单的图片相似度计算（基于像素差异）"""
    try:
        # 调整图片大小到相同尺寸
        size = (64, 64)
        img1_resized = img1.resize(size)
        img2_resized = img2.resize(size)
        
        # 转换为灰度
        img1_gray = img1_resized.convert('L')
        img2_gray = img2_resized.convert('L')
        
        # 计算像素差异
        pixels1 = list(img1_gray.getdata())
        pixels2 = list(img2_gray.getdata())
        
        # 计算均方误差
        mse = sum((p1 - p2) ** 2 for p1, p2 in zip(pixels1, pixels2)) / len(pixels1)
        
        # 转换为相似度（0-1之间）
        similarity = max(0, 1 - mse / 10000)
        return similarity
    except Exception as e:
        print(f"计算相似度失败: {str(e)}")
        return 0.0

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

def calculate_comprehensive_similarity(product1, product2, img1=None, img2=None):
    """计算综合相似度"""
    try:
        # 图片相似度（权重40%）
        image_similarity = 0.0
        if img1 and img2:
            image_similarity = simple_image_similarity(img1, img2)
        
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
    """找到相似的商品（改进版综合评分）"""
    # 下载所有图片
    images = {}
    valid_products = []
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_product = {executor.submit(download_image, product["image_url"]): (i, product) for i, product in enumerate(products) if product["image_url"]}
        for future in concurrent.futures.as_completed(future_to_product):
            idx, product = future_to_product[future]
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

    # 综合相似度比较
    similar_groups = {}
    group_id = 0
    processed = set()
    
    for i, (idx1, product1) in enumerate(valid_products):
        if idx1 in processed:
            continue
            
        current_group = [{'product': product1, 'index': idx1}]
        processed.add(idx1)
        
        for j, (idx2, product2) in enumerate(valid_products[i+1:], i+1):
            if idx2 in processed:
                continue
            
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

