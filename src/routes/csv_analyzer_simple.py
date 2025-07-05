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
def find_similar_products_simple(products, similarity_threshold=0.8):
    """找到相似的商品（简化版）"""
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
            except Exception as exc:
                print(f"图片下载生成异常: {exc}")

    # 简单的相似度比较
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
                
            if idx1 in images and idx2 in images:
                similarity = simple_image_similarity(images[idx1], images[idx2])
                if similarity >= similarity_threshold:
                    current_group.append({"product": product2, "index": idx2})
                    processed.add(idx2)
        
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
                'threshold': 0.8,
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

