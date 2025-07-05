import os
import csv
import io
import requests
import cv2
import numpy as np
from PIL import Image
from flask import Blueprint, request, jsonify
from flask_cors import cross_origin
from skimage.metrics import structural_similarity as ssim
from sklearn.cluster import DBSCAN
import tempfile
import hashlib

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

def resize_image(image, target_size=(256, 256)):
    """调整图片大小"""
    return image.resize(target_size, Image.Resampling.LANCZOS)

def calculate_image_similarity(img1, img2):
    """计算两张图片的相似度"""
    try:
        # 转换为numpy数组
        img1_array = np.array(img1)
        img2_array = np.array(img2)
        
        # 转换为灰度图
        gray1 = cv2.cvtColor(img1_array, cv2.COLOR_RGB2GRAY)
        gray2 = cv2.cvtColor(img2_array, cv2.COLOR_RGB2GRAY)
        
        # 计算结构相似性
        similarity, _ = ssim(gray1, gray2, full=True)
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
        
        for row in csv_reader:
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
        
        return products
    except Exception as e:
        print(f"解析CSV失败: {str(e)}")
        return []

def find_similar_products(products, similarity_threshold=0.8):
    """找到相似的商品"""
    # 下载所有图片
    images = {}
    valid_products = []
    
    for i, product in enumerate(products):
        if product['image_url']:
            image = download_image(product['image_url'])
            if image:
                # 调整图片大小
                resized_image = resize_image(image)
                images[i] = resized_image
                valid_products.append((i, product))
    
    # 计算相似度矩阵
    similarity_matrix = []
    product_indices = [idx for idx, _ in valid_products]
    
    for i, idx1 in enumerate(product_indices):
        row = []
        for j, idx2 in enumerate(product_indices):
            if i == j:
                similarity = 1.0
            elif i < j:
                similarity = calculate_image_similarity(images[idx1], images[idx2])
            else:
                similarity = similarity_matrix[j][i]  # 使用已计算的值
            row.append(similarity)
        similarity_matrix.append(row)
    
    # 使用DBSCAN聚类找到相似商品组
    # 将相似度转换为距离（1 - 相似度）
    distance_matrix = []
    for row in similarity_matrix:
        distance_row = [1 - sim for sim in row]
        distance_matrix.append(distance_row)
    
    # 使用DBSCAN聚类
    clustering = DBSCAN(eps=1-similarity_threshold, min_samples=2, metric='precomputed')
    cluster_labels = clustering.fit_predict(distance_matrix)
    
    # 组织结果
    similar_groups = {}
    for i, label in enumerate(cluster_labels):
        if label != -1:  # -1表示噪声点（不属于任何聚类）
            if label not in similar_groups:
                similar_groups[label] = []
            original_idx = product_indices[i]
            similar_groups[label].append({
                'product': products[original_idx],
                'index': original_idx
            })
    
    return similar_groups, similarity_matrix, product_indices

@csv_analyzer_bp.route('/upload', methods=['POST'])
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
        similar_groups, similarity_matrix, product_indices = find_similar_products(all_products)
        
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

