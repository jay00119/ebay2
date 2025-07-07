import requests
from bs4 import BeautifulSoup
from flask import Blueprint, request, jsonify
from flask_cors import cross_origin
from deep_translator import GoogleTranslator
import re
from collections import Counter
import time
import random

title_scraper_bp = Blueprint('title_scraper', __name__)

# German stop words
GERMAN_STOP_WORDS = {
    "der", "die", "das", "den", "dem", "des", "ein", "eine", "einer", "eines", "einem", "einen",
    "und", "oder", "aber", "doch", "sondern", "denn", "wenn", "als", "wie", "wo", "was", "wer",
    "mit", "für", "von", "zu", "bei", "nach", "vor", "über", "unter", "durch", "gegen", "ohne",
    "um", "an", "auf", "aus", "in", "ist", "sind", "war", "waren", "hat", "haben", "wird", "werden",
    "ich", "du", "er", "sie", "es", "wir", "ihr", "sich", "mich", "dich", "uns", "euch", "ihm", "ihr",
    "nicht", "nur", "auch", "noch", "schon", "mehr", "sehr", "so", "dann", "hier", "da", "dort",
    "neu", "gebraucht", "original", "genuine", "brand", "marke", "set", "kit", "pack", "piece", "stück"
}

def get_headers():
    """获取更完整的请求头，模拟真实浏览器"""
    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0'
    }

def scrape_ebay_titles(url, max_pages=4):
    """抓取eBay商品标题"""
    session = requests.Session()
    headers = get_headers()
    
    all_titles = []
    current_url = url
    
    for page_num in range(min(max_pages, 4)):  # 最多4页
        try:
            print(f"正在抓取第 {page_num + 1} 页: {current_url}")
            
            # 添加随机延迟，避免被识别为机器人
            time.sleep(random.uniform(1, 3))
            
            # 重试机制
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    response = session.get(current_url, headers=headers, timeout=30, allow_redirects=True)
                    response.raise_for_status()
                    break
                except requests.exceptions.RequestException as e:
                    if attempt == max_retries - 1:
                        raise e
                    time.sleep(random.uniform(2, 5))  # 重试前等待
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # 策略1: 优先使用精确的eBay标题选择器
            ebay_title_selectors = [
                'h3.textual-display.bsig__title__text',  # 精确选择器
                'h3.s-item__title',  # eBay搜索结果页面的标题选择器
                '.s-item__title a',  # eBay商品标题链接
                '.s-item__title span',  # eBay商品标题文本
                '.s-item__title',  # eBay商品标题容器
                'h3[data-testid="item-title"]',  # eBay新版页面的标题选择器
                'a[data-testid="item-title-link"]',  # eBay标题链接选择器
                'h3.it-ttl',  # eBay旧版标题选择器
                '.it-ttl a',  # eBay旧版标题链接
                '.lvtitle a',  # eBay列表视图标题
                '.vip .it-ttl a',  # eBay VIP商品标题
                'a[href*="/itm/"]',  # 包含商品ID的链接
                '.x-item-title-label',  # eBay商品标题标签
                'div.s-item__title a',  # eBay商品标题div中的链接
                'div.s-item__title'  # eBay商品标题div
            ]
            
            page_titles = []
            for title_selector in ebay_title_selectors:
                elements = soup.select(title_selector)
                for element in elements:
                    if len(all_titles) >= 200:  # 达到目标数量，停止抓取
                        break
                    
                    # 获取文本内容，处理不同类型的元素
                    if element.name == 'a':
                        title_text = element.get_text(strip=True)
                    else:
                        # 如果是容器元素，尝试找到其中的链接或文本
                        link = element.find('a')
                        if link:
                            title_text = link.get_text(strip=True)
                        else:
                            title_text = element.get_text(strip=True)
                    
                    # 清理和验证标题文本
                    if (title_text and 
                        len(title_text) > 5 and 
                        len(title_text) < 200 and  # 避免过长的文本
                        title_text not in all_titles and
                        'Shop on eBay' not in title_text and
                        'New Listing' not in title_text and
                        'Sponsored' not in title_text and
                        'Anzeige' not in title_text and
                        'Zur vorherigen Folie' not in title_text and
                        'Zur nächsten Folie' not in title_text and
                        'Artikel zum Beobachten' not in title_text):
                        all_titles.append(title_text)
                        page_titles.append(title_text)
                
                if len(all_titles) >= 200:
                    break
            
            print(f"第 {page_num + 1} 页提取到 {len(page_titles)} 个标题")
            
            # 如果已经达到目标数量，停止抓取
            if len(all_titles) >= 200:
                break
            
            # 查找下一页链接
            next_page_link = None
            next_page_selectors = [
                'a[rel="next"]',
                '.pagination__next',
                '.pagination__next-btn',
                '.s-pagination__next',
                'a.pagination__next-link',
                'a.s-pagination__next-link'
            ]
            
            for selector in next_page_selectors:
                next_link_element = soup.select_one(selector)
                if next_link_element and next_link_element.get('href'):
                    next_page_link = next_link_element.get('href')
                    if not next_page_link.startswith('http'):
                        # 处理相对链接
                        from urllib.parse import urljoin
                        next_page_link = urljoin(current_url, next_page_link)
                    break
            
            if next_page_link and page_num < max_pages - 1:
                current_url = next_page_link
            else:
                break  # 没有下一页或已达到最大页数，停止循环
                
        except requests.exceptions.RequestException as e:
            print(f"抓取第 {page_num + 1} 页失败: {str(e)}")
            continue  # 继续尝试下一页
    
    print(f"总共抓取到 {len(all_titles)} 个标题")
    return all_titles

def simple_tokenize_and_count(text):
    """简单的德语文本分词和词频统计"""
    # 转换为小写
    text = text.lower()
    
    # 提取德语单词（包括德语特殊字符）
    words = re.findall(r'\b[a-zA-ZäöüßÄÖÜ]+\b', text)
    
    # 过滤停用词和短词
    filtered_words = [
        word for word in words 
        if word not in GERMAN_STOP_WORDS and len(word) > 2
    ]
    
    # 统计词频
    word_counts = Counter(filtered_words)
    return word_counts

def translate_words_batch(words, target_lang='en'):
    """批量翻译词汇"""
    try:
        translator = GoogleTranslator(source='auto', target=target_lang)
        translations = {}
        
        for word, count in words:
            try:
                translated = translator.translate(word)
                translations[word] = translated
                time.sleep(0.1)  # 避免请求过快
            except Exception as e:
                print(f"翻译 '{word}' 失败: {str(e)}")
                translations[word] = word  # 翻译失败时保持原词
        
        return translations
    except Exception as e:
        print(f"翻译过程出错: {str(e)}")
        return {word: word for word, count in words}

@title_scraper_bp.route('/scrape', methods=['POST'])
@cross_origin()
def scrape_titles():
    """抓取商品标题的API端点"""
    try:
        data = request.get_json()
        if not data or 'url' not in data:
            return jsonify({'error': '请提供有效的URL'}), 400
        
        url = data['url']
        max_pages = data.get('max_pages', 4)
        
        # 验证URL
        if 'ebay' not in url.lower():
            return jsonify({'error': '请提供有效的eBay URL'}), 400
        
        print(f"开始抓取URL: {url}")
        
        # 抓取标题
        titles = scrape_ebay_titles(url, max_pages)
        
        if not titles:
            return jsonify({
                'success': False,
                'error': '未能抓取到任何商品标题。可能原因：1) 网站有反爬虫保护 2) 页面结构不支持 3) 网址无效',
                'suggestions': [
                    '请尝试其他eBay商品列表页面',
                    '确保链接是商品搜索结果页面',
                    '检查网址是否正确'
                ]
            }), 400
        
        print(f"总共抓取到 {len(titles)} 个标题")
        
        # 合并所有标题进行分词分析
        combined_text = " ".join(titles)
        word_counts = simple_tokenize_and_count(combined_text)
        top_words = word_counts.most_common(50)
        
        print(f"分词完成，总词数: {sum(word_counts.values())}，高频词: {len(top_words)}")
        
        # 翻译为英文和中文
        print("开始翻译...")
        english_translations = translate_words_batch(top_words, 'en')
        chinese_translations = translate_words_batch(top_words, 'zh')
        
        # 准备返回数据
        total_words = sum(word_counts.values())
        result = {
            'success': True,
            'total_titles': len(titles),
            'titles': titles[:100],  # 只返回前100个标题用于显示
            'all_titles_count': len(titles),
            'word_analysis': {
                'total_words': total_words,
                'unique_words': len(top_words),
                'top_words': [
                    {
                        'word': word,
                        'count': count,
                        'frequency': round(count / total_words * 100, 2),
                        'english': english_translations.get(word, word),
                        'chinese': chinese_translations.get(word, word)
                    }
                    for word, count in top_words
                ]
            },
            'scraping_info': {
                'pages_scraped': max_pages,
                'url': url
            }
        }
        
        return jsonify(result)
        
    except requests.exceptions.Timeout:
        return jsonify({
            'success': False,
            'error': '请求超时，请稍后重试或检查网络连接'
        }), 408
    except requests.exceptions.ConnectionError:
        return jsonify({
            'success': False,
            'error': '网络连接失败，请检查网址是否正确'
        }), 503
    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code if e.response else 500
        if status_code == 403:
            error_msg = '访问被拒绝，该网站可能有反爬虫保护'
        elif status_code == 404:
            error_msg = '页面不存在，请检查网址是否正确'
        elif status_code == 503:
            error_msg = '服务暂时不可用，请稍后重试'
        else:
            error_msg = f'HTTP错误 {status_code}，请稍后重试'
        
        return jsonify({
            'success': False,
            'error': error_msg,
            'status_code': status_code
        }), 500
    except Exception as e:
        print(f"抓取过程出错: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'抓取失败: {str(e)}',
            'suggestions': [
                '请检查网址格式是否正确',
                '尝试其他eBay商品列表页面',
                '稍后重试'
            ]
        }), 500

@title_scraper_bp.route('/test', methods=['GET'])
@cross_origin()
def test_scraper():
    """测试端点"""
    return jsonify({'message': '标题抓取器API正常工作', 'status': 'ok'})

