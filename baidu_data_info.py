from lxml import etree
import requests
import time
import random
import json
import html
import os
from concurrent.futures import ThreadPoolExecutor
import threading
import re
'''
需要的第三方模块模块有:lxml,requests
如果没有这些,自行pip或uv下载

'''
class Baidu_Tieba_page_data:
    def __init__(self):
        self.proxies =  {
                            'http': 'socks5h://127.0.0.1:7897',
                            'https': 'socks5h://127.0.0.1:7897'
                        }
        self.hard = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest"
        }
        self.baidu_url = "https://tieba.baidu.com/" # 用于拼接子帖子的链接
        self.damain = "https://gss0.bdstatic.com/6LZ1dD3d1sgCo2Kml5_Y_D3/sys/portrait/item/" # 用于拼接头像图片链接
        self.url = "https://tieba.baidu.com/p/9683804480"
        self.page_data = "paged_data" # 保存帖子的文件夹
        self.img_path = "img_folder" # 保存吧友头像和所发的图片的文件夹
        self.img_son = "img_son=" # 保存每一个子帖子图片的文件夹
        self.main_page_data = "main_page_data" # 保存主帖子的数据
        self.biadu_post = "https://tieba.baidu.com/f?kw=动漫抱枕&ie=utf-8&pn=" # kw=输入的帖子,默认在第一页
        self.session = requests.Session()
        self.post_data = self.session.get(url=self.biadu_post,headers=self.hard,proxies=self.proxies,timeout=15)
        self.post_data.raise_for_status()
        self.post_data.encoding = self.post_data.apparent_encoding
        self.post_data = self.post_data.text
        self.path_all_main_post = "主贴吧数据.csv"
        self.tcount_lock = threading.Lock() # 多线程安全的增加计数器的锁
        self.img_count = 0
        self.user_data_count = 0
        self.user_avatar_img = 0
        self.post_main_count = 0
        if not os.path.exists(self.page_data):
            os.makedirs(self.page_data,exist_ok=True)
        if not os.path.exists(self.img_path):
            os.makedirs(self.img_path,exist_ok=True)
        if not os.path.exists(self.main_page_data):
            os.makedirs(self.main_page_data,exist_ok=True)
        self.check_in = re.compile(r'本吧签到人数：.*?</p>')# 匹配帖子签到的人数
        self.focus_number = re.compile(r'<span class="card_numLabel">(.*?)</span>\s*<span class="card_menNum">(.*?)</span>\s*<span class="card_numLabel">(.*?)</span>\s*<span class="card_infoNum">(.*?)</span>') # 匹配帖子的关注数量和帖子的总数量
        self.comment = re.compile(r'<code class="pagelet_html" id="pagelet_html_frs-list/pagelet/thread_list" style="display:none;">[.\n\s\S]*?</code>') # 匹配注释掉的内容
        self.title_ = re.compile(r'<div class="head_main">[.\n\s\S]*?</code>')
        
    # 处理签到人数
    def check_number(self):
        bs64_str = self.check_in.findall(self.post_data,re.S) # 获取帖子签到人数
        return bs64_str
    
    # 处理注释的数据
    def data_deal_with(self,response):
        all_sunpage = ''.join(self.comment.findall(response,re.S))
        all_sunpage = all_sunpage.replace('<code class="pagelet_html" id="pagelet_html_frs-list/pagelet/thread_list" style="display:none;"><!--','')
        et = all_sunpage.replace('--></code>','')
        et = etree.HTML(et)
        return et
    
    # 处理帖子的关注数量和帖子的总数量
    def focus_number_(self):
        post_data = self.post_data
        data_html = ''.join(self.title_.findall(self.post_data,re.S))
        data_html = data_html.replace('<div class="head_main"><!--','')
        data_html = data_html.replace('--></code>','')
        et = etree.HTML(data_html)
        title = et.xpath('//div[@class="card_title"]/a/text()')[0].strip() # 获得贴吧标题
        focus_number_data = self.focus_number.findall(post_data,re.S)
        focus_number_list = []
        focus_number_list.append(focus_number_data[0][0] + focus_number_data[0][1])
        focus_number_list.append(focus_number_data[0][2] + focus_number_data[0][3])
        path_data = "主贴吧数据.csv"
        path_data = os.path.join(self.main_page_data,path_data)
        focus = focus_number_list[0]
        focus:str = focus.replace(',',"").strip()
        focus_:str = focus_number_list[1]
        focus_ = focus_.replace(",","").strip()
        check_number = self.check_number()
        check_number =check_number[0].replace("</p>","")
        with open(path_data,'a',encoding='utf-8') as f:
            data = f"{title}\n{focus},{focus_}{check_number}\n"
            f.write(data)
            self.post_main_count += 1
            print(f'---------------------------------------已处理贴吧数据{self.post_main_count}条----------------------------------------------')
            
    # 处理贴吧页数
    def post_info(self):
        self.focus_number_()
        for index in range(0,3): # 默认最多爬3页,想爬更多自行调整参数
            url = self.biadu_post + f"{index * 50}"
            response = self.session.get(url=url,headers=self.hard,proxies=self.proxies,timeout=15)
            time.sleep(2)
            response.raise_for_status()
            if (response.status_code == 404): # 页面超出范围
                return
            response.encoding = response.apparent_encoding
            response = response.text
            et = self.data_deal_with(response)
            post_all = et.xpath('//div[@class="t_con cleafix"]') # 获得每个子帖子
            if index == 0:
                main_all_data,main_title_href = self.top_post(post_all)
                path_ = os.path.join(self.main_page_data,self.path_all_main_post)
                with open(path_,'a',encoding='utf-8') as f:
                    f.write(main_all_data)
                    self.post_main_count += 1
                    print(f'---------------------------------------已处理贴吧数据{self.post_main_count}条----------------------------------------------')
            print(f"---------------------------------------已获得当前页面的帖子数量为{len(post_all)}----------------------------------------------")
            list_son_url = []
            list_son_url.append(main_title_href)
            for i in range(1,len(post_all)):
                x = post_all[i]
                data = x.xpath('.//a[@class="j_th_tit "]')[0]
                title_text = data.get("title").strip() # 获得标题文本
                title_href = data.get("href") # 获得子帖子链接
                list_son_url.append(title_href)
                author_data = x.xpath('.//span[@class="tb_icon_author "]|.//span[@class="tb_icon_author no_icon_author"]')[0]
                author = author_data.get("title").strip() # 获得帖子的作者
                release_time = x.xpath('.//span[@class="pull-right is_show_create_time"]/text()') # 获得帖子发布时间
                release_time = ''.join("发布的时间:" + release_time[0].strip())
                reply_num = x.xpath('.//span[@class="threadlist_rep_num center_text"]/text()')[0]
                reply_num = "回复消息数量:" + reply_num
                at_time = x.xpath('.//span[@class="threadlist_reply_date pull_right j_reply_data"]/text()') # 获取帖子最后回复的时间
                at_time = ''.join("最后回复的时间:" + at_time[0].strip())
                all_data = f"帖子标题:{title_text}\n{author}{release_time}{at_time}{reply_num}"
                path_ = os.path.join(self.main_page_data,self.path_all_main_post)
                with open(path_,'a',encoding='utf-8') as f:
                    f.write(all_data)
                    self.post_main_count += 1
                    print(f'---------------------------------------已处理贴吧数据{self.post_main_count}条----------------------------------------------')
                time.sleep(0.5)
            with ThreadPoolExecutor(max_workers=10) as exetst: # 多线程处理多个链接任务
                for url in list_son_url:
                    exetst.submit(self.son_subpage,url)
                
                
                
    
    def top_post(self,post_all):
        data = post_all[0].xpath('.//a[@class="j_th_tit "]')[0]
        title_text = data.get("title").strip() # 获得标题文本
        title_text = f"置顶：{title_text}" # 获得标题文本
        title_href = data.get("href") # 获得子帖子链接
        reply_num = post_all[0].xpath('.//span[@class="threadlist_rep_num center_text"]/text()')[0] # 获得当前子帖子的回复数量
        reply_num = "回复消息数量:" + reply_num
        # print(reply_num)
        # print(title_href)
        author_data = post_all[0].xpath('.//span[@class="tb_icon_author no_icon_author"]')[0]
        author = author_data.get("title").strip() # 获得帖子的作者
        release_time = post_all[0].xpath('.//span[@class="pull-right is_show_create_time"]/text()') # 获得帖子发布时间
        # print(author,release_time)
        all_data = f"帖子标题:{title_text}\n{author}{release_time}{reply_num}"
        return all_data,title_href
        
    # 处理子帖子内容
    def son_subpage(self):
        try:
            url_data = self.session.get(url=self.url,headers=self.hard,proxies=self.proxies,timeout=random.randint(5,20))
            url_data.raise_for_status()
            print("开始爬取贴吧页面！")
            response = etree.HTML(url_data.text)
            # 获得当前帖子的页数
            number = response.xpath('//div[@class="p_thread thread_theme_7"]/div/ul/li[2]')
            number_of_pages_list = []
            for line in number:
                number_of_pages_list.append(line.xpath('./span[1]/text()'))
                number_of_pages_list.append(line.xpath('./span[2]/text()'))
                print(line.xpath('./span[1]/text()'))
                print(line.xpath('./span[2]/text()'))
            print(f"----------------------当前帖子总共有{number_of_pages_list[0]}条回复贴------------------------")
            number = int(number_of_pages_list[1][0])
            n = number // 2
            print(n)
            for index in range(n):
                print(f"  -----------------------正在处理第{index + 1}页----------------------------- ")
                print(f"  -----------------------还剩{n - index - 1}页待处理------------------------------")
                # 当前页面帖子用户的名称(不含评论回复的名称)
                response = self.session.get(url=self.url + f"?pn={index + 1}",headers=self.hard,proxies=self.proxies,timeout=15)
                response.raise_for_status()
                et = etree.HTML(response.text)
                # 获得所有搂的元素
                all_list = et.xpath('//div[@class="l_post l_post_bright j_l_post clearfix  "]')
                # 对每层搂进行处理
                for index,x in enumerate(all_list):
                    self.floor(index,x)
                title = et.xpath('//h3[@class="core_title_txt pull-left text-overflow  "]/text()')
                username_url = et.xpath('//li[@class="d_name"]/a/text()')
                username_url_data = []
                for i,x in enumerate(username_url):
                    data = str(i + 1) + "搂：" + x
                    username_url_data.append(data)
                element_list = [] # 存储当前搂底下的详细数据
                data_list = et.xpath('//div[@class="post-tail-wrap"]') # 处理ip等数据
                for index in data_list:
                    ip_data = index.xpath('./span[1]/text()') # 获得ip附属地
                    equipment = index.xpath('./span[4]/a/text()') # 获得设备数据
                    element_num = index.xpath('./span[5]/text()') # 获得搂数
                    time_data = index.xpath('./span[6]/text()') # 获得发布的时间
                    element_list.append(ip_data + equipment + element_num + time_data)
                # 获取当前页面所有的图片链接
                img_src = et.xpath('//img[@class="BDE_Image"]/@src')
                img_hard = self.hard
                img_hard["referer"] = self.url
                img_url_ = self.session.get(url=self.url,headers=img_hard,proxies=self.proxies,timeout=15)
                img_dome = etree.HTML(img_url_.text)

                # 获取当前页面所有吧友的头像(不含评论回复)
                user_img = img_dome.xpath('//li[@class="icon"]/div[@class="icon_relative j_user_card"]')
                user_img_data = []
                for line in user_img:
                    data = line.get("data-field")
                    if data:
                        data_line = html.unescape(data)
                        data = json.loads(data_line)
                        url = self.damain + data.get("id")
                        print(url)
                        user_img_data.append(url)
                        time.sleep(1)

                # Comment = img_dome.xpath('div//[@class="lzl_cnt"]')
                # 当前页面帖子用户的文本(不包含评论回复)
                divs = et.xpath('//div[contains(@id, "post_content_")]')
                # 获取当前页面所有吧友的评论
                result_list = []
                for div in divs:
                    # 获取div的完整文本内容
                    full_text = div.xpath('string(.)').strip()
                    result_list.append(full_text)
                user_data = [] # 吧友名称和评论的文本
                for user,text in zip(username_url_data,result_list):
                    user_data.append([user,text])
                # with ThreadPoolExecutor(max_workers=10) as extsts:
                #     self.output_info(user_data,user_img_data,img_src)
        except requests.exceptions.RequestException as e:
            print(f"错误：访问页面失败。URL: {self.url}，原因: {e}")
        except Exception as e:
            print(f"页面时发生未知错误: {self.url}, {e}")

        """
        https://gss0.bdstatic.com/6LZ1dD3d1sgCo2Kml5_Y_D3/sys/portrait/item/tb.1.8ee95ece.8OWfmn7gMV8qyGyOZH7Hjw?t=1743135592
        """
        # 处理当前搂的方法
    def floor(self,i,floor):
        all_list_data = [] # 存储当前搂的所有数据
        poster_img_list = [] # 存储当前搂主发的图片
        avater_list = [] # 存储当前搂主的头像
        text_list = [] # 存储当前楼主的文本内容和名称
        try:
            poster_name = floor.xpath('//li[@class="d_name"]/a/text()')[i] # 处理楼主名称
            poster_avatar = floor.xpath('//li[@class="icon"]/div[@class="icon_relative j_user_card"]')[i]# 处理楼主头像
            poster_avatar = poster_avatar.get("data-field")
            if poster_avatar:
                poster_avatar = html.unescape(poster_avatar)
                js_poster = json.loads(poster_avatar)
                img_poster_tou = self.damain + js_poster.get('id')
            text = floor.xpath('//div[contains(@id, "post_content_")]')[i] # 处理楼主正文
            comment_data = floor.xpath('//div[@class="j_lzl_container core_reply_wrapper"]')
            if comment_data:
                pass
            text = text.xpath('string(.)').strip()
            poster_name = str(poster_name)
            text = str(text)
            print(f"{poster_name}：{text}")
            post_data = f"{poster_name}：{text}"
            text_list.append(post_data)
            self.comment_info(i)
            return
        except IndexError as e:
            print(f"列表发生越界{e}")
        except Exception as e:
            print(e)
        except EOFError as e:
            print(e)
    
    def output_info(self,user_name_text,user_img,img_src):
        try:
            print(f"共获取了{len(user_name_text)}条吧友数据和{len(user_img)}张吧友头像还有{len(img_src)}张图片")
            for line,img in zip(user_name_text,user_img):
                print(f"开始处理第{self.user_data_count}条吧友数据和第{self.user_avatar_img}张吧友头像")
                img:str
                img_data = img.split('=')
                img_data = img_data[-1] + '.jpg'
                path_img = line[0] + img_data
                img_folder_ = os.path.join(self.img_path,path_img)
                avatar_headers = self.hard.copy()
                avatar_headers["Referer"] = self.url
                data_img = self.session.get(url=img,headers=avatar_headers,timeout=15,proxies=self.proxies,stream=True)
                data_img.raise_for_status()
                with open(img_folder_,'wb') as f:
                    for chunk in data_img.iter_content(chunk_size=8192):
                        f.write(chunk)
                user_data_text = "吧友数据" + ".txt"
                page_data_ = os.path.join(self.page_data,user_data_text)
                
                with open(page_data_,'a',encoding='utf-8') as f:
                    if isinstance(line,list):
                        line_content_str = '：'.join(map(str,line))
                        f.write(line_content_str + "\n")
                time.sleep(0.5)
                self.user_data_count += 1
                self.user_avatar_img += 1
            for i in img_src:
                print(i)
                post_image_headers = self.hard.copy()
                post_image_headers["Referer"] = self.url
                data_img = self.session.get(url=i,headers=post_image_headers,timeout=15,proxies=self.proxies,stream=True)
                path_img_name = str(time.time()).replace('.',"") + '.jpg'
                path_img = os.path.join(self.img_path,path_img_name)
                with open(path_img,'wb') as f:
                    for chkkin in data_img.iter_content(chunk_size=8192):
                        f.write(chkkin)
                time.sleep(0.6)
                self.img_count += 1
        except requests.exceptions.RequestException as e:
                print(f"错误：下载图片失败。URL: {img}，原因: {e}")
                if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code == 403:
                    print("提示：收到 403 Forbidden。请检查 Referer 是否正确，User-Agent 是否被接受。")
                # 添加对其他可能错误的处理...
        except IOError as e:
                print(f"错误：保存文件失败。路径: {self.img_path}, 原因: {e}")
        except Exception as e:
                print(f"下载或保存过程中发生未知错误: {img}, {e}")    

    def run(self):
        print("开始贴吧爬取任务")
        self.Main_Subpage()
        time.sleep(2) # 处理完一个分类后暂停 2 秒
        print("所有分页处理完毕！")
        
if __name__ in "__main__":
    dome = Baidu_Tieba_page_data() # 获得类实例
    dome.post_info()
    #dome.close_driver()
    print(f"共爬取了{dome.user_data_count}条吧友数据\n和{dome.img_count}吧友的图片\n{dome.user_avatar_img}张吧友的头像")