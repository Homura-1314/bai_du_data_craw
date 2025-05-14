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
                            'http': 'socks5h://xxx.x.x.x:xxxx',
                            'https': 'socks5h://xxx.x.x.x:xxxx'
                        }
        self.hard = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest"
        }
        self.baidu_url = "https://tieba.baidu.com/" # 用于拼接子帖子的链接
        self.damain = "https://gss0.bdstatic.com/6LZ1dD3d1sgCo2Kml5_Y_D3/sys/portrait/item/" # 用于拼接头像图片链接
        self.url = "https://tieba.baidu.com/你想爬取的单个帖子的id" # 如果你只是想爬取单个帖子,在son_subpage方法中开始的注释解掉，函数从son_subpage启动即可
        self.page_data = "paged_data" # 保存帖子的文件夹
        self.img_path = "img_folder" # 保存吧友头像和所发的图片的文件夹
        self.img_son = "img_son" # 保存每一个子帖子图片的文件夹
        self.son_page = "son_page" # 保存子帖子的文件
        self.main_page_data = "main_page_data" # 保存主贴吧的数据
        self.biadu_post = "https://tieba.baidu.com/f?kw=你输入的贴吧名称&ie=utf-8&pn=" # kw=输入的帖子,默认在第一页
        self.session = requests.Session()
        self.post_data = self.session.get(url=self.biadu_post,headers=self.hard,proxies=self.proxies,timeout=15)
        self.post_data.raise_for_status()
        self.post_data.encoding = self.post_data.apparent_encoding
        self.post_data = self.post_data.text
        self.path_all_main_post = "主贴吧数据.txt"
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
        path_data = os.path.join(self.main_page_data,self.path_all_main_post)
        focus = focus_number_list[0]
        focus:str = focus.replace(',',"").strip()
        focus_:str = focus_number_list[1]
        focus_ = focus_.replace(",","").strip()
        check_number = self.check_number()
        check_number =check_number[0].replace("</p>","")
        with open(path_data,'a',encoding='utf-8') as f:
            data = f"""{title}\n今日{focus},{focus_}人\n{check_number}个\n"""
            f.write(data)
            self.post_main_count += 1
            print(f'---------------------------------------已处理贴吧数据{self.post_main_count}条----------------------------------------------')

    # 处理贴吧页数
    def post_info(self):
        self.focus_number_()
        for index in range(0,3): # 默认最多爬3页,想爬更多自行调整参数
            url = self.biadu_post + f"{index * 50}"
            time.sleep(random.uniform(2, 6))
            response = self.session.get(url=url,headers=self.hard,proxies=self.proxies,timeout=15)
            time.sleep(2)
            response.raise_for_status()
            if (response.status_code == 404): # 页面超出范围
                return
            response.encoding = response.apparent_encoding
            response = response.text
            et = self.data_deal_with(response)
            post_all = et.xpath('//div[@class="t_con cleafix"]') # 获得每个子帖子
            print(f"---------------------------------------已获得当前页面的帖子数量为{len(post_all)}----------------------------------------------")
            path_ = os.path.join(self.main_page_data,self.path_all_main_post)
            if index == 0:
                main_all_data,main_title_href = self.top_post(post_all)
                with open(path_,'a',encoding='utf-8') as f:
                    f.write(main_all_data)
                    self.post_main_count += 1
                    print(f'---------------------------------------已处理贴吧数据{self.post_main_count}条----------------------------------------------')

            with open(path_,'a',encoding='utf-8') as f:
                index_data = (
                    f"========"
                    f"-第{index + 1}页-"
                    f"========\n"
                            )
                f.write(index_data)
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
                if at_time == []:
                    at_time = '无'
                at_time = ''.join("最后回复的时间:" + at_time[0].strip())
                if "-" not in at_time:
                    at_time = "今天" + at_time
                all_data = f"帖子标题:{title_text}\t{author}\t{release_time}\t{at_time}\tn今日{reply_num}个\n"
                    
                path_ = os.path.join(self.main_page_data,self.path_all_main_post)
                with open(path_,'a',encoding='utf-8') as f:
                    f.write(all_data)
                    self.post_main_count += 1
                    print(f'---------------------------------------已处理贴吧数据{self.post_main_count}条----------------------------------------------')
            for i,url in enumerate(list_son_url):
                self.son_subpage(url,i)
                time.sleep(random.uniform(2, 6))
    
    def top_post(self,post_all):
        data = post_all[0].xpath('.//a[@class="j_th_tit "]')[0]
        title_text = data.get("title").strip() # 获得标题文本
        top_text = "[置顶]"
        title_href = data.get("href") # 获得子帖子链接
        reply_num = post_all[0].xpath('.//span[@class="threadlist_rep_num center_text"]/text()')[0] # 获得当前子帖子的回复数量
        reply_num = "回复消息数量:" + reply_num
        # print(reply_num)
        # print(title_href)
        if post_all[0].xpath('.//span[@class="tb_icon_author no_icon_author"]') != []:
            author_data = post_all[0].xpath('.//span[@class="tb_icon_author no_icon_author"]')[0]
        else:
            author_data = post_all[0].xpath('.//span[@class="tb_icon_author "]')[0]
        author = author_data.get("title").strip() # 获得帖子的作者
        release_time = post_all[0].xpath('.//span[@class="pull-right is_show_create_time"]/text()') # 获得帖子发布时间
        # print(author,release_time)
        all_data_v3 = (
            f"-----------------------------------------------------------------------------------------------\n"
            f"{top_text}帖子标题:{title_text}{author}\t发布时间:{release_time[0]}\t{reply_num}个\n"
            f"------------------------------------------------------------------------------------------------\n"
        )
        return all_data_v3,title_href
        
    # 处理子帖子内容
    def son_subpage(self,url_,num):
        try:
            # url = self.url
            time.sleep(random.uniform(2, 6))
            url = self.baidu_url + url_
            url_data = self.session.get(url=url,headers=self.hard,proxies=self.proxies,timeout=random.randint(5,20))
            url_data.raise_for_status()
            print("'---------------------------------------开始爬取帖子页面！----------------------------------------------")
            response = etree.HTML(url_data.text)
            # 获得当前帖子的页数
            number = response.xpath('//div[@class="p_thread thread_theme_7"]/div/ul/li[2]')
            number_of_pages_list = []
            for line in number:
                number_of_pages_list.append(line.xpath('./span[1]/text()'))
                number_of_pages_list.append(line.xpath('./span[2]/text()'))
                # print(line.xpath('./span[1]/text()'))
                # print(line.xpath('./span[2]/text()'))
            print(f"---------------------------------------当前帖子总共有{number_of_pages_list[0][0]}条回复贴----------------------------------------------")
            if int(number_of_pages_list[0][0]) > 30: # 你可根据回复贴数量,来查看自己是否需要这么多
                print("不符合需求，跳过当前循环")
                return
            number = int(number_of_pages_list[1][0])
            n = number
            all_list_data = [] # 收集搂的文本等内容
            # 对每层搂进行处理
            poster_img_list = [] # 存储搂主发的图片链接
            poster_img = [] # 存储楼主头像链接
            # print(n)
            for index in range(n):
                time.sleep(random.uniform(2, 6))
                print(f"---------------------------------------正在处理第{index + 1}页-----------------------------------------------------")
                print(f"---------------------------------------还剩{n - index - 1}页待处理---------------------------------------")
                # 当前页面帖子用户的名称(不含评论回复的名称)
                response = self.session.get(url=url + f"?pn={index + 1}",headers=self.hard,proxies=self.proxies,timeout=15)
                response.raise_for_status()
                et = etree.HTML(response.text)
                # 获得所有搂的元素
                all_list = et.xpath('//div[@class="l_post l_post_bright j_l_post clearfix  "]')
                all_list_data = [] # 收集搂的文本等内容
                # 对每层搂进行处理
                poster_img_list = [] # 存储搂主发的图片链接
                poster_img = [] # 存储楼主头像链接
                for index,x in enumerate(all_list):
                    poster_name,img_poster_tou,text,img = self.floor(index,x)
                    data_list = et.xpath('//div[@class="post-tail-wrap"]') # 处理ip等数据
                    ip_data = data_list[index].xpath('./span[1]/text()') # 获得ip附属地
                    equipment = data_list[index].xpath('./span[4]/a/text()') # 获得设备数据
                    element_num = data_list[index].xpath('./span[5]/text()') # 获得搂数
                    time_data = data_list[index].xpath('./span[6]/text()') # 获得发布的时间
                    if equipment == []:
                        equipment = '无'
                    if time_data == []:
                        time_data = '无'
                    
                    data = f"{poster_name}=?{text}=?{ip_data[0]}=?{equipment[0]}=?{element_num[0]}=?{time_data[0]}"
                    all_list_data.append(data)
                    poster_img_list.append(img)
                    poster_img.append(img_poster_tou)
                   time.sleep(random.uniform(2, 6))
               time.sleep(random.uniform(2, 6))
            with ThreadPoolExecutor(max_workers=10) as extsts:
                extsts.submit(self.output_info,all_list_data,poster_img_list,poster_img,num)
                    time.sleep(random.uniform(2, 6))
        except requests.exceptions.RequestException as e:
            print(f"错误：访问页面失败。URL: {url_}，原因: {e}")
        except Exception as e:
            print(f"页面时发生未知错误: {url_}, {e}")

        """
        https://gss0.bdstatic.com/6LZ1dD3d1sgCo2Kml5_Y_D3/sys/portrait/item/tb.1.8ee95ece.8OWfmn7gMV8qyGyOZH7Hjw?t=1743135592
        """
        # 处理当前搂的方法
    def floor(self,i,floor):
        try:
            print(f"------------------------------------正在处理第{i}搂------------------------------------------------------")
            poster_name = floor.xpath('//li[@class="d_name"]/a/text()')[i] # 处理楼主名称
            poster_avatar = floor.xpath('//li[@class="icon"]/div[@class="icon_relative j_user_card"]')[i]# 处理楼主头像
            poster_avatar = poster_avatar.get("data-field")
            if poster_avatar:
                poster_avatar = html.unescape(poster_avatar)
                js_poster = json.loads(poster_avatar)
                img_poster_tou = self.damain + js_poster.get('id') # 头像链接拼接
            text = floor.xpath('//div[contains(@id, "post_content_")]')[i] # 处理楼主正文
            text = text.xpath('string(.)').strip()
            poster_name = str(poster_name)
            text = str(text)
            img = floor.xpath('//img[@class="BDE_Image"]') # 存储图片链接
            if img != [] and i < len(img):
                img = img[i].get('src')
            else:
                img = 0
            print(f"{poster_name}：{text},{img_poster_tou},{img}")
            return poster_name,img_poster_tou,text,img
        except IndexError as e:
            print(f"列表发生越界{e}")
        except Exception as e:
            print(e)
        except EOFError as e:
            print(e)
        #写入文件
    def output_info(self,all_list_data,poster_img_list,poster_img,num):
        try:
            print(f"共获取了{len(all_list_data)}条吧友数据和{len(poster_img)}张吧友头像还有{len(poster_img_list)}张图片")
            time.sleep(random.uniform(2, 6))
            for line,img in zip(all_list_data,poster_img):
                line:str
                img:str
                with self.tcount_lock:
                    print(f"---------------------------------------开始处理第{self.user_data_count + 1}条吧友数据和第{self.user_avatar_img + 1}张吧友头像---------------------------------------")
                sub_img =  self.img_son + f"{num + 1}"
                path = os.path.join(self.img_path,sub_img)
                if not os.path.exists(path):
                    os.makedirs(path,exist_ok=True)
                path_img = line.split('=?')[0].replace('：',"").strip() + '.jpg'
                img_folder_ = os.path.join(path,path_img)
                avatar_headers = self.hard.copy()
                avatar_headers["Referer"] = self.baidu_url
                time.sleep(random.uniform(2, 6))
                data_img = self.session.get(url=img,headers=avatar_headers,timeout=15,proxies=self.proxies,stream=True)
                data_img.raise_for_status()
                with open(img_folder_,'wb') as f:
                    for chunk in data_img.iter_content(chunk_size=8192):
                        f.write(chunk)
                sub_path = self.son_page + f"{num + 1}"
                path_data = os.path.join(self.page_data,sub_path)
                if not os.path.exists(path_data):
                    os.makedirs(path_data,exist_ok=True)
                page_text = self.son_page + f"{num + 1}.txt"
                page_data_ = os.path.join(path_data,page_text)
                index = -1
                line_list = line.split('=?')
                for i,x in enumerate(line_list):
                    if x == '':
                        continue
                    if x[-1] in "楼" and len(x) == 2:
                        index = i
                        break
                if index != -1: 
                    data = line_list[index]
                    floor = line.replace(data,"")
                    line = data + floor
                line = line.replace('=?',"")
                with open(page_data_,'a',encoding='utf-8') as f:
                        f.write(line + "\n")
                with self.tcount_lock:
                    self.user_data_count += 1
                    self.user_avatar_img += 1
                time.sleep(0.5)
            try:
                for i,url in enumerate(poster_img_list):
                    with self.tcount_lock:
                        print(f"---------------------------------------开始处理第{self.img_count + 1}张图片---------------------------------------")
                    if not url or not isinstance(url, str):
                        continue
                    img_hard = self.hard.copy()
                    img_hard["referer"] = self.baidu_url
                    time.sleep(random.uniform(2, 3))
                    img_ = self.session.get(url=url,headers=img_hard,proxies=self.proxies,timeout=15,stream=True) # 对图片发起请求
                    img_.raise_for_status()
                    # 尝试从 URL 的最后一部分提取，并处理可能的查询参数
                    filename_part_from_url = url.split('/')[-1].split('?')[-1]
                    if not filename_part_from_url:
                        img_data_for_filename = str(time.time()).replace('.', '')
                    else:
                        img_data_for_filename = filename_part_from_url
                    img_poster = img_data_for_filename + ".jpg"
                    path_img_name = os.path.join(self.img_path,img_poster)
                    with open(path_img_name,'wb') as f:
                        for chunk in img_.iter_content(chunk_size=8192):
                            f.write(chunk)
                    with self.tcount_lock:
                        self.img_count += 1
                    time.sleep(random.uniform(2, 6))
            except requests.exceptions.RequestException as e:
                    print(f"错误：提取poster_img_list列表下载图片失败。URL: {url}，原因: {e}")
                    if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code == 403:
                        print("提示：收到 403 Forbidden。请检查 Referer 是否正确，User-Agent 是否被接受。")
                # 添加对其他可能错误的处理...
            except IOError as e:
                print(f"错误：保存文件失败。路径: {self.img_path}, 原因: {e}")
            except Exception as e:
                print(f"提取poster_img_list列表下载或保存过程中发生未知错误: {url}, {e}")
        except requests.exceptions.RequestException as e:
                print(f"错误：提取poster_img列表链接中图片失败。URL: {url}，原因: {e}")
                if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code == 403:
                    print("提示：收到 403 Forbidden。请检查 Referer 是否正确，User-Agent 是否被接受。")
                # 添加对其他可能错误的处理...
        except IOError as e:
                print(f"错误：保存文件失败。路径: {self.img_path}, 原因: {e}")
        except Exception as e:
                print(f"提取poster_img列表链接中下载或保存过程中发生未知错误: {img}, {e}")

    def run(self):
        print("------------------------------开始贴吧爬取任务！------------------------------")
        self.post_info()
        
if __name__ in "__main__":
    dome = Baidu_Tieba_page_data() # 获得类实例
    dome.run()
    print(f"共爬取了主帖子数量{dome.post_main_count}条帖子，{dome.user_data_count}条吧友数据\n和{dome.img_count}吧友的图片\n{dome.user_avatar_img}张吧友的头像")
